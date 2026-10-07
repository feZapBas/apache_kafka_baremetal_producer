#include "measurement.h"

#include "dma_work.h"
#include "event_format.h"
#include "metrics.h"
#include "time_utils.h"

#include "xstatus.h"
#include "xil_printf.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>


/* ================================================================
 * Button GPIO
 * ================================================================ */

static XGpio button_gpio;

static u32 previous_buttons =
    0U;


#define METRIC_EXPORT_LINE_BYTES 192U

static uint32_t metric_export_index =
    0U;

static uint8_t metric_export_active =
    0U;

static uint8_t metric_export_header_sent =
    0U;


static const char metric_csv_header[] =
    "METRIC_HEADER,"
    "run,"
    "source,"
    "frame,"
    "fpga_timestamp_ticks,"
    "dma_start_us,"
    "dma_done_us,"
    "publish_us,"
    "send_us,"
    "ack_us,"
    "dma_latency_us,"
    "queue_latency_us,"
    "send_ack_us,"
    "publish_ack_us,"
    "kafka_error\n";


static int format_metric_csv(
    const metric_record_t *record,
    char *buffer,
    uint32_t buffer_size
)
{
    if (
        record == NULL ||
        buffer == NULL ||
        buffer_size == 0U
    ) {

        return -1;
    }


    uint32_t dma_latency =
        record->dma_done_us -
        record->dma_start_us;


    uint32_t queue_latency =
        record->send_us -
        record->publish_us;


    uint32_t send_ack_latency =
        record->ack_us -
        record->send_us;


    uint32_t publish_ack_latency =
        record->ack_us -
        record->publish_us;


    int length =
        snprintf(
            buffer,
            (size_t)buffer_size,

            "METRIC,"
            "%u,%u,%u,%u,"
            "%u,%u,%u,%u,%u,"
            "%u,%u,%u,%u,%d\n",

            (unsigned)record->run_id,
            (unsigned)record->source_id,
            (unsigned)record->frame_counter,
            (unsigned)record->fpga_timestamp_ticks,

            (unsigned)record->dma_start_us,
            (unsigned)record->dma_done_us,
            (unsigned)record->publish_us,
            (unsigned)record->send_us,
            (unsigned)record->ack_us,

            (unsigned)dma_latency,
            (unsigned)queue_latency,
            (unsigned)send_ack_latency,
            (unsigned)publish_ack_latency,

            (int)record->kafka_error
        );


    if (
        length <= 0 ||
        (uint32_t)length >= buffer_size
    ) {

        return -1;
    }


    return length;
}


/* ================================================================
 * Button helper
 * ================================================================ */

static u32 buttons_read(void)
{
    return
        XGpio_DiscreteRead(
            &button_gpio,
            BTN_GPIO_CHANNEL
        ) &
        BTN_MASK;
}


/* ================================================================
 * Initialization
 * ================================================================ */

int measurement_init(
    measurement_t *measurement
)
{
    if (measurement == NULL) {

        return XST_FAILURE;
    }


    memset(
        measurement,
        0,
        sizeof(*measurement)
    );


    XGpio_Config *config =
        XGpio_LookupConfig(
            BTN_GPIO_DEVICE_ID
        );


    if (config == NULL) {

        xil_printf(
            "Button GPIO LookupConfig failed\r\n"
        );

        return XST_FAILURE;
    }


    int status =
        XGpio_CfgInitialize(
            &button_gpio,
            config,
            config->BaseAddress
        );


    if (status != XST_SUCCESS) {

        xil_printf(
            "Button GPIO initialization failed: %d\r\n",
            status
        );

        return XST_FAILURE;
    }


    /*
     * Buttons are inputs.
     */
    XGpio_SetDataDirection(
        &button_gpio,
        BTN_GPIO_CHANNEL,
        BTN_MASK
    );


    previous_buttons =
        buttons_read();


    measurement->state =
        MEAS_IDLE;


    /*
     * Safety:
     * no source should generate data before a run starts.
     */
    dma_stream_disable_all();


    return XST_SUCCESS;
}


/* ================================================================
 * Start measurement
 * ================================================================ */

int measurement_start(
    measurement_t *measurement,
    kafka_producer_t *kafka,
    uint8_t source,
    uint32_t event_count
)
{
    if (
        measurement == NULL ||
        kafka == NULL
    ) {

        return XST_FAILURE;
    }


    if (
        measurement->state == MEAS_RUNNING ||
        measurement->state == MEAS_DRAINING
    ) {

        return XST_FAILURE;
    }


    /*
     * Only start when Kafka is connected and ready.
     */
    if (
        kafka_get_state(kafka) !=
        KAFKA_READY
    ) {

        xil_printf(
            "Kafka not READY\r\n"
        );

        return XST_FAILURE;
    }


    /*
     * Do not mix two runs in the local Kafka
     * queue/pipeline.
     */
    if (!kafka_is_drained(kafka)) {

        xil_printf(
            "Kafka queue/pipeline not empty\r\n"
        );

        return XST_FAILURE;
    }


    /*
     * Validate selected hardware source.
     */
    if (
        source != DMA_SOURCE_0 &&
        source != DMA_SOURCE_1
    ) {

        return XST_FAILURE;
    }


    /* ------------------------------------------------------------
     * New run
     * ------------------------------------------------------------ */

    measurement->run_id++;


    measurement->source =
        source;


    measurement->target_events =
        event_count;


    measurement->generated_events =
        0U;


    measurement->dma_errors =
        0U;


    measurement->publish_rejected =
        0U;


    /* ------------------------------------------------------------
     * Reset software statistics
     * ------------------------------------------------------------ */

    metrics_reset();

    kafka_reset_run_stats(
        kafka
    );


    metric_export_index =
        0U;

    metric_export_active =
        0U;

    metric_export_header_sent =
        0U;


    /*
     * IMPORTANT:
     *
     * Both generators remain disabled here.
     *
     * dma_read_event() will:
     *
     *   1. arm the selected S2MM DMA
     *   2. enable only the corresponding generator
     *
     * This avoids generating stream words before the
     * destination DMA is ready.
     */
    if (dma_prepare_run(source) != XST_SUCCESS) {
        xil_printf(
            "Failed to prepare source %u\r\n",
            (unsigned)source
        );

        return XST_FAILURE;
    }


    measurement->state =
        MEAS_RUNNING;


    xil_printf(
        "\r\n"
        "RUN_START,"
        "run=%u,"
        "source=%u,"
        "target=%u,"
        "pipeline=%u\r\n",

        (unsigned)measurement->run_id,
        (unsigned)measurement->source,
        (unsigned)measurement->target_events,
        (unsigned)KAFKA_PIPELINE_DEPTH
    );


    return XST_SUCCESS;
}


/* ================================================================
 * Finish FPGA generation
 * ================================================================ */

static void finish_generation(
    measurement_t *measurement
)
{
    /*
     * Stop both generators immediately after the
     * requested number of events has been acquired.
     */
    dma_stream_disable_all();


    measurement->state =
        MEAS_DRAINING;


    xil_printf(
        "RUN_GENERATION_DONE,events=%u\r\n",
        (unsigned)measurement->generated_events
    );
}


/* ================================================================
 * Acquire one FPGA event
 * ================================================================ */

static void acquire_one_event(
    measurement_t *measurement,
    kafka_producer_t *kafka
)
{
    fpga_event_t event;


    /*
     * PS timestamp immediately before requesting
     * the FPGA/DMA event.
     */
    uint32_t dma_start_us =
        ps_time_us();


    int dma_status =
        dma_read_event(
            measurement->source,
            &event
        );


    /*
     * PS timestamp immediately after S2MM completion.
     */
    uint32_t dma_done_us =
        ps_time_us();


    if (
        dma_status !=
        XST_SUCCESS
    ) {

        measurement->dma_errors++;

        return;
    }


    measurement->generated_events++;


    /* ------------------------------------------------------------
     * Encode Kafka payload
     *
     * bytes 0..3 = frame counter
     * bytes 4..7 = FPGA timestamp
     * ------------------------------------------------------------ */

    uint8_t payload[
        KAFKA_EVENT_BYTES
    ];


    event_encode_payload(
        payload,
        &event
    );


    /* ------------------------------------------------------------
     * Metadata used locally for measurements
     * ------------------------------------------------------------ */

    kafka_event_meta_t meta;


    memset(
        &meta,
        0,
        sizeof(meta)
    );


    meta.run_id =
        measurement->run_id;


    meta.source_id =
        measurement->source;


    meta.frame_counter =
        event.frame_counter;


    meta.fpga_timestamp_ticks =
        event.timestamp_ticks;


    meta.dma_start_us =
        dma_start_us;


    meta.dma_done_us =
        dma_done_us;


    /* ------------------------------------------------------------
     * Publish
     * ------------------------------------------------------------ */

    int result =
        kafka_publish(
            kafka,
            payload,
            KAFKA_EVENT_BYTES,
            &meta
        );


    if (result != 0) {

        measurement->publish_rejected++;
    }


    /* ------------------------------------------------------------
     * End generation after N valid DMA events
     * ------------------------------------------------------------ */

    if (
        measurement->generated_events >=
        measurement->target_events
    ) {

        finish_generation(
            measurement
        );
    }
}


/* ================================================================
 * Measurement state machine
 * ================================================================ */

void measurement_tick(
    measurement_t *measurement,
    kafka_producer_t *kafka
)
{
    /* ------------------------------------------------------------
     * Button edge detection
     * ------------------------------------------------------------ */

    u32 buttons =
        buttons_read();


    u32 pressed =
        buttons &
        ~previous_buttons;


    previous_buttons =
        buttons;


    /* ------------------------------------------------------------
     * Start new measurement
     * ------------------------------------------------------------ */

    if (
        measurement->state == MEAS_IDLE ||
        measurement->state == MEAS_COMPLETE
    ) {

        /*
         * BTN0
         *
         * DMA0
         * white
         * enable[0]
         */
        if (
            (pressed & BTN_DMA0_MASK) !=
            0U
        ) {

            measurement_start(
                measurement,
                kafka,
                DMA_SOURCE_0,
                DEFAULT_EVENTS_PER_RUN
            );

            return;
        }


        /*
         * BTN1
         *
         * DMA1
         * snow
         * enable[1]
         */
        if (
            (pressed & BTN_DMA1_MASK) !=
            0U
        ) {

            measurement_start(
                measurement,
                kafka,
                DMA_SOURCE_1,
                DEFAULT_EVENTS_PER_RUN
            );

            return;
        }
    }


    /* ------------------------------------------------------------
     * Acquire events
     * ------------------------------------------------------------ */
    if (measurement->state == MEAS_RUNNING) {

        /*
         * Software backpressure.
         *
         * If Kafka has no local buffering capacity left,
         * do not arm another DMA transaction.
         *
         * main() will continue processing Ethernet/lwIP,
         * allowing Kafka ACKs to arrive and drain the queue.
         */
        if (kafka_can_accept(kafka)) {

            acquire_one_event(
                measurement,
                kafka
            );
        }

        return;
    }


    /* ------------------------------------------------------------
     * Finish measured path, then export metrics through Kafka.
     *
     * The metric export begins only after every measured event has
     * received its Kafka ProduceResponse. Therefore metric traffic
     * is outside the measurement interval.
     * ------------------------------------------------------------ */

    if (
        measurement->state ==
        MEAS_DRAINING
    ) {

        /*
         * First phase:
         * wait until the measured event path has fully drained.
         */
        if (!metric_export_active) {

            if (!kafka_is_drained(kafka)) {
                return;
            }


            /*
             * Safety:
             * generators must remain disabled.
             */
            dma_stream_disable_all();


            xil_printf(
                "\r\n"
                "RUN_COMPLETE,run=%u\r\n",

                (unsigned)measurement->run_id
            );


            xil_printf(
                "FPGA_DMA_EVENTS=%u\r\n",
                (unsigned)measurement->generated_events
            );


            xil_printf(
                "DMA_ERRORS=%u\r\n",
                (unsigned)measurement->dma_errors
            );


            xil_printf(
                "PUBLISH_REJECTED=%u\r\n",
                (unsigned)measurement->publish_rejected
            );


            /*
             * These statistics describe only the measured
             * detector-event path. Metric export has not begun yet.
             */
            kafka_print_stats(
                kafka
            );


            xil_printf(
                "METRIC_RECORDS=%u\r\n",
                (unsigned)metrics_count()
            );


            xil_printf(
                "METRIC_EXPORT_START,"
                "records=%u,"
                "topic=zynq-metrics\r\n",

                (unsigned)metrics_count()
            );


            metric_export_index =
                0U;

            metric_export_header_sent =
                0U;

            metric_export_active =
                1U;

            return;
        }


        /*
         * Second phase:
         * publish one CSV message per measurement_tick().
         *
         * kafka_publish_metrics() is direct/pipelined and does not
         * use the detector-event queue. If the pipeline is full it
         * returns 1 and the next main-loop iteration retries.
         */
        if (!metric_export_header_sent) {

            int result =
                kafka_publish_metrics(
                    kafka,
                    (const uint8_t *)metric_csv_header,
                    (uint16_t)strlen(metric_csv_header)
                );


            if (result == 0) {

                metric_export_header_sent =
                    1U;

            } else if (result < 0) {

                xil_printf(
                    "METRIC_EXPORT_ERROR,header\r\n"
                );

                metric_export_active =
                    0U;

                measurement->state =
                    MEAS_COMPLETE;
            }


            return;
        }


        if (
            metric_export_index <
            metrics_count()
        ) {

            metric_record_t record;

            if (
                metrics_get(
                    metric_export_index,
                    &record
                ) != 0
            ) {

                xil_printf(
                    "METRIC_EXPORT_ERROR,"
                    "index=%u,"
                    "reason=metrics_get\r\n",

                    (unsigned)metric_export_index
                );

                metric_export_active =
                    0U;

                measurement->state =
                    MEAS_COMPLETE;

                return;
            }


            char line[
                METRIC_EXPORT_LINE_BYTES
            ];


            int length =
                format_metric_csv(
                    &record,
                    line,
                    sizeof(line)
                );


            if (length < 0) {

                xil_printf(
                    "METRIC_EXPORT_ERROR,"
                    "index=%u,"
                    "reason=format\r\n",

                    (unsigned)metric_export_index
                );

                metric_export_active =
                    0U;

                measurement->state =
                    MEAS_COMPLETE;

                return;
            }


            int result =
                kafka_publish_metrics(
                    kafka,
                    (const uint8_t *)line,
                    (uint16_t)length
                );


            if (result == 0) {

                metric_export_index++;

            } else if (result < 0) {

                xil_printf(
                    "METRIC_EXPORT_ERROR,"
                    "index=%u,"
                    "reason=publish\r\n",

                    (unsigned)metric_export_index
                );

                metric_export_active =
                    0U;

                measurement->state =
                    MEAS_COMPLETE;
            }


            return;
        }


        /*
         * All CSV messages have been submitted.
         * Wait for their ProduceResponses before allowing a new run.
         */
        if (kafka_is_drained(kafka)) {

            xil_printf(
                "METRIC_EXPORT_DONE,"
                "records=%u\r\n",

                (unsigned)metric_export_index
            );


            metric_export_active =
                0U;

            measurement->state =
                MEAS_COMPLETE;
        }


        return;
    }
}
