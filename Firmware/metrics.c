#include "metrics.h"

#include "xil_printf.h"

#include <string.h>

static metric_record_t records[METRICS_CAPACITY];
static uint32_t record_count = 0U;

void metrics_reset(void)
{
    record_count = 0U;
    memset(records, 0, sizeof(records));
}

int metrics_add(const metric_record_t *record)
{
    if (record == NULL) {
        return -1;
    }

    if (record_count >= METRICS_CAPACITY) {
        return -1;
    }

    records[record_count++] = *record;
    return 0;
}

uint32_t metrics_count(void)
{
    return record_count;
}

int metrics_get(uint32_t index,
                metric_record_t *record)
{
    if (record == NULL) {
        return -1;
    }

    if (index >= record_count) {
        return -1;
    }

    *record = records[index];
    return 0;
}

void metrics_dump_csv(void)
{
    xil_printf(
        "\r\n"
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
        "kafka_error"
        "\r\n"
    );

    for (uint32_t i = 0U; i < record_count; ++i) {
        const metric_record_t *r = &records[i];

        uint32_t dma_latency =
            r->dma_done_us - r->dma_start_us;

        uint32_t queue_latency =
            r->send_us - r->publish_us;

        uint32_t send_ack_latency =
            r->ack_us - r->send_us;

        uint32_t publish_ack_latency =
            r->ack_us - r->publish_us;

        xil_printf(
            "METRIC,"
            "%u,%u,%u,%u,"
            "%u,%u,%u,%u,%u,"
            "%u,%u,%u,%u,%d\r\n",

            (unsigned)r->run_id,
            (unsigned)r->source_id,
            (unsigned)r->frame_counter,
            (unsigned)r->fpga_timestamp_ticks,

            (unsigned)r->dma_start_us,
            (unsigned)r->dma_done_us,
            (unsigned)r->publish_us,
            (unsigned)r->send_us,
            (unsigned)r->ack_us,

            (unsigned)dma_latency,
            (unsigned)queue_latency,
            (unsigned)send_ack_latency,
            (unsigned)publish_ack_latency,

            (int)r->kafka_error
        );
    }
}
