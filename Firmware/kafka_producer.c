#include "kafka_producer.h"

#include "crc32c.h"
#include "metrics.h"
#include "time_utils.h"

#include "lwip/tcp.h"
#include "lwip/ip_addr.h"

#include "xil_printf.h"

#include <stdint.h>
#include <string.h>
#include <limits.h>

#define LOG(fmt, ...) \
    xil_printf("[KAFKA] " fmt "\r\n", ##__VA_ARGS__)

#ifndef KAFKA_METRICS_TOPIC
#define KAFKA_METRICS_TOPIC "zynq-metrics"
#endif

#define KAFKA_METRICS_MAX_PAYLOAD 192U
#define KAFKA_METRICS_SOURCE_SENTINEL UINT32_MAX

static int kafka_meta_is_measurement(const kafka_event_meta_t *meta)
{
    return (
        meta != NULL &&
        meta->source_id != KAFKA_METRICS_SOURCE_SENTINEL
    );
}

/* ------------------------------------------------------------------ */
/* Serialization helpers                                               */
/* ------------------------------------------------------------------ */

static uint8_t *put_i16(uint8_t *p, int16_t value)
{
    uint16_t v = (uint16_t)value;
    p[0] = (uint8_t)((v >> 8) & 0xFFU);
    p[1] = (uint8_t)(v & 0xFFU);
    return p + 2;
}

static uint8_t *put_i32(uint8_t *p, int32_t value)
{
    uint32_t v = (uint32_t)value;
    p[0] = (uint8_t)((v >> 24) & 0xFFU);
    p[1] = (uint8_t)((v >> 16) & 0xFFU);
    p[2] = (uint8_t)((v >>  8) & 0xFFU);
    p[3] = (uint8_t)(v & 0xFFU);
    return p + 4;
}

static uint8_t *put_i64(uint8_t *p, int64_t value)
{
    uint64_t v = (uint64_t)value;

    for (int i = 7; i >= 0; --i) {
        *p++ = (uint8_t)((v >> (i * 8)) & 0xFFULL);
    }

    return p;
}

static uint8_t *put_varint(uint8_t *p, int32_t value)
{
    uint32_t uv =
        ((uint32_t)value << 1) ^
        (uint32_t)(value >> 31);

    while (uv > 0x7FU) {
        *p++ = (uint8_t)((uv & 0x7FU) | 0x80U);
        uv >>= 7;
    }

    *p++ = (uint8_t)(uv & 0x7FU);
    return p;
}

static uint8_t *put_str(uint8_t *p, const char *string)
{
    int16_t length = (int16_t)strlen(string);
    p = put_i16(p, length);
    memcpy(p, string, length);
    return p + length;
}

static int32_t read_i32(const uint8_t *p)
{
    return (int32_t)(
        ((uint32_t)p[0] << 24) |
        ((uint32_t)p[1] << 16) |
        ((uint32_t)p[2] <<  8) |
         (uint32_t)p[3]
    );
}

static int16_t read_i16(const uint8_t *p)
{
    return (int16_t)(
        ((uint16_t)p[0] << 8) |
         (uint16_t)p[1]
    );
}

/* ------------------------------------------------------------------ */
/* ApiVersions request                                                 */
/* ------------------------------------------------------------------ */

static uint16_t build_api_versions_request(uint8_t *buffer,
                                           int32_t correlation_id)
{
    uint8_t *p = buffer + 4;
    uint8_t *body = p;

    p = put_i16(p, 18); /* ApiVersions */
    p = put_i16(p, 0);  /* version 0 */
    p = put_i32(p, correlation_id);
    p = put_str(p, KAFKA_CLIENT_ID);

    uint16_t body_length =
        (uint16_t)(p - body);

    put_i32(buffer, body_length);

    return body_length + 4U;
}

/* ------------------------------------------------------------------ */
/* ProduceRequest v3                                                   */
/* ------------------------------------------------------------------ */

static uint16_t build_produce_request(uint8_t *buffer,
                                      int32_t correlation_id,
                                      const char *topic,
                                      const uint8_t *payload,
                                      uint16_t payload_length)
{
    uint8_t *p = buffer + 4;
    uint8_t *body = p;

    /* Request header */
    p = put_i16(p, 0); /* Produce */
    p = put_i16(p, 3); /* version 3 */
    p = put_i32(p, correlation_id);
    p = put_str(p, KAFKA_CLIENT_ID);

    /* Produce fields */
    p = put_i16(p, -1);   /* transactional_id = null */
    p = put_i16(p, 1);    /* acks = 1 */
    p = put_i32(p, 5000); /* timeout_ms */

    /* topics */
    p = put_i32(p, 1);
    p = put_str(p, topic);

    /* one partition */
    p = put_i32(p, 1);
    p = put_i32(p, 0);

    /* records bytes length */
    uint8_t *records_size_ptr = p;
    p += 4;

    uint8_t *batch_start = p;

    /* RecordBatch */
    p = put_i64(p, 0); /* baseOffset */

    uint8_t *batch_length_ptr = p;
    p += 4;

    p = put_i32(p, -1); /* partitionLeaderEpoch */
    *p++ = 2;            /* magic */

    uint8_t *crc_ptr = p;
    p += 4;

    uint8_t *crc_data_start = p;

    p = put_i16(p, 0);   /* attributes */
    p = put_i32(p, 0);   /* lastOffsetDelta */
    p = put_i64(p, 0);   /* firstTimestamp */
    p = put_i64(p, 0);   /* maxTimestamp */
    p = put_i64(p, -1);  /* producerId */
    p = put_i16(p, -1);  /* producerEpoch */
    p = put_i32(p, -1);  /* baseSequence */
    p = put_i32(p, 1);   /* records count */

    uint8_t record[KAFKA_METRICS_MAX_PAYLOAD + 32U];
    uint8_t *r = record;

    r = put_varint(r, 0);              /* attributes */
    r = put_varint(r, 0);              /* timestampDelta */
    r = put_varint(r, 0);              /* offsetDelta */
    r = put_varint(r, -1);             /* key = null */
    r = put_varint(r, payload_length); /* value size */

    memcpy(r, payload, payload_length);
    r += payload_length;

    r = put_varint(r, 0);              /* headers count */

    uint16_t record_length =
        (uint16_t)(r - record);

    p = put_varint(p, record_length);

    memcpy(p, record, record_length);
    p += record_length;

    /* CRC32C over RecordBatch fields after the CRC field */
    uint32_t crc_length =
        (uint32_t)(p - crc_data_start);

    uint32_t crc =
        crc32c(crc_data_start, crc_length);

    crc_ptr[0] = (uint8_t)(crc >> 24);
    crc_ptr[1] = (uint8_t)(crc >> 16);
    crc_ptr[2] = (uint8_t)(crc >> 8);
    crc_ptr[3] = (uint8_t)crc;

    put_i32(
        batch_length_ptr,
        (int32_t)(p - (batch_length_ptr + 4))
    );

    put_i32(
        records_size_ptr,
        (int32_t)(p - batch_start)
    );

    uint16_t total =
        (uint16_t)(p - body);

    put_i32(buffer, total);

    return total + 4U;
}

/* ------------------------------------------------------------------ */
/* ProduceResponse v3                                                  */
/* ------------------------------------------------------------------ */

static int16_t parse_produce_response(const uint8_t *buffer,
                                      uint16_t total_length)
{
    const uint8_t *p = buffer + 4;
    const uint8_t *end =
        buffer + total_length;

    /* correlation_id */
    if (p + 4 > end) {
        return INT16_MIN;
    }
    p += 4;

    /* responses array length */
    if (p + 4 > end) {
        return INT16_MIN;
    }

    int32_t topic_count = read_i32(p);
    p += 4;

    if (topic_count < 1) {
        return INT16_MIN;
    }

    /* topic string */
    if (p + 2 > end) {
        return INT16_MIN;
    }

    int16_t topic_length = read_i16(p);
    p += 2;

    if (topic_length < 0 ||
        p + topic_length > end) {
        return INT16_MIN;
    }

    p += topic_length;

    /* partition response array */
    if (p + 4 > end) {
        return INT16_MIN;
    }

    int32_t partition_count = read_i32(p);
    p += 4;

    if (partition_count < 1) {
        return INT16_MIN;
    }

    /* partition index */
    if (p + 4 > end) {
        return INT16_MIN;
    }
    p += 4;

    /* error_code */
    if (p + 2 > end) {
        return INT16_MIN;
    }

    return read_i16(p);
}

/* ------------------------------------------------------------------ */
/* Pipeline helpers                                                    */
/* ------------------------------------------------------------------ */

static int find_free_slot(kafka_producer_t *kp)
{
    for (int i = 0; i < KAFKA_PIPELINE_DEPTH; ++i) {
        if (!kp->slots[i].in_use) {
            return i;
        }
    }

    return -1;
}

static int find_slot_by_correlation(kafka_producer_t *kp,
                                    int32_t correlation_id)
{
    for (int i = 0; i < KAFKA_PIPELINE_DEPTH; ++i) {
        if (kp->slots[i].in_use &&
            kp->slots[i].correlation_id == correlation_id) {
            return i;
        }
    }

    return -1;
}

/* ------------------------------------------------------------------ */
/* Pending-event queue                                                 */
/* ------------------------------------------------------------------ */

static int queue_event(kafka_producer_t *kp,
                       const uint8_t *payload,
                       uint16_t length,
                       const kafka_event_meta_t *meta,
                       uint32_t publish_us)
{
    if (length != KAFKA_EVENT_BYTES) {
        kp->events_dropped++;
        return -1;
    }

    if (kp->q_count >= KAFKA_QUEUE_DEPTH) {
        kp->events_dropped++;
        return -1;
    }

    kafka_queued_event_t *event =
        &kp->queue[kp->q_tail];

    memcpy(event->data, payload, length);

    event->len = length;
    event->meta = *meta;
    event->publish_us = publish_us;
    event->valid = 1U;

    kp->q_tail =
        (uint8_t)((kp->q_tail + 1U) % KAFKA_QUEUE_DEPTH);

    kp->q_count++;
    kp->events_queued++;

    return 0;
}

/*
 * Return values:
 *   0  request accepted by lwIP
 *   1  temporarily unable to send
 *  -1  fatal/error path
 */
static int send_produce(kafka_producer_t *kp,
                        const char *topic,
                        const uint8_t *payload,
                        uint16_t length,
                        const kafka_event_meta_t *meta,
                        uint32_t publish_us)
{
    if (kp->inflight_count >= KAFKA_PIPELINE_DEPTH) {
        return 1;
    }

    int slot = find_free_slot(kp);

    if (slot < 0) {
        return 1;
    }

    int32_t correlation_id =
        kp->correlation_id_next++;

    uint16_t request_length =
        build_produce_request(
            kp->tx_bufs[slot],
            correlation_id,
            topic,
            payload,
            length
        );

    err_t err =
        tcp_write(
            kp->pcb,
            kp->tx_bufs[slot],
            request_length,
            TCP_WRITE_FLAG_COPY
        );

    if (err == ERR_MEM) {
        kp->tcp_err_mem++;
        return 1;
    }

    if (err != ERR_OK) {
        kp->tcp_errors++;
        kp->state = KAFKA_ERROR;
        return -1;
    }

    /*
     * This timestamp means:
     * "lwIP accepted the complete ProduceRequest into its TCP send buffer".
     */
    uint32_t send_us =
        ps_time_us();

    kafka_slot_t *s =
        &kp->slots[slot];

    s->correlation_id =
        correlation_id;

    s->meta =
        *meta;

    s->publish_us =
        publish_us;

    s->send_us =
        send_us;

    s->in_use =
        1U;

    kp->tx_lens[slot] =
        request_length;

    kp->inflight_count++;

    if (kafka_meta_is_measurement(meta)) {
        kp->events_tcp_written++;
    }

    err =
        tcp_output(kp->pcb);

    if (err != ERR_OK) {
        kp->tcp_output_errors++;
    }

    return 0;
}

static void drain_queue(kafka_producer_t *kp)
{
    while (kp->q_count > 0U &&
           kp->inflight_count < KAFKA_PIPELINE_DEPTH &&
           kp->state == KAFKA_READY) {

        kafka_queued_event_t *event =
            &kp->queue[kp->q_head];

        if (!event->valid) {
            break;
        }

        int result =
            send_produce(
                kp,
                KAFKA_TOPIC,
                event->data,
                event->len,
                &event->meta,
                event->publish_us
            );

        if (result != 0) {
            break;
        }

        event->valid = 0U;

        kp->q_head =
            (uint8_t)((kp->q_head + 1U) % KAFKA_QUEUE_DEPTH);

        kp->q_count--;
    }
}

/* ------------------------------------------------------------------ */
/* lwIP callbacks                                                      */
/* ------------------------------------------------------------------ */

static err_t on_connected(void *arg,
                          struct tcp_pcb *pcb,
                          err_t err)
{
    kafka_producer_t *kp =
        (kafka_producer_t *)arg;

    if (err != ERR_OK) {
        kp->state = KAFKA_ERROR;
        return err;
    }

    LOG("connected");

    kp->state =
        KAFKA_SENDING_API_VERSIONS;

    int32_t correlation_id =
        kp->correlation_id_next++;

    uint16_t length =
        build_api_versions_request(
            kp->tx_bufs[0],
            correlation_id
        );

    err =
        tcp_write(
            pcb,
            kp->tx_bufs[0],
            length,
            TCP_WRITE_FLAG_COPY
        );

    if (err != ERR_OK) {
        kp->state = KAFKA_ERROR;
        return err;
    }

    tcp_output(pcb);

    return ERR_OK;
}

static err_t on_receive(void *arg,
                        struct tcp_pcb *pcb,
                        struct pbuf *p,
                        err_t err)
{
    kafka_producer_t *kp =
        (kafka_producer_t *)arg;

    if (p == NULL) {
        LOG("broker closed connection");
        kp->pcb = NULL;
        kp->state = KAFKA_ERROR;
        return ERR_OK;
    }

    if (err != ERR_OK) {
        pbuf_free(p);
        return err;
    }

    if ((uint32_t)kp->rx_len + p->tot_len >
        KAFKA_RX_BUF_SIZE) {

        kp->tcp_errors++;

        tcp_recved(pcb, p->tot_len);
        pbuf_free(p);

        kp->state = KAFKA_ERROR;

        return ERR_BUF;
    }

    pbuf_copy_partial(
        p,
        kp->rx_buf + kp->rx_len,
        p->tot_len,
        0
    );

    kp->rx_len +=
        (uint16_t)p->tot_len;

    tcp_recved(pcb, p->tot_len);
    pbuf_free(p);

    /* ApiVersions response */
    if (kp->state ==
        KAFKA_SENDING_API_VERSIONS) {

        if (kp->rx_len < 4U) {
            return ERR_OK;
        }

        int32_t message_size =
            read_i32(kp->rx_buf);

        if (message_size < 0) {
            kp->state = KAFKA_ERROR;
            return ERR_VAL;
        }

        uint32_t total =
            4U + (uint32_t)message_size;

        if (kp->rx_len < total) {
            return ERR_OK;
        }

        memmove(
            kp->rx_buf,
            kp->rx_buf + total,
            kp->rx_len - total
        );

        kp->rx_len -=
            (uint16_t)total;

        kp->state = KAFKA_READY;

        LOG("READY pipeline=%d",
            KAFKA_PIPELINE_DEPTH);

        drain_queue(kp);
    }

    /* ProduceResponse(s) */
    while (kp->state == KAFKA_READY &&
           kp->rx_len >= 4U) {

        int32_t message_size =
            read_i32(kp->rx_buf);

        if (message_size < 0) {
            kp->tcp_errors++;
            kp->state = KAFKA_ERROR;
            break;
        }

        uint32_t total =
            4U + (uint32_t)message_size;

        if (total > KAFKA_RX_BUF_SIZE) {
            kp->tcp_errors++;
            kp->state = KAFKA_ERROR;
            break;
        }

        if (kp->rx_len < total) {
            break;
        }

        /*
         * PS timestamp when a complete ProduceResponse
         * is available to the callback.
         */
        uint32_t ack_us =
            ps_time_us();

        int32_t correlation_id =
            read_i32(kp->rx_buf + 4);

        int16_t error_code =
            parse_produce_response(
                kp->rx_buf,
                (uint16_t)total
            );

        int slot =
            find_slot_by_correlation(
                kp,
                correlation_id
            );

        if (slot >= 0) {
            kafka_slot_t *s =
                &kp->slots[slot];

            int is_measurement =
                kafka_meta_is_measurement(&s->meta);

            if (is_measurement) {
                metric_record_t metric;
                memset(&metric, 0, sizeof(metric));

                metric.run_id =
                    s->meta.run_id;

                metric.source_id =
                    s->meta.source_id;

                metric.frame_counter =
                    s->meta.frame_counter;

                metric.fpga_timestamp_ticks =
                    s->meta.fpga_timestamp_ticks;

                metric.dma_start_us =
                    s->meta.dma_start_us;

                metric.dma_done_us =
                    s->meta.dma_done_us;

                metric.publish_us =
                    s->publish_us;

                metric.send_us =
                    s->send_us;

                metric.ack_us =
                    ack_us;

                metric.kafka_error =
                    error_code;

                if (metrics_add(&metric) != 0) {
                    kp->kafka_errors++;
                }
            }

            s->in_use = 0U;

            if (kp->inflight_count > 0U) {
                kp->inflight_count--;
            }

            if (is_measurement) {
                if (error_code == 0) {
                    kp->events_acked++;
                } else {
                    kp->kafka_errors++;
                }
            } else if (error_code != 0) {
                LOG(
                    "metrics export Kafka error=%d",
                    (int)error_code
                );
            }
        } else {
            kp->responses_unmatched++;
        }

        memmove(
            kp->rx_buf,
            kp->rx_buf + total,
            kp->rx_len - total
        );

        kp->rx_len -=
            (uint16_t)total;
    }

    drain_queue(kp);

    return ERR_OK;
}

static void on_error(void *arg,
                     err_t err)
{
    kafka_producer_t *kp =
        (kafka_producer_t *)arg;

    LOG("TCP error=%d", err);

    kp->tcp_errors++;

    for (int i = 0;
         i < KAFKA_PIPELINE_DEPTH;
         ++i) {

        if (kp->slots[i].in_use) {
            kp->slots[i].in_use = 0U;
            kp->inflight_lost++;
        }
    }

    kp->inflight_count = 0U;
    kp->pcb = NULL;
    kp->state = KAFKA_ERROR;
}

static err_t on_sent(void *arg,
                     struct tcp_pcb *pcb,
                     u16_t len)
{
    (void)arg;
    (void)pcb;
    (void)len;

    return ERR_OK;
}

/* ------------------------------------------------------------------ */
/* Public API                                                          */
/* ------------------------------------------------------------------ */

void kafka_producer_init(kafka_producer_t *kp,
                         const char *broker_ip)
{
    memset(kp, 0, sizeof(*kp));

    kp->state =
        KAFKA_IDLE;

    kp->broker_port =
        KAFKA_BROKER_PORT;

    kp->correlation_id_next =
        1;

    if (!ipaddr_aton(
            broker_ip,
            &kp->broker_ip)) {

        LOG("invalid broker IP: %s",
            broker_ip);

        kp->state =
            KAFKA_ERROR;

        return;
    }

    LOG("broker=%s:%u pipeline=%d",
        broker_ip,
        KAFKA_BROKER_PORT,
        KAFKA_PIPELINE_DEPTH);
}

void kafka_producer_connect(kafka_producer_t *kp)
{
    kp->rx_len = 0U;

    if (kp->pcb != NULL) {
        tcp_abort(kp->pcb);
        kp->pcb = NULL;
    }

    kp->pcb =
        tcp_new();

    if (kp->pcb == NULL) {
        LOG("tcp_new failed");
        kp->state = KAFKA_ERROR;
        return;
    }

    tcp_arg(kp->pcb, kp);
    tcp_recv(kp->pcb, on_receive);
    tcp_err(kp->pcb, on_error);
    tcp_sent(kp->pcb, on_sent);

    kp->state =
        KAFKA_CONNECTING;

    err_t err =
        tcp_connect(
            kp->pcb,
            &kp->broker_ip,
            kp->broker_port,
            on_connected
        );

    if (err != ERR_OK) {
        LOG("tcp_connect failed: %d", err);

        tcp_abort(kp->pcb);
        kp->pcb = NULL;
        kp->state = KAFKA_ERROR;
    }
}

int kafka_publish(kafka_producer_t *kp,
                  const uint8_t *payload,
                  uint16_t length,
                  const kafka_event_meta_t *meta)
{
    if (kp == NULL ||
        payload == NULL ||
        meta == NULL ||
        length != KAFKA_EVENT_BYTES) {

        return -1;
    }

    kp->publish_calls++;

    uint32_t publish_us =
        ps_time_us();

    if (kp->state != KAFKA_READY) {
        return queue_event(
            kp,
            payload,
            length,
            meta,
            publish_us
        );
    }

    int result =
        send_produce(
            kp,
            KAFKA_TOPIC,
            payload,
            length,
            meta,
            publish_us
        );

    if (result == 0) {
        return 0;
    }

    if (result == 1) {
        return queue_event(
            kp,
            payload,
            length,
            meta,
            publish_us
        );
    }

    kp->events_dropped++;
    return -1;
}

int kafka_publish_metrics(kafka_producer_t *kp,
                          const uint8_t *payload,
                          uint16_t length)
{
    if (kp == NULL ||
        payload == NULL ||
        length == 0U ||
        length > KAFKA_METRICS_MAX_PAYLOAD) {

        return -1;
    }

    /*
     * Metrics are exported only after the measured event path
     * has drained. They are never inserted into the detector-event
     * software queue, so they cannot perturb or be confused with
     * the measured event stream.
     */
    if (kp->state != KAFKA_READY) {
        return 1;
    }

    if (kp->q_count != 0U ||
        kp->inflight_count >= KAFKA_PIPELINE_DEPTH) {

        return 1;
    }

    kafka_event_meta_t meta;
    memset(&meta, 0, sizeof(meta));

    meta.source_id =
        KAFKA_METRICS_SOURCE_SENTINEL;

    return send_produce(
        kp,
        KAFKA_METRICS_TOPIC,
        payload,
        length,
        &meta,
        ps_time_us()
    );
}

kafka_state_t kafka_get_state(const kafka_producer_t *kp)
{
    return kp->state;
}

int kafka_is_drained(const kafka_producer_t *kp)
{
    return (
        kp->q_count == 0U &&
        kp->inflight_count == 0U
    );
}
int kafka_can_accept(const kafka_producer_t *kp)
{
    if (kp == NULL) {
        return 0;
    }

    /*
     * Do not acquire a new detector event unless the
     * producer is fully connected and operational.
     */
    if (kp->state != KAFKA_READY) {
        return 0;
    }

    /*
     * Reserve at least one position in the software queue.
     *
     * Even if a pipeline slot appears free, tcp_write()
     * may transiently return ERR_MEM. In that case
     * kafka_publish() falls back to queue_event().
     *
     * Therefore q_count < KAFKA_QUEUE_DEPTH is the
     * conservative condition that guarantees that
     * kafka_publish() still has somewhere to retain
     * the event instead of dropping it.
     */
    if (kp->q_count >= KAFKA_QUEUE_DEPTH) {
        return 0;
    }

    return 1;
}
void kafka_reset_run_stats(kafka_producer_t *kp)
{
    kp->publish_calls = 0U;
    kp->events_tcp_written = 0U;
    kp->events_queued = 0U;
    kp->events_dropped = 0U;
    kp->events_acked = 0U;

    kp->kafka_errors = 0U;
    kp->tcp_err_mem = 0U;
    kp->tcp_errors = 0U;
    kp->tcp_output_errors = 0U;
    kp->responses_unmatched = 0U;
    kp->inflight_lost = 0U;
}
void kafka_print_stats(const kafka_producer_t *kp)
{
    xil_printf(
        "\r\n=== KAFKA STATS ===\r\n"
    );

    xil_printf(
        "publish calls       : %u\r\n",
        (unsigned)kp->publish_calls
    );

    xil_printf(
        "tcp_write accepted  : %u\r\n",
        (unsigned)kp->events_tcp_written
    );

    xil_printf(
        "queued              : %u\r\n",
        (unsigned)kp->events_queued
    );

    xil_printf(
        "dropped             : %u\r\n",
        (unsigned)kp->events_dropped
    );

    xil_printf(
        "ACK OK              : %u\r\n",
        (unsigned)kp->events_acked
    );

    xil_printf(
        "Kafka errors        : %u\r\n",
        (unsigned)kp->kafka_errors
    );

    xil_printf(
        "lwIP ERR_MEM        : %u\r\n",
        (unsigned)kp->tcp_err_mem
    );

    xil_printf(
        "TCP errors          : %u\r\n",
        (unsigned)kp->tcp_errors
    );

    xil_printf(
        "tcp_output errors   : %u\r\n",
        (unsigned)kp->tcp_output_errors
    );

    xil_printf(
        "unmatched responses : %u\r\n",
        (unsigned)kp->responses_unmatched
    );

    xil_printf(
        "inflight lost       : %u\r\n",
        (unsigned)kp->inflight_lost
    );
}
