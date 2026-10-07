#ifndef KAFKA_PRODUCER_H
#define KAFKA_PRODUCER_H

#include <stdint.h>

#include "lwip/tcp.h"
#include "event_format.h"

#define KAFKA_BROKER_PORT 9092
#define KAFKA_TOPIC       "zynq-events"
#define KAFKA_METRICS_TOPIC "zynq-metrics"
#define KAFKA_CLIENT_ID   "cora-z7"

#ifndef KAFKA_PIPELINE_DEPTH
#define KAFKA_PIPELINE_DEPTH 1
#endif

#define KAFKA_QUEUE_DEPTH 8U
#define KAFKA_RX_BUF_SIZE 512U
#define KAFKA_TX_BUF_SIZE 512U

typedef enum {
    KAFKA_IDLE = 0,
    KAFKA_CONNECTING,
    KAFKA_SENDING_API_VERSIONS,
    KAFKA_READY,
    KAFKA_ERROR
} kafka_state_t;

typedef struct {
    uint32_t run_id;
    uint32_t source_id;

    uint32_t frame_counter;
    uint32_t fpga_timestamp_ticks;

    uint32_t dma_start_us;
    uint32_t dma_done_us;
} kafka_event_meta_t;

typedef struct {
    int32_t correlation_id;
    uint8_t in_use;

    kafka_event_meta_t meta;

    uint32_t publish_us;
    uint32_t send_us;
} kafka_slot_t;

typedef struct {
    uint8_t data[KAFKA_EVENT_BYTES];
    uint16_t len;
    uint8_t valid;

    kafka_event_meta_t meta;
    uint32_t publish_us;
} kafka_queued_event_t;

typedef struct {
    kafka_state_t state;
    struct tcp_pcb *pcb;
    ip_addr_t broker_ip;
    uint16_t broker_port;

    kafka_slot_t slots[KAFKA_PIPELINE_DEPTH];
    uint8_t tx_bufs[KAFKA_PIPELINE_DEPTH][KAFKA_TX_BUF_SIZE];
    uint16_t tx_lens[KAFKA_PIPELINE_DEPTH];

    uint8_t inflight_count;
    int32_t correlation_id_next;

    uint8_t rx_buf[KAFKA_RX_BUF_SIZE];
    uint16_t rx_len;

    kafka_queued_event_t queue[KAFKA_QUEUE_DEPTH];
    uint8_t q_head;
    uint8_t q_tail;
    uint8_t q_count;

    /* Measured detector-event statistics only. */
    uint32_t publish_calls;
    uint32_t events_tcp_written;
    uint32_t events_queued;
    uint32_t events_dropped;
    uint32_t events_acked;

    uint32_t kafka_errors;
    uint32_t tcp_err_mem;
    uint32_t tcp_errors;
    uint32_t tcp_output_errors;
    uint32_t responses_unmatched;
    uint32_t inflight_lost;
} kafka_producer_t;

void kafka_producer_init(kafka_producer_t *kp,
                         const char *broker_ip);

void kafka_producer_connect(kafka_producer_t *kp);

int kafka_publish(kafka_producer_t *kp,
                  const uint8_t *payload,
                  uint16_t length,
                  const kafka_event_meta_t *meta);

/*
 * Publishes one CSV metric record to KAFKA_METRICS_TOPIC.
 * Return values:
 *   0 = accepted by lwIP
 *   1 = retry later (pipeline/TCP not ready)
 *  -1 = invalid/fatal path
 *
 * Metric messages do not create metric_record_t entries and are not
 * included in the measured detector-event counters.
 */
int kafka_publish_metrics(kafka_producer_t *kp,
                          const uint8_t *payload,
                          uint16_t length);

kafka_state_t kafka_get_state(const kafka_producer_t *kp);
int kafka_is_drained(const kafka_producer_t *kp);
int kafka_can_accept(const kafka_producer_t *kp);

void kafka_reset_run_stats(kafka_producer_t *kp);
void kafka_print_stats(const kafka_producer_t *kp);

#endif /* KAFKA_PRODUCER_H */
