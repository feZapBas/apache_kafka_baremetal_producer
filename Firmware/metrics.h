#ifndef METRICS_H
#define METRICS_H

#include <stdint.h>

#define METRICS_CAPACITY 6000U

typedef struct {
    uint32_t run_id;
    uint32_t source_id;

    uint32_t frame_counter;
    uint32_t fpga_timestamp_ticks;

    uint32_t dma_start_us;
    uint32_t dma_done_us;

    uint32_t publish_us;
    uint32_t send_us;
    uint32_t ack_us;

    int16_t kafka_error;
} metric_record_t;

void     metrics_reset(void);
int      metrics_add(const metric_record_t *record);
uint32_t metrics_count(void);

/* Copy one stored metric record out of the private metrics buffer. */
int metrics_get(uint32_t index,
                metric_record_t *record);

/* Kept for debugging; normal campaign export now goes through Kafka. */
void metrics_dump_csv(void);

#endif /* METRICS_H */
