#ifndef EVENT_FORMAT_H
#define EVENT_FORMAT_H

#include <stdint.h>

/*
 * FPGA AXI4-Stream event format (64-bit):
 *
 *   TDATA[63:32] = frame_counter
 *   TDATA[31:0]  = timestamp_ticks
 *
 * One FPGA frame = one 64-bit AXI4-Stream beat.
 *
 * Kafka payload:
 *   bytes 0..3 = frame_counter, big-endian
 *   bytes 4..7 = timestamp_ticks, big-endian
 */
#define FPGA_EVENT_BYTES  8U
#define KAFKA_EVENT_BYTES 8U

typedef struct {
    uint32_t frame_counter;
    uint32_t timestamp_ticks;
} fpga_event_t;

static inline void event_put_u32_be(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)((v >> 24) & 0xFFU);
    p[1] = (uint8_t)((v >> 16) & 0xFFU);
    p[2] = (uint8_t)((v >>  8) & 0xFFU);
    p[3] = (uint8_t)( v        & 0xFFU);
}

static inline uint32_t event_get_u32_be(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) |
           ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] <<  8) |
            (uint32_t)p[3];
}

static inline void event_encode_payload(uint8_t *payload,
                                        const fpga_event_t *event)
{
    event_put_u32_be(&payload[0], event->frame_counter);
    event_put_u32_be(&payload[4], event->timestamp_ticks);
}

#endif /* EVENT_FORMAT_H */
