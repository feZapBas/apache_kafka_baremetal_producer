#ifndef MEASUREMENT_H
#define MEASUREMENT_H

#include <stdint.h>

#include "xgpio.h"
#include "xparameters.h"

#include "kafka_producer.h"

/* AXI GPIO connected to BTN0/BTN1 in the current block design. */
#define BTN_GPIO_DEVICE_ID XPAR_GPIO_0_DEVICE_ID
#define BTN_GPIO_CHANNEL   1U

#define BTN_DMA0_MASK 0x01U
#define BTN_DMA1_MASK 0x02U
#define BTN_MASK      (BTN_DMA0_MASK | BTN_DMA1_MASK)

#define DEFAULT_EVENTS_PER_RUN 5000U

typedef enum {
    MEAS_IDLE = 0,
    MEAS_RUNNING,
    MEAS_DRAINING,
    MEAS_COMPLETE
} measurement_state_t;

typedef struct {
    measurement_state_t state;

    uint32_t run_id;
    uint8_t source;

    uint32_t target_events;
    uint32_t generated_events;

    uint32_t dma_errors;
    uint32_t publish_rejected;
} measurement_t;

int measurement_init(measurement_t *measurement);

int measurement_start(measurement_t *measurement,
                      kafka_producer_t *kafka,
                      uint8_t source,
                      uint32_t event_count);

void measurement_tick(measurement_t *measurement,
                      kafka_producer_t *kafka);

#endif /* MEASUREMENT_H */
