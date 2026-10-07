#ifndef DMA_WORK_H
#define DMA_WORK_H

#include <stdint.h>

#include "xaxidma.h"
#include "xgpio.h"
#include "xil_types.h"
#include "xparameters.h"

#include "event_format.h"

#define DMA0_DEVICE_ID XPAR_AXIDMA_0_DEVICE_ID
#define DMA1_DEVICE_ID XPAR_AXIDMA_1_DEVICE_ID

/*
 * AXI_GPIO_1 drives the enable input used by white/snow
 * in the current Vivado block design.
 */
#define DMA_ENABLE_0_MASK  0x00000001U
#define DMA_ENABLE_1_MASK  0x00000002U
#define DMA_ENABLE_NONE    0x00000000U

void dma_stream_disable_all(void);
void dma_stream_select(uint8_t source);
#define ENABLE_GPIO_DEVICE_ID XPAR_GPIO_1_DEVICE_ID
#define ENABLE_GPIO_CHANNEL   1U

/*
 * One event = one 64-bit AXI4-Stream beat.
 */
#define DMA_TRANSFER_BYTES 8U
#define DMA_BUFFER_ALIGN   32U
#define DMA_TIMEOUT_US     1000000U

#define DMA_SOURCE_0 0U
#define DMA_SOURCE_1 1U

int  dma_initialization(void);
int  dma_read_event(uint8_t source, fpga_event_t *event);
void dma_stream_enable(int enable);
void dma_stream_reset(void);
int dma_prepare_run(uint8_t source);

#endif /* DMA_WORK_H */
