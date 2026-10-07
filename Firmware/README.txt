Vitis bare-metal Kafka timestamp measurement
==============================================

Files
-----
event_format.h
time_utils.h / time_utils.c
crc32c.h / crc32c.c
dma_work.h / dma_work.c
metrics.h / metrics.c
kafka_producer.h / kafka_producer.c
measurement.h / measurement.c
main.c

Assumed hardware event format
-----------------------------
AXI4-Stream width: 64 bits

TDATA[63:32] = frame_counter
TDATA[31:0]  = timestamp_ticks
TKEEP        = 8'hFF
TLAST        = 1 on the same beat
TVALID       = asserted while the event is pending

One FPGA event = one 64-bit beat = 8 bytes.

Vivado/Vitis assumptions
------------------------
DMA0: XPAR_AXIDMA_0_DEVICE_ID
DMA1: XPAR_AXIDMA_1_DEVICE_ID

AXI_GPIO_0: push buttons
AXI_GPIO_1: enable output to white/snow

If the regenerated xparameters.h uses different instance IDs,
update dma_work.h and measurement.h.

Kafka
-----
Edit KAFKA_BROKER_IP in main.c.

Default:
topic       = zynq-events
client id   = cora-z7
broker port = 9092
acks        = 1
pipeline    = 4
events/run  = 5000

Payload sent to Kafka
---------------------
8 bytes total:

bytes 0..3 : frame_counter, big-endian
bytes 4..7 : fpga_timestamp_ticks, big-endian

Metrics
-------
FPGA:
  fpga_timestamp_ticks

PS:
  dma_start_us
  dma_done_us
  publish_us
  send_us
  ack_us

Derived:
  dma_latency_us     = dma_done_us - dma_start_us
  queue_latency_us   = send_us - publish_us
  send_ack_us        = ack_us - send_us
  publish_ack_us     = ack_us - publish_us

IMPORTANT:
fpga_timestamp_ticks and PS microseconds do not yet share a common
time origin. Do not calculate ack_us - fpga_timestamp_ticks until
a PL-to-PS clock mapping/synchronization method is implemented.

Clean-run warning
-----------------
The white/snow enable signal resets the custom generators, but it does
not necessarily reset or empty an AXI-Stream FIFO that already contains
events. If a previous run can leave buffered words in the FIFO, add a
FIFO reset or an explicit drain mechanism before using frame_counter=0
as proof of a clean run.

Measurement output
------------------
Per-event xil_printf calls are avoided during acquisition. The CSV is
printed only after generation stops and the Kafka queue/pipeline drains.

Before a 5000-event run, temporarily set DEFAULT_EVENTS_PER_RUN to 10
and verify that frame_counter is continuous and timestamp_ticks advances.
