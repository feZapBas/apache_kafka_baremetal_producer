# Bare-Metal Apache Kafka Producer for Zynq-7000

Bare-metal implementation of an Apache Kafka producer for FPGA-based data acquisition on a Xilinx Zynq-7000 SoC.

The project implements the Apache Kafka Wire Protocol directly on the Zynq Processing System (PS), using the lwIP raw TCP/IP API and without Linux, an RTOS, or an intermediate gateway computer.

The implementation was developed and experimentally evaluated on a Digilent Cora Z7-07S board.

---

## 1. System overview

The acquisition and publication path is:

```text
FPGA event source
      |
      v
AXI4-Stream
      |
      v
AXI DMA
      |
      v
Zynq Processing System
      |
      | Bare-metal C application
      | Kafka Wire Protocol
      | lwIP raw TCP API
      v
Gigabit Ethernet
      |
      v
Apache Kafka broker
```

Two independent Programmable Logic (PL) event sources are connected to the Processing System through separate AXI DMA paths.

The Kafka producer executes entirely in bare-metal software on the ARM Processing System.

No intermediate Linux gateway is used between the FPGA acquisition path and the Kafka broker.

---

## 2. Event format

Each FPGA event consists of one 64-bit AXI4-Stream transfer:

```text
TDATA[63:32] = frame_counter
TDATA[31:0]  = timestamp_ticks
TKEEP        = 0xFF
TLAST        = 1
```

The corresponding Kafka application payload is therefore exactly:

```text
8 bytes
```

The upper 32 bits contain a monotonically increasing frame counter.

The lower 32 bits contain the PL timestamp counter.

The PL timestamp is retained in raw counter units. PL and PS timestamps are not assumed to share a common epoch and are therefore not directly subtracted from each other.

---

## 3. Bare-metal Kafka producer

The producer implements the Kafka Wire Protocol directly in C.

The implementation includes:

- Kafka `ProduceRequest` version 3
- Kafka Record Batch format
- `acks=1`
- CRC-32C calculation
- fixed-width big-endian serialization
- signed variable-length integer encoding
- correlation-ID-based response matching
- asynchronous response parsing
- static request buffers
- static in-flight state
- static software queue
- lwIP raw callback API
- explicit finite-state machine
- configurable request pipeline depth
- back-pressure handling without blocking the network-processing loop

Dynamic memory allocation is not required in the publication path.

---

## 4. Request pipeline

The maximum number of concurrent Kafka `ProduceRequest` messages is configured at compile time through:

```c
KAFKA_PIPELINE_DEPTH
```

The evaluated configurations were:

```text
D = 1
D = 2
D = 4
D = 8
```

The software queue depth was fixed to:

```text
Q = 8
```

When all in-flight request slots are occupied, events may remain in the software queue until a Kafka `ProduceResponse` releases a slot.

Network processing continues while producer capacity is unavailable.

Because `Q` remains fixed while `D` changes, increasing the pipeline depth also increases the total producer state available for outstanding and queued events. The experiment should therefore be interpreted as a request-concurrency sweep rather than as a constant-total-buffer-capacity experiment.

---

## 5. DMA acquisition

Two independent AXI DMA channels are used.

```text
source 0 -> DMA 0
source 1 -> DMA 1
```

Each DMA acquisition transfers one 64-bit event.

The receive buffers are cache-aligned. Cache maintenance is performed around each DMA transaction.

After the DMA transfer completes, the application extracts:

```text
frame_counter
fpga_timestamp_ticks
```

before passing the event to the Kafka producer.

---

## 6. Producer-side instrumentation

The firmware records producer-side timing for each acknowledged event.

The exported metric format is:

```text
run
source
frame
fpga_timestamp_ticks
dma_start_us
dma_done_us
publish_us
send_us
ack_us
dma_latency_us
queue_latency_us
send_ack_us
publish_ack_us
kafka_error
```

The PS-domain intervals are defined as:

```text
DMA latency
    = dma_done_us - dma_start_us

Queue latency
    = send_us - publish_us

Send-to-ACK interval
    = ack_us - send_us

Publish-to-ACK interval
    = ack_us - publish_us
```

### `send_us`

`send_us` is recorded after a successful `tcp_write()` operation.

It therefore marks local acceptance of the request data by lwIP.

It does **not** represent the time at which the Ethernet frame physically leaves the interface.

### `ack_us`

`ack_us` is recorded when the corresponding Kafka `ProduceResponse` is processed by the lwIP receive callback.

Consequently, the `send_us -> ack_us` interval includes multiple components that are not individually timestamped:

```text
local TCP processing
Ethernet transmission
network transport
Kafka broker processing
Kafka response transmission
lwIP receive processing
```

The current instrumentation does not assign the measured interval to any individual component.

---

## 7. Kafka topics

Two Kafka topics are used:

```text
zynq-events
zynq-metrics
```

### `zynq-events`

Contains the measured 8-byte event payloads.

### `zynq-metrics`

Contains the measurement records generated by the instrumentation.

Measurement records are first retained in PS memory.

They are exported to `zynq-metrics` only after all measured event requests in a run have been acknowledged.

Metric-export traffic is therefore excluded from the timed event-delivery phase.

---

## 8. Experimental network configuration

The measurement setup used a direct Gigabit Ethernet connection between the Zynq board and the workstation running the Kafka broker.

```text
Host / Kafka broker : 10.10.10.1/24
Zynq board          : 10.10.10.2/24
```

The bare-metal producer connects to:

```text
10.10.10.1:9092
```

Kafka communication from the Docker host uses:

```text
localhost:29092
```

The tested topic configuration uses:

```text
partitions         = 1
replication factor = 1
acks               = 1
```

---

## 9. Kafka broker in Docker

The measurement campaign used Apache Kafka running in a Docker container on the host workstation.

Container name:

```text
kafka-zynq
```

Container image:

```text
apache/kafka
```

The repository contains Docker-related setup information under:

```text
docker/
```

The external Kafka listener used by the Zynq producer is:

```text
10.10.10.1:9092
```

The listener used from the host is:

```text
localhost:29092
```

### Checking the running broker

```bash
docker ps
```

### Listing Kafka topics

```bash
docker exec kafka-zynq \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 \
  --list
```

### Checking topic configuration

For the event topic:

```bash
docker exec kafka-zynq \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 \
  --describe \
  --topic zynq-events
```

For the metrics topic:

```bash
docker exec kafka-zynq \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 \
  --describe \
  --topic zynq-metrics
```

---

## 10. Exporting measurement data from Kafka

The raw metric stream can be exported after an acquisition campaign with:

```bash
docker exec kafka-zynq \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic zynq-metrics \
  --from-beginning \
  --timeout-ms 5000 \
  > pipeline1_metrics_raw.txt \
  2> pipeline1_consumer.log
```

The same procedure was used for the evaluated pipeline depths:

```text
pipeline1_metrics_raw.txt
pipeline2_metrics_raw.txt
pipeline4_metrics_raw.txt
pipeline8_metrics_raw.txt
```

Each pipeline dataset contains:

```text
20 runs
5000 events per run
100000 METRIC records
```

Repeated `METRIC_HEADER` records are expected because one header is exported for each run.

---

## 11. Cleaning the exported metric files

If a single CSV header is required, the raw output can be reduced with:

```bash
awk '
/^METRIC_HEADER/ { if (!header++) print; next }
/^METRIC,/ { print }
' pipeline1_metrics_raw.txt > pipeline1_metrics.csv
```

The raw files should be retained unchanged for reproducibility.

---

## 12. Measurement campaign

The complete campaign used:

| Parameter | Value |
|---|---:|
| Platform | Digilent Cora Z7-07S |
| SoC | Xilinx Zynq-7000 XC7Z007S |
| Link | Gigabit Ethernet |
| Kafka acknowledgement mode | `acks=1` |
| Topic partitions | 1 |
| Replication factor | 1 |
| Event payload | 8 bytes |
| Events per run | 5000 |
| Pipeline depths | 1, 2, 4, 8 |
| Runs per depth | 20 |
| Runs per source and depth | 10 |
| DMA sources | 2 |
| Total runs | 80 |
| Total metric records | 400,000 |

Source 1 and source 0 were alternated between successive runs.

---

## 13. Dataset integrity

Each run was independently checked before statistical analysis.

For all 80 runs:

```text
records              = 5000
first frame          = 0
last frame           = 4999
sequence errors      = 0
duplicate frames     = 0
missing frames       = 0
Kafka errors         = 0
latency mismatches   = 0
```

The complete campaign contains:

```text
400000 acknowledged event metric records
```

These integrity checks apply to the metric records exported by the producer.

They do not constitute an independent host-side decode of the binary `zynq-events` payload.

---

## 14. Data analysis

The measurement analysis is implemented in:

```text
data_analisis.py
```

The script expects:

```text
pipeline1_metrics_raw.txt
pipeline2_metrics_raw.txt
pipeline4_metrics_raw.txt
pipeline8_metrics_raw.txt
```

Run with:

```bash
python3 data_analisis.py
```

or:

```bash
python3 data_analisis.py \
  --input-dir . \
  --results-dir results
```

If the raw datasets are unavailable, the script can also reuse an existing:

```text
run_summary.csv
```

---

## 15. Python dependencies

The analysis script requires:

```text
numpy
pandas
scipy
matplotlib
```

Install them with:

```bash
python3 -m pip install numpy pandas scipy matplotlib
```

---

## 16. Statistical analysis

The independent experimental unit is one 5000-event run.

The script performs:

- parsing of the raw `METRIC` records
- per-run integrity checks
- 32-bit timestamp rollover handling
- steady acknowledgement-rate calculation
- operational throughput calculation
- DMA latency analysis
- queue latency analysis
- send-to-acknowledgement analysis
- publish-to-acknowledgement analysis
- run-level median calculation
- run-level P95 calculation
- run-level P99 calculation
- bootstrap 95% confidence intervals
- Shapiro-Wilk tests
- Levene variance tests
- Kruskal-Wallis tests across pipeline depths
- pairwise Mann-Whitney U tests
- Holm correction for multiple comparisons
- rank-biserial effect sizes
- Kruskal-Wallis epsilon-squared effect sizes
- publication-figure generation

Bootstrap analysis uses a deterministic random seed for reproducibility.

---

## 17. Main measured results

Mean operational throughput was approximately:

```text
D = 1 :  5.3 kevents/s
D = 2 :  9.4-9.6 kevents/s
D = 4 : 16.2 kevents/s
D = 8 : 16.8-17.5 kevents/s
```

The lowest measured median publish-to-acknowledgement intervals among the tested configurations occurred at `D = 4`:

```text
source 0 : 659.5 us
source 1 : 664.0 us
```

The corresponding P95 values were:

```text
source 0 : 827.1 us
source 1 : 809.0 us
```

At `D = 8`, mean throughput increased, while the median publish-to-acknowledgement interval increased to approximately:

```text
source 0 : 832 us
source 1 : 859 us
```

The experiment therefore reports throughput and latency separately rather than reducing both measurements to a single pipeline-depth criterion.

---

## 18. Vivado design

The programmable-logic subsystem was implemented with Vivado 2024.2.

The Block Design can be exported with:

```tcl
write_bd_tcl -force zynq_sys_bd.tcl
```

A complete Vivado project reconstruction script can also be generated with:

```tcl
write_project_tcl -force kafka_zynq_project.tcl
```

Vivado Tcl files are preferred over generated implementation directories for source control.

---

## 19. Implemented clock and timing

The constrained PL clock reported by Vivado is:

```text
Clock      : clk_fpga_0
Period     : 7.692 ns
Frequency  : 130.005 MHz
Jitter     : 0.231 ns
```

Post-implementation timing analysis reported:

```text
Setup WNS : +0.982 ns
Setup TNS : 0 ns

Hold WHS  : +0.018 ns
Hold THS  : 0 ns

WPWS      : +2.596 ns
TPWS      : 0 ns
```

Vivado reported no failing endpoints for the specified timing constraints.

The 130.005 MHz value is the implemented and constrained operating PL clock.

It is not presented as a measured maximum operating frequency.

---

## 20. PL resource utilization

The complete implemented design uses approximately:

| Resource | Utilization |
|---|---:|
| LUT | 22% |
| LUTRAM | 3% |
| FF | 17% |
| BRAM | 24% |
| I/O | 2% |
| BUFG | 3% |

The complete hierarchical design contains:

```text
3203 LUT
4758 FF
10 RAMB36
4 RAMB18
0 DSP
```

Each AXI DMA instance uses approximately:

```text
1007 LUT
1570 FF
4 RAMB36
1 RAMB18
```

The Kafka protocol implementation itself executes in the Processing System rather than as replicated programmable-logic hardware.

---

## 21. Vivado power estimate

Vivado estimated:

```text
Total on-chip power : 1.54 W
Dynamic power       : 1.422 W
Static power        : 0.119 W
PS7 dynamic power   : 1.384 W
```

The estimated junction temperature was:

```text
42.8 °C
```

The Vivado power report classified the estimate confidence as:

```text
Low
```

These values are tool estimates and are not external board-level power measurements.

---

## 22. Measurement scope

The experimental campaign uses finite 5000-event runs.

The measurements do not characterize long-duration saturation behavior.

The PL timestamp and the PS timing domain are retained separately because a common timestamp epoch has not been established.

The producer instrumentation records local `tcp_write()` acceptance rather than physical Ethernet transmission.

The experiment therefore measures producer-visible timing intervals and does not separately resolve:

```text
wire transmission time
network propagation
broker processing
response transmission
```

---

## 23. Repository contents

The repository contains the files required to reproduce the implementation and measurement analysis, including:

```text
Bare-metal C sources
Kafka producer implementation
Measurement instrumentation
Metrics collection
Vivado design sources / Tcl scripts
Docker / Kafka configuration
Raw measurement data
Processed statistical tables
Python analysis scripts
Publication figures
Vivado implementation reports
```

Generated Vivado implementation products do not need to be committed when they can be regenerated from the project sources and Tcl scripts.

---

## 24. Related publication

This repository accompanies the manuscript:

> **Gateway-Free Kafka Streaming from FPGA-Based Detectors:  
> A Bare-Metal Pipelined Producer on Zynq-7000**

Publication information will be added after publication.

---

## 25. Author

**Fernanda Zapata Bascuñán**
Universidad Nacional de la Defensa
Email: `fzapata541@alumnos.iua.edu.ar`

GitHub: [feZapBas](https://github.com/feZapBas)

---

## 26. License

See the [`LICENSE`](LICENSE) file included in this repository.
