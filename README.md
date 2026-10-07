# Bare-Metal Apache Kafka Producer for Zynq-7000

Bare-metal implementation of an Apache Kafka producer for FPGA-based data acquisition on a Xilinx Zynq-7000 SoC.

The project implements the Kafka Wire Protocol directly on the Zynq Processing System (PS), using the lwIP raw TCP/IP API and without Linux, an RTOS, or an intermediate gateway computer.

The implementation was developed and evaluated on a Digilent Cora Z7-07S board.

## Overview

The data path is:

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
      |  Bare-metal C application
      |  Kafka Wire Protocol
      |  lwIP raw TCP API
      v
Gigabit Ethernet
      |
      v
Apache Kafka broker
```

Each FPGA event consists of one 64-bit AXI4-Stream word:

```text
TDATA[63:32] = frame counter
TDATA[31:0]  = FPGA timestamp
TKEEP        = 0xFF
TLAST        = 1
```

The corresponding Kafka application payload is therefore 8 bytes.

The FPGA timestamp is retained in raw counter units. FPGA and processor timestamps are not assumed to share a common epoch.

## Kafka producer

The producer implements the Kafka protocol directly in bare-metal C.

Main implementation characteristics:

- Apache Kafka Wire Protocol
- `ProduceRequest` version 3
- `acks=1`
- Kafka Record Batch serialization
- CRC-32C calculation
- big-endian fixed-width field serialization
- variable-length integer encoding
- correlation-ID-based response matching
- static memory allocation
- lwIP raw callback API
- cooperative, non-blocking execution
- configurable number of concurrent Kafka requests
- software queue for back-pressure handling

The pipeline depth is selected at compile time through:

```c
KAFKA_PIPELINE_DEPTH
```

The configurations evaluated in the accompanying measurement campaign are:

```text
D = 1, 2, 4, 8
```

The software queue depth was fixed to:

```text
Q = 8
```

Because `D` changes while `Q` remains fixed, the experiment changes both request concurrency and total producer state. It should therefore be interpreted as a request-concurrency sweep rather than a constant-buffer-capacity experiment.

## FPGA / PS interface

Two independent event sources are connected to the Processing System through separate AXI DMA paths.

Each DMA acquisition transfers one 64-bit event.

For every acknowledged event, the measurement firmware records:

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

`send_us` is recorded after a successful `tcp_write()` call. It therefore represents local acceptance by lwIP and does not represent physical Ethernet transmission.

`ack_us` is recorded when the Kafka `ProduceResponse` is processed by the receive callback.

## Kafka topics

Two Kafka topics are used:

```text
zynq-events
zynq-metrics
```

`zynq-events` contains the measured 8-byte event payloads.

Measurement records are stored in PS memory while the event stream is running. After all events in a run have been acknowledged, the records are exported to `zynq-metrics`.

This prevents metric-export traffic from being included in the timed event-delivery phase.

## Experimental configuration

The measurement campaign used:

| Parameter | Value |
|---|---:|
| Platform | Digilent Cora Z7-07S |
| SoC | Xilinx Zynq-7000 XC7Z007S |
| Network | Gigabit Ethernet |
| Kafka acknowledgement mode | `acks=1` |
| Kafka partitions | 1 |
| Replication factor | 1 |
| Events per run | 5000 |
| Runs per pipeline depth | 20 |
| Runs per source and depth | 10 |
| DMA sources | 2 |
| Pipeline depths | 1, 2, 4, 8 |
| Total runs | 80 |
| Total event metric records | 400,000 |

The two DMA sources were alternated between runs.

## Dataset integrity

The measurement dataset was checked on a per-run basis.

For all 80 runs:

- 5000 metric records were present;
- the frame sequence started at 0;
- the frame sequence ended at 4999;
- no duplicate frames were found;
- no missing frames were found;
- no sequence discontinuities were found;
- no Kafka errors were recorded;
- the reported latency fields matched the corresponding timestamp differences.

These checks apply to the producer-exported metric records. They are not an independent host-side decode of the binary `zynq-events` stream.

## Analysis

The statistical analysis is implemented in:

```text
data_analisis.py
```

The script expects the raw metric files:

```text
pipeline1_metrics_raw.txt
pipeline2_metrics_raw.txt
pipeline4_metrics_raw.txt
pipeline8_metrics_raw.txt
```

Run:

```bash
python3 data_analisis.py
```

or specify input and output directories:

```bash
python3 data_analisis.py \
    --input-dir . \
    --results-dir results
```

### Python dependencies

```text
numpy
pandas
scipy
matplotlib
```

They can be installed with:

```bash
python3 -m pip install numpy pandas scipy matplotlib
```

The analysis treats each 5000-event run as the independent experimental unit.

It performs:

- run-integrity validation;
- operational and steady throughput calculation;
- DMA, queue, send-to-acknowledgement, and publish-to-acknowledgement latency analysis;
- bootstrap 95% confidence intervals;
- Shapiro-Wilk tests;
- Levene tests;
- Kruskal-Wallis tests;
- pairwise Mann-Whitney U tests;
- Holm correction for multiple comparisons;
- effect-size calculation;
- publication-figure generation.

The analysis script can also reuse an existing `run_summary.csv` when the raw metric files are not available.

## Reproducing the Kafka broker

The experimental setup used a Kafka broker running in Docker on the host workstation.

The Zynq board and host were configured as:

```text
Host: 10.10.10.1
Zynq: 10.10.10.2
Kafka port: 9092
```

The measured event topic was configured with:

```text
partitions = 1
replication.factor = 1
acks = 1
```

The exact Docker/Kafka setup used for an experiment should be recorded together with the corresponding dataset.

## Vivado design

The programmable-logic design can be reconstructed from the exported Vivado Tcl files included in this repository.

The design was implemented using Vivado 2024.2.

The implemented PL clock used in the measurement design has:

```text
Period    = 7.692 ns
Frequency = 130.005 MHz
```

Post-implementation timing analysis reported:

```text
Setup WNS = +0.982 ns
Setup TNS = 0 ns

Hold WHS  = +0.018 ns
Hold THS  = 0 ns
```

Vivado reported no failing timing endpoints for the specified timing constraints.

The reported frequency is the implemented and constrained operating clock. It is not presented as a measured maximum operating frequency.

## Resource utilization

The complete implemented design uses approximately:

| Resource | Utilization |
|---|---:|
| LUT | 22% |
| LUTRAM | 3% |
| FF | 17% |
| BRAM | 24% |
| I/O | 2% |
| BUFG | 3% |

The two AXI DMA blocks account for a substantial fraction of the PL resources. Each DMA instance uses approximately:

```text
1007 LUT
1570 FF
4 RAMB36
1 RAMB18
```

The Kafka producer itself executes in the Processing System and therefore does not correspond to a replicated PL datapath.

## Power estimate

Vivado estimated:

```text
Total on-chip power : 1.54 W
Dynamic power       : 1.422 W
Static power        : 0.119 W
```

The Vivado power report classified the estimate confidence as `Low`.

These values are tool estimates and should not be interpreted as externally measured board power.

## Measurement scope

The reported measurements use finite runs of 5000 events.

They do not characterize long-duration saturation behavior.

The FPGA and PS timestamp domains are kept separate because a common time origin has not been established.

The interval between local `tcp_write()` acceptance and Kafka acknowledgement includes several components that are not independently timestamped:

```text
TCP transmission
network transport
broker processing
Kafka response transmission
lwIP receive processing
```

The current instrumentation therefore does not assign this interval to any individual mechanism.

## Related publication

This repository accompanies the work:

> **Gateway-Free Kafka Streaming from FPGA-Based Detectors:  
> A Bare-Metal Pipelined Producer on Zynq-7000**

Publication information will be added after publication.

## Authors

**Fernanda Zapata Bascuñán**  
Universidad Nacional de la Defensa
Email: fzapata541@alumnos.iua.edu.ar


## License

See the [`LICENSE`](LICENSE) file included in this repository.
