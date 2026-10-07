/*
 * Copyright (C) 2009 - 2019 Xilinx, Inc.
 * All rights reserved.
 *
 * TCP echo server + envío de frames DMA por lwIP.
 */

#include <stdio.h>
#include <string.h>

#include "lwip/err.h"
#include "lwip/tcp.h"
#include "lwip/netif.h"

#include "xil_types.h"
#include "xil_printf.h"

#include "kafka_producer.h"
#include "measurement.h"
#include "echo.h"

/*
 * Si DMA_TRANSFER_SIZE ya está definido en otro .h, este bloque no lo pisa.
 * Aquí se asume que representa 64 palabras de 32 bits.
 */
#ifndef DMA_TRANSFER_SIZE
#define DMA_TRANSFER_SIZE    64U
#endif

#define TCP_SERVER_PORT      7U
#define TCP_CHUNK            1024U

#define FRAME_HEADER_BYTES   8U
#define FRAME_PAYLOAD_BYTES  (DMA_TRANSFER_SIZE * sizeof(u32))
#define FRAME_TOTAL_BYTES    (FRAME_HEADER_BYTES + FRAME_PAYLOAD_BYTES)

/* Instancias globales */
kafka_producer_t g_kafka;
//measurement_t g_meas;
struct netif *netif_ptr;
volatile uint8_t g_acq_running = 0;

/*
 * Variables TCP/frame.
 * Si alguna de estas ya existe en otro archivo, deja aquí solo el extern
 * correspondiente para evitar definición duplicada.
 */
static struct tcp_pcb *client_pcb = NULL;

static u8_t trigger_buf[FRAME_TOTAL_BYTES];
static u8_t *pending_ptr = NULL;
static u16_t pending_len = 0;
static volatile u8_t tcp_busy = 0;

volatile u8_t dma_done = 0;
static u16_t ctr_T1 = 0;

/* Prototipos locales */
static err_t sent_callback(void *arg, struct tcp_pcb *tpcb, u16_t len);
static err_t push_data(struct tcp_pcb *tpcb, u16_t bytes_to_send, u8_t *write_head);
static err_t push_pending_data(struct tcp_pcb *tpcb);

err_t recv_callback(void *arg, struct tcp_pcb *tpcb, struct pbuf *p, err_t err);
err_t accept_callback(void *arg, struct tcp_pcb *newpcb, err_t err);
/* echo.c — agregar junto a las otras variables globales */

/* Buffer para datos DMA listos para publicar por Kafka.
 * Contiene datos de DMA0 (ANDESPix) + DMA1 (TriggerUMD)
 * 256 words × 4 bytes × 2 canales = 2 KB
 */
u8 test_buf[65536];   /* tamaño original del extern */
int transfer_data(void);
int start_application(void);
void print_app_header(void);
int generate_frame(const u32 *buffer);

int generate_frame(const u32 *buffer)
{
    if (buffer == NULL) {
        xil_printf("generate_frame: buffer NULL\r\n");
        return 0;
    }

    if (client_pcb == NULL) {
        xil_printf("generate_frame: no TCP client connected\r\n");
        return 0;
    }

    /*
     * Evita pisar trigger_buf mientras todavía quedan bytes pendientes
     * de un frame anterior.
     */
    if (pending_len != 0U || tcp_busy != 0U) {
        xil_printf("generate_frame: TCP busy, frame skipped\r\n");
        return 0;
    }

    /*
     * Header de 8 bytes:
     * [0..1] contador
     * [2..6] texto "andes"
     * [7]    reservado
     * [8..]  payload DMA
     */
    trigger_buf[0] = (u8_t)((ctr_T1 >> 8) & 0xFFU);
    trigger_buf[1] = (u8_t)(ctr_T1 & 0xFFU);
    memcpy(&trigger_buf[2], "andes", 5);
    trigger_buf[7] = 0U;

    memcpy(&trigger_buf[FRAME_HEADER_BYTES], buffer, FRAME_PAYLOAD_BYTES);

    ctr_T1++;

    if (push_data(client_pcb, FRAME_TOTAL_BYTES, trigger_buf) != ERR_OK) {
        return 0;
    }

    dma_done = 0U;
    return 1;
}

static err_t push_data(struct tcp_pcb *tpcb, u16_t bytes_to_send, u8_t *write_head)
{
    if (tpcb == NULL || write_head == NULL || bytes_to_send == 0U) {
        return ERR_ARG;
    }

    pending_ptr = write_head;
    pending_len = bytes_to_send;
    tcp_busy = 0U;

    return push_pending_data(tpcb);
}

static err_t push_pending_data(struct tcp_pcb *tpcb)
{
    err_t status = ERR_OK;

    if (tpcb == NULL) {
        return ERR_ARG;
    }

    while (pending_len > 0U) {
        u16_t sndbuf = tcp_sndbuf(tpcb);

        if (sndbuf == 0U) {
            tcp_busy = 1U;
            break;
        }

        u16_t packet_size = pending_len;

        if (packet_size > sndbuf) {
            packet_size = sndbuf;
        }

        if (packet_size > TCP_CHUNK) {
            packet_size = TCP_CHUNK;
        }

        status = tcp_write(tpcb,
                           pending_ptr,
                           packet_size,
                           TCP_WRITE_FLAG_COPY);

        if (status == ERR_MEM) {
            tcp_busy = 1U;
            break;
        }

        if (status != ERR_OK) {
            xil_printf("tcp_write error: %d\r\n", status);
            tcp_busy = 1U;
            return status;
        }

        pending_ptr += packet_size;
        pending_len -= packet_size;
    }

    status = tcp_output(tpcb);
    if (status != ERR_OK) {
        xil_printf("tcp_output error: %d\r\n", status);
        return status;
    }

    if (pending_len == 0U) {
        pending_ptr = NULL;
        tcp_busy = 0U;
    }

    return ERR_OK;
}

static err_t sent_callback(void *arg, struct tcp_pcb *tpcb, u16_t len)
{
    (void)arg;
    (void)len;

    if (pending_len > 0U) {
        return push_pending_data(tpcb);
    }

    tcp_busy = 0U;
    return ERR_OK;
}

int transfer_data(void)
{
    return 0;
}

err_t accept_callback(void *arg, struct tcp_pcb *newpcb, err_t err)
{
    static int connection = 1;

    (void)arg;

    if (err != ERR_OK || newpcb == NULL) {
        return ERR_VAL;
    }

    client_pcb = newpcb;

    tcp_recv(newpcb, recv_callback);
    tcp_sent(newpcb, sent_callback);

    tcp_arg(newpcb, (void *)(UINTPTR)connection);
    connection++;

    xil_printf("TCP client connected\r\n");

    return ERR_OK;
}

void print_app_header(void)
{
    xil_printf("\r\n\r\n--- Kafka Baremetal Producer ---\r\n");
    xil_printf("Topic : %s\r\n", KAFKA_TOPIC);
    xil_printf("Port  : %d\r\n", KAFKA_BROKER_PORT);
}

int start_application(void)
{
    struct tcp_pcb *pcb;
    err_t err;

    pcb = tcp_new_ip_type(IPADDR_TYPE_ANY);
    if (pcb == NULL) {
        xil_printf("Error creating PCB. Out of Memory\r\n");
        return -1;
    }

    err = tcp_bind(pcb, IP_ANY_TYPE, TCP_SERVER_PORT);
    if (err != ERR_OK) {
        xil_printf("Unable to bind to port %d: err = %d\r\n",
                   TCP_SERVER_PORT,
                   err);
        tcp_close(pcb);
        return -2;
    }

    tcp_arg(pcb, NULL);

    pcb = tcp_listen(pcb);
    if (pcb == NULL) {
        xil_printf("Out of memory while tcp_listen\r\n");
        return -3;
    }

    tcp_accept(pcb, accept_callback);

    xil_printf("TCP echo server started @ port %d\r\n", TCP_SERVER_PORT);

    return 0;
}

err_t recv_callback(void *arg, struct tcp_pcb *tpcb, struct pbuf *p, err_t err)
{
    (void)arg;

    if (err != ERR_OK) {
        if (p != NULL) {
            pbuf_free(p);
        }
        return err;
    }

    if (p == NULL) {
        if (client_pcb == tpcb) {
            client_pcb = NULL;
            pending_ptr = NULL;
            pending_len = 0U;
            tcp_busy = 0U;
        }

        tcp_recv(tpcb, NULL);
        tcp_sent(tpcb, NULL);
        tcp_close(tpcb);

        xil_printf("TCP client disconnected\r\n");
        return ERR_OK;
    }

    tcp_recved(tpcb, p->tot_len);

    /*
     * Echo simple del primer pbuf.
     * Si esperas paquetes encadenados, conviene recorrer la cadena pbuf.
     */
    if (tcp_sndbuf(tpcb) >= p->len) {
        err = tcp_write(tpcb, p->payload, p->len, TCP_WRITE_FLAG_COPY);
        if (err == ERR_OK) {
            tcp_output(tpcb);
        }
    } else {
        xil_printf("no space in tcp_sndbuf\r\n");
    }

    pbuf_free(p);

    return ERR_OK;
}
