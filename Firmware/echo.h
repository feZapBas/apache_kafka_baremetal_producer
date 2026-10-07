/*
 * echo.h
 *
 * Header para Kafka Baremetal Producer / TCP echo server.
 */

#ifndef ECHO_H
#define ECHO_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

#include "lwip/err.h"
#include "lwip/tcp.h"
#include "lwip/netif.h"

#include "kafka_producer.h"
#include "measurement.h"

/* Instancias globales definidas en echo.c */
extern kafka_producer_t g_kafka;
//extern measurement_t g_meas;
extern struct netif *netif_ptr;
extern volatile uint8_t g_acq_running;

/* Callbacks TCP */
err_t recv_callback(void *arg, struct tcp_pcb *tpcb, struct pbuf *p, err_t err);
err_t accept_callback(void *arg, struct tcp_pcb *newpcb, err_t err);

/* Funciones de aplicación */
int transfer_data(void);
int start_application(void);
void print_app_header(void);

#ifdef __cplusplus
}
#endif

#endif /* ECHO_H */
