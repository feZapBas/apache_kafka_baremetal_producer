#include <stdio.h>
#include <stdint.h>

#include "xparameters.h"
#include "xstatus.h"

#include "platform.h"
#include "platform_config.h"

#include "netif/xadapter.h"

#include "lwip/init.h"
#include "lwip/tcp.h"
#include "lwip/etharp.h"

#if LWIP_IPV6 == 0
#include "lwip/ip4_addr.h"

#if LWIP_DHCP == 1
#include "lwip/dhcp.h"
#endif
#endif

#include "xil_printf.h"

#include "echo.h"
#include "dma_work.h"
#include "kafka_producer.h"
#include "measurement.h"
#include "time_utils.h"

#define KAFKA_BROKER_IP "10.10.10.1"

extern volatile int TcpFastTmrFlag;
extern volatile int TcpSlowTmrFlag;

void tcp_fasttmr(void);
void tcp_slowtmr(void);

#if LWIP_IPV6 == 0 && LWIP_DHCP == 1
extern volatile int dhcp_timoutcntr;
#endif

static struct netif server_netif;

/* platform_zynq.c references this symbol. */
struct netif *echo_netif = &server_netif;

static kafka_producer_t kafka;
static measurement_t measurement;

#if LWIP_IPV6 == 0
static void print_ip(const char *label,
                     const ip4_addr_t *ip)
{
    xil_printf(
        "%s%d.%d.%d.%d\r\n",
        label,
        ip4_addr1(ip),
        ip4_addr2(ip),
        ip4_addr3(ip),
        ip4_addr4(ip)
    );
}

static void print_ip_settings(const ip4_addr_t *ip,
                              const ip4_addr_t *mask,
                              const ip4_addr_t *gateway)
{
    print_ip("Board IP : ", ip);
    print_ip("Netmask  : ", mask);
    print_ip("Gateway  : ", gateway);
}
#endif

int main(void)
{
    unsigned char mac_address[] = {
        0x00, 0x0A, 0x35,
        0x00, 0x01, 0x02
    };

#if LWIP_IPV6 == 0
    ip_addr_t ipaddr;
    ip_addr_t netmask;
    ip_addr_t gateway;
#endif

    init_platform();

    xil_printf(
        "\r\n"
        "============================================\r\n"
        " Bare-metal Kafka timestamp measurement\r\n"
        " AXI4-Stream width : 64 bits\r\n"
        " Event             : frame + timestamp\r\n"
        " Event size        : 8 bytes\r\n"
        "============================================\r\n"
    );

    lwip_init();

#if LWIP_IPV6 == 0
#if LWIP_DHCP == 1
    ipaddr.addr = 0U;
    netmask.addr = 0U;
    gateway.addr = 0U;
#else
    IP4_ADDR(&ipaddr, 10, 10, 10, 2);
    IP4_ADDR(&netmask, 255, 255, 255, 0);
    IP4_ADDR(&gateway, 10, 10, 10, 1);
#endif
#endif

#if LWIP_IPV6 == 0
    if (!xemac_add(
            echo_netif,
            &ipaddr,
            &netmask,
            &gateway,
            mac_address,
            PLATFORM_EMAC_BASEADDR)) {

        xil_printf("ERROR: xemac_add failed\r\n");
        cleanup_platform();
        return -1;
    }
#else
    if (!xemac_add(
            echo_netif,
            NULL,
            NULL,
            NULL,
            mac_address,
            PLATFORM_EMAC_BASEADDR)) {

        xil_printf("ERROR: xemac_add failed\r\n");
        cleanup_platform();
        return -1;
    }
#endif

    netif_set_default(echo_netif);
    netif_set_up(echo_netif);

#if LWIP_IPV6 == 0
    print_ip_settings(
        netif_ip4_addr(echo_netif),
        netif_ip4_netmask(echo_netif),
        netif_ip4_gw(echo_netif)
    );
#endif

#ifndef SDT
    platform_enable_interrupts();
#endif

#if LWIP_IPV6 == 0
    err_t arp_status =
        etharp_request(
            echo_netif,
            netif_ip4_gw(echo_netif)
        );

    xil_printf(
        "Forced ARP request status=%d\r\n",
        (int)arp_status
    );
#endif

    start_application();

    if (dma_initialization() != XST_SUCCESS) {
        xil_printf("ERROR: DMA initialization failed\r\n");
        cleanup_platform();
        return -1;
    }

    if (measurement_init(&measurement) != XST_SUCCESS) {
        xil_printf("ERROR: measurement initialization failed\r\n");
        cleanup_platform();
        return -1;
    }

    kafka_producer_init(
        &kafka,
        KAFKA_BROKER_IP
    );

    kafka_producer_connect(&kafka);

    xil_printf(
        "\r\n"
        "Controls:\r\n"
        "  BTN0 -> DMA0 measurement\r\n"
        "  BTN1 -> DMA1 measurement\r\n"
        "\r\n"
    );

    uint32_t last_retry_us = 0U;

    while (1) {
        /* Always service Ethernet first. */
        xemacif_input(echo_netif);

        if (TcpFastTmrFlag) {
            tcp_fasttmr();
            TcpFastTmrFlag = 0;
        }

        if (TcpSlowTmrFlag) {
            tcp_slowtmr();
            TcpSlowTmrFlag = 0;
        }

        if (kafka_get_state(&kafka) == KAFKA_ERROR) {
            uint32_t now = ps_time_us();

            if ((uint32_t)(now - last_retry_us) >= 2000000U) {
                xil_printf("Kafka reconnect...\r\n");
                kafka_producer_connect(&kafka);
                last_retry_us = now;
            }
        }

        measurement_tick(
            &measurement,
            &kafka
        );
    }

    cleanup_platform();
    return 0;
}
