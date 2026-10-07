#include "dma_work.h"

#include "xil_cache.h"
#include "xil_printf.h"
#include "xstatus.h"
#include "sleep.h"

#include "time_utils.h"

#include <stdint.h>


/* ================================================================
 * DMA / GPIO instances
 * ================================================================ */

static XAxiDma dma0;
static XAxiDma dma1;

static XGpio enable_gpio;


/* ================================================================
 * Enable mapping
 *
 * Hardware convention:
 *
 *   enable[0] -> white -> DMA0
 *   enable[1] -> snow  -> DMA1
 *
 * ================================================================ */

#define DMA_ENABLE_NONE        0x00000000U
#define DMA_ENABLE_0_MASK      0x00000001U
#define DMA_ENABLE_1_MASK      0x00000002U


/* ================================================================
 * DMA buffers
 *
 * One AXI4-Stream event = one 64-bit beat.
 * ================================================================ */

static u64 dma0_buffer[1]
    __attribute__((aligned(DMA_BUFFER_ALIGN)));

static u64 dma1_buffer[1]
    __attribute__((aligned(DMA_BUFFER_ALIGN)));


/* ================================================================
 * DMA status helper
 * ================================================================ */

static void print_dma_status(
    XAxiDma *dma,
    const char *name
)
{
    u32 status =
        XAxiDma_ReadReg(
            dma->RegBase + XAXIDMA_RX_OFFSET,
            XAXIDMA_SR_OFFSET
        );

    xil_printf(
        "%s S2MM status = 0x%08x\r\n",
        name,
        (unsigned)status
    );
}


/* ================================================================
 * Stream enable control
 * ================================================================ */

void dma_stream_disable_all(void)
{
    XGpio_DiscreteWrite(
        &enable_gpio,
        ENABLE_GPIO_CHANNEL,
        DMA_ENABLE_NONE
    );
}


void dma_stream_select(uint8_t source)
{
    u32 enable_value =
        DMA_ENABLE_NONE;

    switch (source) {

    case DMA_SOURCE_0:

        /*
         * DMA0 -> white
         * enable[0] = 1
         * enable[1] = 0
         */
        enable_value =
            DMA_ENABLE_0_MASK;

        break;


    case DMA_SOURCE_1:

        /*
         * DMA1 -> snow
         * enable[0] = 0
         * enable[1] = 1
         */
        enable_value =
            DMA_ENABLE_1_MASK;

        break;


    default:

        enable_value =
            DMA_ENABLE_NONE;

        break;
    }

    XGpio_DiscreteWrite(
        &enable_gpio,
        ENABLE_GPIO_CHANNEL,
        enable_value
    );
}


/*
 * Kept for compatibility with previous code.
 *
 * It now means:
 *   enable = 0 -> both generators disabled
 *   enable = 1 -> both generators enabled
 *
 * Do NOT use this during measurements because we now
 * want independent stream control.
 */
void dma_stream_enable(int enable)
{
    XGpio_DiscreteWrite(
        &enable_gpio,
        ENABLE_GPIO_CHANNEL,
        enable ?
            (DMA_ENABLE_0_MASK | DMA_ENABLE_1_MASK) :
            DMA_ENABLE_NONE
    );
}


/*
 * Compatibility function.
 *
 * Resets both generators by forcing both enable signals low.
 *
 * Measurements should use dma_stream_disable_all()
 * and dma_stream_select() instead.
 */
void dma_stream_reset(void)
{
    dma_stream_disable_all();

    usleep(20);
}


/* ================================================================
 * DMA initialization helper
 * ================================================================ */

static int init_dma(
    XAxiDma *dma,
    u16 device_id,
    const char *name
)
{
    XAxiDma_Config *config =
        XAxiDma_LookupConfig(
            device_id
        );

    if (config == NULL) {

        xil_printf(
            "%s: LookupConfig failed\r\n",
            name
        );

        return XST_FAILURE;
    }


    int status =
        XAxiDma_CfgInitialize(
            dma,
            config
        );

    if (status != XST_SUCCESS) {

        xil_printf(
            "%s: CfgInitialize failed: %d\r\n",
            name,
            status
        );

        return XST_FAILURE;
    }


    /*
     * This application uses AXI DMA Simple Mode.
     */
    if (XAxiDma_HasSg(dma)) {

        xil_printf(
            "%s: SG mode enabled; Simple DMA expected\r\n",
            name
        );

        return XST_FAILURE;
    }


    /*
     * Polling mode: interrupts are not used.
     */
    XAxiDma_IntrDisable(
        dma,
        XAXIDMA_IRQ_ALL_MASK,
        XAXIDMA_DEVICE_TO_DMA
    );


    return XST_SUCCESS;
}


/* ================================================================
 * DMA recovery after timeout
 * ================================================================ */

static int reset_dma_after_error(
    XAxiDma *dma,
    const char *name
)
{
    XAxiDma_Reset(
        dma
    );


    uint32_t reset_start_us =
        ps_time_us();


    while (!XAxiDma_ResetIsDone(dma)) {

        if (
            (uint32_t)(
                ps_time_us() -
                reset_start_us
            ) >
            DMA_TIMEOUT_US
        ) {

            xil_printf(
                "%s: DMA reset timeout\r\n",
                name
            );

            return XST_FAILURE;
        }
    }


    /*
     * Restore polling configuration after reset.
     */
    XAxiDma_IntrDisable(
        dma,
        XAXIDMA_IRQ_ALL_MASK,
        XAXIDMA_DEVICE_TO_DMA
    );


    return XST_SUCCESS;
}


/* ================================================================
 * Receive one 64-bit event
 * ================================================================ */

static int receive_event(
    XAxiDma *dma,
    u64 *buffer,
    const char *name,
    uint8_t source
)
{
    UINTPTR address =
        (UINTPTR)buffer;


    /*
     * Destination memory is cacheable.
     *
     * Flush before S2MM so no dirty cache line can later
     * overwrite the data written by DMA.
     */
    Xil_DCacheFlushRange(
        address,
        DMA_TRANSFER_BYTES
    );


    /*
     * First arm S2MM.
     *
     * Important:
     * do NOT check XAxiDma_Busy() before SimpleTransfer().
     * A freshly initialized DMA can be HALTED while IDLE
     * is not asserted.
     */
    int status =
        XAxiDma_SimpleTransfer(
            dma,
            address,
            DMA_TRANSFER_BYTES,
            XAXIDMA_DEVICE_TO_DMA
        );


    if (status != XST_SUCCESS) {

        xil_printf(
            "%s: SimpleTransfer failed: %d\r\n",
            name,
            status
        );

        print_dma_status(
            dma,
            name
        );

        return XST_FAILURE;
    }


    /*
     * The DMA is now armed.
     *
     * Enable ONLY the source associated with this DMA.
     *
     * DMA0 -> white
     * DMA1 -> snow
     */
    dma_stream_select(
        source
    );


    uint32_t wait_start_us =
        ps_time_us();


    /*
     * Wait for one complete S2MM transfer.
     *
     * The HDL must provide one complete AXI frame,
     * including TLAST.
     */
    while (
        XAxiDma_Busy(
            dma,
            XAXIDMA_DEVICE_TO_DMA
        )
    ) {

        if (
            (uint32_t)(
                ps_time_us() -
                wait_start_us
            ) >
            DMA_TIMEOUT_US
        ) {

            xil_printf(
                "%s: DMA timeout\r\n",
                name
            );

            print_dma_status(
                dma,
                name
            );


            /*
             * Stop both FPGA generators immediately.
             */
            dma_stream_disable_all();


            /*
             * The timed-out S2MM transaction is still active.
             * Reset the DMA so the next measurement does not
             * fail with SimpleTransfer status = XST_FAILURE.
             */
            if (
                reset_dma_after_error(
                    dma,
                    name
                ) != XST_SUCCESS
            ) {

                return XST_FAILURE;
            }


            return XST_FAILURE;
        }
    }


    /*
     * DMA has written memory.
     * Remove stale CPU cache contents.
     */
    Xil_DCacheInvalidateRange(
        address,
        DMA_TRANSFER_BYTES
    );


    return XST_SUCCESS;
}


/* ================================================================
 * Public initialization
 * ================================================================ */

int dma_initialization(void)
{
    /* ------------------------------------------------------------
     * Enable GPIO
     * ------------------------------------------------------------ */

    XGpio_Config *gpio_config =
        XGpio_LookupConfig(
            ENABLE_GPIO_DEVICE_ID
        );


    if (gpio_config == NULL) {

        xil_printf(
            "Enable GPIO LookupConfig failed\r\n"
        );

        return XST_FAILURE;
    }


    int status =
        XGpio_CfgInitialize(
            &enable_gpio,
            gpio_config,
            gpio_config->BaseAddress
        );


    if (status != XST_SUCCESS) {

        xil_printf(
            "Enable GPIO initialization failed: %d\r\n",
            status
        );

        return XST_FAILURE;
    }


    /*
     * Both GPIO bits are outputs.
     */
    XGpio_SetDataDirection(
        &enable_gpio,
        ENABLE_GPIO_CHANNEL,
        0x00000000U
    );


    /*
     * Start with both event generators disabled.
     */
    dma_stream_disable_all();


    /* ------------------------------------------------------------
     * DMA0
     * ------------------------------------------------------------ */

    if (
        init_dma(
            &dma0,
            DMA0_DEVICE_ID,
            "DMA0"
        ) != XST_SUCCESS
    ) {

        return XST_FAILURE;
    }


    /* ------------------------------------------------------------
     * DMA1
     * ------------------------------------------------------------ */

    if (
        init_dma(
            &dma1,
            DMA1_DEVICE_ID,
            "DMA1"
        ) != XST_SUCCESS
    ) {

        return XST_FAILURE;
    }


    xil_printf(
        "DMA0 and DMA1 initialized\r\n"
    );

    xil_printf(
        "AXI event size: %u bytes\r\n",
        (unsigned)DMA_TRANSFER_BYTES
    );


    return XST_SUCCESS;
}

static int dma_reset_channel(
    XAxiDma *dma,
    const char *name
)
{
    XAxiDma_Reset(dma);

    uint32_t t0 = ps_time_us();

    while (!XAxiDma_ResetIsDone(dma)) {

        if ((uint32_t)(ps_time_us() - t0) > DMA_TIMEOUT_US) {
            xil_printf("%s: reset timeout\r\n", name);
            return XST_FAILURE;
        }
    }

    return XST_SUCCESS;
}
int dma_prepare_run(uint8_t source)
{
    /*
     * Hold both acquisition chains in reset.
     */
    dma_stream_disable_all();

    /*
     * Diagnostic margin.
     * This is outside the measurement interval.
     */
    usleep(20);

    if (source == DMA_SOURCE_0) {

        if (dma_reset_channel(&dma0, "DMA0") != XST_SUCCESS) {
            return XST_FAILURE;
        }

        dma0_buffer[0] = 0xDEADBEEFDEADBEEFULL;

        Xil_DCacheFlushRange(
            (UINTPTR)dma0_buffer,
            DMA_TRANSFER_BYTES
        );

    } else if (source == DMA_SOURCE_1) {

        if (dma_reset_channel(&dma1, "DMA1") != XST_SUCCESS) {
            return XST_FAILURE;
        }

        dma1_buffer[0] = 0xDEADBEEFDEADBEEFULL;

        Xil_DCacheFlushRange(
            (UINTPTR)dma1_buffer,
            DMA_TRANSFER_BYTES
        );

    } else {
        return XST_FAILURE;
    }

    /*
     * Keep reset asserted a little longer after DMA reset.
     */
    usleep(20);

    return XST_SUCCESS;
}
/* ================================================================
 * Read one FPGA event
 * ================================================================ */

int dma_read_event(
    uint8_t source,
    fpga_event_t *event
)
{
    if (event == NULL) {

        return XST_FAILURE;
    }


    u64 raw;


    switch (source) {

    /* ------------------------------------------------------------
     * DMA0 / white
     * ------------------------------------------------------------ */

    case DMA_SOURCE_0:

        if (
            receive_event(
                &dma0,
                dma0_buffer,
                "DMA0",
                DMA_SOURCE_0
            ) != XST_SUCCESS
        ) {

            return XST_FAILURE;
        }


        raw =
            dma0_buffer[0];

        break;


    /* ------------------------------------------------------------
     * DMA1 / snow
     * ------------------------------------------------------------ */

    case DMA_SOURCE_1:

        if (
            receive_event(
                &dma1,
                dma1_buffer,
                "DMA1",
                DMA_SOURCE_1
            ) != XST_SUCCESS
        ) {

            return XST_FAILURE;
        }


        raw =
            dma1_buffer[0];

        break;


    default:

        xil_printf(
            "Invalid DMA source: %u\r\n",
            (unsigned)source
        );

        return XST_FAILURE;
    }


    /*
     * HDL convention:
     *
     *   TDATA[63:32] = frame_counter
     *   TDATA[31:0]  = timestamp_ticks
     */
    event->frame_counter =
        (uint32_t)(
            raw >> 32
        );


    event->timestamp_ticks =
        (uint32_t)(
            raw &
            0xFFFFFFFFULL
        );


    return XST_SUCCESS;
}
