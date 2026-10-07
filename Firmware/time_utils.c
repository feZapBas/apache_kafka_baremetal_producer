#include "time_utils.h"

#include "xtime_l.h"

uint32_t ps_time_us(void)
{
    XTime ticks;
    XTime_GetTime(&ticks);

    uint64_t seconds =
        (uint64_t)ticks / (uint64_t)COUNTS_PER_SECOND;

    uint64_t remainder =
        (uint64_t)ticks % (uint64_t)COUNTS_PER_SECOND;

    uint64_t microseconds =
        seconds * 1000000ULL +
        (remainder * 1000000ULL) /
        (uint64_t)COUNTS_PER_SECOND;

    return (uint32_t)microseconds;
}
