#ifndef TIME_UTILS_H
#define TIME_UTILS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

/*
 * Monotonic Processing-System time in microseconds.
 *
 * The 32-bit result wraps approximately every 71.6 minutes.
 * For intervals shorter than 2^32 us, use unsigned subtraction:
 *
 *     uint32_t dt = end - start;
 */
uint32_t ps_time_us(void);

#ifdef __cplusplus
}
#endif

#endif /* TIME_UTILS_H */
