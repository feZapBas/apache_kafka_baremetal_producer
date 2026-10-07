#ifndef CRC32C_H
#define CRC32C_H

#include <stdint.h>
#include <stddef.h>

uint32_t crc32c(const uint8_t *data, size_t length);

#endif /* CRC32C_H */
