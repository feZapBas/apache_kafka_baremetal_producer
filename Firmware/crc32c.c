#include "crc32c.h"

/*
 * CRC-32C (Castagnoli), reflected polynomial 0x82F63B78.
 * Kafka RecordBatch magic=2 uses CRC32C.
 */
uint32_t crc32c(const uint8_t *data, size_t length)
{
    uint32_t crc = 0xFFFFFFFFU;

    for (size_t i = 0; i < length; ++i) {
        crc ^= (uint32_t)data[i];

        for (unsigned bit = 0; bit < 8U; ++bit) {
            uint32_t mask = (uint32_t)-(int32_t)(crc & 1U);
            crc = (crc >> 1) ^ (0x82F63B78U & mask);
        }
    }

    return ~crc;
}
