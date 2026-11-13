#ifndef HASH_H
#define HASH_H

#include <stdint.h>

void MurmurHash3_x86_32 ( const void * key, int len, uint32_t seed, void * out );

#endif