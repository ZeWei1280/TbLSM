/*
 * Copyright 2016, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * skiplist_buffer.h -- sorted list collection implementation
 */

#ifndef SKIPLIST_MAP_H
#define SKIPLIST_MAP_H

#include <libpmemobj.h>
#include <string>
#include "pmem/pmem_latency.h"

#ifndef SKIPLIST_MAP_TYPE_OFFSET
#define SKIPLIST_MAP_TYPE_OFFSET 2020
#endif

/* value 100bytes */
// #define NUM_OF_PRE_ALLOC_NODE 28300 // = Just pre-alloc
// #define NUM_OF_USE_ALLOC_NODE 28300 // = MAX_SKIPLIST_NODE_SIZE
// For YCSB
#define NUM_OF_PRE_ALLOC_NODE 58830 // = Just pre-alloc
#define NUM_OF_USE_ALLOC_NODE 58830 // = MAX_SKIPLIST_NODE_SIZE

#define PRE_ALLOC_KEY_SIZE 26 // 16
#define STRING_PADDING 0 // \0
#define NUM_OF_TAG_BYTES 8
#define VARINT_LENGTH 5

#define SKIPLIST_LEVELS_NUM 12

#define USE_BINARY_INSERTION 0
// #define USE_BINARY_INSERTION 1

#define LEVEL_11_POINT ( NUM_OF_USE_ALLOC_NODE / 2 )
#define LEVEL_10_POINT ( LEVEL_11_POINT / 2)
#define LEVEL_9_POINT ( LEVEL_10_POINT / 2)
#define LEVEL_8_POINT ( LEVEL_9_POINT / 2)
#define LEVEL_7_POINT ( LEVEL_8_POINT / 2)
#define LEVEL_6_POINT ( LEVEL_7_POINT / 2)
#define LEVEL_5_POINT ( LEVEL_6_POINT / 2)
#define LEVEL_4_POINT ( LEVEL_5_POINT / 2)
#define LEVEL_3_POINT ( LEVEL_4_POINT / 2)
#define LEVEL_2_POINT ( LEVEL_3_POINT / 2)
#define LEVEL_1_POINT ( LEVEL_2_POINT / 2)

namespace leveldb{
uint32_t GetKeyLengthFromBuffer(char* buf);
char* GetKeyFromBuffer(char* buf);
char* GetKeyAndLengthFromBuffer(char* buf, uint32_t* key_len);
char* GetValueFromBuffer(char* buf);
char* GetValueAndLengthFromBuffer(char* buf, uint32_t* value_len);

struct skiplist_map_node;
TOID_DECLARE(struct skiplist_map_node, SKIPLIST_MAP_TYPE_OFFSET + 0);

int skiplist_map_check(PMEMobjpool* pop, TOID(struct skiplist_map_node) map);
int skiplist_map_create(PMEMobjpool* pop, TOID(struct skiplist_map_node)* map,
	TOID(struct skiplist_map_node) current_node,
	int index, void* arg);
int skiplist_map_destroy(PMEMobjpool* pop, TOID(struct skiplist_map_node)* map);
int skiplist_map_insert(PMEMobjpool* pop, 
		TOID(struct skiplist_map_node) map, 
		TOID(struct skiplist_map_node)* current_node,
		char* key, char* buffer_ptr, int key_len, int index, uint16_t refTimes /*zewei*/);
int skiplist_map_insert_by_ptr(PMEMobjpool* pop, 
		TOID(struct skiplist_map_node) map, 
		TOID(struct skiplist_map_node)* current_node,
		char* buffer_ptr, int key_len, int index, uint16_t refTimes/*zewei*/);
int skiplist_map_insert_null_node(PMEMobjpool* pop, 
		TOID(struct skiplist_map_node) map, 
		TOID(struct skiplist_map_node)* current_node,
		int index);
int skiplist_map_remove(PMEMobjpool* pop,
		TOID(struct skiplist_map_node) map, char* key);
int skiplist_map_remove_free(PMEMobjpool* pop,
		TOID(struct skiplist_map_node) map, char* key);
int skiplist_map_clear(PMEMobjpool* pop, TOID(struct skiplist_map_node) map);
// [Deprecated]
// char* skiplist_map_get(PMEMobjpool* pop, TOID(struct skiplist_map_node) map,
// 		char* key);
PMEMoid* skiplist_map_get_OID(PMEMobjpool* pop,TOID(struct skiplist_map_node) map, char* key);
PMEMoid* skiplist_map_get_prev_OID(PMEMobjpool* pop, TOID(struct skiplist_map_node) map, char* key);
PMEMoid* skiplist_map_get_first_OID(PMEMobjpool* pop, TOID(struct skiplist_map_node) map);
PMEMoid* skiplist_map_get_last_OID(PMEMobjpool* pop, TOID(struct skiplist_map_node) map);
/*----------------*/
PMEMoid* test_skiplist_map_get_prev_OID(PMEMobjpool* pop, TOID(struct skiplist_map_node) map, char* key);
/*----------------*/


int skiplist_map_lookup(PMEMobjpool* pop, TOID(struct skiplist_map_node) map,
		char* key);
int skiplist_map_foreach(PMEMobjpool* pop, TOID(struct skiplist_map_node) map,
	int (*cb)(char* key, char* buffer_ptr, int key_len, void* arg), void* arg);
int skiplist_map_is_empty(PMEMobjpool* pop, TOID(struct skiplist_map_node) map);
} // namespace leveldb

#endif /* SKIPLIST_MAP_H */
