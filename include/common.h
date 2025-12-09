#include <cstdint>
#include <cassert>
#pragma once
//typedef int32_t Key_t; // 8 bytes
//typedef int32_t Val_t; // 8 bytes

typedef uint64_t Key_t; // 8 bytes
typedef uint64_t Val_t; // 8 bytes

const int MAX_LEVEL = 20;

const double SEARCH_STABILITY_COEFFICIENT_BY_LEVEL[MAX_LEVEL] = {
    3.0, 3.0, 
    2.0, 2.0, 1.5,
    1.5, 1.5, 1.5, 1.5, 1.5, 
    1.2, 1.2, 1.2, 1.2, 1.2,
    1.2, 1.2, 1.2, 1.2, 1.2
};

#define WORKERQUEUE_NUM 1
#define L1_CACHE_LINE_SIZE 64
#define L1_CACHE_LINE_MASK (~(L1_CACHE_LINE_SIZE - 1))
#define RECLAIM_THRESHOLD 4*1024*1024
#define RECLAIM_RETRY_THRESHOLD 500000000
#define PERSISTENT_THRESHOLD 1*1024*1024
#define MAX_NODES 600000
#define MAX_VALUE_NODES 15000000
#define MAX_REBALANCE_THREADS 1

#ifndef unlikely
#define unlikely(x) __builtin_expect(!!(x), 0)
#endif



//#define DBG 1
//#define LOG_DEBUG 1
