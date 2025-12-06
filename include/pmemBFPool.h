#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <libpmem.h>
#include <libpmemobj.h>
#include <atomic>
#include <vector>
#include "pmemManager.h"
#include "node.h"
#pragma once

#define INDEX_POOL_LAYOUT_NAME "bf_pool"
#define NODE_POOL_SIZE ((60LL*1024*1024*1024))

using namespace std;

class PmemBFPool {
private:
    string fileName = "/mnt/pmem1/pmemBFPool";
    std::vector<BloomFilter*> pmemBFPool;
    int numNodes;
    std::atomic<int> currentIdx;
public:
    PmemBFPool(size_t numNodes) : numNodes(numNodes){
        root_obj *root = nullptr;
        init(root);
        currentIdx.store(0);
    }

    bool init(root_obj *root);

~PmemBFPool() {
        // Deallocate memory blocks
        for (BloomFilter* bf : pmemBFPool) {
            delete[] bf;
        }
    }

    size_t getCurrentIdx() {
        return currentIdx.load();
    }

    bool resetCurrentIdx(int newIdx) {
        currentIdx.store(newIdx);
        return true;
    }

    BloomFilter* at(size_t index) {
        if (index >= pmemBFPool.size()) {
            return nullptr;
        }
        return pmemBFPool[index];
    }
};
