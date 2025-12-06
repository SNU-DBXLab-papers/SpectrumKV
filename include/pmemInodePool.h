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

#define INDEX_POOL_LAYOUT_NAME "index_pool"
#define NODE_POOL_SIZE ((60LL*1024*1024*1024))

using namespace std;

class PmemInodePool {
private:
    string fileName = "/mnt/pmem1/pmemInodePool";
    std::vector<Inode*> pmemInodePool;
    int nodeSize;
    int numNodes;
    std::atomic<int> currentIdx;
public:
#if ENABLE_PMEM_STATS
    std::shared_mutex stats_mtx;
#endif
    PmemInodePool(size_t nodeSize, size_t numNodes) : nodeSize(nodeSize), numNodes(numNodes){
        root_obj *root = nullptr;
        init(root);
        currentIdx.store(0);
    }

    bool init(root_obj *root);

    ~PmemInodePool() {
        // Deallocate memory blocks
        for (Inode* node : pmemInodePool) {
            delete[] node;
        }
    }

    size_t getCurrentIdx() {
        return currentIdx.load();
    }

    bool resetCurrentIdx(int newIdx) {
        currentIdx.store(newIdx);
        return true;
    }

    Inode* getCurrentNode() {
        return pmemInodePool[currentIdx.load()];
    }

    Inode *getNextNode() {
        int idx = currentIdx.fetch_add(1);
        if (idx >= numNodes) {
            return nullptr;
        }
        return pmemInodePool[idx];
    }

    Inode * popNode() {
        if (pmemInodePool.empty()) {
            return nullptr;
        }

        Inode* inode = pmemInodePool.back();
        pmemInodePool.pop_back();
        return inode;
    }

    void push(Inode *inode) {
        pmemInodePool.push_back(inode);
    }

    Inode * at(size_t index) {
        if (index >= pmemInodePool.size()) {
            return nullptr;
        }
        return pmemInodePool[index];
    }

    bool extend(PMEMobjpool *pop, size_t extendNumNodes);

    void printStats(int level, long vnode_count);
};
