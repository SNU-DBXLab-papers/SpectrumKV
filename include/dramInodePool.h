#pragma once
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <libpmem.h>
#include <libpmemobj.h>
#include <vector>
#include <atomic>
#include "dramManager.h"
#include "common.h"
#include "node.h"


//dramInodePool size
#define NODE_POOL_SIZE ((60LL*1024*1024*1024))

class DramInodePool {
private:
    std::vector<Inode*> dramInodePool;
    int nodeSize;
    int numNodes;
    std::atomic<int> currentIdx;
public:
    DramInodePool(size_t nodeSize, size_t numNodes) : nodeSize(nodeSize), numNodes(numNodes){
        init();
        currentIdx.store(0);
    }

    bool init();

    ~DramInodePool() {
        // Deallocate memory blocks
        for (Inode* node : dramInodePool) {
            delete[] node;
        }
    }

    size_t getCurrentIdx() {
        return currentIdx.load();
    }

    void setCurrentIdx(size_t idx) {
        currentIdx.store(idx);
    }

    Inode* getCurrentNode() {
        return dramInodePool[currentIdx.load()];
    }

    Inode* getNextNode() {
        int idx = currentIdx.fetch_add(1);
        if (idx >= numNodes) {
            std::cout << "Exceeding the maximum number of nodes in dramInodePool, idx: " << idx << std::endl;
            return nullptr;
        }
        Inode *node = dramInodePool[idx];
        assert(node->getId() >= 0);
#ifdef DBG
        int id = node->getId();
        if (id == 35)
            cout << "this is node 35" << endl;
        cout << "allocate inode : " << id << endl;
#endif
        return node;
    }

    Inode* popNode() {
        if (dramInodePool.empty()) {
            return nullptr;
        }

        Inode* inode = dramInodePool.back();
        dramInodePool.pop_back();
        return inode;
    }

    void push(Inode *inode) {
        dramInodePool.push_back(inode);
    }

    Inode * at(size_t index) {
        if (index >= dramInodePool.size()) {
            std::cout << "invalid index " << index <<" beyond dramInodePool capacity" << std::endl;
            return nullptr;
        }
        return dramInodePool[index];
    }

    bool extend(void *indexPool, size_t extendNumNodes);

    int getPoolSize() {
        return dramInodePool.size();
    }
};