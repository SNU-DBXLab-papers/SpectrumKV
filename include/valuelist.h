#include <iostream>
#include "node.h"
#include "pmemVnodePool.h"
#include "common.h"


#pragma once
// Value list class on pmem

class ValueList {
public:
    PmemVnodePool *pmemVnodePool;
    Vnode *head;
    BloomFilter bf[MAX_VALUE_NODES]; // Bloom filters for each vnode
public:
    ValueList();
    bool insert(Key_t key, Val_t value);
    bool insert(Vnode* startNode, Vnode* vnode);
    bool append(Vnode* curVnode, Vnode* nextVnode);
    bool split(Vnode* &curVnode, Vnode* &nextVnode);
    bool update(Key_t key, Val_t value);
    bool remove(Key_t key);
    bool recovery();
    Vnode *getNext(Vnode *curNode);
    Vnode* getHeader()
    {
        return head;
    }
    int getKeyPos(Key_t key);
};
