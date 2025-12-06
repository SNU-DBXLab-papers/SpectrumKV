#include "pmemVnodePool.h"
#define VALUEPOOL 0
using namespace std;

int PmemVnodePool::init(root_obj *root) {
    size_t vp_size = 10UL * 1024UL * 1024UL * 1024UL; 
    bool isCreate;
    bool ret = PmemManager::createOrOpenPool(VALUEPOOL, fileName.c_str(), vp_size, (void **)&root, isCreate);
    if (!ret) {
        std::cout << "Failed to create or open pool: " << fileName << std::endl;
        return -1;
    } 

    // To allocate the vnode pool. 1. allocate memory. 2. cast into vodes 3. pot them into vector.
    PMEMobjpool *pop = (PMEMobjpool *)PmemManager::getPoolStartAddress(VALUEPOOL);
    if(isCreate) {
        int ret_val = pmemobj_alloc(pop, &root->ptr[0], nodeSize * MAX_NODES, 0, NULL, NULL);
        if (ret_val) {
            std::cout << "Failed to allocate memory for root->ptr[0]" << std::endl;
            return -1;
        }
        void *vnodePool = pmemobj_direct(root->ptr[0]);
        void *currentPoolAddr = vnodePool;
        for(int i = 0; i < numNodes; i++) {
            Vnode *vnode = (Vnode *) new (currentPoolAddr) Vnode(i);
            pmemVnodePool.push_back(vnode);
            currentPoolAddr = static_cast<char *>(currentPoolAddr) + nodeSize;
        }
        PmemManager::flushToNVM(0, (char *)vnodePool, nodeSize * numNodes);
        return 0;
    }else {
        void *vnodePool = pmemobj_direct(root->ptr[0]);
        for(int i = 0; i < numNodes; i++) {
            Vnode *vnode = (Vnode *)vnodePool;
            pmemVnodePool.push_back(vnode);
            vnodePool = static_cast<char *>(vnodePool) + nodeSize;
        }
        Vnode *vnode = pmemVnodePool[numNodes - 1];
        return vnode->hdr.next;
    }
}

bool PmemVnodePool::extend(PMEMobjpool *pop, size_t extendNumNodes) {
    if (this->numNodes + extendNumNodes > MAX_NODES) {
        std::cout << "Exceeding the maximum number of nodes" << std::endl;
        exit(-1);
    }
    PMEMoid root = pmemobj_root(pop, sizeof(PMEMoid));
    root_obj *rootObj = (root_obj *)pmemobj_direct(root);
    void *vnodePool = pmemobj_direct(rootObj->ptr[0]);
    void *currentPoolAddr = static_cast<char *>(vnodePool) + this->numNodes * nodeSize;
    for (size_t i = this->numNodes; i < extendNumNodes; ++i) {
        Vnode *vnode = (Vnode *) new (currentPoolAddr) Vnode(i);
        pmemVnodePool.push_back(vnode);
        currentPoolAddr = static_cast<char *>(currentPoolAddr) + nodeSize;
    }
    return true;
}
