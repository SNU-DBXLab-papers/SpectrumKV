#include "pmemBFPool.h"
#define BFPOOL 4
using namespace std;

bool PmemBFPool::init(root_obj *root) {
    size_t bp_size = 10UL * 1024UL * 1024UL * 1024UL; 
    bool isCreate;
    bool ret = PmemManager::createOrOpenPool(BFPOOL, fileName.c_str(), bp_size, (void **)&root, isCreate);
    if (!ret) {
        std::cout << "Failed to create or open pool: " << fileName << std::endl;
        return false;
    }

    // To allocate the vnode pool. 1. allocate memory. 2. cast into vodes 3. pot them into vector.
    PMEMobjpool *pop = (PMEMobjpool *)PmemManager::getPoolStartAddress(BFPOOL);
    if(isCreate) {
        int ret_val = pmemobj_alloc(pop, &root->ptr[0], sizeof(BloomFilter) * numNodes, 0, NULL, NULL);
        if (ret_val) {
            std::cout << "Failed to allocate memory for root->ptr[0]" << std::endl;
            return false;
        }
        void *BFPool = pmemobj_direct(root->ptr[0]);
        void *currentPoolAddr = BFPool;
        for(int i = 0; i < numNodes; i++) {
            BloomFilter *bf = (BloomFilter *) new (currentPoolAddr) BloomFilter();
            pmemBFPool.push_back(bf);
            currentPoolAddr = static_cast<char *>(currentPoolAddr) + sizeof(BloomFilter);
        }
        PmemManager::flushToNVM(4, (char *)BFPool, sizeof(BloomFilter) * numNodes);
    }else {
        void *BFPool = pmemobj_direct(root->ptr[0]);
        for(int i = 0; i < numNodes; i++) {
            BloomFilter *bf = (BloomFilter *)BFPool;
            pmemBFPool.push_back(bf);
            BFPool = static_cast<char *>(BFPool) + sizeof(BloomFilter);
        }
    }
    return true;    
}

