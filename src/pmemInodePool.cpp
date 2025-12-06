#include "pmemInodePool.h"
#define INDEXPOOL 1
using namespace std;

bool PmemInodePool::init(root_obj *root) {
    size_t vp_size = 10UL * 1024UL * 1024UL * 1024UL; 
    bool isCreate;
    bool ret = PmemManager::createOrOpenPool(INDEXPOOL, fileName.c_str(), vp_size, (void **)&root, isCreate);
    if (!ret) {
        std::cout << "Failed to create or open pool: " << fileName << std::endl;
        return false;
    }

    // To allocate the vnode pool. 1. allocate memory. 2. cast into vodes 3. pot them into vector.
    PMEMobjpool *pop = (PMEMobjpool *)PmemManager::getPoolStartAddress(INDEXPOOL);
    nodeSize = sizeof(Inode);
    if(isCreate) {
        int ret_val = pmemobj_alloc(pop, &root->ptr[0], nodeSize * MAX_NODES, 0, NULL, NULL);
        if (ret_val) {
            std::cout << "Failed to allocate memory for root->ptr[0]" << std::endl;
            return false;
        }
        void *inodePool = pmemobj_direct(root->ptr[0]);
        void *currentPoolAddr = inodePool;
        for(int i = 0; i < numNodes; i++) {
            Inode *inode = (Inode *) new (currentPoolAddr) Inode(i,0,0);
            pmemInodePool.push_back(inode);
            currentPoolAddr = static_cast<char *>(currentPoolAddr) + nodeSize;
        }
        PmemManager::flushToNVM(0, (char *)inodePool, nodeSize * numNodes);
    }else {
        void *inodePool = pmemobj_direct(root->ptr[0]);
        for(int i = 0; i < numNodes; i++) {
            Inode *inode = (Inode *)inodePool;
            pmemInodePool.push_back(inode);
            inodePool = static_cast<char *>(inodePool) + nodeSize;
        }
    }
    return true;    
}

bool PmemInodePool::extend(PMEMobjpool *pop, size_t extendNumNodes) {
    if (this->numNodes + extendNumNodes > MAX_NODES) {
        std::cout << "Exceeding the maximum number of nodes" << std::endl;
        exit(-1);
    }
    PMEMoid root = pmemobj_root(pop, sizeof(PMEMoid));
    root_obj *rootObj = (root_obj *)pmemobj_direct(root);
    void *inodePool = pmemobj_direct(rootObj->ptr[0]);
    void *currentPoolAddr = static_cast<char *>(inodePool) + this->numNodes * nodeSize;
    for (size_t i = this->numNodes; i < extendNumNodes; ++i) {
        Inode *inode = (Inode *) new (currentPoolAddr) Inode(i);
        pmemInodePool.push_back(inode);
        currentPoolAddr = static_cast<char *>(currentPoolAddr) + nodeSize;
    }
    return true;
}

void PmemInodePool::printStats(int level, long vnode_count) {
    std::vector<Inode*> header(level + 1);
    std::vector<Inode*> tail(level + 1);
    for(int i = 0; i <= level; ++i) {
        header[i] = this->at(MAX_LEVEL -1 - i);
        tail[i] = this->at(2 * MAX_LEVEL -1 - i);
    }
    //cout << "***********************************" << endl;
    //cout << "PmemInodePool Stats:" << std::endl;

    // 记录每一层的 inode 数，复用到后面做性能估算
    std::vector<long long> level_inode_cnt(level + 1, 0);

    for(int i = 0; i <= level; ++i) {
        Inode* current = header[i];
        long long count = 0;
        while (current->hdr.next != tail[i]->getId()) {
            current = pmemInodePool[current->hdr.next];
            count++;
        }
        level_inode_cnt[i] = count;
        //std::cout << "Level " << i << " has " << count << " inodes." << std::endl;
    }

    // === 以下是基于当前层级分布与 vnode_count 的性能指标估算 ===
    // 1. 估算索引层期望比较次数 E_index
    double E_index = 0.0;
    if (level >= 0) {
        // 顶层入口访问，视为 1 次
        E_index += 1.0;
    }
    // 从最高层逐层向下：fanout = L_{i-1} / L_i，期望扫描 fanout/2
    for (int i = level; i > 0; --i) {
        double upper = static_cast<double>(level_inode_cnt[i]);     // L_i
        double lower = static_cast<double>(level_inode_cnt[i - 1]); // L_{i-1}
        if (upper > 0.0) {
            double fanout = lower / upper;
            E_index += fanout / 2.0;
        }
    }

    // 2. 估算数据层（vnode 链表）期望比较次数 E_data
    double E_data = 0.0;
    if (level_inode_cnt[0] > 0 && vnode_count > 0) {
        double Vtotal = static_cast<double>(vnode_count);
        double avg_vnodes_per_inode = Vtotal / static_cast<double>(level_inode_cnt[0]);
        E_data = avg_vnodes_per_inode / 2.0;
    }

    double E_search = E_index + E_data;
    double IndexEfficiency = 0.0;
    if (E_search > 0.0 && vnode_count > 0) {
        IndexEfficiency = static_cast<double>(vnode_count) / E_search;
    }

    std::cout << "Estimated E_index : " << E_index <<": "<< level_inode_cnt[0] << std::endl;
    //std::cout << "Estimated E_data  : " << E_data  << std::endl;
    //std::cout << "PMEM Estimated E_search: " << E_search << std::endl;
    //std::cout << "IndexEfficiency (vnodes per comparison): "<< IndexEfficiency << std::endl;

    //cout << "End of PmemInodePool Stats." << std::endl;
    //cout << "***********************************" << endl;

}
