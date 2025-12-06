#include "common.h"
#include "checkpoint.h"
#include "dramSkiplist.h"
#include "pmemInodePool.h"
#include "pmemBFPool.h"
#include "recoveryManager.h"
#include "spinLock.h"
#include "valuelist.h"
#include "workerThread.h"
#include "insert_tracker.h"
#include <boost/lockfree/spsc_queue.hpp>
#include <thread>
#include <queue>
#include <mutex>
#pragma once


extern std::queue<CheckpointVector *> g_checkpointQueue;

class TandemIndex {
    public:
        TandemIndex();
        ~TandemIndex();

        bool insert(Key_t key, Val_t value);
        //void remove(int key);
        bool update(Key_t key, Val_t value);
        bool scan(Key_t key, size_t range, std::priority_queue<Key_t, std::vector<Key_t>, std::greater<Key_t>> &result);
        bool scan(Key_t key, size_t range, std::vector<Val_t> &result);
        Val_t lookup(Key_t key);
        void recover(Key_t key);
        bool insertWithNewInodes(Key_t key, Val_t value);

        bool insertWithoutIndex(Key_t key, Val_t value);
        bool insertWithNewInodes(Key_t key, Val_t value, Vnode* &vnode);
        bool insertInVnodeChain(Vnode* &vnode, BloomFilter* &bloom, Key_t key, Val_t value);
        bool moveToNextVnodeForInsert(Vnode* &vnode, BloomFilter* &bloom, std::unique_lock<std::shared_mutex> &vnode_lock);
        bool handleNodeFullAndSplit(Vnode* &vnode, BloomFilter* &bloom, 
                                         Key_t key, Val_t value, Vnode* &newNode, BloomFilter* &newBloom);
        bool updateParentInodeAfterSplit(Inode *parent_inode, Vnode *targetVnode, std::vector<Inode *> &updates, int &last_idx, int &idx_to_next_level);

        //std::thread *workerThread;kk
        std::thread *logFlushThread = nullptr;
        std::thread *logMergeThread = nullptr;
        std::thread *rebalanceThread[MAX_REBALANCE_THREADS] = {nullptr,};


        //void createWorkerThread();
        void createLogFlushThread();
        void createLogMergeThread();
        void createRebalanceThread();
        void mergeScanResultsPQ(std::priority_queue<Key_t, std::vector<Key_t>, std::greater<Key_t>> &dst,
                              std::priority_queue<Key_t, std::vector<Key_t>, std::greater<Key_t>> &src);
        void mergeScanResultsVec(std::vector<Key_t> &dst,
                              std::vector<Key_t> &src);
        void rebalanceThreadExec(int id);
        void logFlushThreadExec(int id);
        void logMergeThreadExec(int id);
        //void workerThreadExec();
        
        // 重平衡队列相关方法
        void addToRebalanceQueue(Inode* &inode);
        bool getFromRebalanceQueue(Inode* &inode);
        void addToRebalanceMap(Inode *child, Inode *parent);
        bool getFromRebalanceMap(Inode *child, Inode *parent);

        void maybeActivateHotRegion();
        bool isDataLoaded()
        {
            return is_data_loaded;
        }

        void setDataLoaded(bool loaded)
        {
            is_data_loaded = loaded;
        }

        void printStatus();

    private:
        DramSkiplist *mainIndex;
        DramInodePool *dramInodePool;
        PmemInodePool *pmemRecoveryArray;
        PmemBFPool *pmemBFPool;
        //PmemSkiplist *shadowIndex;
        ValueList *valueList;
        CkptLog *ckptLog;
        RecoveryManager *recoveryManager;
        bool needToRebalance;
        bool is_data_loaded;
        
        // 重平衡队列相关成员
        std::queue<Inode *> rebalanceQueue;
        std::mutex rebalanceQueueMutex;
        //std::mutex printMutex;
        std::shared_mutex printMutex;
        std::unordered_set<Inode *> rebalancingInodes;
        std::unordered_set<Inode *> nodesInRebalanceProcess;
};