#include "workerThread.h"
#include "tandemIndex.h"


LogFlushThread::LogFlushThread(int tid, CkptLog *cklog, PmemInodePool *pmemInodePool) {
    this->id = tid;
    this->ckptLog = cklog;
    this->pmemInodePool = pmemInodePool;
}

LogFlushThread::~LogFlushThread() {
    // 析构前确保把 [durable, produced) 全部刷盘
    ckptLog->forcePersist();
}

void LogFlushThread::LogFlushOperation() {
    try {
        ckptLog->tryFlushOnce();
    } catch (std::exception &e) {
        std::cout << "Exception in LogFlushOperation: " << e.what() << std::endl;
    }
}

LogMergeThread::LogMergeThread(int tid, CkptLog *cklog, PmemInodePool *pmemInodePool) {
    this->id = tid;
    this->ckptLog = cklog;
    this->pmemInodePool = pmemInodePool;
}

LogMergeThread::~LogMergeThread() {
    if(!ckptLog->isLogEmpty()) {
        ckptLog->forceReclaim(pmemInodePool);
    }
}

void LogMergeThread::logMergeOperation(double dram_search_efficiency, long vnode_count) {
    try {
        ckptLog->reclaim(dram_search_efficiency, vnode_count, pmemInodePool);
    } catch (std::exception &e) {
        std::cout << "Exception in logMergeOperation: " << e.what() << std::endl;
    }
}