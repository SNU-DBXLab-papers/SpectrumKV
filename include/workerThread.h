#include "common.h"
#include "checkpoint.h"
#include "pmemInodePool.h"
#include "dramSkiplist.h"
#include "ckpt_log.h"
#include <queue>
#include <boost/lockfree/spsc_queue.hpp>
#pragma once

enum Operation {
    INSERT = 0,
    DELETE = 1,
    UPDATE = 2,
    LOOKUP = 3
};

class wq_entry{
    Key_t key;
    Val_t value;
    int ops;
    wq_entry(Key_t _key, Val_t _value, int _ops) {
        this->key = _key;
        this->value = _value;
        this->ops = _ops;
    }
};

class WorkerThread {
public:
    WorkerThread();
    ~WorkerThread();
    void workerOperation();
};

class LogFlushThread {
private:
    CkptLog *ckptLog;
    PmemInodePool *pmemInodePool;
    DramSkiplist *index;
    int id;
public:
    LogFlushThread(int tid, CkptLog *cklog, PmemInodePool *pmemInodePool);
    ~LogFlushThread();
    void LogFlushOperation();
};

class LogMergeThread {
private:
    CkptLog *ckptLog;
    PmemInodePool *pmemInodePool;
    int id;
public:
    LogMergeThread(int tid, CkptLog *cklog, PmemInodePool *pmemInodePool);
    ~LogMergeThread();
    void logMergeOperation(double dram_search_efficiency, long vnode_count);
    //bool isCkptLogEmpty();
};

