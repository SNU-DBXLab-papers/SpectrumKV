#include "common.h"
#include "checkpoint.h"
#include "pmemInodePool.h"
#include "dramSkiplist.h"
#include "dramInodePool.h"

#pragma once
class RecoveryManager {
public:
    RecoveryManager(PmemInodePool *&pmemRecoveryArray);
    ~RecoveryManager();
    int recoveryOperation();
    DramInodePool *getDramInodePool();
    DramInodePool *dramInodePool;
    DramSkiplist *mainIndex;
    PmemInodePool *pmemRecoveryArray;
};