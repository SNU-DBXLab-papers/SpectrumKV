#include "recoveryManager.h"

RecoveryManager::RecoveryManager(PmemInodePool *&pmemRecoveryArray) {
    this->pmemRecoveryArray = pmemRecoveryArray;
}

int RecoveryManager::recoveryOperation() {
    Inode *superNode = pmemRecoveryArray->at(MAX_NODES - 1);
    int32_t last_index = 0;
    dramInodePool = new DramInodePool(sizeof(Inode), MAX_NODES);
    if(superNode->hdr.next != 0) {
        last_index = superNode->hdr.next;
        auto pmemPool = pmemRecoveryArray->at(0);
        auto dramPool = dramInodePool->at(0);
        PmemManager::memcpyToDRAM(1, reinterpret_cast<char *>(dramPool), reinterpret_cast<char *>(pmemPool), sizeof(Inode) * (last_index ));
        dramInodePool->setCurrentIdx(last_index);
        return superNode->hdr.level;
    }
    return 1;
}

DramInodePool *RecoveryManager::getDramInodePool() {
    return dramInodePool;
}