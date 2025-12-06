#include "checkpoint.h"
#include "tandemIndex.h"

CheckpointVector::CheckpointVector() {
    // Initialize the checkpoint's position
}

void CheckpointVector::push(ckp_entry *entry) {
    std::unique_lock<std::shared_mutex> lock(vecLock);
    ckpvec.push_back(entry);
}

ckp_entry *CheckpointVector::pop()
{
    std::unique_lock<std::shared_mutex> lock(vecLock);
    if(ckpvec.size() > 0) {
        ckp_entry *entry = ckpvec.back();
        ckpvec.pop_back();
        return entry;
    }else {
        return nullptr;
    }
}


CheckpointQueue::CheckpointQueue() {
    this->checkpointQueue = &g_checkpointQueue;
    // Initialize the checkpoint's position
    queueLock = new std::mutex();
}

void CheckpointQueue::push(CheckpointVector *vec) {
    queueLock->lock();
    checkpointQueue->push(vec);
    queueLock->unlock();
}

CheckpointVector *CheckpointQueue::pop() {
    CheckpointVector *vec;
    queueLock->lock();
   // if(checkpointQueue->pop(entry)) {
   if((vec = checkpointQueue->front())) {
        checkpointQueue->pop();
        queueLock->unlock();
        return vec;
    }else {
        queueLock->unlock();
        return nullptr;
    }
}
#if 0
void CheckpointQueue::push(CheckpointVector *vec) {
    checkpointQueue->push(vec);
}

CheckpointVector *CheckpointQueue::pop() {
    CheckpointVector *vec; 
    if(checkpointQueue->pop(vec)) {
        return vec;
    }else {
        return nullptr;
    }
}
#endif

bool CheckpointQueue::isEmpty() {
    queueLock->lock();
    bool ret = checkpointQueue->empty();
    queueLock->unlock();
    return ret;
}