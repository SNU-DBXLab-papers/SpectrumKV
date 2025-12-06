#include "common.h"
#include <mutex>
#include "node.h"
#include <boost/lockfree/spsc_queue.hpp>
#pragma once

class ckp_entry{
public: 
    ckp_entry(int _id, int _offset, int _length, void *_content) {
        this->id = _id;
        this->inode_offset = _offset;
        this->length = _length;
        this->content = _content;
    }

    ckp_entry(Inode *inode) {
        id = inode->hdr.id;
        inode_offset = 0;
        length = sizeof(Inode);
        content = inode;
    }
    int id;
    int inode_offset;
    int length;
    void *content;
};

class CheckpointVector {
    public:
        CheckpointVector();
#if 0
        static CheckpointVector *getPerThreadInstance();
        static void setPerThreadInstance(CheckpointVector *ptr) {perThreadVector = ptr;}
        static CheckpointVector *getCheckpointVector();
        static void enqPerThreadVector(int id, int inode_offset, int length, void *content);
        static void enqPerThreadVector(ckp_entry *entry);
#endif
        void push(ckp_entry *entry);
        ckp_entry *pop();
        void lock() {vecLock.lock();}
        void unlock() {vecLock.unlock();}
        std::shared_mutex vecLock;
        std::vector<ckp_entry *> ckpvec;
};

class CheckpointQueue {
    public:
        CheckpointQueue();
        void push(CheckpointVector *vec);
        CheckpointVector *pop();
        bool isEmpty();
        std::mutex *queueLock;
    private:
        //boost::lockfree::spsc_queue<ckp_entry *, boost::lockfree::capacity<1000000>> *checkpointQueue;
        std::queue<CheckpointVector *> *checkpointQueue;
};

#if 0
class CheckpointQueue {
    public:
        CheckpointQueue();
        void push(ckp_entry *entry);
        ckp_entry *pop();
        bool isEmpty();
        std::mutex *queueLock;
    private:
        //boost::lockfree::spsc_queue<ckp_entry *, boost::lockfree::capacity<1000000>> *checkpointQueue;
        std::queue<ckp_entry *> *checkpointQueue;
};
#endif

