#include <libpmemobj.h>
#include <string>
#include <iostream>
#include <unistd.h>
#include "common.h"
#pragma once
using namespace std;

typedef struct root_obj {
    PMEMoid ptr[2];
    //    PMEMoid ptr2;
} root_obj;

class PmemManager {
public:
        static void *pmemPool[6]; // dram
    public:
        static void *getPoolStartAddress(int poolId) {
            return pmemPool[poolId];
        }

        static bool createOrOpenPool(int poolId, string path, size_t poolSize, void **rootp, bool &isCreate) {
            PMEMobjpool *pop = nullptr;
            if(access(path.c_str(), F_OK) != 0) {
                std::cout << "File does not exists: " << path << std::endl;
                pop = pmemobj_create(path.c_str(), "pmemvaluepool", poolSize, 0666);
                if (pop == nullptr) {
                    std::cout << "Failed to create pool: " << path << ", error code: " << errno << std::endl;
                    return false;
                }
                isCreate = true;
                std::cout << "Created pool: " << path << std::endl;                
            } else {
                std::cout << "File exist: " << path << std::endl;
                pop = pmemobj_open(path.c_str(), "pmemvaluepool");
                if(pop != NULL) {
                    std::cout << "Opened pool: " << path << std::endl;
                } else {
                    std::cout << "Failed to open pool: " << path << std::endl;
                    return false;
                }
                isCreate = false;
            }
            pmemPool[poolId] = reinterpret_cast<void *>(pop);
            PMEMoid root = pmemobj_root(pop, sizeof(root_obj));
            *rootp = (root_obj*)pmemobj_direct(root);
            return true;
        }

        static bool closePool(int poolId) {
            PMEMobjpool *pop = (PMEMobjpool *)pmemPool[poolId];
            pmemobj_close(pop);
            return true;
        }

        static void *alloc(int poolId, size_t size) {
            PMEMobjpool *pop = (PMEMobjpool *)pmemPool[poolId];
            PMEMoid oid;
            int ret = pmemobj_alloc(pop, &oid, size, 0, NULL, NULL);
            if (ret) {
                return nullptr;
            }
            return pmemobj_direct(oid);
        }

        // 一次性持久化（flush + drain）
        static inline void flushToNVM(int poolId, char *data, size_t size) {
            PMEMobjpool *pop = (PMEMobjpool *)pmemPool[poolId];
            pmemobj_persist(pop, data, size); // 等效于 pmemobj_flush + pmemobj_drain
        }

        // 批量用：仅发出flush，不等待
        static inline void flushNoDrain(int poolId, const void *addr, size_t len) {
            if (!addr || len == 0) return;
            PMEMobjpool *pop = (PMEMobjpool *)pmemPool[poolId];
            pmemobj_flush(pop, addr, len); // 仅 CLWB/CLFLUSHOPT，且不含栅栏
        }

        // 批量用：在所有 flush 之后统一等待
        static inline void drain(int poolId) {
            PMEMobjpool *pop = (PMEMobjpool *)pmemPool[poolId];
            pmemobj_drain(pop); // 等待前面的 flush/stores 达到持久域（一次 sfence）
        }

        // 若需要“拷贝并持久化一次”的便利接口，可继续使用：
        static inline void memcpyToNVM(int poolId, char *dest, char *src, size_t size) {
            PMEMobjpool *pop = (PMEMobjpool *)pmemPool[poolId];
            pmemobj_memcpy_persist(pop, dest, src, size); // 内部等效 memcpy + persist
        }

        // 可选：提供 nodrain 版本，配合 drain 批量使用（若你选择 libpmemobj 的扩展API或自写组合）
        // static inline void memcpyNoDrain(...) { pmemobj_memcpy(pop, ...); pmemobj_flush(pop, dest, size); }
        // 之后统一 pmemobj_drain(pop);
        static inline void memcpyToDRAM(int poolId, char *dest, char *src, size_t size) {
            [[maybe_unused]]PMEMobjpool *pop = (PMEMobjpool *)pmemPool[poolId];
            memcpy(dest, src, size);
        }

        static inline unsigned char *align_ptr_to_cacheline(void *p)
        {
            return (unsigned char *)(((unsigned long)p + ~L1_CACHE_LINE_MASK) &
			L1_CACHE_LINE_MASK);
        }

        static inline unsigned long align_uint_to_cacheline(unsigned int size)
        {
            return (size + ~L1_CACHE_LINE_MASK) & L1_CACHE_LINE_MASK;
        }
};