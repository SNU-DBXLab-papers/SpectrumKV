#include <libpmemobj.h>
#include <string>
#include <iostream>
#include <unistd.h>
using namespace std;

class DramManager {
    private:
        static void *dramPool[6]; // dram
    public:
        static void *getPoolStartAddress(int poolId) {
            return dramPool[poolId];
        }

        static bool createPool(int poolId, size_t poolSize) {
            dramPool[poolId] = malloc(poolSize);
            if(dramPool[poolId] == nullptr) {
                std::cout << "Failed to create pool: " << poolId << ", error code: " << errno << std::endl;
                return false;
            }
            return true;
        }

        static bool closePool(int poolId) {
            free(dramPool[poolId]);
            return true;
        }

        static void alloc(int poolId, size_t size) {
        }
};