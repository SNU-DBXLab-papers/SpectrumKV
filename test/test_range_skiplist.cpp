#include "skiplist.h"
// Node class for skip list
#if 0
int main() {
    // Initialize random seed
    srand(time(nullptr));

    // Create a skip list
    SkipList skipList;

    // Insert keys into the skip list
    skipList.insert(3);
    skipList.insert(6);
    skipList.insert(2);
    skipList.insert(9);
    skipList.insert(5);

    // Print the skip list
    skipList.print();

    // Search for a key in the skip list
    int key = 6;
    if (skipList.search(key)) {
        std::cout << key << " is found in the skip list." << std::endl;
    } else {
        std::cout << key << " is not found in the skip list." << std::endl;
    }

    return 0;
}
#endif
