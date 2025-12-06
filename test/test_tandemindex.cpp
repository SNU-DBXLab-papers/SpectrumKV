#include "../include/tandemIndex.h"
// Node class for skip list

int main() {
    // Initialize random seed
    srand(time(nullptr));

    // Create a skip list
    TandemIndex skipList;

    // Insert keys into the skip list
    skipList.insert(3,3);
    skipList.insert(6,6);
    skipList.insert(2,2);
    skipList.insert(9,9);
    skipList.insert(5,5);
    skipList.insert(10,5);
    skipList.insert(11,5);
    skipList.insert(12,5);


    // Print the skip list
    skipList.print();

    // Search for a key in the skip list
    int key = 6;
    if (skipList.lookup(key)) {
        std::cout << key << " is found in the skip list." << std::endl;
    } else {
        std::cout << key << " is not found in the skip list." << std::endl;
    }

    return 0;
}
