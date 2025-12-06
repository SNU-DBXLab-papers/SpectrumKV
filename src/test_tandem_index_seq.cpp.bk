#if 0
#include "tandemIndex.h"
#include <cassert>
int main() {
    TandemIndex list;
  //  for(Key_t i = 0; i < std::numeric_limits<Key_t>::max(); i++) {
    for(Key_t i = 1; i < 10000; i++) {
        list.insert(i, i);
        if(list.lookup(i) != i) {
            std::cout << "Failed to insert " << i << std::endl;
            assert(false);
        }
        if(list.lookup(1) != 1) {
            std::cout << "Failed to check 1" << i << std::endl;
            assert(false);
        }
    }

    // Lookup nodes

	std::cout << "Lookup 1: " << list.lookup(1)  << std::endl;
	std::cout << "Lookup 5: " << list.lookup(5)  << std::endl;
	std::cout << "Lookup 3: " << list.lookup(3)  << std::endl;
	std::cout << "Lookup 6: " << list.lookup(6)  << std::endl;
	std::cout << "Lookup 1: " << list.lookup(1)  << std::endl;
    std::cout << "Lookup 7: " << list.lookup(7) << std::endl;
    std::cout << "Lookup 8: " << list.lookup(8) << std::endl;
    std::cout << "Lookup 9: " << list.lookup(9) << std::endl;
    std::cout << "Lookup 33: " << list.lookup(33) << std::endl;
     // Delete nodes
    //list.remove(10);
    //list.remove(20);

    return 0;

}
#endif
