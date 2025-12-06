#include "tandemIndex.h"
#include <cassert>
#include <cstdlib>
#include <ctime>
#if 0
int main() {
    TandemIndex list;
  //  for(Key_t i = 0; i < std::numeric_limits<Key_t>::max(); i++) {
    for(Key_t i = 1; i < 3000; i++) {
        int random_value = std::rand() % 100 + 1;
        list.insert(random_value, random_value);
        if(list.lookup(random_value) != random_value) {
            std::cout << "Failed to insert " << random_value << std::endl;
            assert(false);
        }
    }

    // Lookup nodes

    //list.remove(10);
    //list.remove(20);

    return 0;

}
#endif
