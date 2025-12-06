
#include <utility>
#include <iostream>
#include <cstdlib>
#include <ctime>
#include <limits>
#include <cstring>
#include "node.h"
#include "nodepool.h"
#pragma once

// SkipList class
#if 0
class SkipList {
private:
	Node* header[MAX_LEVEL];
    int level;

public:
    SkipList() {
        level = 1;
        //head is the top layer first node
        Node *pivot = reinterpret_cast<Inode *>(NodePool::pop());
		Node *pivot = new Inode(std::numeric_limits<int>::min());
		for(int i = MAX_LEVEL - 1; i > 0; i--) {
			header[i] = pivot;
			if(i > 1) {
				Inode *update = new Inode(std::numeric_limits<int>::min());
				pivot->down = update;
				pivot = update;
			}
		}
        Vnode *node = new Vnode(std::numeric_limits<int>::min(), std::numeric_limits<int>::min());
        header[0] = reinterpret_cast<Inode *>(node);
        pivot->down = node;
    }

    ~SkipList() {
		for (int i = MAX_LEVEL - 1; i >= 0; i--) {
			Node* current = header[i];
			while (current != nullptr) {
				Node* next = current->next;
				delete current;
				current = next;
			}
		}
    }

    // Generate a random level for a new node
    int randomLevel() {
        int level = 1;
        while (rand() % 2 == 0 && level < MAX_LEVEL) {
            level++;
        }
        return level;
    }

    // Insert a key into the skip list
    void insert(int key, [[maybe_unused]] int value) {
        Node* current = header[level - 1];
        Node* update[MAX_LEVEL];
		memset(update, 0, sizeof(Node *)*MAX_LEVEL);

        for (int i = level - 1; i >= 0; i--) {
            while (current->next != nullptr && current->next->key < key) {
                current = current->next;
            }
            if (update[i] == nullptr) {
                update[i] = current;
            }
            current = current->down;
        }

        if (current == nullptr || current->key != key) {
            int newLevel = randomLevel();

            if (newLevel > level) {
                for(int i = level; i < newLevel; i++) {
                    update[i] = header[i];
                }
                level = newLevel;
            }

            Node* newNode = new Node(key);

            while (newLevel > 0) {
                newNode->next = update[newLevel - 1]->next;
                update[newLevel - 1] -> next = newNode;
				if(newLevel > 1) {
					Node* newDownNode = new Node(key);
					newNode->down = newDownNode;
					newNode = newDownNode;
				}
                newLevel--;
            }
        }
    }

    // Search for a key in the skip list
    bool lookup(int key) {
        Node* current = header[level - 1];
        for (int i = level - 1; i >= 0; i--) {
            while (current->next != nullptr && current->next->key < key) {
                current = current->next;
            }
			if(i > 0)
				current = current->down;
        }
        return (current->next != nullptr && current->next->key == key);
    }

    // checkpoint the modification into nvram
    void checkpoint();
    // recover dram index from nvram 
    void recover();

    void update(int key, int value)
    {

    }

    void remove(int key)
    {

    }

    // Print the skip list
    void print() {
        for (int i = level - 1; i >= 0; i--) {
            std::cout << "Level " << i << ": ";
            Node* current = header[i];
            while( current != nullptr) {
				std::cout << current->key << " ";
                current = current->next;
            }
            std::cout << std::endl;
        }
    }

};
#endif
