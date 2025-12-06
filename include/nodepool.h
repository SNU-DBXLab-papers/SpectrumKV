#include <iostream>
#include <vector>
#include "node.h"
#pragma once
#if 0
class NodePool {
private:
    std::vector<Node*> nodePool;
    size_t nodeSize;
    size_t numNodes;

public:
    NodePool(size_t nodeSize, size_t numNodes) : nodeSize(nodeSize), numNodes(numNodes) {
        // Allocate memory blocks
        Node* Nodes = new Node[numNodes];
        for (size_t i = 0; i < numNodes; ++i) {
            nodePool.push_back(Nodes+i);
        }
    }

    ~NodePool() {
        // Deallocate memory blocks
        for (Node* node : nodePool) {
            delete[] node;
        }
    }

    static Node *pop() {
        if (nodePool.empty()) {
            return nullptr;
        }

        Node* node = nodePool.back();
        nodePool.pop_back();
        return node;
    }

    static void push(void *block)
    {
        nodePool.push_back(block);
    }

    static void extend(size_t numBlocks) {
        Node* Nodes = new Node[numBlocks];
        for (size_t i = 0; i < numBlocks; ++i) {
            nodePool.push_back(Nodes+i);
        }
    }

};
#endif