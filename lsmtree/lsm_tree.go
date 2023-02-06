package lsmtree

import (
	"fmt"
)

// Node node
type Node struct {
	Key   string
	Value []byte
	Left  *Node
	Right *Node
}

// LSMTree LSMTree
type LSMTree struct {
	Root *Node
}

// NewLSMTree new struct
func NewLSMTree() *LSMTree {
	return &LSMTree{}
}

// Insert insert tree node
func (lsm *LSMTree) Insert(key string, value []byte) {
	node := &Node{key, value, nil, nil}

	if lsm.Root == nil {
		lsm.Root = node
		return
	}

	currentNode := lsm.Root

	for {
		if currentNode.Key >= key {
			if currentNode.Left == nil {
				currentNode.Left = node
				return
			}
			currentNode = currentNode.Left
		} else {
			if currentNode.Right == nil {
				currentNode.Right = node
				return
			}
			currentNode = currentNode.Right
		}
	}

}

// Find find tree node val
func (lsm *LSMTree) Find(key string) []byte {
	currentNode := lsm.Root

	for {
		if currentNode == nil {
			return nil
		}

		if currentNode.Key == key {
			return currentNode.Value
		}

		if currentNode.Key > key {
			currentNode = currentNode.Left
		} else {
			currentNode = currentNode.Right
		}
	}
}

// Delete delete tree node val
func (lsm *LSMTree) Delete(key string) {
	currentNode := lsm.Root
	parentNode := lsm.Root

	for {
		if currentNode == nil {
			return
		}

		if currentNode.Key == key {
			if currentNode.Left == nil && currentNode.Right == nil {
				if parentNode.Key > key {
					parentNode.Left = nil
				} else {
					parentNode.Right = nil
				}
				return
			} else if currentNode.Left == nil {
				if parentNode.Key > key {
					parentNode.Left = currentNode.Right
				} else {
					parentNode.Right = currentNode.Right
				}
				return
			} else if currentNode.Right == nil {
				if parentNode.Key > key {
					parentNode.Left = currentNode.Left
				} else {
					parentNode.Right = currentNode.Left
				}
				return
			} else {
				node := currentNode.Right
				for {
					if node.Left != nil {
						node = node.Left
					} else {
						break
					}
				}
				node.Left = currentNode.Left
				if parentNode.Key > key {
					parentNode.Left = currentNode.Right
				} else {
					parentNode.Right = currentNode.Right
				}
				return
			}
		}

		parentNode = currentNode

		if currentNode.Key > key {
			currentNode = currentNode.Left
		} else {
			currentNode = currentNode.Right
		}
	}
}

// Print print tree node val
func (lsm *LSMTree) Print() {
	if lsm.Root == nil {
		fmt.Println("Tree is empty")
		return
	}
	printTree(lsm.Root)
}

func printTree(currentNode *Node) {
	if currentNode == nil {
		return
	}
	fmt.Printf("key: %v value: %v\n", currentNode.Key, currentNode.Value)
	printTree(currentNode.Left)
	printTree(currentNode.Right)
}
