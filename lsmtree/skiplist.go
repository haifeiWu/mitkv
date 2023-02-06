package lsmtree

import (
	"fmt"
	"math/rand"
)

// MaxLevel max skiplist level
const MaxLevel = 16

// SkipListNode node
type SkipListNode struct {
	key     string
	value   interface{}
	forward []*SkipListNode
}

// SkipList struct
type SkipList struct {
	head   *SkipListNode
	level  int
	length int
}

// NewSkipList new sikplist
func NewSkipList() *SkipList {
	return &SkipList{
		head:   &SkipListNode{forward: make([]*SkipListNode, MaxLevel)},
		level:  1,
		length: 0,
	}
}

func (s *SkipList) randomLevel() int {
	level := 1
	for i := 0; i < MaxLevel-1; i++ {
		if rand.Int31()&0xFFFF == 1 {
			level++
		}
	}
	if level > MaxLevel {
		level = MaxLevel
	}
	return level
}

// Insert insert skiplist node val
func (s *SkipList) Insert(key string, value interface{}) {
	update := make([]*SkipListNode, MaxLevel)
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
		update[i] = x
	}

	level := s.randomLevel()
	if level > s.level {
		for i := s.level; i < level; i++ {
			update[i] = s.head
		}
		s.level = level
	}

	x = &SkipListNode{
		key:     key,
		value:   value,
		forward: make([]*SkipListNode, level),
	}
	for i := 0; i < level; i++ {
		x.forward[i] = update[i].forward[i]
		update[i].forward[i] = x
	}

	s.length++
}

// Search search skiplist node val
func (s *SkipList) Search(key string) (interface{}, bool) {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
		if x.forward[i] != nil && x.forward[i].key == key {
			return x.forward[i].value, true
		}
	}
	return nil, false
}

// Delete delete skiplist node val
func (s *SkipList) Delete(key string) {
	update := make([]*SkipListNode, MaxLevel)
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
		update[i] = x
	}

	x = x.forward[0]
	if x != nil && x.key == key {
		for i := 0; i < s.level; i++ {
			if update[i].forward[i] != x {
				break
			}
			update[i].forward[i] = x.forward[i]
		}
		for s.level > 1 && s.head.forward[s.level-1] == nil {
			s.level--
		}
		s.length--
	}
}

// Print print skiplist node val
func (s *SkipList) Print() {
	fmt.Printf("SkipList length: %d, level: %d\n", s.length, s.level)
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil {
			fmt.Printf("[%v %v]", x.forward[i].key, x.forward[i].value)
			x = x.forward[i]
		}
		fmt.Println()
	}
}
