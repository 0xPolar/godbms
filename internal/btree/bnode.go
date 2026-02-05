package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

/*

Node Format
	| type | nkeys | pointers  | offsets    | key-values | unused |
	| 2B   | 2B   | nkeys × 8B | nkeys × 2B | ...        |        |

Key-values array
	| key_size | val_size | key | val |
	|    2B    |    2B    | ... | ... |
*/

const (
	BNODE_NODE = 1
	BNODE_LEAF = 2
)

const (
	BTREE_PAGE_SIZE    = 4096
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
	HEADER             = 4
)

type BNode []byte

func assert(cond bool) {
	if !cond {
		panic("assertion failure")
	}
}

// header getters
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

// header setter
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// read and write the child pointers array
func (node BNode) getPtr(idx uint16) uint16 {
	assert(idx < node.nkeys())
	pos := 4 + 8*idx

	return binary.LittleEndian.Uint16(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint16) {
	assert(idx < node.nkeys())
	pos := 4 + 8*idx

	binary.LittleEndian.PutUint16(node[pos:], val)
}

// read offsets array
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}

	pos := 4 + 8*node.nkeys() + 2*(idx-1)
	return binary.LittleEndian.Uint16(node[pos:])
}

func offsetPos(node BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys())
	return 4 + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node[offsetPos(node, idx):], offset)
}

func (node BNode) kvPos(idx uint16) uint16 {
	assert(idx <= node.nkeys())

	return 4 + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys())

	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])

	return node[pos+4:][:klen]
}

func (node BNode) getValue(idx uint16) []byte {
	assert(idx < node.nkeys())

	pos := node.kvPos(idx)
	kLen := binary.LittleEndian.Uint16(node[pos+0:])
	vLen := binary.LittleEndian.Uint16(node[pos+2:])

	return node[pos+4+kLen:][:vLen]
}

func nodeAppendKV(new BNode, idx uint16, ptr uint16, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)

	// KeyValue position
	pos := new.kvPos(idx)

	// KeyValue sizes
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(key)))

	// KeyValue data
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)

	// update the offset value for the next key
	new.setOffset(idx, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)

	nodeAppendRange(new, old, 0, 0, idx)                   // copy the keys before 'idx'
	nodeAppendKV(new, idx, 0, key, val)                    // the new key
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx) // copy the keys after 'idx'
}

// copy multiple keys, values, and pointers into the position
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	for i := uint16(0); i < n; i++ {
		dst, src := dstNew+i, srcOld+i
		nodeAppendKV(new, dst, old.getPtr(src), old.getKey(src), old.getValue(src))
	}
}

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

func leafDelete(new BNode, old BNode, idx uint16)

func nodeMerge(new BNode, left BNode, right BNode)

func nodeReplace2Kid(new BNode, old BNode, idx uint16, ptr uint64, key []byte)

// find the last position is less than or equal to the key
func nodeLookupLE(node BNode, key []byte) uint16 {
	nKeys := node.nkeys()

	var i uint16

	for i = 0; i < nKeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)

		if cmp == 0 {
			return i
		} else if cmp > 0 {
			return i - 1
		}

	}

	return i - 1
}

func nodeSplit2(left BNode, right BNode, old BNode) {
	assert(old.nkeys() >= 2)

	// the inital guess
	nleft := old.nkeys() / 2

	// try to fir the left half
	left_bytes := func() uint16 {
		return 4 + 8*nleft + 2*nleft + old.getOffset(nleft)
	}

	for left_bytes() > BTREE_PAGE_SIZE {
		nleft--
	}
	assert(nleft >= 1)

	right_bytes := func() uint16 {
		return old.nbytes() - left_bytes() + 4
	}

	for right_bytes() > BTREE_PAGE_SIZE {
		nleft++
	}
	assert(nleft < old.nkeys())

	nright := old.nkeys() - nleft

	// new nodes
	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)

	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)

	// NOTE: the left half may still be too big
	assert(right.nbytes() <= BTREE_PAGE_SIZE)
}

// sit a node if it's too big. the results are 1-3 nodes
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} // not split
	}

	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // might be split later
	right := BNode(make([]byte, BTREE_PAGE_SIZE))

	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	// left nodes still contains too many keys
	leftLeft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftLeft, middle, left)
	assert(leftLeft.nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftLeft, middle, right}
}

func checkLimit(key []byte, value []byte) error {
	if len(key) >= BTREE_MAX_KEY_SIZE && len(value) >= BTREE_MAX_VAL_SIZE {
		return fmt.Errorf("length of key or value too large!")
	}
	return nil
}
