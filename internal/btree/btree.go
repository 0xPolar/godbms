package btree

import "bytes"

type BTree struct {
	root uint64

	get func(uint64) []byte
	new func([]byte) uint64
	del func(uint64)
}

func (tree *BTree) Insert(key []byte, value []byte) error {
	if err := checkLimit(key, value); err != nil {
		return err
	}

	if tree.root == 0 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)

		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 0, 1, key, value)

		tree.root = tree.new(root)
		return nil
	}

	node := treeInsert(tree, tree.get(tree.root), key, value)

	nsplit, split := nodeSplit3(node)
	tree.del(tree.root)

	if nsplit > 1 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)

		for i, knode := range split[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(knode, uint16(i), uint16(ptr), key, nil)
		}

		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}

	return nil
}

func (tree *BTree) Delete(key []byte) (bool, error)

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// The extra size alls it to exceed 1 page temporarily
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))

	idx := nodeLookupLE(node, key)
	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(new, node, idx, key, val) // if found update the node
		} else {
			leafInsert(new, node, idx+1, key, val) // if not insert new node
		}
	case BNODE_NODE: // internal node, walk into child node
		// recursive insertion to the kid node
		kptr := node.getPtr(idx)
		knode := treeInsert(tree, tree.get(uint64(kptr)), key, val)

		// after insertion split the result
		nsplit, split := nodeSplit3(knode)

		// deallocate the old kid node
		tree.del(uint64(kptr))

		// update the kid links
		nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
	default:
		panic("bad node!")
	}

	return new
}

func nodeReplaceKidN(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), uint16(tree.new(node)), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := BNode(tree.get(uint64(node.getPtr(idx - 1))))

		merged := sibling.nbytes() + updated.nbytes() - HEADER

		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling // left
		}
	}

	if idx+1 < node.nkeys() {
		sibling := BNode(tree.get(uint64(node.getPtr(idx + 1))))
		merged := sibling.btype() + updated.nbytes() - HEADER

		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling
		}
	}

	return 0, BNode{}
}

func treeDelete(tree *BTree, node BNode, key []byte) BNode

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(uint64(idx)), key)

	if len(updated) == 0 {
		return BNode{} // not found
	}
	tree.del(uint64(kptr))

	new := BNode(make([]byte, BTREE_PAGE_SIZE))

	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(uint64(node.getPtr(idx - 1)))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))

	case mergeDir < 0:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(uint64(node.getPtr(idx + 1)))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))

	case mergeDir == 0 && updated.nkeys() == 0:
		assert(node.nkeys() == 1 && idx == 0)
		new.setHeader(BNODE_NODE, 0)

	case mergeDir == 0 && updated.nkeys() > 0:
		nodeReplaceKidN(tree, new, node, idx, updated)
	}

	return new
}
