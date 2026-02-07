package io

import (
	BTree "github.com/0xPolar/godb/internal/btree"
)

type KV struct {
	Path string

	fd   int
	tree BTree.BTree
}

func (db *KV) Open() error

func (db *KV) Get(key []byte) ([]byte, error)

func (db *KV) Set(key []byte, val []byte) error

func (db *KV) Delete(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, updateFile(db)
}
