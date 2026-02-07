package io

import (
	"fmt"
	"os"
	"path"
	"syscall"
)

// test
func updateFile(db *KV) error {
	// write new nodes
	if err := writePages(db); err != nil {
		return err
	}

	// fsync to enforce the order between 1 and 3
	if err := syscall.Fsync((db.fd)); err != nil {
		return err
	}

	// update the root pointer atomically
	if err := updateRoot(db); err != nil {
		return err
	}

	return syscall.Fsync(db.fd)
}

func writePages(db *KV) error

func updateRoot(db *KV) error

func createFileSync(file string) (int, error) {
	// obtain the directory fd
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("failed to open directory: %w", err)
	}
	defer syscall.Close(dirfd)

	// open or create the file
	flags = os.O_RDWR | os.O_CREATE
	fd, err := syscall.Openat(dirfd, path.Base(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("fsync directory: %W", err)
	}

	return fd, nil
}
