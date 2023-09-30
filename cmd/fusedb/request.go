package main

import (
	"github.com/hanwen/go-fuse/v2/fs"
)

type DBFSRequest struct {
	Root bool
	Group string
	Data string
}

func (r *DBFSRequest) IsRoot() bool {
	return r.Root
}

func (r *DBFSRequest) IsGroup() bool {
	return r.Group != "" && r.Data == ""
}

func (r *DBFSRequest) IsData() bool {
	return r.Group != "" && r.Data != ""
}

func ParseRequest(n *fs.Inode, target string) *DBFSRequest {
	if n.IsRoot() {
		if target != "" {
			return &DBFSRequest{Root: false, Group: target}
		}
		return &DBFSRequest{Root: true}
	}

	parent1, inode1 := n.Parent()
	if inode1 == nil {
		if target != "" {
			return &DBFSRequest{Root: false, Group: target}
		}
		return &DBFSRequest{Root: true}
	}

	if inode1.IsRoot() {
		if target != "" {
			return &DBFSRequest{Root: false, Group: parent1, Data: target}
		}
		return &DBFSRequest{Root: false, Group: parent1}
	}

	parent2, inode2 := inode1.Parent()
	if inode2 == nil {
		if target != "" {
			return &DBFSRequest{Root: false, Group: parent1, Data: target}
		}
		return &DBFSRequest{Root: false, Group: parent1}
	}

	if inode2.IsRoot() {
		if target != "" {
			return nil
		}
		return &DBFSRequest{Root: false, Group: parent2, Data: parent1}
	}

	return nil
}
