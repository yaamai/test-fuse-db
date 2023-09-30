package main

import (
	"log"
	"syscall"
	"context"
	"bytes"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type DBFS struct {
	db *DB
}

type DBFSNode struct {
	fs.Inode
	RootData *DBFS
}

type DBFSFileHandle struct {
	node *DBFSNode
	buffer bytes.Buffer
}

func (h *DBFSFileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return 0
}

func (h *DBFSFileHandle) Release(ctx context.Context) syscall.Errno {
	log.Println("buffer:", h.buffer.Len())

	req := h.node.parseRequest("")
	err := h.node.RootData.db.PutData(req.Group, req.Data, h.buffer.String())
	if err != nil {
		return syscall.ENOENT
	}

	return 0
}

type DBFSRequest struct {
	Root bool
	Group string
	Data string
}

func (n *DBFSNode) parseRequest(target string) *DBFSRequest {
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

func (n *DBFSNode) EmbeddedInode() *fs.Inode {
	return &n.Inode
}

func (n *DBFSNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	req := n.parseRequest("")
	if req == nil {
		return nil, syscall.ENOENT
	}

	if req.Root {
		groups, err := n.RootData.db.ListGroups()
		if err != nil {
			return nil, syscall.ENOENT
		}

		entries := []fuse.DirEntry{}
		for _, g := range groups {
			entries = append(entries, fuse.DirEntry{syscall.S_IFDIR, g.Name, g.ID})
		}
		return fs.NewListDirStream(entries), 0
	}

	if req.Group != "" && req.Data == "" {
		datas, err := n.RootData.db.ListDatas(req.Group)
		if err != nil {
			return nil, syscall.ENOENT
		}

		entries := []fuse.DirEntry{}
		for _, d := range datas {
			entries = append(entries, fuse.DirEntry{
				syscall.S_IFREG,
				d.Name,
				d.ID,
			})
		}

		return fs.NewListDirStream(entries), 0
	}

	return nil, syscall.ENOENT
}

func (n *DBFSNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	req := n.parseRequest(name)
	if req == nil {
		return nil, syscall.ENOENT
	}

	if req.Group != "" && req.Data == "" {
		group, err := n.RootData.db.GetGroup(req.Group)
		if err != nil || group == nil {
			return nil, syscall.ENOENT
		}

		node := &DBFSNode{RootData: n.RootData}
		attr := fs.StableAttr{
			Mode: syscall.S_IFDIR,
			Gen: 1,
			Ino: group.ID,
		}
		return n.NewInode(ctx, node, attr), 0
	}

	if req.Data != "" {
		data, err := n.RootData.db.GetData(req.Group, req.Data)
		if err != nil || data == nil {
			return nil, syscall.ENOENT
		}

		node := &DBFSNode{RootData: n.RootData}
		attr := fs.StableAttr{
			Mode: syscall.S_IFREG,
			Gen: 1,
			Ino: data.ID,
		}
		return n.NewInode(ctx, node, attr), 0
	}

	return nil, syscall.ENOENT
}

func (n *DBFSNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Println("open:", n.parseRequest(""))
	return &DBFSFileHandle{node: n}, 0, 0
}

func (n *DBFSNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	req := n.parseRequest("")

	if req == nil {
		return syscall.ENOENT
	}

	if req.Root {
		return 0
	}

	if req.Group != "" && req.Data == "" {
		out.Mode = syscall.S_IFDIR|0o755
		return 0
	}

	if req.Data != "" {
		data, err := n.RootData.db.GetData(req.Group, req.Data)
		if err != nil || data == nil {
			return syscall.ENOENT
		}
		out.Size = uint64(len(data.Data))
		return 0
	}

	return 0
}

func (n *DBFSNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return 0
}

func (n *DBFSNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	req := n.parseRequest("")

	if req == nil {
		return nil, syscall.ENOENT
	}

	if req.Group == "" || req.Data == "" {
		return nil, syscall.ENOENT
	}

	data, err := n.RootData.db.GetData(req.Group, req.Data)
	if err != nil || data == nil {
		return nil, syscall.ENOENT
	}
	return fuse.ReadResultData([]byte(data.Data)), 0
}

func (n *DBFSNode) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	req := n.parseRequest("")

	if req == nil {
		return 0, syscall.ENOENT
	}

	if req.Group == "" || req.Data == "" {
		return 0, syscall.ENOENT
	}

	written, err := f.(*DBFSFileHandle).buffer.Write(data)
	if err != nil {
		return 0, syscall.ENOENT
	}

	return uint32(written), 0
}

func main() {
	db, err := sqlx.Open("postgres", "postgres://postgres:password@localhost/postgres?sslmode=disable")
	if err != nil {
		log.Printf("sql.Open error %s", err)
	}

	root := &DBFSNode{RootData: &DBFS{db: &DB{DB: db, dataNames: []string{"hoge", "fuga"}}}}
	
	server, err := fs.Mount("/tmp/aa", root, &fs.Options{
		MountOptions: fuse.MountOptions{
			DirectMount: false,
			Debug: true,
		},
	})
	if err != nil {
		log.Panic(err)
	}

	server.Wait()
}
