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
	req *DBFSRequest
	buffer bytes.Buffer
}

func (h *DBFSFileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return 0
}

func (h *DBFSFileHandle) Release(ctx context.Context) syscall.Errno {
	log.Println("buffer:", h.buffer.Len())

	req := ParseRequest(&h.node.Inode, "")
	err := h.node.RootData.db.PutData(req.Group, req.Data, h.buffer.String())
	if err != nil {
		return syscall.ENOENT
	}

	return 0
}


func (h *DBFSFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if h.req.IsRoot() || h.req.IsGroup() {
		return nil, syscall.ENOTSUP

	} else if h.req.IsData() {
		data, err := h.node.RootData.db.GetData(h.req.Group, h.req.Data)
		if err != nil {
			return nil, syscall.EIO
		}
		if data == nil {
			return nil, syscall.ENOENT
		}
		copy(dest, []byte(data.Data)[off:])
		return fuse.ReadResultData(dest), 0

	} else {
		return nil, syscall.ENOSYS
	}
}

func (h *DBFSFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if h.req.IsRoot() || h.req.IsGroup() {
		return 0, syscall.ENOTSUP

	} else if h.req.IsData() {
		written, err := h.buffer.Write(data)
		if err != nil {
			return 0, syscall.ENOENT
		}

		return uint32(written), 0
	} else {
		return 0, syscall.ENOSYS
	}
}

func (n *DBFSNode) EmbeddedInode() *fs.Inode {
	return &n.Inode
}

func (n *DBFSNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	req := ParseRequest(&n.Inode, "")
	if req == nil {
		return nil, syscall.EINVAL
	}

	if req.IsRoot() {
		groups, err := n.RootData.db.ListGroups()
		if err != nil {
			return nil, syscall.EIO
		}

		entries := []fuse.DirEntry{}
		for _, g := range groups {
			entries = append(entries, fuse.DirEntry{syscall.S_IFDIR, g.Name, g.ID})
		}
		return fs.NewListDirStream(entries), 0
	} else if req.IsGroup() {
		datas, err := n.RootData.db.ListDatas(req.Group)
		if err != nil {
			return nil, syscall.EIO
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
	} else {
		return nil, syscall.ENOSYS
	}
}

func (n *DBFSNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	req := ParseRequest(&n.Inode, name)
	if req == nil {
		return nil, syscall.EINVAL
	}

	if req.IsGroup() {
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
	} else if req.IsData() {
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
	} else {
		return nil, syscall.ENOSYS
	}
}

func (n *DBFSNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	req := ParseRequest(&n.Inode, "")
	if req == nil {
		return syscall.EINVAL
	}

	if req.IsRoot() {
		out.Mode = syscall.S_IFDIR|0o755
		return 0

	} else if req.IsGroup() {
		out.Mode = syscall.S_IFDIR|0o755
		return 0

	} else if req.IsData() {
		data, err := n.RootData.db.GetData(req.Group, req.Data)
		if err != nil {
			return syscall.EIO
		}
		if data == nil {
			return syscall.ENOENT
		}
		out.Size = uint64(len(data.Data))
		out.Mode = syscall.S_IFREG|0o666
		return 0

	} else {
		return syscall.ENOSYS
	}
}

func (n *DBFSNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	req := ParseRequest(&n.Inode, "")
	if req == nil {
		return nil, 0, syscall.EINVAL
	}

	return &DBFSFileHandle{node: n, req: req}, 0, 0
}

func (h *DBFSNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	log.Println("setattr")
	return 0
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
