package main

import (
	"log"
	"syscall"
	"context"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type DataGroup struct {
	ID uint64 `db:"id"`
	Name string `db:"name"`
}

type DBFS struct {
	db *sqlx.DB
	datas []string
}

type DBFSNode struct {
	fs.Inode
	RootData *DBFS
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
		datas := []DataGroup{}
		err := n.RootData.db.Select(&datas, "SELECT id, name FROM data;")
		log.Println("readdir: query", err, len(datas))

		entries := []fuse.DirEntry{}
		for _, data := range datas {
			entries = append(entries, fuse.DirEntry{syscall.S_IFDIR, data.Name, data.ID*10})
		}
		return fs.NewListDirStream(entries), 0
	}

	if req.Group != "" && req.Data == "" {
		datas := []DataGroup{}
		err := n.RootData.db.Select(&datas, "SELECT id, name FROM data WHERE name = $1;", req.Group)
		log.Println("query", err, len(datas))

		if err != nil || len(datas) != 1 {
			return nil, syscall.ENOENT
		}

		baseInodeNum := datas[0].ID
		entries := []fuse.DirEntry{}
		for idx, d := range n.RootData.datas {
			entries = append(entries, fuse.DirEntry{
				syscall.S_IFREG,
				d,
				baseInodeNum + uint64(idx+1),
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
		datas := []DataGroup{}
		err := n.RootData.db.Select(&datas, "SELECT id, name FROM data WHERE name = $1;", req.Group)
		if err != nil || len(datas) != 1 {
			return nil, syscall.ENOENT
		}

		node := &DBFSNode{RootData: n.RootData}
		attr := fs.StableAttr{
			Mode: syscall.S_IFDIR,
			Gen: 1,
			Ino: datas[0].ID*10,
		}
		return n.NewInode(ctx, node, attr), 0
	}

	if req.Data != "" {
		datas := []DataGroup{}
		err := n.RootData.db.Select(&datas, "SELECT id, name FROM data WHERE name = $1;", req.Group)
		if err != nil || len(datas) != 1 {
			return nil, syscall.ENOENT
		}
		baseInodeNum := datas[0].ID

		var inodeNum uint64
		for idx, d := range n.RootData.datas {
			if d == req.Data {
				inodeNum = uint64(idx+1)
			}
		}
		if inodeNum == 0 {
			return nil, syscall.ENOENT
		}

		node := &DBFSNode{RootData: n.RootData}
		attr := fs.StableAttr{
			Mode: syscall.S_IFREG,
			Gen: 1,
			Ino: baseInodeNum+inodeNum,
		}
		return n.NewInode(ctx, node, attr), 0
	}

	return nil, syscall.ENOENT
}

func (n *DBFSNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Println("open:", n.parseRequest(""))
	return nil, 0, 0
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
		out.Size = 123
		return 0
	}

	return 0
}

func main() {
	db, err := sqlx.Open("postgres", "postgres://postgres:password@localhost/postgres?sslmode=disable")
	if err != nil {
		log.Printf("sql.Open error %s", err)
	}

	root := &DBFSNode{RootData: &DBFS{db: db, datas: []string{"ho", "ge", "fu"}}}
	
	server, err := fs.Mount("/tmp/aa", root, &fs.Options{
		MountOptions: fuse.MountOptions{
			DirectMount: false,
			Debug: false,
		},
	})
	if err != nil {
		log.Panic(err)
	}

	server.Wait()
}

/*
type DBFS struct {
}

type DBFSLister struct {
}

func (d *DBFS) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFDIR|0755
	log.Println("getattr", d.IsRoot(), out.Attr)
	return 0
}




func (d *DBFS) EmbeddedInode() *fs.Inode {
	return &d.Inode
}

func (d *DBFS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Println("lookup", name, d.IsRoot(), d.Inode.String())

	datas := []DataGroup{}
	err := d.db.Select(&datas, "SELECT id, name FROM data WHERE name = $1;", name)
	log.Println("query", err, len(datas))

	return d.NewInode(ctx, d, fs.StableAttr{syscall.S_IFDIR, datas[0].ID, 0}), 0
}


	mntDir := "/tmp/aa"
	root := &DBFS{db: db}


	log.Printf("Mounted on %s", mntDir)
	log.Printf("Unmount by calling 'fusermount -u %s'", mntDir)

	// Wait until unmount before exiting
	server.Wait()
*/
