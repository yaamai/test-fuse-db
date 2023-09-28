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
}

type DBFSNode struct {
	fs.Inode
	RootData *DBFS
}

func (n *DBFSNode) EmbeddedInode() *fs.Inode {
	return &n.Inode
}

func (n *DBFSNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// request to root
	if n.IsRoot() {
		datas := []DataGroup{}
		err := n.RootData.db.Select(&datas, "SELECT id, name FROM data;")
		log.Println("readdir: query", err, len(datas))

		entries := []fuse.DirEntry{}
		for _, data := range datas {
			entries = append(entries, fuse.DirEntry{syscall.S_IFDIR, data.Name, data.ID*10})
		}
		return fs.NewListDirStream(entries), 0
	}

	// request to group
	name := n.Path(n.Root())
	log.Println("readdir:", name)

	datas := []DataGroup{}
	err := n.RootData.db.Select(&datas, "SELECT id, name FROM data WHERE name = $1;", name)
	log.Println("query", err, len(datas))

	if err != nil || len(datas) != 1 {
		return nil, syscall.ENOENT
	}

	entries := []fuse.DirEntry{
		{syscall.S_IFREG, "dummy", datas[0].ID+1},
	}
	return fs.NewListDirStream(entries), 0
}

func (n *DBFSNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Println("lookup:", n.Path(n.Root()))

	if n.IsRoot() {
		datas := []DataGroup{}
		err := n.RootData.db.Select(&datas, "SELECT id, name FROM data WHERE name = $1;", name)
		log.Println("query", err, len(datas))

		node := &DBFSNode{RootData: n.RootData}
		attr := fs.StableAttr{
			Mode: syscall.S_IFDIR,
			Gen: 1,
			Ino: datas[0].ID*10,
		}
		return n.NewInode(ctx, node, attr), 0
	}

	datas := []DataGroup{}
	err := n.RootData.db.Select(&datas, "SELECT id, name FROM data WHERE name = $1;", n.Path(n.Root()))
	log.Println("query", err, len(datas))

	node := &DBFSNode{RootData: n.RootData}
	attr := fs.StableAttr{
		Mode: syscall.S_IFREG,
		Gen: 1,
		Ino: datas[0].ID+1,
	}
	return n.NewInode(ctx, node, attr), 0
}

func main() {
	db, err := sqlx.Open("postgres", "postgres://postgres:password@localhost/postgres?sslmode=disable")
	if err != nil {
		log.Printf("sql.Open error %s", err)
	}

	root := &DBFSNode{RootData: &DBFS{db: db}}
	
	server, err := fs.Mount("/tmp/aa", root, &fs.Options{
		MountOptions: fuse.MountOptions{
			DirectMount: true,
			Debug: true,
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
