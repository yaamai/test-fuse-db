package main

import (
	"log"
	"syscall"
	"context"
//	"path/filepath"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type DBFS struct {
	fs.Inode
	db *sqlx.DB
}

type DBFSLister struct {
}

func (d *DBFS) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFDIR|0755
	log.Println("getattr", d.IsRoot(), out.Attr)
	return 0
}

type DataGroup struct {
	ID uint64 `db:"id"`
	Name string `db:"name"`
}

func (d *DBFS) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	datas := []DataGroup{}
	err := d.db.Select(&datas, "SELECT id, name FROM data;")

	log.Println("query", err, len(datas))

	entries := []fuse.DirEntry{}
	for _, data := range datas {
		entries = append(entries, fuse.DirEntry{syscall.S_IFDIR, data.Name, data.ID*10})
	}
	return fs.NewListDirStream(entries), 0
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


func main() {
	db, err := sqlx.Open("postgres", "postgres://postgres:password@localhost/postgres?sslmode=disable")
	if err != nil {
		log.Printf("sql.Open error %s", err)
	}
	
	mntDir := "/tmp/aa"
	root := &DBFS{db: db}

	server, err := fs.Mount(mntDir, root, &fs.Options{
		MountOptions: fuse.MountOptions{DirectMount: true, Debug: true},
	})
	if err != nil {
		log.Panic(err)
	}

	log.Printf("Mounted on %s", mntDir)
	log.Printf("Unmount by calling 'fusermount -u %s'", mntDir)

	// Wait until unmount before exiting
	server.Wait()
}
