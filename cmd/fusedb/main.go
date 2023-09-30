package main

import (
	"log"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/jmoiron/sqlx"
	"github.com/alecthomas/kong"
	_ "github.com/lib/pq"
)

var CLI struct {
	DSN string `default:"postgres://postgres:password@localhost/postgres?sslmode=disable"`
	Filenames []string `json:"filenames"`
	MountPoint string `arg:"" json:"mount_point"`
	Debug bool `json:"debug"`
	DirectMount bool `json:"direct_mount"`
}

func main() {
	kong.Parse(&CLI, kong.DefaultEnvars("fusedb"), kong.Configuration(kong.JSON, "fusedb.json"))

	db, err := sqlx.Open("postgres", CLI.DSN)
	if err != nil {
		log.Fatalln(err)
	}

	dbfs := &DBFS{db: &DB{DB: db, dataNames: CLI.Filenames}}
	root := &DBFSNode{RootData: dbfs}
	
	server, err := fs.Mount(CLI.MountPoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			DirectMount: CLI.DirectMount,
			Debug: CLI.Debug,
		},
	})
	if err != nil {
		log.Fatalln(err)
	}

	server.Wait()
}
