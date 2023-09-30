package main

import (
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type DB struct {
	*sqlx.DB
	dataNames []string
}

type DataGroup struct {
	ID uint64 `db:"id"`
	Name string `db:"name"`
}

type Data struct {
	ID uint64 `db:"id"`
	Name string `db:"name"`
	Data string `db:"data"`
}

func (d *DB) CreateGroup(groupName string) (error) {
	_, err := d.Queryx("INSERT INTO data(name) VALUES($1);", groupName)
	return err
}

func (d *DB) PutData(groupName string, dataName string, data string) error {
	_, err := d.Queryx("UPDATE data SET " + dataName + " = $1 WHERE name = $2;", data, groupName)
	return err
}

func (d *DB) ListGroups() ([]DataGroup, error) {
	datas := []DataGroup{}
	// to assign unique id across group and data, group-id *16 (max 16 datas)
	err := d.Select(&datas, "SELECT id*16 AS id, name FROM data;")
	if err != nil {
		return nil, err
	}
	return datas, nil
}

func (d *DB) ListDatas(groupName string) ([]Data, error) {
	group, err := d.GetGroup(groupName)
	if err != nil {
		return nil, err
	}
	if group == nil {
		return []Data{}, nil
	}

	baseInodeNum := group.ID
	datas := []Data{}
	for idx, d := range d.dataNames {
		datas = append(datas, Data{ID: baseInodeNum + uint64(idx+1), Name: d})
	}

	return datas, nil
}

func (d *DB) GetGroup(groupName string) (*DataGroup, error) {
	datas := []DataGroup{}
	// to assign unique id across group and data, group-id *16 (max 16 datas)
	err := d.Select(&datas, "SELECT id*16 AS id, name FROM data WHERE name = $1;", groupName)
	if err != nil {
		return nil, err
	}

	if len(datas) == 0 {
		return nil, nil
	}

	return &datas[0], nil
}

func (d *DB) GetData(groupName string, dataName string) (*Data, error) {
	// get index in dataNames list
	var index uint64
	for idx, n := range d.dataNames {
		if n == dataName {
			index = uint64(idx) + 1
		}
	}
	if index == 0 {
		return nil, nil
	}

	datas := []Data{}
	err := d.Select(&datas, "SELECT id, to_json(" + dataName + ")::text AS data FROM data WHERE name = $1;", groupName)
	if err != nil {
		return nil, err
	}

	if len(datas) == 0 {
		return nil, nil
	}

	datas[0].ID = datas[0].ID*16 + index
	datas[0].Name = dataName
	return &datas[0], nil
}
