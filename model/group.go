package model

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/compose/mysql"
	"gopkg.in/gorp.v1"
)

const (
	GroupStatusNormal  = 1
	GroupStatusDeleted = 0
)

type Group struct {
	UUID       string `db:"uuid" json:"uuid"`
	Owner      string `db:"owner" json:"owner"`
	Name       string `db:"name" json:"name"`
	Desc       string `db:"desc" json:"desc"`
	CreateTime int64  `db:"create_time" json:"create_time"`
	Status     int    `db:"status" json:"status"`
}

func ListGroups(src gorp.SqlExecutor) ([]*Group, error) {
	sql := `SELECT uuid, owner, name, desc, create_time, status FROM group;`

	groups := make([]*Group, 0)
	_, err := src.Select(&groups, sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return groups, err
}

func AddGroup(tx *gorp.Transaction, group *Group) error {
	sql := "INSERT INTO `group` (uuid, owner, name, desc, create_time, status) "
	sql += "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"

	args, _ := mysql.BuildSqlArgs(
		group.UUID,
		group.Owner,
		group.Name,
		group.Desc,
		group.CreateTime,
		group.Status,
	)
	if _, err := tx.Exec(sql, args...); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func DeleteGroupByUUID(tx *gorp.Transaction, uuids ...string) error {
	if len(uuids) == 0 {
		return nil
	}
	sql := "UPDATE `group` SET `status` = ? WHERE `uuid` IN (%s);"
	sql = fmt.Sprintf(sql, mysql.SqlPlaceholders(len(uuids)))

	args, _ := mysql.BuildSqlArgs(GroupStatusDeleted, uuids)
	_, err := tx.Exec(sql, args...)
	return errors.Trace(err)
}

func UpdateGroup(tx *gorp.Transaction, group *Group) error {
	sql := "UPDATE `group` "
	sql += "SET owner=?, `name`=?, `desc`=?, `create_time`=?, `status`=? "
	sql += "WHERE `uuid` = ? "

	args, _ := mysql.BuildSqlArgs(
		group.Owner,
		group.Name,
		group.Desc,
		group.CreateTime,
		group.Status,
		group.UUID,
	)
	if _, err := tx.Exec(sql, args...); err != nil {
		return errors.Trace(err)
	}
	return nil
}
