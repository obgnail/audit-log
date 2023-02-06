package main

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/compose/mysql"
	"gopkg.in/gorp.v1"
)

const (
	UserStatusNormal  = 1
	UserStatusDeleted = 0
)

type User struct {
	UUID   string `db:"uuid" json:"uuid"`
	Name   string `db:"name" json:"name"`
	Email  string `db:"email" json:"email"`
	Status int    `db:"status" json:"status"`
}

func ListUsers(src gorp.SqlExecutor) ([]*User, error) {
	sql := `SELECT uuid, name, email, status FROM user;`

	users := make([]*User, 0)
	_, err := src.Select(&users, sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return users, err
}

func AddUser(tx *gorp.Transaction, user *User) error {
	sql := "INSERT INTO `user` (uuid, name, email, status) VALUES (?, ?, ?, ?);"
	args, _ := mysql.BuildSqlArgs(user.UUID, user.Name, user.Email, user.Status)
	if _, err := tx.Exec(sql, args...); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func DeleteUserByUUID(tx *gorp.Transaction, uuids ...string) error {
	if len(uuids) == 0 {
		return nil
	}
	sql := "UPDATE `user` SET `status` = ? WHERE `uuid` IN (%s);"
	sql = fmt.Sprintf(sql, mysql.SqlPlaceholders(len(uuids)))
	args, _ := mysql.BuildSqlArgs(UserStatusDeleted, uuids)
	_, err := tx.Exec(sql, args...)
	return errors.Trace(err)
}

func UpdateUser(tx *gorp.Transaction, user *User) error {
	sql := "UPDATE `user` SET name=?, `email`=? WHERE `uuid` = ? "
	args, _ := mysql.BuildSqlArgs(user.Name, user.Email, user.UUID)
	if _, err := tx.Exec(sql, args...); err != nil {
		return errors.Trace(err)
	}
	return nil
}
