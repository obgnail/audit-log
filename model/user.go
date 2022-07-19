package model

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/compose/mysql"
	"gopkg.in/gorp.v1"
	"time"
)

const (
	UserStatusNormal  = 1
	UserStatusDeleted = 0
)

type User struct {
	UUID       string  `db:"uuid" json:"uuid"`
	Name       string  `db:"name" json:"name"`
	NamePinyin string  `db:"name_pinyin" json:"name_pinyin"`
	Email      *string `db:"email" json:"email"`
	Avatar     string  `db:"avatar" json:"avatar"`
	Phone      *string `db:"phone" json:"phone"`
	Status     int     `db:"status" json:"status"`
	CreateTime int64   `db:"create_time" json:"create_time"`
	ModifyTime int64   `db:"modify_time" json:"modify_time"`
}

func ListUsers(src gorp.SqlExecutor) ([]*User, error) {
	sql := `SELECT uuid, name, name_pinyin, email, avatar, phone, status, create_time, modify_time FROM user;`

	users := make([]*User, 0)
	_, err := src.Select(&users, sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return users, err
}

func AddUser(tx *gorp.Transaction, user *User) error {
	sql := "INSERT INTO `user` (uuid, name, name_pinyin, email, avatar, phone, status, create_time, modify_time) "
	sql += "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"

	args, _ := mysql.BuildSqlArgs(
		user.UUID,
		user.Name,
		user.NamePinyin,
		*user.Email,
		user.Avatar,
		*user.Phone,
		user.Status,
		user.CreateTime,
		user.ModifyTime,
	)
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
	sql = fmt.Sprintf(sql, mysql.SqlPlaceholds(len(uuids)))

	args, _ := mysql.BuildSqlArgs(UserStatusDeleted, uuids)
	_, err := tx.Exec(sql, args...)
	return errors.Trace(err)
}

func UpdateUserModifyTime(tx *gorp.Transaction, userUUID string) error {
	sql := "UPDATE `user` SET modify_time=? WHERE `uuid` = ? "
	args, _ := mysql.BuildSqlArgs(
		time.Now().UnixNano(),
		userUUID,
	)
	if _, err := tx.Exec(sql, args...); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func UpdateUser(tx *gorp.Transaction, user *User) error {
	sql := "UPDATE `user` "
	sql += "SET name=?, `name_pinyin`=?, `email`=?, `avatar`=?, `phone`=?, modify_time=? "
	sql += "WHERE `uuid` = ? "

	args, _ := mysql.BuildSqlArgs(
		user.Name,
		user.NamePinyin,
		*user.Email,
		user.Avatar,
		*user.Phone,
		user.ModifyTime,
		user.UUID,
	)
	if _, err := tx.Exec(sql, args...); err != nil {
		return errors.Trace(err)
	}
	return nil
}
