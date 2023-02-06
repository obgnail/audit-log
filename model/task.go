package model

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/compose/mysql"
	"gopkg.in/gorp.v1"
)

const (
	TaskStatusNormal  = 1
	TaskStatusDeleted = 0
)

type Task struct {
	UUID       string `db:"uuid" json:"uuid" dbmode:"r"`
	Owner      string `db:"owner" json:"owner"`
	CreateTime int64  `db:"create_time" json:"create_time" dbmode:"r"`
	GroupUUID  string `db:"group_uuid" json:"group_uuid"`
	Status     int    `db:"status" json:"status"`
	Path       string `db:"path" json:"path"`
	Name       string `db:"name" json:"name"`
	Desc       string `db:"desc" json:"desc"`
	ParentUUID string `db:"parent_uuid" json:"parent_uuid"`
}

func ListTasks(src gorp.SqlExecutor) ([]*Task, error) {
	sql := `SELECT uuid, owner, create_time, group_uuid, status, path, name, desc, parent_uuid FROM task;`

	tasks := make([]*Task, 0)
	_, err := src.Select(&tasks, sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tasks, err
}

func AddTask(tx *gorp.Transaction, task *Task) error {
	sql := "INSERT INTO `task` (uuid, owner, create_time, group_uuid, status, path, name, desc, parent_uuid) "
	sql += "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"

	args, _ := mysql.BuildSqlArgs(
		task.UUID,
		task.Owner,
		task.CreateTime,
		task.GroupUUID,
		task.Status,
		task.Path,
		task.Name,
		task.Desc,
		task.ParentUUID,
	)
	if _, err := tx.Exec(sql, args...); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func DeleteTaskByUUID(tx *gorp.Transaction, uuids ...string) error {
	if len(uuids) == 0 {
		return nil
	}
	sql := "UPDATE `task` SET `status` = ? WHERE `uuid` IN (%s);"
	sql = fmt.Sprintf(sql, mysql.SqlPlaceholders(len(uuids)))

	args, _ := mysql.BuildSqlArgs(TaskStatusDeleted, uuids)
	_, err := tx.Exec(sql, args...)
	return errors.Trace(err)
}

func UpdateTask(tx *gorp.Transaction, task *Task) error {
	sql := "UPDATE `task` "
	sql += "SET owner=?, `create_time`=?, `group_uuid`=?, `status`=?, `path`=?, `name`=?, `desc`=?, `parent_uuid`=? "
	sql += "WHERE `uuid` = ? "

	args, _ := mysql.BuildSqlArgs(
		task.Owner,
		task.CreateTime,
		task.GroupUUID,
		task.Status,
		task.Path,
		task.Name,
		task.Desc,
		task.ParentUUID,
		task.UUID,
	)
	if _, err := tx.Exec(sql, args...); err != nil {
		return errors.Trace(err)
	}
	return nil
}
