package canal

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/juju/errors"
	"github.com/obgnail/audit-log/config"
)

func NewRiver(addr string, user string, password string) (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = addr
	cfg.User = user
	cfg.Password = password
	cfg.Flavor = "mysql"
	cfg.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return c, nil
}

func NewDefaultRiver(addr string, user string, password string, handler canal.EventHandler) error {
	c, err := NewRiver(addr, user, password)
	if err != nil {
		return errors.Trace(err)
	}
	coords, err := c.GetMasterPos()
	if err != nil {
		return errors.Trace(err)
	}

	c.SetEventHandler(handler)

	if err := c.RunFrom(coords); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func InitCanal() error {
	connStr := fmt.Sprintf(`%s:%d`, config.String("mysql_host", "127.0.0.1"), config.Int("mysql_port", 3306))
	user := config.String("mysql_user", "root")
	password := config.String("mysql_password", "root")
	dbs := config.StringSlice("canal_listen_schema")
	tables := config.StringSlice("canal_filter_table")

	handler := NewBinlogHandler(dbs, tables)
	err := NewDefaultRiver(connStr, user, password, handler)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
