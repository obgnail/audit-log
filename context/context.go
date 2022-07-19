package context

import (
	"fmt"
	"strconv"
	"strings"
)

var (
	CorruptedDataError = fmt.Errorf("CorruptedDataError")
	MismatchError      = fmt.Errorf("MismatchError")
)

type Context struct {
	Type   int    `db:"context_type"`
	Param1 string `db:"context_param_1"`
	Param2 string `db:"context_param_2"`
}

// 将 Context 序列化成可读字符串
func (c Context) String() string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(c.Type))
	b.WriteByte('.')
	if len(c.Param1) > 0 {
		b.WriteString(c.Param1)
	}
	b.WriteByte('.')
	if len(c.Param2) > 0 {
		b.WriteString(c.Param2)
	}
	return b.String()
}

// 根据可读字符串生成 Context
func FromString(s string) (Context, error) {
	c := Context{}
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return c, CorruptedDataError
	}
	t, err := strconv.Atoi(parts[0])
	if err != nil {
		return c, MismatchError
	}
	c.Type = t
	if len(parts[1]) > 0 {
		c.Param1 = parts[1]
	}
	if len(parts[2]) > 0 {
		c.Param2 = parts[2]
	}
	return c, nil
}

func BuildContext(ctxType int, params ...string) Context {
	param1 := ""
	param2 := ""
	if len(params) == 1 {
		param1 = params[0]
	}
	if len(params) == 2 {
		param1 = params[0]
		param2 = params[1]
	}
	return Context{
		Type:   ctxType,
		Param1: param1,
		Param2: param2,
	}
}
