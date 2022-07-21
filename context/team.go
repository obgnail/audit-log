package context

const (
	ContextTypeTeam = iota // 有需要可以继续定义其他的context
)

func TeamContext(teamUUID string) Context {
	return Context{
		Type:   ContextTypeTeam,
		Param1: teamUUID,
	}
}
