package router

import (
	"gokv/redis/database"
	"strings"
)

const (
	FlagWrite    = 0
	FlagReadOnly = 1
)

func RegisterCommand(
	name string,
	executor database.ExecFunc,
	prepare database.PreFunc,
	rollback database.UndoFunc,
	arity int,
	flags int,
) {
	name = strings.ToLower(name)
	database.CmdTable[name] = &database.Command{
		Executor: executor,
		Prepare:  prepare,
		Undo:     rollback,
		Arity:    arity,
		Flags:    flags,
	}
}

func isReadOnlyCommand(name string) bool {
	name = strings.ToLower(name)
	cmd, ok := database.CmdTable[name]
	if !ok {
		return false
	}
	return cmd.Flags&FlagReadOnly > 0
}
