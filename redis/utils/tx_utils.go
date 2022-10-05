package transaction

import (
	"gokv/redis/aof"
	"gokv/redis/database"
	"gokv/redis/protocol"
	"gokv/utils"
)

func ReadFirstKey(args [][]byte) ([]string, []string) {
	// assert len(args) > 0
	key := string(args[0])
	return nil, []string{key}
}

func WriteFirstKey(args [][]byte) ([]string, []string) {
	key := string(args[0])
	return []string{key}, nil
}

func RollbackFirstKey(sdb *database.SingleDB, args [][]byte) []database.CmdLine {
	key := string(args[0])
	return rollbackGivenKeys(sdb, key)
}

func rollbackGivenKeys(sdb *database.SingleDB, keys ...string) []database.CmdLine {
	// rollbackGivenKeys 是在实际执行事务命令之前
	var undoCmdLines []database.CmdLine
	for _, key := range keys {
		entity, exists := sdb.GetEntity(key)
		if !exists {
			undoCmdLines = append(undoCmdLines, utils.ToCmdLine("DEL", key))
		} else {
			undoCmdLines = append(
				undoCmdLines,
				utils.ToCmdLine("DEL", key),
				aof.EntityToCmd(key, entity).Args,
				sdb.ToTTLCmd(key).(*protocol.MultiBulkReply).Args,
			)
		}
	}
	return undoCmdLines
}
