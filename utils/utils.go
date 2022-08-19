package utils

func ToCmdLine(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for i, arg := range cmd {
		args[i] = []byte(arg)
	}
	return args
}

func ToCmdLine2[T string | []byte](cmdName string, args ...T) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(cmdName)
	for i, arg := range args {
		result[i+1] = []byte(arg)
	}
	return result
}
