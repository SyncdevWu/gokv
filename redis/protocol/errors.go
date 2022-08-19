package protocol

var (
	unknownErrBytes      = []byte("-Err unknown\r\n")
	syntaxErrBytes       = []byte("-Err syntax error\r\n")
	wrongTypeErrBytes    = []byte("-Err Operation against a key holding the wrong kind of value\r\n")
	theUnknownErrReply   = &UnknownErrReply{}
	theSyntaxErrReply    = &SyntaxErrReply{}
	theWrongTypeErrReply = &WrongTypeErrReply{}
)

type UnknownErrReply struct{}

func (r *UnknownErrReply) ToBytes() []byte {
	return unknownErrBytes
}

func (r *UnknownErrReply) Error() string {
	res := string(unknownErrBytes)
	return res[1 : len(res)-len(CRLF)]
}

func NewUnknownErrReply() *UnknownErrReply {
	return theUnknownErrReply
}

type ArgNumErrReply struct {
	Cmd string
}

func (r *ArgNumErrReply) Error() string {
	res := string(r.ToBytes())
	return res[1 : len(res)-len(CRLF)]
}

func (r *ArgNumErrReply) ToBytes() []byte {
	return []byte("-ERR wrong number of arguments for '" + r.Cmd + "' command\r\n")
}

func NewArgNumErrReply(cmd string) *ArgNumErrReply {
	return &ArgNumErrReply{
		Cmd: cmd,
	}
}

type SyntaxErrReply struct{}

func (r *SyntaxErrReply) Error() string {
	res := string(r.ToBytes())
	return res[1 : len(res)-len(CRLF)]
}

func (r *SyntaxErrReply) ToBytes() []byte {
	return syntaxErrBytes
}

func NewSyntaxErrReply() *SyntaxErrReply {
	return theSyntaxErrReply
}

type WrongTypeErrReply struct{}

func (r *WrongTypeErrReply) Error() string {
	res := string(r.ToBytes())
	return res[1 : len(res)-len(CRLF)]
}

func (r *WrongTypeErrReply) ToBytes() []byte {
	return wrongTypeErrBytes
}

func NewWrongTypeErrReply() *WrongTypeErrReply {
	return theWrongTypeErrReply
}

type ProtocolErrReply struct {
	Msg string
}

func (r *ProtocolErrReply) Error() string {
	res := string(r.ToBytes())
	return res[1 : len(res)-len(CRLF)]
}

func (r *ProtocolErrReply) ToBytes() []byte {
	return []byte("-ERR Protocol error: '" + r.Msg + "'\r\n")
}

func NewProtocolErrReply(msg string) *ProtocolErrReply {
	return &ProtocolErrReply{
		Msg: msg,
	}
}
