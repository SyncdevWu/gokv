package parser

import (
	"bytes"
	"gokv/redis/protocol"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseStream(t *testing.T) {
	replies := []protocol.Reply{
		protocol.NewIntReply(1),
		protocol.NewStatusReply("OK"),
		protocol.NewErrReply("ERR unknown"),
		protocol.NewBulkReply([]byte("a\r\nb")), // test binary safe
		protocol.NewNullBulkReply(),
		protocol.NewMultiBulkReply([][]byte{
			[]byte("a"),
			[]byte("\r\n"),
		}),
		protocol.NewEmptyMultiBulkReply(),
	}
	reqs := bytes.Buffer{}
	for _, re := range replies {
		reqs.Write(re.ToBytes())
	}
	reqs.Write([]byte("set a a" + protocol.CRLF)) // test text reply
	expected := make([]protocol.Reply, len(replies))
	copy(expected, replies)
	expected = append(expected, protocol.NewMultiBulkReply([][]byte{
		[]byte("set"), []byte("a"), []byte("a"),
	}))

	ch := ParseStream(bytes.NewReader(reqs.Bytes()))
	i := 0
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF {
				return
			}
			t.Error(payload.Err)
			return
		}
		if payload.Data == nil {
			t.Error("empty data")
			return
		}
		exp := expected[i]
		i++
		assert.Equal(t, exp.ToBytes(), payload.Data.ToBytes())
	}
}
