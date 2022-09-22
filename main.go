package main

import (
	"bytes"
	"go-fluentd-demo/library"
	"io"
	"net"
	"strings"

	"github.com/cihub/seelog"
	"github.com/tinylib/msgp/msgp"
)

func main() {
	lsn, err := net.Listen("tcp", ":24224")
	if err != nil {
		panic(err)
	}
	for {
		if conn, err := lsn.Accept(); err != nil {
			continue
		} else {
			go handleConn(conn)
		}
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	var (
		reader = msgp.NewReader(conn)
		v      = library.FluentBatchMsg{nil, nil, nil} // tag, time, messages
		err    error
		eof    = msgp.WrapError(io.EOF)
		// ok     bool
	)

	for {
		seelog.Flush()

		if err = v.DecodeMsg(reader); err == eof {
			// seelog.Debugf("remote %v closed", conn.RemoteAddr().String())
			return
		} else if err != nil {
			seelog.Debugf("decode message failed: %v", err)
			return
		}

		if len(v) < 2 {
			seelog.Debugf("discard msg since unknown message format, length should be 2: %+v", v)
			continue
		}

		for i, vv := range v {
			seelog.Debugf("[%v] [%T]", i, vv)
			// if str, ok := vv.(string); ok {
			// 	seelog.Debugf("string ==> %v", str)
			// }
		}

		var tag string
		switch msgTag := v[0].(type) {
		case []byte:
			tag = string(msgTag)
		case string:
			tag = msgTag
		default:
			seelog.Debugf("discard msg since unknown message format, message[0] is not `[]byte` or string: %+v", v[0])
			continue
		}
		seelog.Debugf("got message tag: %v", tag)

		switch msgBody := v[1].(type) {
		case []interface{}:
			for _, entryI := range msgBody {
				var ok bool
				msg := &library.FluentMsg{}
				if msg.Message, ok = entryI.([]interface{})[1].(map[string]interface{}); !ok {
					seelog.Debugf("discard msg since unknown message format, cannot decode: %v", tag)
					continue
				}
				msg.Tag = tag
				ProcessMessage(msg)
			}
		case []byte, string: // embedded format
			var reader2 *msgp.Reader
			if m, ok := msgBody.([]byte); ok {
				reader2 = msgp.NewReader(bytes.NewReader(m))
			} else if m, ok := msgBody.(string); ok {
				reader2 = msgp.NewReader(strings.NewReader(m))
			}
			// reader2 = msgp.NewReader(bytes.NewReader(msgBody))

			v2 := library.FluentBatchMsg{nil, nil, nil} // tag, time, messages

			for {
				if err = v2.DecodeMsg(reader2); err == eof {
					seelog.Debugf("decodeMsg = eof")
					break
				} else if err != nil {
					seelog.Debugf("discard msg since unknown message format, cannot decode")
					continue
				} else if len(v2) < 2 {
					seelog.Debugf("discard msg since unknown message format, length should be 2")
					continue
				} else {
					var ok bool
					msg := &library.FluentMsg{}
					if msg.Message, ok = v2[1].(map[string]interface{}); !ok {
						seelog.Debugf("discard msg since unknown message format")
						continue
					}
					msg.Tag = tag
					ProcessMessage(msg)
				}
			}
		default:
			if len(v) < 3 {
				seelog.Debugf("discard msg since unknown message format for length, length should be 3")
				continue
			}
			msg := &library.FluentMsg{}

			switch msgBody := v[2].(type) {
			case map[string]interface{}:
				msg.Message = msgBody
			default:
				seelog.Debugf("discard msg since unknown msg format")
				continue
			}
			msg.Tag = tag
			ProcessMessage(msg)
		}
	}
}

func ProcessMessage(msg *library.FluentMsg) {
	// seelog.Debugf("message: %v", msg.Message)
	for k, v := range msg.Message {
		if m, ok := v.([]uint8); ok {
			seelog.Debugf("> [%v] %+v", k, string(m))
		} else if m, ok := v.(string); ok {
			seelog.Debugf(">> [%v] %+v", k, m)
		} else {
			seelog.Debugf("[%v] %T", k, v)
		}
	}
}
