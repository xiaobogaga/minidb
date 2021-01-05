package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/xiaobogaga/storage"
	"github.com/xiaobogaga/util"
	"io"
	"net"
	"os"
	"time"
)

type ConnectionWrapperInterface interface {
	CurrentDB() *string
	SendErrMsg(msg ErrMsg)
	SendQueryResult(ret *storage.RecordBatch) ErrMsg
}

var connectionWrapperLog = util.GetLog("ConnectionWrapper")

type connectionWrapper struct {
	id            uint32
	conn          net.Conn
	readTimeout   time.Duration
	writeTimeout  time.Duration
	packetCounter byte
	ctx           context.Context
	session       Session
}

func NewConnectionWrapper(readTimeout, writeTimeout time.Duration, ctx context.Context) *connectionWrapper {
	return &connectionWrapper{
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
		ctx:          ctx,
	}
}

type ErrCodeType byte

const (
	ErrorOk ErrCodeType = iota
	ErrorOkQuery
	ErrorNetUnknown
	ErrorNetTimeout
	ErrorNetClosed
	ErrorNetPacketOutOfOrder
	ErrUnknownCommand
	ErrSyntax
	ErrQuery
	ErrSendQueryResult
	ErrMsgFormat
	ErrPacketType
)

func wrapNetErrToErrMsg(err error) ErrMsg {
	if err == nil {
		return OkMsg
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return ErrMsg{errCode: ErrorNetTimeout, Msg: err.Error()}
	}
	if err == io.EOF {
		return ErrMsg{errCode: ErrorNetClosed, Msg: err.Error()}
	}

	return ErrMsg{errCode: ErrorNetUnknown, Msg: err.Error()}
}

func packetCounterErrMsg(packetCounter byte, expectedPacketCounter byte) ErrMsg {
	if packetCounter != expectedPacketCounter {
		return ErrMsg{errCode: ErrorNetPacketOutOfOrder, Msg: ErrCodeMsgMap[ErrorNetPacketOutOfOrder]}
	}
	return OkMsg
}

// The package will send to client. The format look like this:
// +-------------+-----------------+---------+
// + package len + package counter + package +
// +-------------+-----------------+---------+
func WritePacket(conn net.Conn, packetCounter byte, packet bytes.Buffer, writeTimeout time.Duration) ErrMsg {
	err := conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	errMsg := wrapNetErrToErrMsg(err)
	if !errMsg.IsOk() {
		return errMsg
	}
	packetLen := packet.Len()
	bs := int4ToBytes(uint32(packetLen))
	bs = append(bs, packetCounter)
	buf := bytes.NewBuffer(bs)
	buf.Write(packet.Bytes())
	_, err = buf.WriteTo(conn)
	errMsg = wrapNetErrToErrMsg(err)
	return errMsg
}

// The client package looks like:
// +-------------+------------------+------------+
// + package len +  package counter +  packet    +
// +-------------+------------------+------------+
func ReadPacket(conn net.Conn, packetCounter byte, readTimeout time.Duration) ([]byte, ErrMsg) {
	err := conn.SetReadDeadline(time.Now().Add(readTimeout))
	errMsg := wrapNetErrToErrMsg(err)
	if !errMsg.IsOk() {
		return nil, errMsg
	}
	bs := [5]byte{}
	_, err = io.ReadFull(conn, bs[:])
	errMsg = wrapNetErrToErrMsg(err)
	if !errMsg.IsOk() {
		return nil, errMsg
	}
	packetLen := BytesToInt4(bs[:4])
	clientPacketCounter := bs[4]
	if clientPacketCounter != packetCounter {
		return nil, packetCounterErrMsg(clientPacketCounter, packetCounter)
	}
	packet := make([]byte, packetLen)
	_, err = io.ReadFull(conn, packet)
	return packet, wrapNetErrToErrMsg(err)
}

type MsgType byte

const (
	OkMsgType = iota
	ErrMsgType
	DataMsgType
)

// There are several different packet types, before list the packet type,
// The packet encoding looks like this:
// +-------------+-------+
// + packet type +  msg  +
// +-------------+-------+
// So each message has its own specific msg format.

// Send OK_Packet
// +-------------+------------+--------+
// + packet type + msg len    +  msg   +
// +-------------+------------+--------+
// +      0      +
// +-------------+
func (wrap *connectionWrapper) sendOk(okMsg ErrMsg) ErrMsg {
	buf := bytes.Buffer{}
	buf.WriteByte(OkMsgType)
	message := okMsg.Msg
	buf.Write(int4ToBytes(uint32(len(message))))
	buf.Write([]byte(message))
	connectionWrapperLog.InfoF("send ok packet.")
	return WritePacket(wrap.conn, wrap.packetCounter, buf, wrap.writeTimeout)
}

// Send Err_Packet.
// +-------------+-----------+-----------+
// + packet type +  err_code + pack len  +
// +-------------+-----------+-----------+
// +      1      +
// +-------------+
func (wrap *connectionWrapper) sendErr(err ErrMsg) ErrMsg {
	buf := bytes.Buffer{}
	buf.WriteByte(ErrMsgType)
	buf.WriteByte(byte(err.errCode))
	buf.Write(int4ToBytes(uint32(len(err.Msg))))
	buf.Write([]byte(err.Msg))
	connectionWrapperLog.InfoF("send err packet. err: %+v, msg: %s", err, err.Msg)
	return WritePacket(wrap.conn, wrap.packetCounter, buf, wrap.writeTimeout)
}

var ErrCodeMsgMap = map[ErrCodeType]string{
	ErrorOk:                  "Ok",
	ErrorOkQuery:             "OK: no more data.",
	ErrorNetPacketOutOfOrder: "protocol read an unexpected order packet",
	ErrUnknownCommand:        "protocol reads an unknown command",
	ErrSyntax:                "parser: %s",
	ErrQuery:                 "query: %s",
	ErrSendQueryResult:       "server send query result failed: %s",
}

func (wrap *connectionWrapper) setConnection(id uint32, conn net.Conn, fromUnixSocket bool) {
	wrap.packetCounter = 0
	wrap.id, wrap.conn = id, conn
	wrap.session = Session{CurrentDB: "", sessionID: uint64(time.Now().Unix())}
}

// Parsing sql commands until exit.
func (wrap *connectionWrapper) parseCommand() {
	defer wrap.conn.Close()
	// currentDataBase := ""
	for {
		select {
		case <-wrap.ctx.Done():
			return
		default:
		}
		command, err := wrap.readCommand()
		if !err.IsOk() {
			connectionWrapperLog.WarnF("err when read command: %s. close connection: %s!", err.Msg, wrap.conn.RemoteAddr())
			return
		}
		exit, err := command.Do(wrap)
		wrap.SendErrMsg(err)
		if exit {
			connectionWrapperLog.InfoF("client quit, close connection: %s!", wrap.conn.RemoteAddr())
			return
		}
	}
}

func (wrap *connectionWrapper) SendErrMsg(msg ErrMsg) {
	// Todo: maybe we need to check sendOk and sendErr status.
	if msg.IsOk() {
		wrap.sendOk(msg)
	} else {
		wrap.sendErr(msg)
	}
	wrap.packetCounter++
}

func (wrap *connectionWrapper) CurrentDB() *string {
	return &wrap.session.CurrentDB
}

var emptyCommand = Command{}

func (wrap *connectionWrapper) readCommand() (Command, ErrMsg) {
	var packet []byte
	var errMsg ErrMsg
	for {
		packet, errMsg = ReadPacket(wrap.conn, wrap.packetCounter, wrap.readTimeout)
		if errMsg.IsTimeout() {
			// connectionWrapperLog.WarnF("read packet failed: err: %v", errMsg)
			continue
		}
		break
	}
	if !errMsg.IsOk() {
		return emptyCommand, errMsg
	}
	return decodeCommand(packet)
}

// Will send a data message to client.
// +-------------+-----------+-----------+
// + packet type + pack len  +   packet  +
// +-------------+-----------+-----------+
// +      2      +
// +-------------+
func (wrap *connectionWrapper) SendQueryResult(data *storage.RecordBatch) ErrMsg {
	bs, _ := json.Marshal(data)
	buf := bytes.Buffer{}
	buf.WriteByte(DataMsgType)
	buf.Write(int4ToBytes(uint32(len(bs))))
	buf.Write(bs)
	return WritePacket(wrap.conn, wrap.packetCounter, buf, wrap.writeTimeout)
}

// For client.
func WriteCommand(conn net.Conn, packetCounter byte, command Command, writeTimeout time.Duration) ErrMsg {
	buf := bytes.Buffer{}
	buf.WriteByte(byte(command.Tp))
	buf.Write(command.Command.Encode())
	return WritePacket(conn, packetCounter, buf, writeTimeout)
}

var emptyMsg = Msg{}

func ReadResp(conn net.Conn, packetCounter byte, readTimeout time.Duration) Msg {
	var packet []byte
	var errMsg ErrMsg
	for {
		packet, errMsg = ReadPacket(conn, packetCounter, readTimeout)
		if errMsg.IsTimeout() {
			continue
		}
		break
	}
	if !errMsg.IsOk() {
		return Msg{TP: ErrMsgType, Msg: errMsg}
	}
	switch packet[0] {
	case OkMsgType:
		msg := decodeOkMsg(packet)
		return Msg{TP: OkMsgType, Msg: msg}
	case ErrMsgType:
		msg := decodeErrMsg(packet)
		return Msg{TP: ErrMsgType, Msg: msg}
	case DataMsgType:
		return decodeQueryMessage(packet)
	default:
		return Msg{TP: ErrMsgType, Msg: ErrMsg{errCode: ErrPacketType, Msg: "unknown message"}}
	}
}

var emptyErrMsg = ErrMsg{}

var (
	okMsgMinLength   = 5
	errMsgMinLength  = 6
	dataMsgMinLength = 5
)

func decodeOkMsg(packet []byte) ErrMsg {
	if len(packet) <= okMsgMinLength {
		return ErrMsg{errCode: ErrMsgFormat, Msg: "wrong ok message format"}
	}
	messageLen := BytesToInt4(packet[1:5])
	message := packet[5 : 5+messageLen]
	ret := ErrMsg{
		errCode: ErrorOk,
		Msg:     string(message),
	}
	return ret
}

func decodeErrMsg(packet []byte) ErrMsg {
	if len(packet) <= errMsgMinLength {
		return ErrMsg{errCode: ErrMsgFormat, Msg: "wrong err message format"}
	}
	errCode := packet[1]
	messageLen := BytesToInt4(packet[2:6])
	ret := ErrMsg{
		errCode: ErrCodeType(errCode),
		Msg:     string(packet[6 : 6+messageLen]),
	}
	return ret
}

func decodeQueryMessage(packet []byte) Msg {
	if len(packet) <= dataMsgMinLength {
		return Msg{TP: ErrMsgType, Msg: ErrMsg{errCode: ErrMsgFormat, Msg: "wrong data message format"}}
	}
	messageLen := BytesToInt4(packet[1:5])
	data := packet[5 : 5+messageLen]
	ret := &storage.RecordBatch{}
	json.Unmarshal(data, ret)
	return Msg{TP: DataMsgType, Msg: ret}
}

type ErrMsg struct {
	errCode ErrCodeType
	Msg     string
}

func (msg ErrMsg) IsOk() bool {
	return msg.errCode == ErrorOk || msg.errCode == ErrorOkQuery
}

func (msg ErrMsg) IsTimeout() bool {
	return msg.errCode == ErrorNetTimeout
}

type Session struct {
	sessionID uint64
	CurrentDB string
}

type Msg struct {
	TP            MsgType
	Msg           interface{}
	PacketCounter byte
}

func (msg Msg) IsErrMsg() bool {
	return msg.TP == ErrMsgType
}

func (msg Msg) IsFatal() bool {
	if !msg.IsErrMsg() || msg.Msg.(ErrMsg).IsOk() {
		return false
	}
	errMsg := msg.Msg.(ErrMsg)
	switch errMsg.errCode {
	case ErrorOk, ErrorNetTimeout, ErrorNetPacketOutOfOrder, ErrMsgFormat, ErrPacketType, ErrSyntax, ErrQuery:
		return false
	default:
		return true
	}
}
