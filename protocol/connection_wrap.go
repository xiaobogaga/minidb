package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"simpleDb/storage"
	"simpleDb/util"
	"time"
)

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
	ErrorOk                  = 0
	ErrorNetWriteInterrupted = 1
	ErrorNetErrorOnWrite     = 2
	ErrorNetReadInterrupted  = 3
	ErrorNetReadError        = 4
	ErrorNetPacketOutOfOrder = 5
	ErrUnknownCommand        = 6
	ErrSyntax                = 7
	ErrQuery                 = 8
	ErrSendQueryResult       = 9
)

// The package will send to client. The format look like this:
// +-------------+-----------------+---------+
// + package len + package counter + package +
// +-------------+-----------------+---------+
func WritePacket(conn net.Conn, packetCounter byte, packet bytes.Buffer, writeTimeout time.Duration) (ErrCodeType, error) {
	err := conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err != nil {
		return ErrorNetWriteInterrupted, err
	}
	packetLen := packet.Len()
	bs := int4ToBytes(uint32(packetLen))
	bs = append(bs, packetCounter)
	buf := bytes.NewBuffer(bs)
	buf.Write(packet.Bytes())
	_, err = buf.WriteTo(conn)
	if err != nil {
		return ErrorNetErrorOnWrite, err
	}
	return ErrorOk, nil
}

// The client package looks like:
// +-------------+------------------+------------+
// + package len +  package counter +  packet    +
// +-------------+------------------+------------+
func ReadPacket(conn net.Conn, packetCounter byte, readTimeout time.Duration) ([]byte, ErrCodeType, error) {
	err := conn.SetReadDeadline(time.Now().Add(readTimeout))
	if err != nil {
		return nil, ErrorNetReadInterrupted, err
	}
	bs := [5]byte{}
	_, err = io.ReadFull(conn, bs[:])
	if err != nil && err != io.EOF {
		return nil, ErrorNetReadError, err
	}

	packetLen := BytesToInt4(bs[:4])
	clientPacketCounter := bs[4]
	if clientPacketCounter != packetCounter {
		return nil, ErrorNetPacketOutOfOrder, err
	}
	packet := make([]byte, packetLen)
	_, err = io.ReadFull(conn, packet)
	if (err != nil && err != io.EOF) || uint32(len(packet)) != packetLen {
		return nil, ErrorNetReadError, err
	}
	return packet, ErrorOk, nil
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
func (wrap *connectionWrapper) sendOk(okMsg ErrMsg) (ErrCodeType, error) {
	buf := bytes.Buffer{}
	buf.WriteByte(OkMsgType)
	message := ErrCodeMsgMap[okMsg.errCode]
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
func (wrap *connectionWrapper) sendErr(err ErrMsg) (ErrCodeType, error) {
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
	ErrorNetWriteInterrupted: "protocol send data to client failed because of timeout",
	ErrorNetErrorOnWrite:     "protocol send data to client failed",
	ErrorNetReadInterrupted:  "protocol read client data failed because of timeout",
	ErrorNetReadError:        "protocol read client data error",
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
			// Todo, maybe we need send err parameters.
			wrap.sendErr(err)
			return
		}
		exit, err := command.Do(wrap)
		wrap.SendErrMsg(err)
		if exit {
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

var emptyCommand = Command{}

func (wrap *connectionWrapper) readCommand() (Command, ErrMsg) {
	packet, errCode, err := ReadPacket(wrap.conn, wrap.packetCounter, wrap.readTimeout)
	if err != nil {
		connectionWrapperLog.WarnF("read packet failed: err: %v", err)
	}
	if errCode >= 0 {
		return emptyCommand, ErrMsg{errCode: errCode, Msg: fmt.Sprintf(ErrCodeMsgMap[errCode])}
	}
	if len(packet) <= 0 {
		return emptyCommand, ErrMsg{errCode: ErrorNetReadError, Msg: fmt.Sprintf(ErrCodeMsgMap[errCode])}
	}
	return decodeCommand(packet)
}

// Will send a data message to client.
// +-------------+-----------+-----------+
// + packet type + pack len  +   packet  +
// +-------------+-----------+-----------+
// +      2      +
// +-------------+
func (wrap *connectionWrapper) SendQueryResult(data *storage.RecordBatch) (ErrCodeType, error) {
	bs, _ := json.Marshal(data)
	buf := bytes.Buffer{}
	buf.WriteByte(DataMsgType)
	buf.Write(int4ToBytes(uint32(len(bs))))
	buf.Write(bs)
	return WritePacket(wrap.conn, wrap.packetCounter, buf, wrap.writeTimeout)
}

// For client.
func WriteCommand(conn net.Conn, packetCounter byte, command Command, writeTimeout time.Duration) (ErrCodeType, error) {
	buf := bytes.Buffer{}
	buf.WriteByte(byte(command.Tp))
	buf.WriteByte(byte(command.Tp))
	buf.Write(command.Command.Encode())
	return WritePacket(conn, packetCounter, buf, writeTimeout)
}

var emptyMsg = Msg{}

func ReadResp(conn net.Conn, packetCounter byte, readTimeout time.Duration) (Msg, error) {
	packet, _, err := ReadPacket(conn, packetCounter, readTimeout)
	if err != nil {
		return emptyMsg, err
	}
	if len(packet) < 0 {
		return emptyMsg, errors.New("wrong packet format")
	}
	switch packet[0] {
	case OkMsgType:
		okMsg, err := decodeOkMsg(packet)
		return Msg{TP: OkMsgType, Msg: okMsg}, err
	case ErrMsgType:
		errMsg, err := decodeErrMsg(packet)
		return Msg{TP: ErrMsgType, Msg: errMsg}, err
	case DataMsgType:
		msg, err := decodeQueryMessage(packet)
		return Msg{TP: DataMsgType, Msg: msg}, err
	default:
		return emptyMsg, errors.New("wrong packet type")
	}
}

var emptyErrMsg = ErrMsg{}

var (
	okMsgMinLength   = 5
	errMsgMinLength  = 6
	dataMsgMinLength = 5
)

func decodeOkMsg(packet []byte) (ErrMsg, error) {
	if len(packet) <= okMsgMinLength {
		return emptyErrMsg, errors.New("wrong ok msg format")
	}
	messageLen := BytesToInt4(packet[1:5])
	message := packet[5 : 5+messageLen]
	ret := ErrMsg{
		errCode: ErrorOk,
		Msg:     string(message),
	}
	return ret, nil
}

func decodeErrMsg(packet []byte) (ErrMsg, error) {
	if len(packet) <= errMsgMinLength {
		return emptyErrMsg, errors.New("wrong err msg format")
	}
	errCode := packet[1]
	messageLen := BytesToInt4(packet[2:6])
	ret := ErrMsg{
		errCode: ErrCodeType(errCode),
		Msg:     string(packet[7 : 7+messageLen]),
	}
	return ret, nil
}

func decodeQueryMessage(packet []byte) (*storage.RecordBatch, error) {
	if len(packet) <= dataMsgMinLength {
		return nil, errors.New("wrong data msg format")
	}
	messageLen := BytesToInt4(packet[1:5])
	data := packet[5 : 5+messageLen]
	ret := &storage.RecordBatch{}
	err := json.Unmarshal(data, ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

type ErrMsg struct {
	errCode ErrCodeType
	Msg     string
}

func (msg ErrMsg) IsOk() bool {
	return msg.errCode == ErrorOk
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
