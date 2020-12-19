package protocol

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"simpleDb/util"
	"time"
)

var connectionWrapperLog = util.GetLog("ConnectionWrapper")

type connectionWrapper struct {
	id            uint32
	conn          net.Conn
	readTimeout   time.Duration
	writeTimeout  time.Duration
	writeBuf      bytes.Buffer
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

	packetLen := decodeInt4Bytes(bs[:4])
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
	wrap.writeBuf.Reset()
	wrap.writeBuf.WriteByte(OkMsgType)
	message := ErrCodeMsgMap[okMsg.errCode]
	wrap.writeBuf.Write(int4ToBytes(uint32(len(message))))
	wrap.writeBuf.Write([]byte(message))
	connectionWrapperLog.InfoF("send ok packet.")
	return WritePacket(wrap.conn, wrap.packetCounter, wrap.writeBuf, wrap.writeTimeout)
}

// Send Err_Packet.
// +-------------+-----------+-----------+
// + packet type +  err_code + pack len  +
// +-------------+-----------+-----------+
// +      1      +
// +-------------+
func (wrap *connectionWrapper) sendErr(err ErrMsg) (ErrCodeType, error) {
	errFormat := ErrCodeMsgMap[err.errCode]
	wrap.writeBuf.Reset()
	wrap.writeBuf.WriteByte(ErrMsgType)
	wrap.writeBuf.WriteByte(byte(err.errCode))
	errMsg := fmt.Sprintf(errFormat, err.Params...)
	wrap.writeBuf.Write([]byte(errMsg))
	connectionWrapperLog.InfoF("send err packet. err: %+v, msg: %s", err, errMsg)
	return WritePacket(wrap.conn, wrap.packetCounter, wrap.writeBuf, wrap.writeTimeout)
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
}

func (wrap *connectionWrapper) setConnection(id uint32, conn net.Conn, fromUnixSocket bool) {
	wrap.packetCounter = 0
	wrap.id, wrap.conn = id, conn
	wrap.writeBuf.Reset()
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
		// Todo: maybe we need to check sendOk and sendErr status.
		if err.IsOk() {
			wrap.sendOk(err)
		} else {
			wrap.sendErr(err)
		}
		wrap.packetCounter++
		if exit {
			return
		}
	}
}

var emptyCommand = Command{}

func (wrap *connectionWrapper) readCommand() (Command, ErrMsg) {
	packet, errCode, err := ReadPacket(wrap.conn, wrap.packetCounter, wrap.readTimeout)
	if err != nil {
		connectionWrapperLog.WarnF("read packet failed: err: %v", err)
	}
	if errCode >= 0 {
		return emptyCommand, ErrMsg{errCode: errCode}
	}
	if len(packet) <= 0 {
		return emptyCommand, ErrMsg{errCode: ErrorNetReadError}
	}
	return decodeCommand(packet)
}

// For client.
func WriteCommand(conn net.Conn, packetCounter byte, command Command, writeTimeout time.Duration) (ErrCodeType, error) {
	buf := bytes.Buffer{}
	buf.WriteByte(byte(command.Tp))
	buf.WriteByte(byte(command.Tp))
	buf.Write(command.Command.Encode())
	return WritePacket(conn, packetCounter, buf, writeTimeout)
}

var emptyErrMsg = ErrMsg{}

func ReadResp(conn net.Conn, packetCounter byte, readTimeout time.Duration) (ErrMsg, error) {
	packet, _, err := ReadPacket(conn, packetCounter, readTimeout)
	if err != nil {
		return emptyErrMsg, err
	}
	if len(packet) < 0 {
		return emptyErrMsg, errors.New("wrong packet format")
	}
	switch packet[0] {
	case OkMsgType:
		return decodeOkMsg(packet)
	case ErrMsgType:
		return decodeErrMsg(packet)
	default:
		return emptyErrMsg, errors.New("wrong packet type")
	}
}

func decodeOkMsg(packet []byte) (ErrMsg, error) {

}

func decodeErrMsg(packet []byte) (ErrMsg, error) {

}

type ErrMsg struct {
	errCode ErrCodeType
	Params  []interface{}
}

func (msg ErrMsg) IsOk() bool {
	return msg.errCode == ErrorOk
}

type Session struct {
	sessionID uint64
	CurrentDB string
}
