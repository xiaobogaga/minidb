package server

import (
	"bufio"
	"bytes"
	"context"
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
	reader        *bufio.Reader
	message       bytes.Buffer
	packetCounter byte
	log           util.SimpleLogWrapper
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
func (wrap *connectionWrapper) writePacket(packet bytes.Buffer) ErrCodeType {
	err := wrap.conn.SetWriteDeadline(time.Now().Add(wrap.writeTimeout))
	if err != nil {
		return ErrorNetWriteInterrupted
	}
	packetLen := packet.Len()
	bs := int4ToBytes(uint32(packetLen))
	bs = append(bs, wrap.packetCounter)
	wrap.packetCounter++
	buf := bytes.NewBuffer(bs)
	buf.Write(packet.Bytes())
	_, err = buf.WriteTo(wrap.conn)
	if err != nil {
		return ErrorNetErrorOnWrite
	}
	return -1
}

// The client package looks like:
// +-------------+------------------+------------+
// + package len +  package counter +  packet    +
// +-------------+------------------+------------+
func (wrap *connectionWrapper) readPacket() ([]byte, ErrCodeType) {
	err := wrap.conn.SetReadDeadline(time.Now().Add(wrap.readTimeout))
	if err != nil {
		return nil, ErrorNetReadInterrupted
	}
	bs := [5]byte{}
	_, err = io.ReadFull(wrap.reader, bs[:])
	if err != nil && err != io.EOF {
		return nil, ErrorNetReadError
	}

	packetLen := decodeInt4Bytes(bs[:4])
	clientPacketCounter := bs[4]
	if clientPacketCounter != wrap.packetCounter {
		return nil, ErrorNetPacketOutOfOrder
	}
	wrap.packetCounter++
	packet := make([]byte, packetLen)
	_, err = io.ReadFull(wrap.reader, packet)
	if (err != nil && err != io.EOF) || uint32(len(packet)) != packetLen {
		return nil, ErrorNetReadError
	}
	return packet, -1
}

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
func (wrap *connectionWrapper) sendOk(okMsg ErrMsg) ErrCodeType {
	wrap.writeBuf.Reset()
	wrap.writeBuf.WriteByte(0)
	message := ErrCodeMsgMap[okMsg.errCode]
	wrap.writeBuf.Write(int4ToBytes(uint32(len(message))))
	wrap.writeBuf.Write([]byte(message))
	wrap.log.InfoF("send ok packet.")
	return wrap.writePacket(wrap.writeBuf)
}

// Send Err_Packet.
// +-------------+-----------+-----------+
// + packet type +  err_code + pack len  +
// +-------------+-----------+-----------+
// +      1      +
// +-------------+
func (wrap *connectionWrapper) sendErr(err ErrMsg) ErrCodeType {
	errFormat := ErrCodeMsgMap[err.errCode]
	wrap.writeBuf.Reset()
	wrap.writeBuf.WriteByte(byte(0x1))
	wrap.writeBuf.WriteByte(byte(err.errCode))
	errMsg := fmt.Sprintf(errFormat, err.Params...)
	wrap.writeBuf.Write([]byte(errMsg))
	wrap.log.InfoF("send err packet. err: %+v, msg: %s", err, errMsg)
	return wrap.writePacket(wrap.writeBuf)
}

var ErrCodeMsgMap = map[ErrCodeType]string{
	ErrorOk:                  "Ok",
	ErrorNetWriteInterrupted: "server send data to client failed because of timeout",
	ErrorNetErrorOnWrite:     "server send data to client failed",
	ErrorNetReadInterrupted:  "server read client data failed because of timeout",
	ErrorNetReadError:        "server read client data error",
	ErrorNetPacketOutOfOrder: "server read an unexpected order packet",
	ErrUnknownCommand:        "server reads an unknown command",
	ErrSyntax:                "parser: %s",
	ErrQuery:                 "query: %s",
}

func (wrap *connectionWrapper) setConnection(id uint32, conn net.Conn, fromUnixSocket bool) {
	wrap.packetCounter = 0
	wrap.id, wrap.conn = id, conn
	wrap.writeBuf.Reset()
	wrap.message.Reset()
	wrap.reader = bufio.NewReader(conn)
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
		if err.IsOk() {
			wrap.sendOk(err)
		} else {
			wrap.sendErr(err)
		}
		if exit {
			return
		}
	}
}

var emptyCommand = Command{}

func (wrap *connectionWrapper) readCommand() (Command, ErrMsg) {
	packet, errCode := wrap.readPacket()
	if errCode >= 0 {
		return emptyCommand, ErrMsg{errCode: errCode}
	}
	if len(packet) <= 0 {
		return emptyCommand, ErrMsg{errCode: ErrorNetReadError}
	}
	return decodeCommand(packet)
}

func (wrap *connectionWrapper) writeCommand(command Command) ErrCodeType {
	// The command Length is: 1 + command.Len()
	wrap.writeBuf.Reset()
	wrap.writeBuf.WriteByte(byte(command.Tp))
	wrap.writeBuf.Write(command.Command.Encode())
	return wrap.writePacket(wrap.writeBuf)
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
