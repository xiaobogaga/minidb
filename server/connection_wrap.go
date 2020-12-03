package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"simpleDb/log"
	"simpleDb/util"
	"time"
)

const (
	// For simplify we don't use native_password plugin yet.
	defaultAuthPlugin = oldPasswordPluginName
	// nativePasswordPluginName = "mysql_native_password"
	oldPasswordPluginName = "mysql_old_password"
	scrambleLen           = 20
	scrambleLen_323       = 8
)

var connectionWrapperLog = log.GetLog("ConnectionWrapper")

type connectionWrapper struct {
	id            uint32
	conn          net.Conn
	scramble      []byte
	rand          *rand.Rand
	serverStatus  uint16
	readTimeout   time.Duration
	writeTimeout  time.Duration
	writeBuf      bytes.Buffer
	reader        *bufio.Reader
	acl           *ACL
	message       bytes.Buffer
	packetCounter byte
	log           log.SimpleLogWrapper
	ctx           context.Context
}

func NewConnectionWrapper(readTimeout, writeTimeout time.Duration, ctx context.Context) *connectionWrapper {
	return &connectionWrapper{
		scramble:     make([]byte, scrambleLen),
		rand:         rand.New(rand.NewSource(time.Now().Unix())),
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
		serverStatus: util.ServerStatusAutoCommit,
		acl:          &ACL{},
		ctx:          ctx,
	}
}

// Port of mysql
// See sql_acl.cc for more detail.
func (wrap *connectionWrapper) aclAuthenticate() ErrCodeType {
	return wrap.doAuthOnce()
}

var unKnownAuthPlugin = errors.New("unknown auth plugin err")

// Perform the first authentication attempt, with the default plugin.
// This sends the server handshake packet, reads the client reply
// with a user name, and performs the authentication. Now only oldPasswordPlugin are supported.
func (wrap *connectionWrapper) doAuthOnce() ErrCodeType {
	errCode := wrap.sendServerHandshakePacket()
	if errCode >= 0 {
		wrap.sendErr(&CommandErr{ErrCode: errCode})
		return errCode
	}
	err := wrap.readClientHandShakeResponsePacket()
	if err != nil {
		wrap.sendErr(err)
		return err.ErrCode
	}
	return wrap.sendOk(0, 0, []byte{0})
}

var TenZeroBytes = make([]byte, 10)

// Write handshake packet data:
//    1           protocol version (always 10)
//    n           server version string, \0-terminated
//    4           thread id
//    8           first 8 bytes of the plugin provided data (scramble)
//    1           \0 byte, terminating the first part of a scramble
//    2           server capabilities (two lower bytes)
//    1           server character set
//    2           server status
//    2           server capabilities (two upper bytes)
//    1           length of the scramble
//    10          reserved, always 0
//    n           rest of the plugin provided data (at least 12 bytes)
//    n           plugin name, \0 terminate
//    1           \0 byte, terminating the second part of a scramble
func (wrap *connectionWrapper) sendServerHandshakePacket() ErrCodeType {
	wrap.writeBuf.WriteByte(util.ProtocolVersion)
	wrap.acl.clientCapabilities = util.ClientBasicFlags
	wrap.writeBuf.WriteString(ServerVersion)
	wrap.writeBuf.WriteByte(util.StringEnd)
	wrap.writeBuf.Write(int4ToBytes(wrap.id))
	wrap.writeBuf.Write(wrap.scramble[:scrambleLen_323])
	wrap.writeBuf.WriteByte(util.StringEnd)
	wrap.writeBuf.Write(int2ToBytes(wrap.acl.clientCapabilities))
	wrap.writeBuf.WriteByte(byte(util.DefaultCharsetNumber))
	wrap.writeBuf.Write(int2ToBytes(uint32(util.ServerStatusClearSet)))
	wrap.writeBuf.Write(int2ToBytes(wrap.acl.clientCapabilities >> 16))
	wrap.writeBuf.WriteByte(scrambleLen)
	wrap.writeBuf.Write(TenZeroBytes)
	wrap.writeBuf.Write(wrap.scramble[scrambleLen_323:])
	wrap.writeBuf.WriteString(defaultAuthPlugin)
	wrap.writeBuf.WriteByte(util.StringEnd)
	wrap.log.InfoF("write server handshake packet.")
	return wrap.writePacket(wrap.writeBuf)
}

// Return username, usePassword, errCode
func (wrap *connectionWrapper) readClientHandShakeResponsePacket() *CommandErr {
	packet, errCode := wrap.readPacket()
	if errCode >= 0 {
		return &CommandErr{ErrCode: errCode}
	}
	if len(packet) < 5 {
		return &CommandErr{ErrCode: ER_NET_READ_ERROR}
	}
	reader := bufio.NewReader(bytes.NewBuffer(packet[5:]))
	flags := bytes2ToInt(packet[:2])
	userNameBytes, err := reader.ReadBytes(byte(0))
	if err != nil && err != io.EOF {
		return &CommandErr{ErrCode: ER_NET_READ_ERROR}
	}
	var authResponse []byte
	var dataBaseName []byte
	userName := string(userNameBytes[:len(userNameBytes)-1])
	if (flags & util.ClientConnectWithDB) > 0 {
		authResponse, err = reader.ReadBytes(0)
		authResponse = authResponse[:len(authResponse)-1]
		if err != nil {
			return &CommandErr{ErrCode: ER_NET_READ_ERROR}
		}
		dataBaseName, err = reader.ReadBytes(0)
		if err != nil {
			return &CommandErr{ErrCode: ER_NET_READ_ERROR}
		}
	} else {
		authResponse = packet[5+len(userNameBytes):]
	}
	user, found := findUser(userName)
	if !found {
		return &CommandErr{ErrCode: ER_ACCESS_DENIED_ERROR, Params: []interface{}{userName, len(authResponse) >= 1}}
	}
	if len(authResponse) == 0 && len(user.userSalt) != 0 {
		// No password
		return &CommandErr{ErrCode: ER_ACCESS_DENIED_ERROR, Params: []interface{}{userName, false}}
	}
	if len(authResponse) == scrambleLen_323 && !checkScramble323(authResponse, wrap.scramble, user.userSalt) {
		return &CommandErr{ErrCode: ER_ACCESS_DENIED_ERROR, Params: []interface{}{userName, true}}
	}
	wrap.acl.user = user
	wrap.acl.databaseName = string(dataBaseName)
	return nil
}

// Fill data with random printable character
func (wrap *connectionWrapper) generateRandomString(data []byte, start, len int) {
	i := start
	for ; i < (start + len); i++ {
		data[i] = wrap.randomCharacter()
	}
	data[i] = 0
}

func (wrap *connectionWrapper) randomCharacter() byte {
	return byte(wrap.rand.Float32()*94) + 33
}

func (wrap *connectionWrapper) writePacket(packet bytes.Buffer) ErrCodeType {
	err := wrap.conn.SetWriteDeadline(time.Now().Add(wrap.writeTimeout))
	if err != nil {
		return ER_NET_WRITE_INTERRUPTED
	}
	packetLen := packet.Len()
	bs := int3ToBytes(uint32(packetLen))
	bs = append(bs, wrap.packetCounter)
	wrap.packetCounter++
	buf := bytes.NewBuffer(bs)
	_, err = buf.WriteTo(wrap.conn)
	if err != nil {
		return ER_NET_ERROR_ON_WRITE
	}
	_, err = packet.WriteTo(wrap.conn)
	if err != nil {
		return ER_NET_ERROR_ON_WRITE
	}
	return -1
}

func (wrap *connectionWrapper) readPacket() ([]byte, ErrCodeType) {
	err := wrap.conn.SetReadDeadline(time.Now().Add(wrap.readTimeout))
	if err != nil {
		return nil, ER_NET_READ_INTERRUPTED
	}
	bs := [4]byte{}
	_, err = io.ReadFull(wrap.reader, bs[:])
	if err != nil && err != io.EOF {
		return nil, ER_NET_READ_ERROR
	}
	packetLen := decodeInt3Bytes(bs[:3])
	clientPacketCounter := bs[3]
	if clientPacketCounter != wrap.packetCounter {
		return nil, ER_NET_PACKETS_OUT_OF_ORDER
	}
	wrap.packetCounter++
	packet := make([]byte, packetLen)
	_, err = io.ReadFull(wrap.reader, packet)
	if (err != nil && err != io.EOF) || uint32(len(packet)) != packetLen {
		return nil, ER_NET_READ_ERROR
	}
	return packet, -1
}

// See OK_Packet Format for more detail.
func (wrap *connectionWrapper) sendOk(okMsg OkMsg) ErrCodeType {
	wrap.writeBuf.Reset()
	wrap.writeBuf.WriteByte(0)
	wrap.writeBuf.Write(lengthEncodedInt(okMsg.affectRows))
	wrap.writeBuf.Write(lengthEncodedInt(okMsg.lastInsertId))
	wrap.writeBuf.Write(int2ToBytes(uint32(wrap.serverStatus)))
	wrap.writeBuf.Write(okMsg.message)
	wrap.log.InfoF("send ok packet.")
	return wrap.writePacket(wrap.writeBuf)
}

func (wrap *connectionWrapper) sendErr(err *CommandErr) ErrCodeType {
	errFormat := ErrorCodeMsgMap[err.ErrCode]
	wrap.writeBuf.Reset()
	wrap.writeBuf.WriteByte(byte(0xff))
	wrap.writeBuf.Write(int2ToBytes(errFormat.errorCode))
	errMsg := fmt.Sprintf(errFormat.errorMsg, err.Params...)
	wrap.writeBuf.Write([]byte(errMsg))
	wrap.writeBuf.WriteByte(util.StringEnd)
	wrap.log.InfoF("send err packet. err: %+v, msg: %s", err, errMsg)
	return wrap.writePacket(wrap.writeBuf)
}

func (wrap *connectionWrapper) writeCommand(command Command) ErrCodeType {
	// The command Length is: 1 + command.Len()
	wrap.writeBuf.Reset()
	wrap.writeBuf.WriteByte(byte(command.Tp))
	wrap.writeBuf.Write(command.Command.Encode())
	return wrap.writePacket(wrap.writeBuf)
}

func (wrap *connectionWrapper) setConnection(id uint32, conn net.Conn, fromUnixSocket bool) {
	wrap.packetCounter = 0
	wrap.id, wrap.conn = id, conn
	if wrap.scramble[scrambleLen-1] != 0 {
		wrap.generateRandomString(wrap.scramble, 0, scrambleLen-1)
	}
	wrap.writeBuf.Reset()
	wrap.message.Reset()
	wrap.reader = bufio.NewReader(conn)
	wrap.acl.host = conn.RemoteAddr().String()
	if fromUnixSocket {
		wrap.acl.host = "localhost"
	}
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
		if err != nil {
			// Todo, maybe we need send err parameters.
			wrap.sendErr(err)
			return
		}
		exit, okMsg, err := command.Do()
		if err == nil {
			// Todo: need to send affectRows and messages.
			wrap.sendOk(okMsg)
		} else {
			wrap.sendErr(err)
		}
		if exit {
			return
		}
	}
}

var emptyCommand = Command{}

func (wrap *connectionWrapper) readCommand() (Command, *CommandErr) {
	packet, errCode := wrap.readPacket()
	if errCode >= 0 {
		return emptyCommand, &CommandErr{ErrCode: errCode}
	}
	if len(packet) <= 0 {
		return emptyCommand, &CommandErr{ErrCode: ER_NET_READ_ERROR}
	}
	return decodeCommand(packet)
}

// See find_mpvio_user method
func findUser(userName string) (User, bool) {
	// todo, currently just a placeholder.
	return User{}, false
}

type OkMsg struct {
	affectRows   uint64
	lastInsertId uint64
	message      []byte
}
