package protocol

import (
	"fmt"
	"minidb/parser"
	"minidb/plan"
	"minidb/util"
	"strings"
)

var commandLog = util.GetLog("Command")

type Command struct {
	Tp      CommandType
	Command CommandInterface
	arg     []byte
}

var OkMsg = ErrMsg{errCode: ErrorOk}

func decodeCommand(packet []byte) (Command, ErrMsg) {
	switch CommandType(packet[0]) {
	case TpComQuery:
		return Command{Tp: TpComQuery, arg: packet[1:], Command: ComQuery(packet[1:])}, OkMsg
	case TpComQuit:
		return Command{Tp: TpComQuit, Command: ComQuit("")}, OkMsg
	case TpComPing:
		return Command{Tp: TpComPing, Command: ComPing("")}, OkMsg
	default:
		return Command{}, ErrMsg{errCode: ErrUnknownCommand}
	}
}

func (c Command) Do(con *connectionWrapper) (exit bool, msg ErrMsg) {
	return c.Command.Do(con, c.arg)
}

type CommandInterface interface {
	// Do is used to do command and return an bool flag to indicate whether close
	// this connection and an errCode.
	Do(con *connectionWrapper, packet []byte) (bool, ErrMsg)
	// Encode returns the encoded bytes of this command which would be sent to the other side..
	Encode() []byte
}

type CommandType byte

// For now, we only consider a limited command.
const (
	TpComQuery CommandType = iota
	TpComQuit
	TpComPing
)

type ComQuit string

// ComQuit just return true to indicate exit.
func (c ComQuit) Do(_ *connectionWrapper, _ []byte) (bool, ErrMsg) {
	commandLog.InfoF("ComQuit: exiting.")
	return true, OkMsg
}

func (c ComQuit) Encode() []byte {
	return nil
}

type ComPing string

func (c ComPing) Do(_ *connectionWrapper, _ []byte) (bool, ErrMsg) {
	commandLog.InfoF("ComPing: we are alive.")
	return false, OkMsg
}

func (c ComPing) Encode() []byte {
	return nil
}

type ComQuery string

func (c ComQuery) Do(conn *connectionWrapper, packet []byte) (bool, ErrMsg) {
	// Parse a query and execute it.
	query := string(packet)
	commandLog.InfoF("ComQuery: try to do a query: %s", query)
	parser := parser.NewParser()
	stms, err := parser.Parse(packet)
	if err != nil {
		return false, makeErrMsg(ErrSyntax, err.Error())
	}
	var msg ErrMsg
	for i, stm := range stms {
		msg = c.HandleOneStm(stm, conn)
		if i == len(stms)-1 {
			// stm is the last stm.
			break
		}
		// Todo: do we need to check msg status.
		conn.SendErrMsg(msg)
	}
	return false, msg
}

func (c ComQuery) HandleOneStm(stm parser.Stm, conn *connectionWrapper) ErrMsg {
	for {
		data, finish, err := plan.Exec(stm, &conn.session.CurrentDB)
		if err != nil {
			return makeErrMsg(ErrQuery, err.Error())
		}
		if data != nil {
			conn.SendQueryResult(data)
		}
		if finish {
			return OkMsg
		}
	}
}

func makeErrMsg(errType ErrCodeType, errMsg string) ErrMsg {
	return ErrMsg{
		errCode: errType,
		Msg:     fmt.Sprintf(ErrCodeMsgMap[errType], errMsg),
	}
}

func (c ComQuery) Encode() []byte {
	return []byte(string(c))
}

func StrToCommand(input string) (Command, error) {
	trimed := strings.TrimSpace(input)
	switch trimed {
	case "ping", "ping;":
		return Command{Tp: TpComPing, Command: ComPing("ping")}, nil
	case "quit", "quit;":
		return Command{Tp: TpComQuit, Command: ComQuit("quit")}, nil
	default:
		return Command{Tp: TpComQuery, Command: ComQuery(input)}, nil
	}
}
