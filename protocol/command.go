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

var OkMsg = ErrMsg{errCode: ErrorOk, Msg: "OK"}
var OkQueryMsg = ErrMsg{errCode: ErrorOk, Msg: "OK. no more data"}

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

func (c Command) Do(con ConnectionWrapperInterface) (exit bool, msg ErrMsg) {
	return c.Command.Do(con, c.arg)
}

type CommandInterface interface {
	// Do is used to do command and return an bool flag to indicate whether close
	// this connection and an errCode.
	Do(con ConnectionWrapperInterface, packet []byte) (bool, ErrMsg)
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
func (c ComQuit) Do(_ ConnectionWrapperInterface, _ []byte) (bool, ErrMsg) {
	commandLog.InfoF("ComQuit: exiting.")
	return true, OkMsg
}

func (c ComQuit) Encode() []byte {
	return nil
}

type ComPing string

func (c ComPing) Do(_ ConnectionWrapperInterface, _ []byte) (bool, ErrMsg) {
	commandLog.InfoF("ComPing: we are alive.")
	return false, OkMsg
}

func (c ComPing) Encode() []byte {
	return nil
}

type ComQuery string

func (c ComQuery) Do(conn ConnectionWrapperInterface, packet []byte) (bool, ErrMsg) {
	// Parse a query and execute it.
	query := string(packet)
	commandLog.InfoF("ComQuery: try to do a query: %s", query)
	parser := parser.NewParser()
	stm, err := parser.Parse(packet)
	if err != nil {
		return false, makeErrMsg(ErrSyntax, err.Error())
	}
	msg := c.HandleOneStm(stm, conn)
	return false, msg
}

func isSelect(stm parser.Stm) bool {
	_, ok := stm.(*parser.SelectStm)
	return ok
}

func (c ComQuery) HandleOneStm(stm parser.Stm, conn ConnectionWrapperInterface) ErrMsg {
	exec, err := plan.MakeExecutor(stm, conn.CurrentDB())
	if err != nil {
		return makeErrMsg(ErrQuery, err.Error())
	}
	for {
		data, err := exec.Exec()
		if err != nil {
			return makeErrMsg(ErrQuery, err.Error())
		}
		if data != nil {
			conn.SendQueryResult(data)
		}
		if data == nil && isSelect(stm) {
			return OkQueryMsg
		}
		if data == nil {
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
