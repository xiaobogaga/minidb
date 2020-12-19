package protocol

import (
	"simpleDb/parser"
	"simpleDb/plan"
	"simpleDb/util"
)

var commandLog = util.GetLog("Command")

type Command struct {
	Tp      CommandType
	Command CommandInterface
	arg     []byte
}

var okMsg = ErrMsg{errCode: ErrorOk}

func decodeCommand(packet []byte) (Command, ErrMsg) {
	switch CommandType(packet[0]) {
	case TpComQuery:
		return Command{Tp: TpComQuery, arg: packet[1:], Command: ComQuery(packet[1:])}, okMsg
	case TpComQuit:
		return Command{Tp: TpComQuit, Command: ComQuit("")}, okMsg
	case TpComInitDB:
		return Command{Tp: TpComInitDB, arg: packet[1:], Command: ComInitDb(packet[1:])}, okMsg
	case TpComPing:
		return Command{Tp: TpComPing, Command: ComPing("")}, okMsg
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
	TpComInitDB
	TpComPing
)

type ComQuit string

// ComQuit just return true to indicate exit.
func (c ComQuit) Do(_ *connectionWrapper, _ []byte) (bool, ErrMsg) {
	commandLog.InfoF("ComQuit: exiting.")
	return true, okMsg
}

func (c ComQuit) Encode() []byte {
	return nil
}

type ComInitDb string

// ComInitDb is used to init a database by sql `use database xxx`.
// Where arg is the database name. return ok if exist and err otherwise.
func (c ComInitDb) Do(con *connectionWrapper, arg []byte) (bool, ErrMsg) {
	dataBaseName := string(arg)
	con.session.CurrentDB = dataBaseName
	commandLog.InfoF("ComInitDb: init another database %s", dataBaseName)
	return false, okMsg
}

func (c ComInitDb) Encode() []byte {
	return []byte(c)
}

type ComPing string

func (c ComPing) Do(_ *connectionWrapper, _ []byte) (bool, ErrMsg) {
	commandLog.InfoF("ComPing: we are alive.")
	return false, okMsg
}

func (c ComPing) Encode() []byte {
	return nil
}

type ComQuery string

func (c ComQuery) Do(con *connectionWrapper, packet []byte) (bool, ErrMsg) {
	// Parse a query and execute it.
	query := string(packet)
	commandLog.InfoF("ComQuery: try to do a query: %s", query)
	parser := parser.NewParser()
	stms, err := parser.Parse(packet)
	if err != nil {
		return false, makeErrMsg(ErrSyntax, err.Error())
	}
	for _, stm := range stms {
		err := plan.Exec(stm, con.session.CurrentDB)
		if err != nil {
			return false, makeErrMsg(ErrQuery, err.Error())
		}
	}
	return false, okMsg
}

func makeErrMsg(errType ErrCodeType, errMsg string) ErrMsg {
	return ErrMsg{
		errCode: errType,
		Params:  []interface{}{errMsg},
	}
}

func (c ComQuery) Encode() []byte {
	return []byte(string(c))
}

func StrToCommand(input string) (Command, error) {

}
