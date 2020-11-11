package server

import (
	"simpleDb/log"
	"simpleDb/parser"
)

var commandLog = log.GetLog("Command")

type Command struct {
	Tp      CommandType
	Command CommandInterface
	arg     []byte
}

func decodeCommand(packet []byte) (Command, *CommandErr) {
	switch CommandType(packet[0]) {
	case TpComQuery:
		return Command{Tp: TpComQuery, arg: packet, Command:}, nil
	case TpComQuit:
		return Command{Tp: TpComQuit, Command:}, nil
	case TpComInitDB:
		return Command{Tp: TpComInitDB, arg: packet, Command:}, nil
	case TpComPing:
		return Command{Tp: TpComPing, Command:}, nil
	default:
		return Command{}, &CommandErr{ErrCode: ER_UNKNOWN_COM_ERROR}
	}
}

func (c Command) Do() (bool, OkMsg, *CommandErr) {
	return c.Command.Do(c.arg)
}

type CommandInterface interface {
	// Do is used to do command and return an bool flag to indicate whether close
	// this connection and an errCode.
	Do(packet []byte) (bool, OkMsg, *CommandErr)
	// Encode returns the encoded bytes of this command which would be sent to the other side..
	Encode() []byte
}

type CommandType byte

// For now, we only consider a limited command.
const (
	// Text Protocol
	TpComQuery CommandType = 0x03
	// Utility Commands
	TpComQuit            CommandType = 0x01
	TpComInitDB          CommandType = 0x02
	TpComFieldList       CommandType = 0x04
	TpComRefresh         CommandType = 0x07
	TpComStatistics      CommandType = 0x08
	TpComProcessInfo     CommandType = 0x0A
	TpComProcessKill     CommandType = 0x0C
	TpComDebug           CommandType = 0x0D
	TpComPing            CommandType = 0x0E
	TpComChangeUser      CommandType = 0x11
	TpComResetConnection CommandType = 0x1F
	TpComSetOption       CommandType = 0x1B
	// Prepared Statements
	TpComStmtPrepare      CommandType = 0x16
	TpComStmtExecute      CommandType = 0x17
	TpComStmtFetch        CommandType = 0x1C
	TpComStmtClose        CommandType = 0x19
	TpComStmtReset        CommandType = 0x1A
	TpComStmtSendLongData CommandType = 0x18
	// Stored Programs
	// Todo: need supporting stored programs.
)

var emptyOkMsg = OkMsg{}

type ComQuit struct{}

// ComQuit just return true to indicate exit.
func (c ComQuit) Do(_ []byte) (bool, OkMsg, *CommandErr) {
	commandLog.InfoF("ComQuit: exiting.")
	return true, emptyOkMsg, nil
}

func (c ComQuit) Encode() []byte {
	return nil
}

type ComInitDb string

// ComInitDb is used to init a database by sql `use database xxx`.
// Where arg is the database name. return ok if exist and err otherwise.
func (c ComInitDb) Do(arg []byte) (bool, OkMsg, *CommandErr) {
	dataBaseName := string(arg)
	commandLog.InfoF("ComInitDb: init another database %s", dataBaseName)
	return true, emptyOkMsg, nil
}

func (c ComInitDb) Encode() []byte {
	return nil
}

type ComPing string

func (c ComPing) Do(_ []byte) (bool, OkMsg, *CommandErr) {
	commandLog.InfoF("ComPing: we are alive.")
	return false, emptyOkMsg, nil
}

func (c ComPing) Encode() []byte {
	return nil
}

type ComQuery string

func (c ComQuery) Do(arg []byte) (bool, OkMsg, *CommandErr) {
	// Parse a query and execute it.
	query := string(arg)
	commandLog.InfoF("ComQuery: try to do a query: %s", query)
	parser := parser.NewParser()
	stms, err := parser.Parse(arg)
	if err != nil {
		return false, emptyOkMsg, &CommandErr{
			ErrCode: 0,
			Params:  nil,
		}
	}
	for _, stm := range stms {
		stm.Execute()
	}
	return false, -1, nil
}

func (c ComQuery) Encode() []byte {
	return nil
}
