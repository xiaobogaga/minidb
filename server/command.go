package server

type Command struct {
	Tp      CommandType
	Command CommandInterface
}

type CommandInterface interface {
	// Do is used to do command and return an bool flag to indicate whether close
	// this connection and an errCode.
	Do() (bool, errCodeType)
	// The length of encoded bytes of this command.
	Len()
	// Encode returns the encoded bytes of this command which would be sent to the other side..
	Encode() []byte
}

type CommandType byte

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

type QuitCommand struct{}

// QuitCommand just return true to indicate exit.
func (c QuitCommand) Do() (bool, errCodeType) {
	return true, -1
}

type ComInitDb string

// ComInitDb is used to init a database by sql `use database xxx`.
func (c ComInitDb) Do() (bool, errCodeType) {
	// dbName := string(c)
	// Must check whether the database exist.
	// Todo
	return true, -1
}

type ComPing string

func (c ComPing) Do() (bool, errCodeType) {

}
