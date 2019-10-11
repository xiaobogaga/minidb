package util

const (
	ClientLongPassword uint32 = 1 << iota
	ClientFoundRows
	ClientLongFalg
	ClientConnectWithDB
	ClientNoSchema
	// Don't use compress for simplify
	ClientCompress
	ClientODBC
	ClientLocalFiles
	ClientIgnoreSpace
	// Dont use this for simplify
	ClientProtocol41
	ClientInteractive
	// For simplify, don't use ssl.
	//ClientSSL
	ClientIgnoreSigpipe
	// For simplify, we don't consider this now.
	ClientTransactions
	ClientReserved
	// For simplify, unset this operation.
	ClientSecureConnection
	ClientMultiStatements
	ClientMultiResults
	ClientPsMultiResults
	// For simplify, unset this operation.
	ClientPluginAuth
	// For simplify, don't use ssl
	ClientSSLVerifyServerCert uint32 = 1 << 30
	ClientRememberOptions     uint32 = 1 << 31
)

var ClientAllFlags = ClientLongPassword | ClientFoundRows |
	ClientLongFalg | ClientConnectWithDB | ClientNoSchema |
	ClientODBC | ClientLocalFiles | ClientIgnoreSpace |
	ClientInteractive | ClientIgnoreSigpipe |
	ClientReserved | ClientMultiStatements | ClientMultiResults |
	ClientPsMultiResults | ClientRememberOptions

var ClientBasicFlags = ClientAllFlags

var ServerVersion = "5.5.4"
var ProtocolVersion byte = 10
var StringEnd byte = 0

// utf8
var DefaultCharsetNumber = 88

const (
	ServerStatusInTrans uint16 = 1 << iota
	ServerStatusAutoCommit
	ServerMoreResultsExist
	ServerQueryNoGoodIndexUsed
	ServerQueryNoIndexUsed
	ServerStatusCursorExists
	ServerStatusLastRowSent
	ServerStatusDbDropped
	ServerStatusNoBackSlashEscapes
	ServerStatusMetaDataChanged
	ServerQueryWasSlow
	ServerPSOutParams
)

var ServerStatusClearSet = ServerQueryNoGoodIndexUsed | ServerQueryNoIndexUsed |
	ServerMoreResultsExist | ServerStatusMetaDataChanged | ServerQueryWasSlow |
	ServerStatusDbDropped | ServerStatusCursorExists | ServerStatusLastRowSent

var MysqlErrMsgSize = 512
