package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"simpleDb/log"
	"time"
)

// A Simple Tcp Server supports mysql protocol
type SimpleServer struct {
	Size               int
	Port               int
	unixSocketAddr     string
	Pool               int
	Listener           *net.TCPListener
	unixListener       net.Listener
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	connectionParsers  []*ConnectionParser
	connectionParserCh chan *ConnectionParser
	ctx                context.Context
	cancel             context.CancelFunc
	logger             log.SimpleLogWrapper
}

var ProtolVersion = 10
var ServerVersion = "Hello :), happy today. v1.0"
var defaultUnixSocketAddr = "/tmp/mysql.sock"
var defaultPoolSize = 16
var defaultTimeout = 1000

func NewServer(port, connectionParserPoolSize int, log *log.SimpleLog) *SimpleServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &SimpleServer{
		Port:               port,
		Pool:               connectionParserPoolSize,
		ReadTimeout:        time.Second * time.Duration(defaultTimeout),
		WriteTimeout:       time.Second * time.Duration(defaultTimeout),
		ctx:                ctx,
		unixSocketAddr:     defaultUnixSocketAddr,
		cancel:             cancel,
		connectionParserCh: make(chan *ConnectionParser, connectionParserPoolSize),
		logger:             log.AddHeader("SimpleServer"),
	}
}

func NewServerWithTimeout(port int, readTimeout, writeTimeout time.Duration, unixSocketAddr string, log *log.SimpleLog) *SimpleServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &SimpleServer{
		Port:               port,
		ReadTimeout:        readTimeout,
		WriteTimeout:       writeTimeout,
		Pool:               defaultPoolSize,
		ctx:                ctx,
		cancel:             cancel,
		unixSocketAddr:     unixSocketAddr,
		connectionParserCh: make(chan *ConnectionParser, defaultPoolSize),
		logger:             log.AddHeader("SimpleServer"),
	}
}

func (server *SimpleServer) Start() error {
	tcpAddress, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", server.Port))
	if err != nil {
		return err
	}
	server.Listener, err = net.ListenTCP("tcp", tcpAddress)
	if err != nil {
		return err
	}
	// TODO: we might need to remove this method call. Before mysql would error
	err = os.RemoveAll(defaultUnixSocketAddr)
	if err != nil {
		server.logger.WarnF("remove % path to init unix socket err: %v", err)
	}
	server.unixListener, err = net.Listen("unix", defaultUnixSocketAddr)
	if err != nil {
		return err
	}
	go server.WaitConnection()
	go server.WaitUnixSocketConnection()
	server.logger.InfoF("server started.")
	return nil
}

func (server *SimpleServer) Close() {
	server.logger.InfoF("close server")
	server.Listener.Close()
	server.unixListener.Close()
	server.cancel()
}

func (server *SimpleServer) WaitConnection() {
	for {
		select {
		case <-server.ctx.Done():
			return
		default:
		}
		connection, err := server.Listener.AcceptTCP()
		if err != nil {
			server.logger.WarnF("accept connection error. err: %v", err)
			continue
		}
		addr := connection.RemoteAddr()
		server.logger.InfoF("accept connection from %s:%s.", addr.Network(), addr.String())
		conParser := server.getParser()
		if conParser != nil {
			go conParser.parseConnection(connection, false)
		}
	}
}

func (server *SimpleServer) WaitUnixSocketConnection() {
	for {
		select {
		case <-server.ctx.Done():
			return
		default:
		}
		connection, err := server.unixListener.Accept()
		if err != nil {
			server.logger.WarnF("accept connection error. err: %v.", err)
			continue
		}
		server.logger.InfoF("accept connection from unix socket.")
		conParser := server.getParser()
		if conParser != nil {
			go conParser.parseConnection(connection, true)
		}
	}
}

func (server *SimpleServer) getLog() *log.SimpleLog {
	return log.GetLog("SimpleServer")
}

func (server *SimpleServer) createNewParser() *ConnectionParser {
	parser := newConnectionParser(server.ReadTimeout, server.WriteTimeout, server.connectionParserCh, server.logger.GetUnderlineLog(), server.ctx)
	server.connectionParsers = append(server.connectionParsers, parser)
	server.Size += 1
	return parser
}

func (server *SimpleServer) getParser() *ConnectionParser {
	if server.Size < server.Pool {
		return server.createNewParser()
	}
	select {
	// Wait until having idle connectionParser.
	case parser := <-server.connectionParserCh:
		return parser
	case <-server.ctx.Done():
		return nil
	}
}

type ConnectionParser struct {
	Count       int
	reuseCh     chan *ConnectionParser
	connWrapper *connectionWrapper
	log         log.SimpleLogWrapper
	ctx         context.Context
	cancel      context.CancelFunc
}

func newConnectionParser(readTimeout, writeTimeout time.Duration, reuseCh chan *ConnectionParser, log *log.SimpleLog, ctx context.Context) *ConnectionParser {
	ctx, cancel := context.WithCancel(ctx)
	return &ConnectionParser{
		reuseCh:     reuseCh,
		Count:       0,
		connWrapper: NewConnectionWrapper(readTimeout, writeTimeout, log, ctx),
		log:         log.AddHeader("ConnectionParser"),
		cancel:      cancel,
	}
}

func (parser *ConnectionParser) parseConnection(connection net.Conn, fromUnixSocket bool) {
	parser.Count++
	// Todo, check whether use parser.Count as id is ok.
	// Now, don't bother to do this.
	parser.connWrapper.setConnection(uint32(parser.Count), connection, fromUnixSocket)
	// First authenticate.
	err := parser.connWrapper.aclAuthenticate()
	if err >= 0 {
		// Close connection.
		// Note: err returned here is ignored.
		parser.log.InfoF("close connection.")
		parser.connWrapper.conn.Close()
		parser.reuseCh <- parser
		return
	}
	// Parsing command until exit.
	parser.log.InfoF("authenticate success. start to parse command")
	parser.connWrapper.parseCommand()
	parser.reuseCh <- parser
}
