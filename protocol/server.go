package protocol

import (
	"context"
	"fmt"
	"net"
	"os"
	"simpleDb/util"
	"time"
)

var (
	serverLog         = util.GetLog("protocol")
	DefaultUnixSocket = "/tmp/minidb.sock"
	defaultPoolSize   = 16
	DefaultTimeout    = 1000
	DefaultPort       = 19840
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
}

func NewServerWithTimeout(port int, readTimeout, writeTimeout time.Duration, unixSocketAddr string) *SimpleServer {
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
	err = os.RemoveAll(DefaultUnixSocket)
	if err != nil {
		serverLog.WarnF("remove % path to init unix socket err: %v", err)
	}
	server.unixListener, err = net.Listen("unix", DefaultUnixSocket)
	if err != nil {
		return err
	}
	go server.WaitConnection()
	go server.WaitUnixSocketConnection()
	serverLog.InfoF("protocol started.")
	return nil
}

func (server *SimpleServer) Close() {
	serverLog.InfoF("close protocol")
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
			serverLog.WarnF("accept connection error. err: %v", err)
			continue
		}
		addr := connection.RemoteAddr()
		serverLog.InfoF("accept connection from %s:%s.", addr.Network(), addr.String())
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
			serverLog.WarnF("accept connection error. err: %v.", err)
			continue
		}
		serverLog.InfoF("accept connection from unix socket.")
		conParser := server.getParser()
		if conParser != nil {
			go conParser.parseConnection(connection, true)
		}
	}
}

func (server *SimpleServer) createNewParser() *ConnectionParser {
	parser := newConnectionParser(server.ReadTimeout, server.WriteTimeout, server.connectionParserCh, server.ctx)
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

var connectionParserLog = util.GetLog("ConnectionParser")

type ConnectionParser struct {
	Count       int
	reuseCh     chan *ConnectionParser
	connWrapper *connectionWrapper
	ctx         context.Context
	cancel      context.CancelFunc
}

func newConnectionParser(readTimeout, writeTimeout time.Duration, reuseCh chan *ConnectionParser, ctx context.Context) *ConnectionParser {
	ctx, cancel := context.WithCancel(ctx)
	return &ConnectionParser{
		reuseCh:     reuseCh,
		Count:       0,
		connWrapper: NewConnectionWrapper(readTimeout, writeTimeout, ctx),
		cancel:      cancel,
	}
}

func (parser *ConnectionParser) parseConnection(connection net.Conn, fromUnixSocket bool) {
	parser.Count++
	// Todo, check whether use parser.Count as id is ok.
	// Now, don't bother to do this.
	parser.connWrapper.setConnection(uint32(parser.Count), connection, fromUnixSocket)
	// Parsing command until exit.
	parser.connWrapper.parseCommand()
	parser.reuseCh <- parser
}
