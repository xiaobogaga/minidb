package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"simpleDb/protocol"
	"simpleDb/util"
	"syscall"
	"time"
)

var (
	host         = flag.String("h", "localhost", "the server host")
	port         = flag.Int("p", protocol.DefaultPort, "the server port")
	readTimeout  = flag.Int("r", protocol.DefaultTimeout, "the read timeout in millisecond")
	writeTimeout = flag.Int("w", protocol.DefaultTimeout, "the write timeout in millisecond")
)

var welcomeMessage = "hi :)."

var prompt = "minidb> "

func showPrompt() {
	fmt.Print(prompt)
}

func printMsg(msg protocol.Msg) {
	switch msg.TP {
	case protocol.OkMsgType:
	case protocol.ErrMsgType:
	case protocol.DataMsgType:
	}
}

func handleResp() {

}

func interact(log util.SimpleLogWrapper, conn net.Conn) {
	var input string
	var packetCounter byte = 0
	for {
		showPrompt()
		_, err := fmt.Scanln(&input)
		if err != nil {
			log.ErrorF("input error: %v", err)
			return
		}
		command, err := protocol.StrToCommand(input)
		if err != nil {
			log.ErrorF("parse command error: %v", err)
			continue
		}
		_, err = protocol.WriteCommand(conn, packetCounter, command,
			time.Millisecond*time.Duration(*writeTimeout))
		if err != nil {
			log.ErrorF("failed to send command: err: %v", err)
			continue
		}
		// Now can wait for server response.
		packetCounter++
		msg, err := protocol.ReadResp(conn, packetCounter, time.Millisecond*time.Duration(*readTimeout))
		if err != nil {
			log.ErrorF("failed to read resp: err: %v", err)
			continue
		}
		printMsg(msg)
	}
}

func main() {
	util.InitLogger("", 1024*4, time.Second*1, true)
	log := util.GetLog("client")
	address := fmt.Sprintf("localhost:%d", port)
	con, err := net.Dial("tcp", address)
	if err != nil {
		log.ErrorF("connect to server failed: %v", err)
		return
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go interact(log, con)
	<-sig
	// Todo: close connection
}
