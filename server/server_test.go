package server

import (
	"os"
	"os/signal"
	"simpleDb/log"
	"simpleDb/util"
	"syscall"
	"testing"
)

func waitUntilClose() {
	exit := make(chan os.Signal)
	signal.Notify(exit, os.Interrupt, os.Kill, syscall.SIGTERM)
	<-exit
}

func TestNewServer(t *testing.T) {
	log.InitConsoleLogger("Server")
	port := 3306
	server := NewServer(port, 1, util.GetLog("Server"))
	err := server.Start()
	if err != nil {
		panic(err)
	}
	defer server.Close()
	waitUntilClose()
}
