package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"simpleDb/protocol"
	"simpleDb/util"
	"syscall"
	"time"
)

var (
	mode         = flag.Bool("s", false, "whether start server only")
	port         = flag.Int("p", protocol.DefaultPort, "the port which the server will listen to")
	unixSocket   = flag.String("socket", protocol.DefaultUnixSocket, "the unix socket this server will listen")
	readTimeout  = flag.Int("r", protocol.DefaultTimeout, "the read timeout in millisecond")
	writeTimeout = flag.Int("w", protocol.DefaultTimeout, "the write timeout in millisecond")
	logPath      = flag.String("log", fmt.Sprintf("/tmp/minidb/minidb-%v.logPath", time.Now().Unix()), "the logPath path")
)

func main() {
	err := util.InitLogger(*logPath, 1024*4, time.Second, true)
	if err != nil {
		fmt.Printf("err: %v", err)
		return
	}
	log := util.GetLog("server")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	server := protocol.NewServerWithTimeout(*port, time.Millisecond*(time.Duration(*readTimeout)),
		time.Millisecond*(time.Duration(*writeTimeout)), *unixSocket)
	server.Start()
	<-sig
	log.InfoF("bye")
	// Todo: close server
}
