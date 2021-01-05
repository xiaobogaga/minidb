package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/xiaobogaga/minidb/protocol"
	"github.com/xiaobogaga/minidb/util"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	mode         = flag.String("m", "", "whether start server or client only: s start server only, c start client only")
	port         = flag.Int("p", protocol.DefaultPort, "the port which the server will listen to")
	unixSocket   = flag.String("socket", protocol.DefaultUnixSocket, "the unix socket this server will listen")
	readTimeout  = flag.Int("r", protocol.DefaultTimeout, "the read timeout in millisecond")
	writeTimeout = flag.Int("w", protocol.DefaultTimeout, "the write timeout in millisecond")
	logPath      = flag.String("log", fmt.Sprintf("/tmp/minidb-%v.log", time.Now().Unix()), "the logPath path")
	debug        = flag.Bool("d", false, "whether enable debug mode, will start with several db and tables")
	host         = flag.String("h", "localhost", "the server host")
)

func main() {
	flag.Parse()
	err := util.InitLogger(*logPath, 1024*4, time.Second, *mode == "s")
	if err != nil {
		fmt.Printf("err: %v", err)
		return
	}
	log := util.GetLog("main")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	ctx, cancel := context.WithCancel(context.Background())
	go shutdown(sig, cancel)
	if *mode == "s" || *mode == "" {
		initServer()
	}
	if *mode == "c" || *mode == "" {
		initClient(ctx)
		cancel()
	}
	<-ctx.Done()
	log.InfoF("bye")
}

func shutdown(sig <-chan os.Signal, cancel context.CancelFunc) {
	select {
	case <-sig:
		cancel()
	}
}
