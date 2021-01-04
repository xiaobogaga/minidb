package main

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"minidb/parser"
	"minidb/plan"
	"minidb/protocol"
	"minidb/util"
	"time"
)

func panicErr(err error) {
	if err == nil {
		return
	}
	panic(err)
}

func initServer() {
	log := util.GetLog("server")
	if *debug {
		log.InfoF("init debug data")
		initDataForDebug()
	}
	server := protocol.NewServerWithTimeout(*port, time.Millisecond*(time.Duration(*readTimeout)),
		time.Millisecond*(time.Duration(*writeTimeout)), *unixSocket)
	server.Start()
}
