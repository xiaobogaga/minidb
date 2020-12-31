package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"minidb/protocol"
	"minidb/storage"
	"minidb/util"
	"net"
	"os"
	"os/signal"
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Return the maximum width of the column in record.
func columnWidth(record *storage.RecordBatch, column int) int {
	field := record.Fields[column]
	ret := len(field.Name)
	col := record.Records[column]
	for i := 0; i < col.Size(); i++ {
		ret = max(ret, storage.FieldLen(field, col.RawValue(i)))
	}
	return ret
}

func columnsWidth(record *storage.RecordBatch) []int {
	ret := make([]int, record.ColumnCount())
	for i := 0; i < record.ColumnCount(); i++ {
		ret[i] = columnWidth(record, i)
	}
	return ret
}

func nHyphen(n int) string {
	buf := bytes.Buffer{}
	for i := 0; i < n; i++ {
		buf.WriteByte('-')
	}
	return buf.String()
}

// Print table columns with col names.
// +------+------+
// +   id +  id2 +
func printHeader(record *storage.RecordBatch, columnWidths []int) {
	// +-width-+
	buf := bytes.Buffer{}
	for i := 0; i < record.ColumnCount(); i++ {
		width := columnWidths[i]
		buf.WriteString("+-")
		buf.WriteString(nHyphen(width))
		buf.WriteString("-")
	}
	buf.WriteString("+\n")
	for i := 0; i < record.ColumnCount(); i++ {
		buf.WriteString("+ ")
		buf.WriteString(fmt.Sprintf("%"+fmt.Sprintf("%ds", columnWidths[i]), record.Fields[i].Name))
		buf.WriteString(" ")
	}
	buf.WriteString("+\n")
	print(buf.String())
}

func printTail(record *storage.RecordBatch, columnWidths []int) {
	buf := bytes.Buffer{}
	for i := 0; i < record.ColumnCount(); i++ {
		width := columnWidths[i]
		buf.WriteString("+-")
		buf.WriteString(nHyphen(width))
		buf.WriteString("-")
	}
	buf.WriteString("+\n")
	print(buf.String())
}

func printRow(record *storage.RecordBatch, row int, columnWidths []int) {
	buf := bytes.Buffer{}
	for i := 0; i < record.ColumnCount(); i++ {
		buf.WriteString("+ ")
		buf.WriteString(fmt.Sprintf("%"+fmt.Sprintf("%ds", columnWidths[i]), record.Records[i].ToString(row)))
		buf.WriteString(" ")
	}
	buf.WriteString("+\n")
	print(buf.String())
}

// For the first time, we will print table header.
// +------+--------+
// + col1 +  col2  +
// +------+--------+
// Then for others, will print data looks like this
// +------+--------+
// + 1    + hello  +
// +------+--------+
func printRecord(record *storage.RecordBatch, needPrintHeader bool, columnWidths []int) {
	if record == nil {
		return
	}
	if needPrintHeader {
		printHeader(record, columnWidths)
	}
	for i := 0; i < record.RowCount(); i++ {
		printTail(record, columnWidths)
		printRow(record, i, columnWidths)
	}
	printTail(record, columnWidths)
}

func printMsg(msg protocol.Msg, printHeader bool, columnWidths []int) {
	switch msg.TP {
	case protocol.OkMsgType, protocol.ErrMsgType:
		println("server: ", msg.Msg.(protocol.ErrMsg).Msg)
	case protocol.DataMsgType:
		printRecord(msg.Msg.(*storage.RecordBatch), printHeader, columnWidths)
	default:
		panic("unknown message type")
	}
}

func handleResp(packetCounter byte, conn net.Conn) error {
	i := 0
	var columnWidths []int
	for {
		msg := protocol.ReadResp(conn, packetCounter, time.Millisecond*time.Duration(*readTimeout))
		if msg.IsFatal() {
			return errors.New(msg.Msg.(protocol.ErrMsg).Msg)
		}
		if i == 0 && msg.TP == protocol.DataMsgType {
			columnWidths = columnsWidth(msg.Msg.(*storage.RecordBatch))
		}
		// Only print header for dataMsg once.
		printMsg(msg, i == 0, columnWidths)
		if msg.TP != protocol.DataMsgType {
			return nil
		}
		if msg.Msg.(*storage.RecordBatch) == nil {
			return nil
		}
		i += 1
	}
}

func interact(cancel context.CancelFunc, log util.SimpleLogWrapper, conn net.Conn) {
	defer cancel()
	var packetCounter byte = 0
	reader := bufio.NewReader(os.Stdin)
	for {
		showPrompt()
		input, _, _ := reader.ReadLine()
		command, err := protocol.StrToCommand(string(input))
		if err != nil {
			log.ErrorF("parse command error: %v", err)
			continue
		}
		errMsg := protocol.WriteCommand(conn, packetCounter, command,
			time.Millisecond*time.Duration(*writeTimeout))
		if !errMsg.IsOk() {
			log.ErrorF("failed to send command: err: %v", err)
			continue
		}
		if command.Tp == protocol.TpComQuit {
			return
		}
		// Now can wait for server response.
		err = handleResp(packetCounter, conn)
		if err != nil {
			log.ErrorF("failed to handle resp: err: %v", err)
			return
		}
		packetCounter++
	}
}

func main() {
	// Initialize log
	err := util.InitLogger("", 1024*4, time.Second*1, true)
	if err != nil {
		fmt.Printf("err: %v", err)
		return
	}
	log := util.GetLog("client")
	// Create connection.
	address := fmt.Sprintf("localhost:%d", *port)
	con, err := net.Dial("tcp", address)
	if err != nil {
		log.ErrorF("connect to server failed: %v", err)
		return
	}
	defer con.Close()
	// Start and wait for connection.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	ctx, cancel := context.WithCancel(context.Background())
	go interact(cancel, log, con)
	select {
	case <-sig:
		println("bye")
	case <-ctx.Done():
		println("bye")
	}
}
