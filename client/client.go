package main

import (
	"bytes"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"simpleDb/protocol"
	"simpleDb/storage"
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
		ret = max(ret, storage.FieldLen(field, col.Values[i]))
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
func printHeader(record *storage.RecordBatch, log util.SimpleLogWrapper, columnWidths []int) {
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
	log.InfoF(buf.String())
}

func printTail(record *storage.RecordBatch, log util.SimpleLogWrapper, columnWidths []int) {
	buf := bytes.Buffer{}
	for i := 0; i < record.ColumnCount(); i++ {
		width := columnWidths[i]
		buf.WriteString("+-")
		buf.WriteString(nHyphen(width))
		buf.WriteString("-")
	}
	buf.WriteString("+\n")
	log.InfoF(buf.String())
}

func printRow(record *storage.RecordBatch, row int, log util.SimpleLogWrapper, columnWidths []int) {
	buf := bytes.Buffer{}
	for i := 0; i < record.ColumnCount(); i++ {
		buf.WriteString("+ ")
		buf.WriteString(fmt.Sprintf("%"+fmt.Sprintf("%ds", columnWidths[i]), record.Records[i].ToString(row)))
		buf.WriteString(" ")
	}
	buf.WriteString("+\n")
	log.InfoF(buf.String())
}

// For the first time, we will print table header.
// +------+--------+
// + col1 +  col2  +
// +------+--------+
// Then for others, will print data looks like this
// +------+--------+
// + 1    + hello  +
// +------+--------+
func printRecord(record *storage.RecordBatch, log util.SimpleLogWrapper, needPrintHeader bool, columnWidths []int) {
	if record == nil {
		return
	}
	if needPrintHeader {
		printHeader(record, log, columnWidths)
	}
	for i := 0; i < record.ColumnCount(); i++ {
		printTail(record, log, columnWidths)
		printRow(record, i, log, columnWidths)
	}
	printTail(record, log, columnWidths)
}

func printMsg(msg protocol.Msg, log util.SimpleLogWrapper, printHeader bool, columnWidths []int) {
	switch msg.TP {
	case protocol.OkMsgType:
		log.InfoF("result: ok")
	case protocol.ErrMsgType:
		log.InfoF("result: failed. err: %v", msg.Msg.(protocol.ErrMsg).Msg)
	case protocol.DataMsgType:
		printRecord(msg.Msg.(*storage.RecordBatch), log, printHeader, columnWidths)
	default:
		panic("unknown message type")
	}
}

func handleResp(packetCounter *byte, conn net.Conn, log util.SimpleLogWrapper) error {
	i := 0
	var columnWidths []int
	for {
		*packetCounter++
		msg, err := protocol.ReadResp(conn, *packetCounter, time.Millisecond*time.Duration(*readTimeout))
		if err != nil {
			return err
		}
		if i == 0 && msg.TP == protocol.DataMsgType {
			columnWidths = columnsWidth(msg.Msg.(*storage.RecordBatch))
		}
		// Only print header for dataMsg once.
		printMsg(msg, log, i == 0, columnWidths)
		if msg.TP != protocol.DataMsgType {
			return nil
		}
		if msg.Msg.(*storage.RecordBatch) == nil {
			return nil
		}
		i += 1
	}
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
		err = handleResp(&packetCounter, conn, log)
		if err != nil {
			log.ErrorF("failed to handle resp: err: %v", err)
			continue
		}
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
