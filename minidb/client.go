package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"minidb/protocol"
	"minidb/storage"
	"minidb/util"
	"net"
	"os"
	"time"
)

var welcomeMessage = "hi :)"

var prompt = "minidb> "

func showPrompt() {
	fmt.Print(prompt)
}

// Return the maximum width of the column in record.
func columnWidth(record *storage.RecordBatch, column int) int {
	field := record.Fields[column]
	ret := len(field.Name)
	col := record.Records[column]
	for i := 0; i < col.Size(); i++ {
		ret = util.Max(ret, storage.FieldLen(field, col.RawValue(i)))
	}
	return ret
}

func columnsWidth(record *storage.RecordBatch) []int {
	ret := make([]int, record.ColumnCount())
	for i := 0; i < record.ColumnCount(); i++ {
		if record.IsRowIdColumn(i) {
			continue
		}
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
		if record.IsRowIdColumn(i) {
			continue
		}
		width := columnWidths[i]
		buf.WriteString("+-")
		buf.WriteString(nHyphen(width))
		buf.WriteString("-")
	}
	buf.WriteString("+\n")
	for i := 0; i < record.ColumnCount(); i++ {
		if record.IsRowIdColumn(i) {
			continue
		}
		buf.WriteString("+ ")
		buf.WriteString(fmt.Sprintf("%"+fmt.Sprintf("%ds", columnWidths[i]), record.Fields[i].Name))
		buf.WriteString(" ")
	}
	buf.WriteString("+\n")
	print(buf.String())
}

// Print table tail: +------+--------+
func printTail(record *storage.RecordBatch, columnWidths []int) {
	buf := bytes.Buffer{}
	for i := 0; i < record.ColumnCount(); i++ {
		if record.IsRowIdColumn(i) {
			continue
		}
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
		if record.IsRowIdColumn(i) {
			continue
		}
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
// Then for others, will print data looks like this
// + 1    + hello  +
func printRecord(record *storage.RecordBatch) {
	if record == nil {
		return
	}
	columnWidths := columnsWidth(record)
	printHeader(record, columnWidths)
	if record.RowCount() != 0 {
		printTail(record, columnWidths)
	}
	for i := 0; i < record.RowCount(); i++ {
		// printTail(record, columnWidths)
		printRow(record, i, columnWidths)
	}
	printTail(record, columnWidths)
}

func printMsg(msg protocol.Msg) {
	switch msg.TP {
	case protocol.OkMsgType, protocol.ErrMsgType:
		println("server: ", msg.Msg.(protocol.ErrMsg).Msg)
	default:
		panic("cannot print such message")
	}
}

func handleResp(packetCounter byte, conn net.Conn) error {
	var records *storage.RecordBatch
	var msg protocol.Msg
	for {
		msg = protocol.ReadResp(conn, packetCounter, time.Millisecond*time.Duration(*readTimeout))
		if msg.IsFatal() {
			return errors.New(msg.Msg.(protocol.ErrMsg).Msg)
		}
		if msg.TP == protocol.DataMsgType && msg.Msg.(*storage.RecordBatch) == nil {
			break
		}
		if msg.TP != protocol.DataMsgType {
			break
		}
		// append records.
		record := msg.Msg.(*storage.RecordBatch)
		if records == nil {
			records = record
		} else {
			records.Append(record)
		}
	}
	printRecord(records)
	printMsg(msg)
	return nil
}

func showWelcomeMessage() {
	println(welcomeMessage)
}

func interact(cancel context.CancelFunc, conn net.Conn) {
	defer cancel()
	var packetCounter byte = 0
	showWelcomeMessage()
	reader := bufio.NewReader(os.Stdin)
	for {
		showPrompt()
		input, _, _ := reader.ReadLine()
		command, err := protocol.StrToCommand(string(input))
		if err != nil {
			fmt.Printf("parse command error: %v\n", err)
			continue
		}
		errMsg := protocol.WriteCommand(conn, packetCounter, command,
			time.Millisecond*time.Duration(*writeTimeout))
		if !errMsg.IsOk() {
			fmt.Printf("failed to send command: err: %v\n", err)
			return
		}
		if command.Tp == protocol.TpComQuit {
			return
		}
		// Now can wait for server response.
		err = handleResp(packetCounter, conn)
		if err != nil {
			fmt.Printf("failed to handle resp: err: %v\n", err)
			return
		}
		packetCounter++
	}
}

func initClient(ctx context.Context) {
	address := fmt.Sprintf("localhost:%d", *port)
	con, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("connect to server failed: %v\n", err)
		return
	}
	defer con.Close()
	// Start and wait for connection.
	ctx2, cancel := context.WithCancel(ctx)
	go interact(cancel, con)
	select {
	case <-ctx2.Done():
		println("bye")
	}
}
