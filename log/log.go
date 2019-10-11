package log

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"
)

// A Simple log library implementation. There is a globalLogger, a map from logName -> SimpleLog. Each SimpleLog has a
// unique logName and savePath to save logs. Also SimpleLog can add headers. For example, by adding a log header
// 'SimpleServer' to a SimpleLog, we use print like `2006/06/11 00:00:00.000000 [SimpleServer] [INFO]: some things happened.` and don't
// need to add SimpleServer to the print method.
// Usage:
// ```golang
//	InitLogger(10240, "./log.log", "DefaultLog")
//	serverLog := GetLog("DefaultLog").AddHeader("Server")
//	serverLog.InfoF("start failed")
// ```

var verbose = flag.Bool("verbose", true, "indicate whether print log on console")
var logLevelMaps = map[int]string{
	INFO:  "INFO",
	DEBUG: "DEBUG",
	WARN:  "WARN",
	ERROR: "ERROR",
	FATAL: "FATAL",
}

const (
	INFO = iota
	DEBUG
	WARN
	ERROR
	FATAL
)

var globalLogLock sync.RWMutex

type SimpleLog struct {
	SavePath   string
	BufferSize int
	Buf        *bytes.Buffer
	BufLock    sync.Mutex
	logFlusher *logFlusher
	logCh      chan *bytes.Buffer
	console    bool
}

type SimpleLogWrapper struct {
	log    *SimpleLog
	header string
}

var globalLogger = map[string]*SimpleLog{}

func GetLog(logName string) *SimpleLog {
	globalLogLock.RLock()
	defer globalLogLock.RUnlock()
	return globalLogger[logName]
}

func CloseLog(logName string) {
	log, ok := globalLogger[logName]
	if !ok {
		return
	}
	log.closeLogger()
}

var ErrReInitializeLog = errors.New("log have been initialized.")

var logBufChCapacity = 1 << 16

// Add a file logger that would print logs to files and also print logs to console if option `verbose` is opened.
func InitFileLogger(logName, savePath string, bufSize int) error {
	globalLogLock.Lock()
	defer globalLogLock.Unlock()
	_, ok := globalLogger[logName]
	if ok {
		return ErrReInitializeLog
	}
	logCh := make(chan *bytes.Buffer, logBufChCapacity)
	flusher, err := newLogFlusher(savePath, logCh)
	if err != nil {
		return err
	}
	newLogger := &SimpleLog{
		SavePath:   savePath,
		BufferSize: bufSize,
		Buf:        new(bytes.Buffer),
		BufLock:    sync.Mutex{},
		logFlusher: flusher,
		logCh:      logCh,
	}
	globalLogger[logName] = newLogger
	go flusher.flushLog()
	return nil
}

// Add a console logger that would only print logs to console.
func InitConsoleLogger(logName string) error {
	globalLogLock.Lock()
	defer globalLogLock.Unlock()
	_, ok := globalLogger[logName]
	if ok {
		return ErrReInitializeLog
	}
	newLogger := &SimpleLog{
		console: true,
	}
	globalLogger[logName] = newLogger
	return nil
}

func (log SimpleLogWrapper) InfoF(format string, params ...interface{}) {
	log.log.printLog(log.header, INFO, format, params...)
}

func (log SimpleLogWrapper) DebugF(format string, params ...interface{}) {
	log.log.printLog(log.header, DEBUG, format, params...)
}

func (log SimpleLogWrapper) WarnF(format string, params ...interface{}) {
	log.log.printLog(log.header, WARN, format, params...)
}

func (log SimpleLogWrapper) ErrorF(format string, params ...interface{}) {
	log.log.printLog(log.header, ERROR, format, params...)
}

func (log SimpleLogWrapper) FatalF(format string, params ...interface{}) {
	log.log.printLog(log.header, FATAL, format, params...)
}

func (log SimpleLogWrapper) GetUnderlineLog() *SimpleLog {
	return log.log
}

func (log *SimpleLog) AddHeader(header string) SimpleLogWrapper {
	return SimpleLogWrapper{header: header, log: log}
}

func (log *SimpleLog) closeLogger() {
	if log.logCh != nil {
		log.doFlushIfNeed(true)
		close(log.logCh)
	}
}

// PrintLog print a log with format like:
// 2006/06/12 00:00:00.000000 [INFO] some thing happened.
func (log *SimpleLog) printLog(header string, level int, format string, a ...interface{}) {
	l := fmt.Sprintf("%s [%s] [%s]: ", time.Now().Format("2006/06/12 00:00:00.000000"), header, logLevelMaps[level])
	l = fmt.Sprintf(l+format, a...)
	if log.console {
		println(l)
		return
	}
	if *verbose {
		println(l)
	}
	log.BufLock.Lock()
	defer log.BufLock.Unlock()
	log.Buf.WriteString(l)
	log.Buf.WriteByte('\n')
	log.doFlushIfNeed(false)
}

func (log *SimpleLog) doFlushIfNeed(force bool) {
	if force {
		buf := log.Buf
		log.Buf = new(bytes.Buffer)
		log.logCh <- buf
		return
	}
	if log.Buf.Len() >= log.BufferSize {
		buf := log.Buf
		log.Buf = new(bytes.Buffer)
		log.logCh <- buf
	}
}

type logFlusher struct {
	fileName string
	f        *os.File
	logCh    <-chan *bytes.Buffer
}

func newLogFlusher(fileName string, logCh <-chan *bytes.Buffer) (*logFlusher, error) {
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &logFlusher{
		fileName: fileName,
		f:        f,
		logCh:    logCh,
	}, nil
}

func (flusher *logFlusher) close() error {
	return flusher.f.Close()
}

func (flusher *logFlusher) flushLog() {
	for buf := range flusher.logCh {
		// NOTE: We ignore the returned value of writeTo.
		buf.WriteTo(flusher.f)
	}
	flusher.close()
}
