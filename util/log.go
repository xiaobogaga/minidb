package util

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
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

const (
	INFO = iota
	DEBUG
	WARN
	ERROR
	FATAL
)

var (
	logLevelMaps = map[int]string{
		INFO:  "INFO",
		DEBUG: "DEBUG",
		WARN:  "WARN",
		ERROR: "ERROR",
		FATAL: "FATAL",
	}
	fileLog            *SimpleLog
	globalLogLock      sync.RWMutex
	globalLogger       = map[string]SimpleLogWrapper{}
	ErrReInitializeLog = errors.New("log have been initialized.")
	ErrClosedLog       = errors.New("log have been closed")
	logBufChCapacity   = 1 << 16
)

type SimpleLog struct {
	SavePath      string
	BufferSize    int
	flushTime     time.Duration
	lastFlushTime time.Time
	Buf           *bytes.Buffer
	lock          sync.Mutex
	logFlusher    *logFlusher
	logCh         chan *bytes.Buffer
	console       bool
	closed        bool
}

type SimpleLogWrapper struct {
	header string
}

func GetLog(logName string) SimpleLogWrapper {
	globalLogLock.RLock()
	defer globalLogLock.RUnlock()
	_, ok := globalLogger[logName]
	if !ok {
		globalLogger[logName] = SimpleLogWrapper{logName}
	}
	return globalLogger[logName]
}

func CloseLog() error {
	return fileLog.closeLogger()
}

func InitLogger(savePath string, bufSize int, flushTime time.Duration, toConsole bool) error {
	globalLogLock.Lock()
	defer globalLogLock.Unlock()
	if fileLog != nil {
		return nil
	}
	logCh := make(chan *bytes.Buffer, logBufChCapacity)
	// create a to console log only.
	if savePath == "" {
		fileLog = &SimpleLog{
			SavePath: savePath,
			lock:     sync.Mutex{},
			console:  toConsole,
		}
		return nil
	}
	// create a file log.
	flusher, err := newLogFlusher(savePath, logCh)
	if err != nil {
		return err
	}
	fileLog = &SimpleLog{
		SavePath:      savePath,
		BufferSize:    bufSize,
		flushTime:     flushTime,
		lastFlushTime: time.Now(),
		Buf:           new(bytes.Buffer),
		lock:          sync.Mutex{},
		logFlusher:    flusher,
		logCh:         logCh,
		console:       toConsole,
	}
	go flusher.flushLog()
	return nil
}

func (log SimpleLogWrapper) InfoF(format string, params ...interface{}) {
	fileLog.printLog(log.header, INFO, format, params...)
}

func (log SimpleLogWrapper) DebugF(format string, params ...interface{}) {
	fileLog.printLog(log.header, DEBUG, format, params...)
}

func (log SimpleLogWrapper) WarnF(format string, params ...interface{}) {
	fileLog.printLog(log.header, WARN, format, params...)
}

func (log SimpleLogWrapper) ErrorF(format string, params ...interface{}) {
	fileLog.printLog(log.header, ERROR, format, params...)
}

func (log SimpleLogWrapper) FatalF(format string, params ...interface{}) {
	fileLog.printLog(log.header, FATAL, format, params...)
}

func (log *SimpleLog) closeLogger() error {
	log.lock.Lock()
	defer log.lock.Unlock()
	if !log.closed {
		log.doFlushIfNeed(true)
		close(log.logCh)
		log.closed = true
		return nil
	}
	return ErrClosedLog
}

func extractFName(f string) string {
	index := strings.LastIndex(f, "/")
	return f[index+1:]
}

// PrintLog print a log with format like:
// 2006/06/12 00:00:00.000000 [INFO] some thing happened.
func (log *SimpleLog) printLog(header string, level int, format string, a ...interface{}) {
	log.lock.Lock()
	defer log.lock.Unlock()
	_, fName, line, _ := runtime.Caller(2)
	fName = extractFName(fName)
	l := fmt.Sprintf("%s [%s:%d] [%s] [%s]: ", time.Now().Format("2006/01/02 15:04:05.000000"),
		fName, line, header, logLevelMaps[level])
	l = fmt.Sprintf(l+format, a...)
	l += "\n"
	if log.console {
		print(l)
	}
	if log.SavePath == "" {
		return
	}
	log.Buf.WriteString(l)
	log.doFlushIfNeed(false)
}

func (log *SimpleLog) doFlushIfNeed(force bool) {
	if log.closed {
		return
	}
	if force || log.Buf.Len() >= log.BufferSize || log.checkFlushTime() {
		buf := log.Buf
		log.Buf = new(bytes.Buffer)
		log.logCh <- buf
		log.lastFlushTime = time.Now()
		return
	}
}

func (log *SimpleLog) checkFlushTime() bool {
	return time.Now().After(log.lastFlushTime.Add(log.flushTime))
}

type logFlusher struct {
	fileName string
	f        *os.File
	logCh    <-chan *bytes.Buffer
}

func newLogFlusher(fileName string, logCh <-chan *bytes.Buffer) (*logFlusher, error) {
	parentPathIndex := strings.LastIndex(fileName, "/")
	if parentPathIndex > 0 {
		parentPath := fileName[0:parentPathIndex]
		os.MkdirAll(parentPath, os.ModePerm)
	}
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