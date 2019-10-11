package log

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInitConsoleLog(t *testing.T) {
	err := InitConsoleLogger("Test")
	assert.Nil(t, err)
	a := GetLog("Test")
	assert.NotNil(t, a)
	log := a.AddHeader("Test")
	assert.NotNil(t, log)
	log.InfoF("Hello world: %s", "name")
}

func TestInitFileLogger(t *testing.T) {

}

func TestSimpleLog_AddHeader(t *testing.T) {

}

func TestSimpleLogWrapper_InfoF(t *testing.T) {

}

func TestSimpleLogWrapper_WarnF(t *testing.T) {

}

func TestSimpleLogWrapper_DebugF(t *testing.T) {

}

func TestSimpleLogWrapper_ErrorF(t *testing.T) {

}

func TestSimpleLogWrapper_FatalF(t *testing.T) {

}
