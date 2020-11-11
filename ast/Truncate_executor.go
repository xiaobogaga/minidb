package ast

import (
	"errors"
)

func (stm *TruncateStm) Execute() error {
	return errors.New("unsupported statement error")
}
