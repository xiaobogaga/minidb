package ast

import (
	"errors"
)

func (stm *RenameStm) Execute() error {
	return errors.New("unsupported statement error")
}
