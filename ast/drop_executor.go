package ast

import (
	"errors"
)

func (stm *DropDatabaseStm) Execute() error {
	return errors.New("unsupported statement error")
}

func (stm *DropTableStm) Execute() error {
	return errors.New("unsupported statement error")
}
