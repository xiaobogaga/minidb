package ast

import (
	"errors"
)

func (stm *CreateTableStm) Execute() error {
	return errors.New("unsupported statement error")
}

func (stm *CreateTableAsSelectStm) Execute() error {
	return errors.New("unsupported statement error")
}

func (stm *CreateTableLikeStm) Execute() error {
	return errors.New("unsupported statement error")
}

func (stm *CreateDatabaseStm) Execute() error {
	return errors.New("unsupported statement error")
}
