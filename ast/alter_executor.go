package ast

import "errors"

func (stm *AlterTableAlterEngineStm) Execute() error {
	return errors.New("unsupported statement error")
}

func (stm *AlterTableCharsetCollateStm) Execute() error {
	return errors.New("unsupported statement error")
}

func (stm *AlterDatabaseStm) Execute() error {
	return errors.New("unsupported statement error")
}

func (stm *AlterTableAlterColumnStm) Execute() error {
	return errors.New("unsupported statement error")
}

func (stm *AlterTableAddIndexOrConstraintStm) Execute() error {
	return errors.New("unsupported statement error")
}

func (stm *AlterTableDropIndexOrConstraintStm) Execute() error {
	return errors.New("unsupported statement error")
}
