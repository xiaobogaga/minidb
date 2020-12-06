package plan

import (
	"errors"
	"simpleDb/storage"
)

var FuncCallMap = map[string]FuncInterface{}

type FuncInterface interface {
	TypeCheck(params []storage.Field) error
	FuncParamSize() int
	F() funcInterface
	ReturnType() storage.FieldTP
}

func charLength(data [][]byte) []byte {
	charData := data[0]
	ret := storage.Decode(storage.Text, charData)
	length := len(ret.(string))
	bytes, _ := storage.Encode(storage.Int, length)
	return bytes
}

type funcInterface func(params [][]byte) []byte

type CharLengthFunc struct {
	FuncName string
	Fn       funcInterface
}

func (charLengthFunc CharLengthFunc) TypeCheck(params []storage.Field) error {
	if len(params) != 1 {
		return errors.New("param size doesn't match")
	}
	param := params[0]
	if !param.CanCascadeTo(storage.Text) {
		return errors.New("param type doesn't match")
	}
	return nil
}

func (charLengthFunc CharLengthFunc) FuncParamSize() int {
	return 1
}

func (charLengthFunc CharLengthFunc) F() funcInterface {
	return charLength
}

type UpperCaseFunc struct {
	FuncName string
	Fn       funcInterface
}

// Todo, other non aggregation func.
