package plan

import (
	"bytes"
	"errors"
	"fmt"
	"minidb/storage"
	"strings"
)

func getFunc(name string, params []LogicExpr) FuncInterface {
	name = strings.ToUpper(name)
	switch name {
	case "CHARLENGTH":
		return CharLengthFunc{Name: "CHARLENGTH", Fn: charLength, Params: params}
	case "MIN":
		return &MinFunc{Name: "MIN", Params: params}
	case "MAX":
		return &MaxFunc{Name: "MAX", Params: params}
	case "SUM":
		return &SumFunc{Name: "SUM", Params: params}
	case "COUNT":
		return &CountFunc{Name: "COUNT", Params: params}
	default:
		return nil
	}
}

type FuncInterface interface {
	TypeCheck() error
	FuncParamSize() int
	F() funcInterface
	ReturnType() storage.FieldTP
	Accumulate(row int, input *storage.RecordBatch)
	AccumulateValue() []byte
	IsAggrFunc() bool
	String() string
}

func charLength(data [][]byte) []byte {
	length := len(data[0])
	bytes := storage.EncodeInt(int64(length))
	return bytes
}

type funcInterface func(params [][]byte) []byte

type CharLengthFunc struct {
	Name   string
	Fn     funcInterface
	Params []LogicExpr
}

func (charLengthFunc CharLengthFunc) TypeCheck() error {
	if len(charLengthFunc.Params) != 1 {
		return errors.New(fmt.Sprintf("%s: param size doesn't match", charLengthFunc.String()))
	}
	param := charLengthFunc.Params[0]
	if !param.toField().IsString() {
		return errors.New(fmt.Sprintf("%s: param type doesn't match", charLengthFunc.String()))
	}
	return nil
}

func (charLengthFunc CharLengthFunc) FuncParamSize() int {
	return 1
}

func (charLengthFunc CharLengthFunc) ReturnType() storage.FieldTP {
	return storage.Int
}

func (charLengthFunc CharLengthFunc) Accumulate(row int, input *storage.RecordBatch) {
	panic("not a aggr function")
}

func (charLengthFunc CharLengthFunc) AccumulateValue() []byte {
	panic("not a aggr function")
}

func (charLengthFunc CharLengthFunc) IsAggrFunc() bool {
	return false
}

func (charLengthFunc CharLengthFunc) String() string {
	bf := bytes.Buffer{}
	bf.WriteString(charLengthFunc.Name)
	bf.WriteString("(")
	for i, param := range charLengthFunc.Params {
		bf.WriteString(param.String())
		if i != len(charLengthFunc.Params)-1 {
			bf.WriteString(", ")
		}
	}
	bf.WriteString(")")
	return bf.String()
}

func (charLengthFunc CharLengthFunc) F() funcInterface {
	return charLength
}

// Todo, other non aggregation func.

type MaxFunc struct {
	Name        string
	Fn          funcInterface
	Params      []LogicExpr
	Accumulator []byte
}

func (max *MaxFunc) TypeCheck() error {
	if len(max.Params) != 1 {
		return errors.New(fmt.Sprintf("%s: param size doesn't match", max.String()))
	}
	return nil
}

func (max *MaxFunc) FuncParamSize() int {
	return 1
}

func (max *MaxFunc) F() funcInterface {
	return nil
}

func (max *MaxFunc) ReturnType() storage.FieldTP {
	return max.Params[0].toField().TP
}

func (max *MaxFunc) Accumulate(row int, input *storage.RecordBatch) {
	data := max.Params[0].EvaluateRow(row, input)
	if len(max.Accumulator) == 0 {
		max.Accumulator = data
		return
	}
	max.Accumulator = storage.Max(max.Accumulator, max.ReturnType(), data, max.ReturnType())
}

func (max *MaxFunc) AccumulateValue() []byte {
	return max.Accumulator
}

func (max *MaxFunc) IsAggrFunc() bool {
	return true
}

func (max *MaxFunc) String() string {
	bf := bytes.Buffer{}
	bf.WriteString(max.Name)
	bf.WriteString("(")
	for i, param := range max.Params {
		bf.WriteString(param.String())
		if i != len(max.Params)-1 {
			bf.WriteString(", ")
		}
	}
	bf.WriteString(")")
	return bf.String()
}

type MinFunc struct {
	Name        string
	Fn          funcInterface
	Params      []LogicExpr
	Accumulator []byte
}

func (min *MinFunc) TypeCheck() error {
	if len(min.Params) != 1 {
		return errors.New(fmt.Sprintf("%s: param size doesn't match", min.String()))
	}
	return nil
}

func (min *MinFunc) FuncParamSize() int {
	return 1
}

func (min *MinFunc) F() funcInterface {
	return nil
}

func (min *MinFunc) ReturnType() storage.FieldTP {
	return min.Params[0].toField().TP
}

func (min *MinFunc) Accumulate(row int, input *storage.RecordBatch) {
	data := min.Params[0].EvaluateRow(row, input)
	if len(min.Accumulator) == 0 {
		min.Accumulator = data
		return
	}
	min.Accumulator = storage.Min(min.Accumulator, min.ReturnType(), data, min.ReturnType())
}

func (min *MinFunc) AccumulateValue() []byte {
	return min.Accumulator
}

func (min *MinFunc) IsAggrFunc() bool {
	return true
}

func (min *MinFunc) String() string {
	bf := bytes.Buffer{}
	bf.WriteString(min.Name)
	bf.WriteString("(")
	for i, param := range min.Params {
		bf.WriteString(param.String())
		if i != len(min.Params)-1 {
			bf.WriteString(", ")
		}
	}
	bf.WriteString(")")
	return bf.String()
}

type CountFunc struct {
	Name        string
	Fn          funcInterface
	Params      []LogicExpr
	Accumulator []byte
}

func (count *CountFunc) TypeCheck() error {
	if len(count.Params) != 1 {
		return errors.New(fmt.Sprintf("%s: param size doesn't match", count.String()))
	}
	return nil
}

func (count *CountFunc) FuncParamSize() int {
	return 1
}

func (count *CountFunc) F() funcInterface {
	return nil
}

func (count *CountFunc) ReturnType() storage.FieldTP {
	return storage.Int
}

func (count *CountFunc) Accumulate(row int, input *storage.RecordBatch) {
	if len(count.Accumulator) == 0 {
		count.Accumulator = storage.EncodeInt(1)
		return
	}
	count.Accumulator = storage.Add(count.Accumulator, storage.Int, storage.EncodeInt(1), storage.Int)
}

func (count *CountFunc) AccumulateValue() []byte {
	return count.Accumulator
}

func (count *CountFunc) IsAggrFunc() bool {
	return true
}

func (count *CountFunc) String() string {
	bf := bytes.Buffer{}
	bf.WriteString(count.Name)
	bf.WriteString("(")
	for i, param := range count.Params {
		bf.WriteString(param.String())
		if i != len(count.Params)-1 {
			bf.WriteString(", ")
		}
	}
	bf.WriteString(")")
	return bf.String()
}

type SumFunc struct {
	Name        string
	Fn          funcInterface
	Params      []LogicExpr
	Accumulator []byte
}

func (sum *SumFunc) TypeCheck() error {
	if len(sum.Params) != 1 {
		return errors.New(fmt.Sprintf("%s: param size doesn't match", sum.String()))
	}
	if !sum.Params[0].toField().IsNumerical() {
		return errors.New(fmt.Sprintf("%s: param type doesn't match", sum.String()))
	}
	return nil
}

func (sum *SumFunc) FuncParamSize() int {
	return 1
}

func (sum *SumFunc) F() funcInterface {
	return nil
}

func (sum *SumFunc) ReturnType() storage.FieldTP {
	return sum.Params[0].toField().TP
}

func (sum *SumFunc) Accumulate(row int, input *storage.RecordBatch) {
	data := sum.Params[0].EvaluateRow(row, input)
	if len(sum.Accumulator) == 0 {
		sum.Accumulator = data
		return
	}
	sum.Accumulator = storage.Add(sum.Accumulator, sum.ReturnType(), data, sum.ReturnType())
}

func (sum *SumFunc) AccumulateValue() []byte {
	return sum.Accumulator
}

func (sum *SumFunc) IsAggrFunc() bool {
	return true
}

func (sum *SumFunc) String() string {
	bf := bytes.Buffer{}
	bf.WriteString(sum.Name)
	bf.WriteString("(")
	for i, param := range sum.Params {
		bf.WriteString(param.String())
		if i != len(sum.Params)-1 {
			bf.WriteString(", ")
		}
	}
	bf.WriteString(")")
	return bf.String()
}
