package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
)

func Add(val1 []byte, tp1 FieldTP, val2 []byte, tp2 FieldTP) []byte {
	if tp1.Name == Int {
		intVal1 := DecodeInt(val1)
		switch tp2.Name {
		case Int:
			intVal2 := DecodeInt(val2)
			val := intVal1 + intVal2
			ret := EncodeInt(val)
			return ret
		case Float:
			floatVal2 := DecodeFloat(val2)
			val := float64(intVal1) + floatVal2
			ret := EncodeFloat(val)
			return ret
		default:
			panic("unsupported type on Add")
		}
	}
	if tp1.Name == Float {
		floatVal1 := DecodeFloat(val1)
		switch tp2.Name {
		case Int:
			intVal2 := DecodeInt(val2)
			val := floatVal1 + float64(intVal2)
			ret := EncodeFloat(val)
			return ret
		case Float:
			floatVal2 := DecodeFloat(val2)
			val := floatVal1 + floatVal2
			ret := EncodeFloat(val)
			return ret
		default:
			panic("unsupported type on Add")
		}
	}
	panic("unknown supported type on Add")
}

func Minus(val1 []byte, tp1 FieldTP, val2 []byte, tp2 FieldTP) []byte {
	if tp1.Name == Int {
		intVal1 := DecodeInt(val1)
		switch tp2.Name {
		case Int:
			intVal2 := DecodeInt(val2)
			val := intVal1 - intVal2
			ret := EncodeInt(val)
			return ret
		case Float:
			floatVal2 := DecodeFloat(val2)
			val := float64(intVal1) - floatVal2
			ret := EncodeFloat(val)
			return ret
		default:
			panic("unsupported type on Minus")
		}
	}
	if tp1.Name == Float {
		floatVal1 := DecodeFloat(val1)
		switch tp2.Name {
		case Int:
			intVal2 := DecodeInt(val2)
			val := floatVal1 - float64(intVal2)
			ret := EncodeFloat(val)
			return ret
		case Float:
			floatVal2 := DecodeFloat(val2)
			val := floatVal1 - floatVal2
			ret := EncodeFloat(val)
			return ret
		default:
			panic("unsupported type on Minus")
		}
	}
	panic("unknown supported type on Minus")
}

func Mul(val1 []byte, tp1 FieldTP, val2 []byte, tp2 FieldTP) []byte {
	if tp1.Name == Int {
		intVal1 := DecodeInt(val1)
		switch tp2.Name {
		case Int:
			intVal2 := DecodeInt(val2)
			val := intVal1 * intVal2
			ret := EncodeInt(val)
			return ret
		case Float:
			floatVal2 := DecodeFloat(val2)
			floatVal1 := float64(intVal1)
			val := floatVal1 * floatVal2
			ret := EncodeFloat(val)
			return ret
		default:
			panic("unsupported type on Mul")
		}
	}
	if tp1.Name == Float {
		floatVal1 := DecodeFloat(val1)
		switch tp2.Name {
		case Int:
			intVal2 := DecodeInt(val2)
			val := floatVal1 * float64(intVal2)
			ret := EncodeFloat(val)
			return ret
		case Float:
			floatVal2 := DecodeFloat(val2)
			val := floatVal1 * floatVal2
			ret := EncodeFloat(val)
			return ret
		default:
			panic("unsupported type on Mul")
		}
	}
	panic("unknown supported type on Mul")
}

func Divide(val1 []byte, tp1 FieldTP, val2 []byte, tp2 FieldTP) []byte {
	if tp1.Name == Int {
		intVal1 := DecodeInt(val1)
		switch tp2.Name {
		case Int:
			intVal2 := DecodeInt(val2)
			val := intVal1 / intVal2
			ret := EncodeInt(val)
			return ret
		case Float:
			floatVal2 := DecodeFloat(val2)
			val := float64(intVal1) / floatVal2
			ret := EncodeFloat(val)
			return ret
		default:
			panic("unsupported type on Divide")
		}
	}
	if tp1.Name == Float {
		floatVal1 := DecodeFloat(val1)
		switch tp2.Name {
		case Int:
			intVal2 := DecodeInt(val2)
			val := floatVal1 / float64(intVal2)
			ret := EncodeFloat(val)
			return ret
		case Float:
			floatVal2 := DecodeFloat(val2)
			val := floatVal1 / floatVal2
			ret := EncodeFloat(val)
			return ret
		default:
			panic("unsupported type on Divide")
		}
	}
	panic("unknown supported type on Divide")
}

func Mod(val1 []byte, tp1 FieldTP, val2 []byte, tp2 FieldTP) []byte {
	if tp1.Name != Int || tp2.Name != Int {
		panic("% cannot be applied to non-integer type")
	}
	intVal1 := DecodeInt(val1)
	intVal2 := DecodeInt(val2)
	val := intVal1 % intVal2
	ret := EncodeInt(val)
	return ret
}

func Negative(tp FieldTP, value []byte) []byte {
	switch tp.Name {
	case Int:
		val := DecodeInt(value)
		return EncodeInt(-val)
	case Float:
		v := DecodeFloat(value)
		return EncodeFloat(-v)
	default:
		panic("unsupported type on Negative")
	}
}

// tp1 And tp2 must be equable type. Return a byte encoded by a bool.
func Equal(val1 []byte, tp1 FieldTP, val2 []byte, tp2 FieldTP) []byte {
	g := compare(val1, tp1, val2, tp2) == 0
	return EncodeBool(g)
}

func NotEqual(val1 []byte, tp1 FieldTP, val2 []byte, tp2 FieldTP) []byte {
	b := Equal(val1, tp1, val2, tp2)
	r := DecodeBool(b)
	return EncodeBool(!r)
}

func Is(val1 []byte, tp1 FieldTP, val2 []byte, tp2 FieldTP) []byte {
	return Equal(val1, tp1, val2, tp2)
}

func Great(val1 []byte, tp1 FieldTP, val2 []byte, tp2 FieldTP) []byte {
	g := compare(val1, tp1, val2, tp2) > 0
	return EncodeBool(g)
}

func GreatEqual(val1 []byte, tp1 FieldTP, val2 []byte, tp2 FieldTP) []byte {
	g := compare(val1, tp1, val2, tp2) >= 0
	return EncodeBool(g)
}

func Less(val1 []byte, tp1 FieldTP, val2 []byte, tp2 FieldTP) []byte {
	g := compare(val1, tp1, val2, tp2) < 0
	return EncodeBool(g)
}

func LessEqual(val1 []byte, tp1 FieldTP, val2 []byte, tp2 FieldTP) []byte {
	g := compare(val1, tp1, val2, tp2) <= 0
	return EncodeBool(g)
}

func Max(val1 []byte, tp1 FieldTP, val2 []byte, tp2 FieldTP) []byte {
	g := compare(val1, tp1, val2, tp2)
	if g >= 0 {
		return val1
	}
	return val2
}

func Min(val1 []byte, tp1 FieldTP, val2 []byte, tp2 FieldTP) []byte {
	g := compare(val1, tp1, val2, tp2)
	if g <= 0 {
		return val1
	}
	return val2
}

// Return 0 if val1 == val2. <0 if val1 < val2 And 1 otherwise.
func compare(val1 []byte, tp1 FieldTP, val2 []byte, tp2 FieldTP) int {
	switch tp1.Name {
	case Text, Char, VarChar, MediumText, Blob, MediumBlob, Date, DateTime, Time:
		// we can compare them by bytes.
		return bytes.Compare(val1, val2)
	case Bool:
		return bytes.Compare(val1, val2)
	case Int, Float:
		v1, v2 := float64(0), float64(0)
		if tp1.Name == Int {
			v1 = float64(DecodeInt(val1))
		}
		if tp1.Name == Float {
			v1 = DecodeFloat(val1)
		}
		if tp2.Name == Float {
			v2 = DecodeFloat(val2)
		} else if tp2.Name == Int {
			v2 = float64(DecodeInt(val2))
		} else {
			panic("unsupported type")
		}
		switch {
		case v1 == v2:
			return 0
		case v1 < v2:
			return -1
		case v1 > v2:
			return 1
		}
		return 0
	default:
		panic("unknown type")
	}
}

func And(val1, val2 []byte) []byte {
	v1 := DecodeBool(val1) && DecodeBool(val2)
	return EncodeBool(v1)
}

func Or(val1, val2 []byte) []byte {
	v1 := DecodeBool(val1) || DecodeBool(val2)
	return EncodeBool(v1)
}

func EncodeInt(val int64) (ret []byte) {
	ret = make([]byte, 8)
	binary.BigEndian.PutUint64(ret, uint64(val))
	return ret
}

func EncodeFloat(val float64) []byte {
	ret := make([]byte, 8)
	binary.BigEndian.PutUint64(ret, math.Float64bits(val))
	return ret
}

func EncodeBool(v bool) []byte {
	ret := make([]byte, 1)
	if v {
		ret[0] = 1
	} else {
		ret[0] = 0
	}
	return ret
}

func DecodeFloat(value []byte) float64 {
	v := binary.BigEndian.Uint64(value)
	return math.Float64frombits(v)
}

func DecodeBool(value []byte) bool {
	return value[0] == 1
}

func DecodeInt(value []byte) int64 {
	return int64(binary.BigEndian.Uint64(value))
}

func Encode(value []byte) []byte {
	tp := InferenceType(value)
	switch tp.Name {
	case Int:
		val, _ := strconv.ParseInt(string(value), 10, 64)
		return EncodeInt(val)
	case Float:
		val, _ := strconv.ParseFloat(string(value), 64)
		return EncodeFloat(val)
	case Bool:
		if strings.ToUpper(string(value)) == "TRUE" {
			return EncodeBool(true)
		}
		return EncodeBool(false)
	default:
		return value[1 : len(value)-1]
	}
}

//func fmtFloat(value float64, tp FieldTP) string {
//	if value < 0 {
//		return fmtNegativeFloat(value, tp)
//	}
//	str := fmt.Sprintf("%f", value)
//	digits, decimals := tp.Range[0], tp.Range[1]
//	splits := strings.Split(str, ".")
//	if len(splits) == 1 {
//		if len(splits[0]) > digits {
//			return splits[0][len(splits[0])-digits:]
//		}
//		return splits[0]
//	}
//	// Now has .
//	digitSize := len(splits[1])
//	if decimals >= digits {
//
//	}
//}
//
//func fmtNegativeFloat(value float64, tp FieldTP) string {
//	str := fmt.Sprintf("%f", value)
//	digits, decimals := tp.Range[0], tp.Range[1]
//	splits := strings.Split(str, ".")
//	if len(splits) == 1 {
//		if len(splits[0]) > digits+1 {
//			return splits[0][len(splits[0])-digits:]
//		}
//		return splits[0]
//	}
//}

func DecodeToString(value []byte, tp FieldTP) string {
	if len(value) == 0 {
		return NULL
	}
	switch tp.Name {
	case Int:
		return fmt.Sprintf("%d", DecodeInt(value))
	case Float:
		return fmt.Sprintf(fmt.Sprintf("%s.%df", "%", tp.Range[1]), DecodeFloat(value))
	case Bool:
		if DecodeBool(value) {
			return "true"
		}
		return "false"
	default:
		return string(value)
	}
}

// func DecodeBigInt(value []byte) string {}

// func DecodeChar(value []byte) byte {}

// func DecodeVarChar(value []byte) []byte {}

// func DecodeDateTime(value []byte) time.Time {}

// func DecodeBlob(value []byte) {}

// func DecodeMediumBlob(value []byte) {}

// func DecodeText(value []byte) {}

// func DecodeMediumText(value []byte) {}

// Return the len of value in this field.
func FieldLen(field Field, value []byte) int {
	switch field.TP.Name {
	case Text, Char, VarChar, MediumText, Blob, MediumBlob, Date, DateTime, Time:
		// we can compare them by bytes.
		return len(value)
	case Bool:
		return 1
	case Int:
		return len(strconv.FormatInt(DecodeInt(value), 10))
	case Float:
		v := DecodeFloat(value)
		return len(fmt.Sprintf(fmt.Sprintf("%s.%df", "%", field.TP.Range[1]), v))
	default:
		panic("unknown type")
	}
}
