package storage

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdd(t *testing.T) {
	val1 := EncodeInt(10)
	val2 := EncodeInt(10)
	intVal := DecodeInt(Add(val1, DefaultFieldTpMap[Int], val2, DefaultFieldTpMap[Int]))
	assert.Equal(t, int64(20), intVal)
	val2 = EncodeFloat(5.01)
	floatVal := DecodeFloat(Add(val1, DefaultFieldTpMap[Int], val2, DefaultFieldTpMap[Float]))
	assert.Equal(t, 15.01, floatVal)
	floatVal = DecodeFloat(Add(val2, DefaultFieldTpMap[Float], val1, DefaultFieldTpMap[Int]))
	assert.Equal(t, 15.01, floatVal)
	val1 = EncodeFloat(5.01)
	floatVal = DecodeFloat(Add(val2, DefaultFieldTpMap[Float], val1, DefaultFieldTpMap[Float]))
	assert.Equal(t, 10.02, floatVal)
}

func TestMinus(t *testing.T) {
	val1 := EncodeInt(10)
	val2 := EncodeInt(10)
	intVal := DecodeInt(Minus(val1, DefaultFieldTpMap[Int], val2, DefaultFieldTpMap[Int]))
	assert.Equal(t, int64(0), intVal)
	val2 = EncodeFloat(5.01)
	floatVal := DecodeFloat(Minus(val1, DefaultFieldTpMap[Int], val2, DefaultFieldTpMap[Float]))
	assert.Equal(t, 4.99, floatVal)
	floatVal = DecodeFloat(Minus(val2, DefaultFieldTpMap[Float], val1, DefaultFieldTpMap[Int]))
	assert.Equal(t, -4.99, floatVal)
	val1 = EncodeFloat(5.01)
	floatVal = DecodeFloat(Minus(val2, DefaultFieldTpMap[Float], val1, DefaultFieldTpMap[Float]))
	assert.Equal(t, float64(0), floatVal)
}

func TestMul(t *testing.T) {
	val1 := EncodeInt(10)
	val2 := EncodeInt(10)
	intVal := DecodeInt(Mul(val1, DefaultFieldTpMap[Int], val2, DefaultFieldTpMap[Int]))
	assert.Equal(t, int64(100), intVal)
	val2 = EncodeFloat(5.01)
	floatVal := DecodeFloat(Mul(val1, DefaultFieldTpMap[Int], val2, DefaultFieldTpMap[Float]))
	assert.True(t, 50.1-floatVal < 0.0001)
	floatVal = DecodeFloat(Mul(val2, DefaultFieldTpMap[Float], val1, DefaultFieldTpMap[Int]))
	assert.True(t, 50.1-floatVal < 0.0001)
	val1 = EncodeFloat(5.01)
	floatVal = DecodeFloat(Mul(val2, DefaultFieldTpMap[Float], val1, DefaultFieldTpMap[Float]))
	assert.True(t, 25.1001-floatVal < 0.000000001)
}

func TestDivide(t *testing.T) {
	val1 := EncodeInt(10)
	val2 := EncodeInt(10)
	intVal := DecodeInt(Divide(val1, DefaultFieldTpMap[Int], val2, DefaultFieldTpMap[Int]))
	assert.Equal(t, int64(1), intVal)
	val2 = EncodeFloat(5.01)
	floatVal := DecodeFloat(Divide(val1, DefaultFieldTpMap[Int], val2, DefaultFieldTpMap[Float]))
	assert.True(t, 10/5.01-floatVal < 0.0000001)
	floatVal = DecodeFloat(Divide(val2, DefaultFieldTpMap[Float], val1, DefaultFieldTpMap[Int]))
	assert.True(t, 5.01/10-floatVal < 0.000000001)
	val1 = EncodeFloat(5.01)
	floatVal = DecodeFloat(Divide(val2, DefaultFieldTpMap[Float], val1, DefaultFieldTpMap[Float]))
	assert.Equal(t, float64(1), floatVal)
}

func TestMod(t *testing.T) {
	val1 := EncodeInt(10)
	val2 := EncodeInt(3)
	intVal := DecodeInt(Mod(val1, DefaultFieldTpMap[Int], val2, DefaultFieldTpMap[Int]))
	assert.Equal(t, int64(1), intVal)
}

func TestNegative(t *testing.T) {
	val := int64(10)
	val2 := DecodeInt(Negative(DefaultFieldTpMap[Int], EncodeInt(val)))
	assert.Equal(t, -val, val2)
	val3 := 10.0
	val4 := DecodeFloat(Negative(DefaultFieldTpMap[Float], EncodeFloat(val3)))
	assert.Equal(t, -val3, val4)
}

func TestEqual(t *testing.T) {
	val1 := int64(10)
	val2 := int64(15)
	assert.False(t, DecodeBool(Equal(EncodeInt(val1), DefaultFieldTpMap[Int], EncodeInt(val2), DefaultFieldTpMap[Int])))
	val3 := 10.01
	val4 := 10.01
	assert.True(t, DecodeBool(Equal(EncodeFloat(val3), DefaultFieldTpMap[Float], EncodeFloat(val4), DefaultFieldTpMap[Float])))
	val5 := float64(10)
	assert.True(t, DecodeBool(Equal(EncodeInt(val1), DefaultFieldTpMap[Int], EncodeFloat(val5), DefaultFieldTpMap[Float])))
}

func TestGreat(t *testing.T) {
	assert.True(t, DecodeBool(Great(EncodeInt(10), DefaultFieldTpMap[Int], EncodeFloat(9.8), DefaultFieldTpMap[Float])))
}

func TestCompare(t *testing.T) {
	assert.True(t, compare(EncodeInt(int64(1)), DefaultFieldTpMap[Int], EncodeFloat(float64(1)), DefaultFieldTpMap[Float]) == 0)
	assert.True(t, compare(EncodeInt(int64(0)), DefaultFieldTpMap[Int], EncodeFloat(float64(1)), DefaultFieldTpMap[Float]) < 0)
	assert.True(t, compare(EncodeInt(int64(1)), DefaultFieldTpMap[Int], EncodeFloat(float64(0)), DefaultFieldTpMap[Float]) > 0)
}

func TestMax(t *testing.T) {
	assert.Equal(t, int64(1), DecodeInt(Max(EncodeInt(int64(1)), DefaultFieldTpMap[Int], EncodeFloat(float64(1)), DefaultFieldTpMap[Float])))
	assert.Equal(t, 2.12, DecodeFloat(Max(EncodeInt(int64(1)), DefaultFieldTpMap[Int], EncodeFloat(2.12), DefaultFieldTpMap[Float])))
}

func TestAnd(t *testing.T) {
	assert.False(t, DecodeBool(And(EncodeBool(true), EncodeBool(false))))
}
