package server

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMakeScrambledPassword323(t *testing.T) {
	passwordTest := []struct {
		password string
		encoded  string
	}{
		{password: "zhuxiaobo", encoded: "0e72faff12515dff"},
		{password: "root", encoded: "67457e226a1a15bd"},
		{password: "127057fljs", encoded: "59744d9b543b582d"},
		{password: " gslj flsjl121 ", encoded: "0ed46b252ca3c00c"},
	}

	for _, data := range passwordTest {
		assert.Equal(t, data.encoded, makeScrambledPassword323([]byte(data.password)))
	}
}

func TestScramble323(t *testing.T) {
	testData := []struct {
		message   string
		password  string
		scrambled string
	}{
		{scrambled: "FTBBWW[R", message: "%RMnay^L?76HJx?n-4]X", password: "zhuxiaobo"},
		{scrambled: "BBDZAWDS", message: ">}|w_Xz~+X[>bUds9^OO", password: "zhuxiaobo"},
		{scrambled: "XB@[FUQW", message: "oJa)Is*py1$a<F,Hd?OP", password: "zhuxiaobo"},
	}
	for _, data := range testData {
		scrambled := scramble323([]byte(data.message), []byte(data.password))
		assert.Equal(t, []byte(data.scrambled), scrambled[:len(scrambled)-1], data.scrambled)
		assert.True(t, checkScramble323(scrambled, []byte(data.message), hashPassword([]byte(data.password), len(data.password))))
	}
}
