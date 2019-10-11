package server

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"math"
)

// Port from mysql.

/*
  The main idea is that no password are sent between client & server on
  connection and that no password are saved in mysql in a decodable form.

  On connection a random string is generated and sent to the client.
  The client generates a new string with a random generator inited with
  the hash values from the password and the sent string.
  This 'check' string is sent to the server where it is compared with
  a string generated from the stored hash_value of the password and the
  random string.

  The password is saved (in user.password) by using the PASSWORD() function in
  mysql.

  This is .c file because it's used in libmysqlclient, which is entirely in C.
  (we need it to be portable to a variety of systems).
  Example:
    update user set password=PASSWORD("hello") where user="test"
  This saves a hashed number as a string in the password field.

  The new authentication is performed in following manner:

  SERVER:  public_seed=create_random_string()
           send(public_seed)

  CLIENT:  recv(public_seed)
           hash_stage1=sha1("password")
           hash_stage2=sha1(hash_stage1)
           reply=xor(hash_stage1, sha1(public_seed,hash_stage2)

           // this three steps are done in scramble()

           send(reply)


  SERVER:  recv(reply)
           hash_stage1=xor(reply, sha1(public_seed,hash_stage2))
           candidate_hash2=sha1(hash_stage1)
           check(candidate_hash2==hash_stage2)

           // this three steps are done in check_scramble()
*/

/*
   Produce an obscure octet sequence from password and random
   string, received from the server. This sequence corresponds to the
   password, but password can not be easily restored from it. The sequence
   is then sent to the server for validation. Trailing zero is not stored
   in the buf as it is not needed.
   This function is used by client to create authenticated reply to the
   server's greeting.
*/
func scramble(password, message []byte) []byte {
	hashStage1 := sha1.Sum(password)
	hashStage2 := sha1.Sum(hashStage1[:])
	publicSeed := sha1.Sum(message)
	enc := sha1.New()
	enc.Write(publicSeed[:])
	ret := enc.Sum(hashStage2[:])
	myCrypt(ret, hashStage1[:])
	return ret
}

/*
   Check that scrambled message corresponds to the password; the function
   is used by server to check that received reply is authentic.
   This function does not check lengths of given strings: message must be
   null-terminated, reply and hash_stage2 must be at least SHA1_HASH_SIZE
   long (if not, something fishy is going on).
*/
func checkScramble(message, hashStage2, scramble []byte) bool {
	enc := sha1.New()
	enc.Write(message)
	ret := enc.Sum(hashStage2)
	// ret ^ ret1 = hashStage1.
	myCrypt(ret, scramble)
	enc.Reset()
	hashstage2Restored := enc.Sum(ret[:])
	return bytes.Equal(hashStage2, hashstage2Restored)
}

func hashPassword(password []byte, len int) []uint32 {
	var nr, add, nr2, tmp uint32 = 1345345333, 7, 0x12345671, 0
	for i := 0; i < len; i++ {
		if password[i] == ' ' || password[i] == '\t' {
			continue
		}
		tmp = uint32(password[i])
		nr ^= (((nr & 63) + add) * tmp) + (nr << 8)
		nr2 += (nr2 << 8) ^ nr
		add += tmp
	}
	res := make([]uint32, 2)
	res[0] = nr & ((uint32(1) << uint32(31)) - uint32(1))
	res[1] = nr2 & ((uint32(1) << uint32(31)) - uint32(1))
	return res
}

type mysqlRand struct {
	seed1       uint32
	seed2       uint32
	maxValue    uint32
	maxValueDbl float64
}

func randomInit(seed1, seed2 uint32) *mysqlRand {
	return &mysqlRand{
		maxValue:    uint32(0x3FFFFFFF),
		maxValueDbl: float64(0x3FFFFFFF),
		seed1:       seed1 % uint32(0x3FFFFFFF),
		seed2:       seed2 % uint32(0x3FFFFFFF),
	}
}

func (rand *mysqlRand) rand() float64 {
	rand.seed1 = (rand.seed1*3 + rand.seed2) % rand.maxValue
	rand.seed2 = (rand.seed1 + rand.seed2 + 33) % rand.maxValue
	return float64(rand.seed1) / rand.maxValueDbl
}

// Used for old client, namely pre at 4.1
func scramble323(message, password []byte) []byte {
	var hashPass, hashMess []uint32
	ret := make([]byte, scrambleLen_323+1)
	if len(password) > 0 {
		hashPass = hashPassword(password, len(password))
		hashMess = hashPassword(message, scrambleLen_323)
		rand := randomInit(hashPass[0]^hashMess[0], hashPass[1]^hashMess[1])
		for i := 0; i < scrambleLen_323; i++ {
			ret[i] = byte(math.Floor(rand.rand()*31) + 64)
		}
		extra := byte(math.Floor(rand.rand() * 31))
		for i := 0; i < scrambleLen_323; i++ {
			ret[i] ^= extra
		}
	}
	ret[len(ret)-1] = 0
	return ret
}

func checkScramble323(scrambled, message []byte, hashPass []uint32) bool {
	var hashMess []uint32
	to := make([]byte, len(scrambled))
	copy(to, scrambled)
	hashMess = hashPassword(message, scrambleLen_323)
	rand := randomInit(hashPass[0]^hashMess[0], hashPass[1]^hashMess[1])
	for i := 0; i < scrambleLen_323; i++ {
		to[i] = byte(math.Floor(rand.rand()*31) + 64)
	}
	extra := byte(math.Floor(rand.rand() * 31))
	for i := 0; i < scrambleLen_323; i++ {
		if scrambled[i] != byte(to[i]^extra) {
			return false
		}
	}
	return true
}

func makeScrambledPassword323(password []byte) string {
	res := hashPassword(password, len(password))
	return fmt.Sprintf("%08x%08x", res[0], res[1])
}

func myCrypt(input1, input2 []byte) {
	for i, b := range input1 {
		input1[i] = b ^ input2[i]
	}
}
