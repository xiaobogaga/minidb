package lexer

import (
	"bytes"
	"fmt"
)

type TokenType int

func (t TokenType) String() string {
	return revertKeyWords[t]
}

// Create table
// Create Database
// Insert
// Delete
// Select
// Update
// Drop
// Truncate
// Rename
// Alter
const (
	// create database `IDENT`;
	// WhiteSpace including " , \t, \n"
	CREATE TokenType = iota
	SPACE            // \s
	DATABASE
	IDENT // a string value
	SEMICOLON

	// create table [IF NOT EXIST] `IDENT` ( `` INT PRIMARY KEY DEFAULT VALUE, `` VARCHAR())
	TABLE
	IF
	NOT
	EXIST
	LEFTBRACKET
	// column types
	BOOL
	INT
	FLOAT
	CHAR
	VARCHAR
	STRING
	DEFAULT

	PRIMARY
	KEY
	COMMA

	// insert into `` values ("", 10, "")
	INSERT
	INTO
	VALUES

	// VALUES TYPE
	TRUE
	FALSE
	INTVALUE
	FLOATVALUE
	CHARVALUE
	STRINGVALUE

	RIGHTBRACKET

	// delete from `IDENT` [where `` = VALUE AND | OR `` = VALUE] [ORDER BY COLS] [LIMIT INTVALUE]
	DELETE
	FROM
	WHERE
	// AND, OR can be as
	AND
	OR
	ORDER
	BY
	LIMIT

	// drop database
	DROP

	// update `` set `` = `` where ...
	UPDATE
	SET

	// select ,,, from `` where `` = ``
	SELECT
	// select * from
	STAR // can also be seen as MULTIPLE
	// WORD
	WORD
	// expression
	// Normally a expression can be defined -> (ope)|ope operations (ope)|ope
	// 								ope     -> (ope) operations (ope)
	// condition expression.
	ASSIGNEQUAL
	CHECKEQUAL
	NOTEQUAL
	GREAT
	GREATEQUAL
	LESS
	LESSEQUAL

	// calculation expression
	ADD
	MINUS
	DIVIDE

	// Truncate [table] tableName
	TRUNCATE
	// Rename tb1 To tb2 | db1 to db2 [, tb1 To tb2 | db1 to db2]
	RENAME
	TO

	// ALTER [TABLE] TB_NAME ([ADD [COLUMN] COL_NAME [FIRST | AFTER COL_NAME]) | ([DROP [COLUMN] COL_NAME]) | (ALTER [COLUMN] COLNAME [SET DEFAULT VALUE | DROP DEFAULT]) |
	// (CHANGE [COLUMN] OLD_COLUMN NEW_COL_NAME COLDEF [FIRST | AFTER COL_NAME])
	ALTER
	COLADD
	FIRST
	AFTER
	COLUMN
	CHANGE
)

type LexicalError string

func (err LexicalError) Error() string {
	return string(err)
}

const (
	StringUnExpecedEndErr = LexicalError("unexpected string end")
	CharUnExpectedEndErr  = LexicalError("unexpected char end")
	CharFormatErr         = LexicalError("char format err")
	IdentFormatErr        = LexicalError("wrong ident")
	WordFormatErr         = LexicalError("wrong word format")
	UnknownTokenErr       = LexicalError("unknown token")
)

type Token struct {
	Tp       TokenType
	StartPos int
	EndPos   int
}

type Lexer struct {
	Tokens []Token
	Data   []byte
	pos    int
}

func NewLexer() *Lexer {
	return &Lexer{}
}

var keyWords = map[string]TokenType{}
var singleCharKeyWordMap = map[byte]TokenType{}
var revertKeyWords = map[TokenType]string{}

func init() {
	// init keyWords map
	keyWords["CREATE"] = CREATE
	keyWords["DATABASE"] = DATABASE
	keyWords["TABLE"] = TABLE
	keyWords["IF"] = IF
	keyWords["NOT"] = NOT
	keyWords["EXIST"] = EXIST
	keyWords["BOOL"] = BOOL
	keyWords["FALSE"] = FALSE
	keyWords["TRUE"] = TRUE
	keyWords["INT"] = INT
	keyWords["FLOAT"] = FLOAT
	keyWords["CHAR"] = CHAR
	keyWords["VARCHAR"] = VARCHAR
	keyWords["STRING"] = STRING
	keyWords["DEFAULT"] = DEFAULT
	keyWords["PRIMARY"] = PRIMARY
	keyWords["KEY"] = KEY
	keyWords["INSERT"] = INSERT
	keyWords["INTO"] = INTO
	keyWords["VALUES"] = VALUES
	keyWords["DELETE"] = DELETE
	keyWords["FROM"] = FROM
	keyWords["WHERE"] = WHERE
	keyWords["AND"] = AND
	keyWords["OR"] = OR
	keyWords["ORDER"] = ORDER
	keyWords["BY"] = BY
	keyWords["LIMIT"] = LIMIT

	keyWords["DROP"] = DROP
	keyWords["UPDATE"] = UPDATE
	keyWords["SET"] = SET
	keyWords["SELECT"] = SELECT
	keyWords["TRUNCATE"] = TRUNCATE
	keyWords["RENAME"] = RENAME
	keyWords["TO"] = TO

	keyWords["ALTER"] = ALTER
	keyWords["ADD"] = COLADD
	keyWords["FIRST"] = FIRST
	keyWords["AFTER"] = AFTER
	keyWords["COLUMN"] = COLUMN
	keyWords["CHANGE"] = CHANGE

	singleCharKeyWordMap['>'] = GREAT
	singleCharKeyWordMap['<'] = LESS
	singleCharKeyWordMap['+'] = ADD
	singleCharKeyWordMap['-'] = MINUS
	singleCharKeyWordMap['/'] = DIVIDE
	singleCharKeyWordMap[')'] = RIGHTBRACKET
	singleCharKeyWordMap['('] = LEFTBRACKET
	singleCharKeyWordMap[','] = COMMA
	singleCharKeyWordMap[';'] = SEMICOLON
	singleCharKeyWordMap['*'] = STAR

	for k, v := range keyWords {
		revertKeyWords[v] = k
	}
	revertKeyWords[COLADD] = "COLADD"
	revertKeyWords[SPACE] = "SPACE"
	revertKeyWords[IDENT] = "IDENT"
	revertKeyWords[SEMICOLON] = "SEMICOLON"
	revertKeyWords[LEFTBRACKET] = "LEFTBRACKET"
	revertKeyWords[RIGHTBRACKET] = "RIGHTBRACKET"
	revertKeyWords[COMMA] = "COMMA"
	revertKeyWords[FALSE] = "FALSE"
	revertKeyWords[TRUE] = "TRUE"
	revertKeyWords[INTVALUE] = "INTVALUE"
	revertKeyWords[FLOATVALUE] = "FLOATVALUE"
	revertKeyWords[CHARVALUE] = "CHARVALUE"
	revertKeyWords[STRINGVALUE] = "STRINGVALUE"
	revertKeyWords[ASSIGNEQUAL] = "ASSIGNEQUAL"
	revertKeyWords[CHECKEQUAL] = "CHECKEQUAL"
	revertKeyWords[GREAT] = "GREAT"
	revertKeyWords[GREATEQUAL] = "GREATEQUAL"
	revertKeyWords[LESS] = "LESS"
	revertKeyWords[LESSEQUAL] = "LESSEQUAL"
	revertKeyWords[ADD] = "ADD"
	revertKeyWords[MINUS] = "MINUS"
	revertKeyWords[DIVIDE] = "DIVIDE"
	revertKeyWords[NOTEQUAL] = "NOTEQUAL"
	revertKeyWords[STAR] = "STAR"
	revertKeyWords[WORD] = "WORD"
}

func (l *Lexer) Reset() {
	l.Data = nil
	l.Tokens = l.Tokens[:0]
	l.pos = 0
}

func (l *Lexer) Lex(data []byte) error {
	data = bytes.TrimSpace(data)
	l.Data = data
	return l.read()
}

func (l *Lexer) read() (err error) {
	for l.pos < len(l.Data) {
		switch l.Data[l.pos] {
		case '=', '!', '>', '<', '+', '-', '*', '/', '(', ')', ',', ';':
			err = l.readChars()
		case '`':
			err = l.readIdent()
		case '\t', ' ', '\n':
			l.readContinuousSpace()
		case '"':
			err = l.readString()
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			err = l.readNumberValue()
		case '\'':
			err = l.readChar()
		default:
			err = l.readWord()
		}
		if err != nil {
			return
		}
	}
	return err
}

func (l *Lexer) readChars() error {
	b := l.Data[l.pos]
	switch b {
	case '!':
		// Next should be =
		l.pos++
		if l.pos >= len(l.Data) {
			return UnknownTokenErr
		}
		b = l.Data[l.pos]
		if b != '=' {
			return UnknownTokenErr
		}
		l.pos++
		l.Tokens = append(l.Tokens, Token{Tp: NOTEQUAL, StartPos: l.pos - 2})
	case '>':
		l.pos++
		if l.pos < len(l.Data) && l.Data[l.pos] == '=' {
			l.pos++
			l.Tokens = append(l.Tokens, Token{Tp: GREATEQUAL, StartPos: l.pos - 2})
		} else {
			l.Tokens = append(l.Tokens, Token{Tp: GREAT, StartPos: l.pos - 1})
		}
	case '<':
		l.pos++
		if l.pos < len(l.Data) && l.Data[l.pos] == '=' {
			l.pos++
			l.Tokens = append(l.Tokens, Token{Tp: LESSEQUAL, StartPos: l.pos - 2})
		} else {
			l.Tokens = append(l.Tokens, Token{Tp: LESS, StartPos: l.pos - 1})
		}
	case '=':
		l.pos++
		if l.pos < len(l.Data) && l.Data[l.pos] == '=' {
			l.pos++
			l.Tokens = append(l.Tokens, Token{Tp: CHECKEQUAL, StartPos: l.pos - 2})
		} else {
			l.Tokens = append(l.Tokens, Token{Tp: ASSIGNEQUAL, StartPos: l.pos - 1})
		}
	//case '(':
	//	ret, err := l.tryReadFloatOrVarchar()
	//	if err != nil {
	//		return err
	//	}
	//	if !ret {
	//		l.pos++
	//		l.Tokens = append(l.Tokens, Token{Tp: LEFTBRACKET, StartPos: l.pos - 2})
	//	}
	default:
		l.pos++
		tp, ok := singleCharKeyWordMap[b]
		if !ok {
			return UnknownTokenErr
		}
		l.Tokens = append(l.Tokens, Token{Tp: tp, StartPos: l.pos - 1})
	}
	return nil
}

func (l *Lexer) readContinuousSpace() {
	for ; l.pos < len(l.Data); l.pos++ {
		if l.Data[l.pos] == ' ' || l.Data[l.pos] == '\t' ||
			l.Data[l.pos] == '\n' {
			continue
		} else {
			break
		}
	}
}

func (l *Lexer) readWord() error {
	// word should start by [0-9] | ['a'-'z'] | ['A' - 'Z'] '_'
	startPos := l.pos
	for ; l.pos < len(l.Data); l.pos++ {
		c := l.Data[l.pos]
		if l.pos == startPos {
			// The first character should be ['a'-'z'], ['A'-'Z']
			if c < 'A' || (c > 'Z' && c < 'a') || c > 'z' {
				return WordFormatErr
			}
			continue
		}
		if c == '_' || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') {
			continue
		} else {
			break
		}
	}
	//	word := string(bytes.ToUpper(l.Data[startPos:l.pos]))
	// 	println("word: ", word)
	keyWord, ok := keyWords[string(bytes.ToUpper(l.Data[startPos:l.pos]))]
	if !ok {
		// Should be a word like TableName, ColumnName etc.
		l.Tokens = append(l.Tokens, Token{Tp: WORD, StartPos: startPos, EndPos: l.pos})
	} else {
		l.Tokens = append(l.Tokens, Token{Tp: keyWord, StartPos: startPos, EndPos: l.pos})
	}
	return nil
}

//func (l *Lexer) tryReadFloatOrVarchar() (bool, error) {
//	if len(l.Tokens) == 0 {
//		return false, nil
//	}
//	tp := l.Tokens[len(l.Tokens)-1]
//	if tp.Tp == FLOAT {
//		l.Tokens = l.Tokens[:len(l.Tokens)-1]
//		if l.readFloat() {
//			return true, nil
//		}
//		return false, FloatFormatErr
//	}
//	if tp.Tp == VARCHAR {
//		l.Tokens = l.Tokens[:len(l.Tokens)-1]
//		if l.readVarchar() {
//			return true, nil
//		}
//		return false, VarcharFormatErr
//	}
//	return false, nil
//}

// var varCharRegex, _ = regexp.Compile(`\(\s*[0-9]+\s*`)

//func (l *Lexer) readVarchar() bool {
//	// normally, varchar should be varchar(100)
//	loc := bytes.IndexByte(l.Data[l.pos:], ')')
//	if loc == -1 || !varCharRegex.Match(l.Data[l.pos:l.pos+loc]) {
//		return false
//	}
//	startPos, err := strconv.Atoi(string(bytes.TrimSpace(l.Data[l.pos+1 : l.pos+loc])))
//	if err != nil {
//		return false
//	}
//	l.pos += loc
//	l.Tokens = append(l.Tokens, Token{Tp: VARCHAR, StartPos: l.pos, EndPos: l.pos + loc})
//	l.pos++
//	return true
//}

func (l *Lexer) matchChar(b byte) bool {
	if l.pos >= len(l.Data) {
		return false
	}
	return l.Data[l.pos] == b
}

//func (l *Lexer) readFloat() bool {
//	// normally, float should be FLOAT(10, 2) | FLOAT
//	loc := bytes.IndexByte(l.Data[l.pos:], ')')
//	if loc == -1 || !floatRegex.Match(l.Data[l.pos:l.pos+loc]) {
//		return false
//	}
//	dotLoc := bytes.IndexByte(l.Data[l.pos:], ',')
//	startPos, err := strconv.Atoi(string(bytes.TrimSpace(l.Data[l.pos+1 : l.pos+dotLoc])))
//	if err != nil || startPos <= 0 {
//		return false
//	}
//	endPos, err := strconv.Atoi(string(bytes.TrimSpace(l.Data[l.pos+dotLoc+1 : l.pos+loc])))
//	if err != nil || endPos <= 0 {
//		return false
//	}
//	l.pos += loc
//	l.Tokens = append(l.Tokens, Token{Tp: FLOAT, StartPos: startPos, EndPos: endPos})
//	l.pos++
//	return true
//}

func (l *Lexer) readString() error {
	// read string until encounter a ending "
	l.pos++
	startPos := l.pos
	for ; l.pos < len(l.Data); l.pos++ {
		if l.Data[l.pos] == '"' {
			l.Tokens = append(l.Tokens, Token{Tp: STRINGVALUE, StartPos: startPos, EndPos: l.pos})
			l.pos++
			return nil
		}
	}
	return StringUnExpecedEndErr
}

func (l *Lexer) readNumberValue() error {
	isFloat := false
	startPos := l.pos
	for ; l.pos < len(l.Data); l.pos++ {
		c := l.Data[l.pos]
		if c == '.' {
			// Float value
			isFloat = true
		} else if c >= '0' && c <= '9' {
			continue
		} else {
			break
		}
	}
	if isFloat {
		l.Tokens = append(l.Tokens, Token{Tp: FLOATVALUE, StartPos: startPos, EndPos: l.pos})
	} else {
		l.Tokens = append(l.Tokens, Token{Tp: INTVALUE, StartPos: startPos, EndPos: l.pos})
	}
	return nil
}

func (l *Lexer) readChar() error {
	if l.pos+2 >= len(l.Data) {
		return CharUnExpectedEndErr
	}
	l.pos += 2
	if l.Data[l.pos] != '\'' {
		return CharFormatErr
	}
	l.Tokens = append(l.Tokens, Token{Tp: CHARVALUE, StartPos: l.pos - 1})
	l.pos++
	return nil
}

// Read until we find a ident
func (l *Lexer) readIdent() error {
	l.pos++
	startPos := l.pos
	// Ident must start with a letter and then can contains 0-9, a-c, A-Z, _
	for ; l.pos < len(l.Data); l.pos++ {
		c := l.Data[l.pos]
		if startPos == l.pos {
			// Must start with letter
			if c < 'A' || (c > 'Z' && c < 'a') || (c > 'z') {
				return IdentFormatErr
			}
			continue
		}
		if c == '`' {
			break
		}
		if c < '0' || (c > '9' && c < 'A') || (c > 'Z' && c != '_' && c < 'a') || (c > 'z') {
			return IdentFormatErr
		}
	}
	l.Tokens = append(l.Tokens, Token{Tp: IDENT, StartPos: startPos, EndPos: l.pos})
	l.pos++
	return nil
}

func (l *Lexer) String() string {
	var buf bytes.Buffer
	i := 0
	for ; i < len(l.Tokens)-1; i++ {
		buf.WriteString(fmt.Sprintf("{%s, StartPos: %d, EndPos: %d},", revertKeyWords[l.Tokens[i].Tp], l.Tokens[i].StartPos, l.Tokens[i].EndPos))
	}
	if i < len(l.Tokens) {
		buf.WriteString(fmt.Sprintf("{%s, StartPos: %d, EndPos: %d}", revertKeyWords[l.Tokens[i].Tp], l.Tokens[i].StartPos, l.Tokens[i].EndPos))
	}
	return buf.String()
}
