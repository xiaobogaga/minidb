package lexer

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
)

type TokenType int

const (
	// First DDL.

	// create database statement:
	// * create {database|schema} [if not exist] database_name [[Default | character set = value] | [Default | collate = value]];
	CREATE TokenType = iota
	DATABASE
	SCHEMA
	IF
	NOT
	EXIST
	IDENT // an ident is a identifier which quoted by ``, it could be a table name, database name, etc.
	WORD  // WORD is like identifier but doesn't has quota, could represent table name, column name, etc.
	DEFAULT
	CHARACTER
	SET
	EQUAL
	COLLATE

	// create table statements:
	// * create table [if not exist] tb_name like orig_tab_name;
	// * create table [if not exist] tb_name2 (
	//    Column_Def..., Index_Def..., Constraint_Def...
	//    ) [engine=value] [[Default | character set = value] | [Default | collate = value]];
	// create table [if not exist] as selectStatement;

	// columnDef:
	// * col_name col_type [not null|null] [default default_value] [AUTO_INCREMENT] [[primary] key] [[unique] key]
	// Index_Def:
	// * {index|key} index_name (col_name, ...)
	// Constraint_Def:
	// * [Constraint] primary key (col_name [,col_name...)
	// * [Constraint] unique {index|key} index_name (col_name [,col_name...)
	// * [Constraint] foreign key index_name (col_name [,col_name...) references tb_name (key...)
	// [on {delete|update}] reference_option]
	// reference_option is like: {restrict | cascade | set null | no action | set default}
	// Restrict is default.
	TABLE
	LIKE
	LEFTBRACKET // not keyword
	// column def keywords
	// Data type
	// It turns out mysql has many different types. For now, only a subset are supported
	BOOL
	INT     // support int()
	BIGINT  // support bigint()
	FLOAT   // support float(m, d)
	CHAR    // char([0-255])
	VARCHAR // varchar([0 - 65535])
	DATETIME
	BLOB
	MEDIUMBLOB
	TEXT
	MEDIUMTEXT

	NULL
	AUTO_INCREMENT
	PRIMARY
	KEY
	UNIQUE
	INDEX
	RIGHTBRACKET
	CONSTRAINT
	FOREIGN
	REFERENCES
	ON
	DELETE
	UPDATE
	RESTRICT
	CASCADE
	NO
	ACTION
	ENGINE

	// Drop database statement is like:
	// * drop {database | schema} [if exists] db_name;
	// Drop table statement is like:
	// * drop table [if exists] tb_name[,tb_name...] [RESTRICT|CASCADE];

	DROP
	EXISTS

	// Rename statement can be rename table statements.
	// It's like:
	// * rename table {tb1 To tb2...}
	RENAME
	TO

	// Truncate table statement is like:
	// * truncate [table] tb_name
	TRUNCATE

	// Alter statement can be alter table statement or alter database statement.
	// Alter table statement is like:
	// * alter [table] tb_name [
	// add 	  [column] col_name col_def |
	// drop   [column] col_name |
	// modify [column] col_def |
	// change [column] old_col_name col_def |
	// add {index|key} indexDef |
	// add [constraint] primaryKeyDef |
	// add [constraint] uniqueKeyDef |
	// add [constraint] foreignKeyDef |
	// drop {index|key} index_name |
	// drop primary key |
	// drop foreign key key_name |
	// engine=value |
	// [[default] | character set = value] |
	// [[default] | collate = value]
	// ]
	// Alter database statement can be:
	// * alter {database | schema} db_name [[Default | character set = value] | [Default | collate = value]]

	ALTER
	ADD
	COLUMN
	MODIFY
	CHANGE

	// Second DML
	// Insert statement is like:
	// * insert into tb_name [( col_name... )] values (expression...)
	INSERT
	INTO
	VALUES

	// Update statement is like:
	// * update table_reference set assignments... WhereStm OrderByStm LimitStm
	// * update table_reference... set assignments... WhereStm
	// A table reference statement is like:
	// * tb_name [as alias] | joined_table
	// where joined_table is like:
	// * table_reference { {left|right} [outer] join table_reference join_specification | inner join table_factor [join_specification]}
	// join_specification is like:
	// on where_condition | using (col...)
	IS
	WHERE
	AS
	LEFT
	RIGHT
	OUTER
	JOIN
	INNER
	USING
	ORDER
	BY
	ASC
	DESC
	LIMIT
	OFFSET

	// Delete statement is like:
	// * delete from tb_name whereStm OrderByStm LimitStm
	// * delete tb1 [as tb1_1]... from table_references WhereStm
	FROM

	// Select statement is like:
	// * select [all | distinct | distinctrow] select_expression... from table_reference... [WhereStm] [GroupByStm] [HavingStm]
	// [OrderByStm] [LimitStm] [for update | lock in share mode]
	SELECT
	STAR
	ALL
	DISTINCT
	DISTINCTROW
	GROUP
	HAVING
	FOR
	LOCK
	IN
	SHARE
	MODE

	// a specific value
	// It can be either a numerical value such as an integer or a float (start with a number or a .), true or false.
	// Or a value with 'xxx', or "xxx", which can interpreted to other type according to the format of the column.
	VALUE

	// There are some special tokens like operations used in expressions
	// condition expression.
	NOTEQUAL   // !=
	GREAT      // >
	GREATEQUAL // >=
	LESS       // <
	LESSEQUAL  // <=
	AND
	OR

	// math expression
	PLUS   // +
	MINUS  // -
	DIVIDE // /
	MUL    // *
	MOD    // %

	// Special characters
	SEMICOLON
	COMMA
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

type LexerError struct {
	LineNumber int
	ErrData    string
}

func (s LexerError) Error() string {
	return fmt.Sprintf("syntax err near %s at %d line", s.ErrData, s.LineNumber)
}

func (l *Lexer) MakeLexerError(lineNumber, startPos int) error {
	// Todo: check set lineNumber = 1 is reasonable.
	return LexerError{
		LineNumber: lineNumber,
		ErrData:    string(l.Data[startPos:]),
	}
}

var (
	keyWords = map[string]TokenType{
		"CREATE":         CREATE,
		"DATABASE":       DATABASE,
		"SCHEMA":         SCHEMA,
		"IF":             IF,
		"NOT":            NOT,
		"EXIST":          EXIST,
		"DEFAULT":        DEFAULT,
		"CHARACTER":      CHARACTER,
		"SET":            SET,
		"EQUAL":          EQUAL,
		"COLLATE":        COLLATE,
		"TABLE":          TABLE,
		"LIKE":           LIKE,
		"BOOL":           BOOL,
		"INT":            INT,
		"BIGINT":         BIGINT,
		"FLOAT":          FLOAT,
		"CHAR":           CHAR,
		"VARCHAR":        VARCHAR,
		"DATETIME":       DATETIME,
		"BLOB":           BLOB,
		"MEDIUMBLOB":     MEDIUMBLOB,
		"TEXT":           TEXT,
		"MEDIUMTEXT":     MEDIUMTEXT,
		"NULL":           NULL,
		"AUTO_INCREMENT": AUTO_INCREMENT,
		"PRIMARY":        PRIMARY,
		"KEY":            KEY,
		"UNIQUE":         UNIQUE,
		"INDEX":          INDEX,
		"CONSTRAINT":     CONSTRAINT,
		"FOREIGN":        FOREIGN,
		"REFERENCES":     REFERENCES,
		"ON":             ON,
		"DELETE":         DELETE,
		"UPDATE":         UPDATE,
		"RESTRICT":       RESTRICT,
		"CASCADE":        CASCADE,
		"NO":             NO,
		"ACTION":         ACTION,
		"ENGINE":         ENGINE,
		"DROP":           DROP,
		"EXISTS":         EXISTS,
		"RENAME":         RENAME,
		"TO":             TO,
		"TRUNCATE":       TRUNCATE,
		"ALTER":          ALTER,
		"ADD":            ADD,
		"COLUMN":         COLUMN,
		"MODIFY":         MODIFY,
		"INSERT":         INSERT,
		"INTO":           INTO,
		"VALUES":         VALUES,
		"IS":             IS,
		"WHERE":          WHERE,
		"AS":             AS,
		"LEFT":           LEFT,
		"RIGHT":          RIGHT,
		"OUTER":          OUTER,
		"JOIN":           JOIN,
		"INNER":          INNER,
		"USING":          USING,
		"ORDER":          ORDER,
		"BY":             BY,
		"ASC":            ASC,
		"DESC":           DESC,
		"LIMIT":          LIMIT,
		"OFFSET":         OFFSET,
		"FROM":           FROM,
		"SELECT":         SELECT,
		"STAR":           STAR,
		"ALL":            ALL,
		"DISTINCT":       DISTINCT,
		"DISTINCTROW":    DISTINCTROW,
		"GROUP":          GROUP,
		"HAVING":         HAVING,
		"FOR":            FOR,
		"LOCK":           LOCK,
		"IN":             IN,
		"SHARE":          SHARE,
		"MODE":           MODE,
		"VALUE":          VALUE,
		"AND":            AND,
		"OR":             OR,
	}
)

func (l *Lexer) Lex(data []byte) ([]Token, error) {
	data = bytes.TrimSpace(data)
	l.Data = data
	return l.read()
}

func (l *Lexer) read() (t []Token, err error) {
	for l.pos < len(l.Data) {
		switch l.Data[l.pos] {
		case '!', '=', '>', '<', '+', '-', '*', '/', '%', '(', ')', ',', ';':
			err = l.readSpecialCharacters()
		case '`':
			err = l.readIdent()
		case '\t', ' ', '\n':
			l.readContinuousSpace()
		case '"', '\'':
			err = l.readValue()
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			err = l.readNumeric()
		default:
			err = l.readWord()
		}
		if err != nil {
			return nil, err
		}
	}
	return l.Tokens, nil
}

// There are some characters which are special but not keywords, like numeric calculation characters: +, -, *, /, %
// And other characters like !=, =, >=, <, <=, readSpecialCharacters will parse them to token.
func (l *Lexer) readSpecialCharacters() error {
	b := l.Data[l.pos]
	switch b {
	case '!':
		// !=
		// Next should be =
		if !l.matchToken('=') {
			return l.MakeLexerError(1, l.pos)
		}
		l.pos += 2
		l.Tokens = append(l.Tokens, Token{Tp: NOTEQUAL, StartPos: l.pos - 2, EndPos: l.pos})
	case '=':
		l.pos++
		l.Tokens = append(l.Tokens, Token{Tp: EQUAL, StartPos: l.pos - 1, EndPos: l.pos})
	case '>':
		// > or >=
		if l.matchToken('=') {
			l.pos += 2
			l.Tokens = append(l.Tokens, Token{Tp: GREATEQUAL, StartPos: l.pos - 2, EndPos: l.pos})
		} else {
			l.pos++
			l.Tokens = append(l.Tokens, Token{Tp: GREAT, StartPos: l.pos - 1, EndPos: l.pos})
		}
	case '<':
		// < or <=
		if l.matchToken('=') {
			l.pos += 2
			l.Tokens = append(l.Tokens, Token{Tp: LESSEQUAL, StartPos: l.pos - 2, EndPos: l.pos})
		} else {
			l.pos++
			l.Tokens = append(l.Tokens, Token{Tp: LESS, StartPos: l.pos - 1, EndPos: l.pos})
		}
	case '+':
		l.pos++
		l.Tokens = append(l.Tokens, Token{Tp: PLUS, StartPos: l.pos - 1, EndPos: l.pos})
	case '-':
		l.pos++
		l.Tokens = append(l.Tokens, Token{Tp: MINUS, StartPos: l.pos - 1, EndPos: l.pos})
	case '*':
		l.pos++
		l.Tokens = append(l.Tokens, Token{Tp: MUL, StartPos: l.pos - 1, EndPos: l.pos})
	case '/':
		l.pos++
		l.Tokens = append(l.Tokens, Token{Tp: DIVIDE, StartPos: l.pos - 1, EndPos: l.pos})
	case '%':
		l.pos++
		l.Tokens = append(l.Tokens, Token{Tp: MOD, StartPos: l.pos - 1, EndPos: l.pos})
	case '(':
		l.pos++
		l.Tokens = append(l.Tokens, Token{Tp: LEFTBRACKET, StartPos: l.pos - 1, EndPos: l.pos})
	case ')':
		l.pos++
		l.Tokens = append(l.Tokens, Token{Tp: RIGHTBRACKET, StartPos: l.pos - 1, EndPos: l.pos})
	case ';':
		l.pos++
		l.Tokens = append(l.Tokens, Token{Tp: SEMICOLON, StartPos: l.pos - 1, EndPos: l.pos})
	case ',':
		l.pos++
		l.Tokens = append(l.Tokens, Token{Tp: COMMA, StartPos: l.pos - 1, EndPos: l.pos})
	default:
		return l.MakeLexerError(1, l.pos)
	}
	return nil
}

// matchToken check whether current byte is a specific byte which will represent a token combined
// with the previous token, and if, it return true and false otherwise.
func (l *Lexer) matchToken(b byte) bool {
	pos := l.pos
	if l.hasRemain() && l.Data[pos+1] == b {
		return true
	}
	return false
}

func (l *Lexer) hasRemain() bool {
	return l.pos+1 < len(l.Data)
}

// readContinuousSpace
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

var wordPattern = regexp.MustCompile("[a-zA-Z]+\\.?[a-zA-Z]*\\.?[a-zA-Z]*")

func (l *Lexer) readWord() error {
	startPos := l.pos
	word := wordPattern.FindString(string(l.Data[startPos:]))
	l.pos += len(word)
	keyWord, ok := keyWords[strings.ToUpper(word)]
	if !ok {
		// Should be a word like TableName, ColumnName etc.
		err := l.matchDotPattern(word)
		if err != nil {
			return err
		}
		l.Tokens = append(l.Tokens, Token{Tp: WORD, StartPos: startPos, EndPos: l.pos})
	} else {
		l.Tokens = append(l.Tokens, Token{Tp: keyWord, StartPos: startPos, EndPos: l.pos})
	}
	return nil
}

func (l *Lexer) matchDotPattern(word string) error {
	// we at most have two .. such as schema.table.column
	splits := strings.Split(word, ".")
	if len(splits) >= 4 {
		return l.MakeLexerError(1, l.pos-len(word))
	}
	for _, p := range splits {
		if len(p) <= 0 {
			return l.MakeLexerError(1, l.pos-len(word))
		}
	}
	return nil
}

var identPattern = regexp.MustCompile("[a-zA-Z]\\w*`")

// Read until we find an ident, ident must match regex: [a-z]|[A-Z][_0-9]*
func (l *Lexer) readIdent() error {
	startPos := l.pos
	if !l.hasRemain() {
		return l.MakeLexerError(1, startPos)
	}
	ident := identPattern.FindString(string(l.Data[startPos+1:]))
	l.pos += len(ident) + 1
	l.Tokens = append(l.Tokens, Token{Tp: IDENT, StartPos: startPos, EndPos: l.pos})
	return nil
}

func (l *Lexer) readValue() error {
	// read string until encounter a ending " or '
	quoteType := l.Data[l.pos]
	startPos := l.pos
	for l.pos++; l.pos < len(l.Data); l.pos++ {
		if l.Data[l.pos] == quoteType {
			l.pos++
			l.Tokens = append(l.Tokens, Token{Tp: VALUE, StartPos: startPos, EndPos: l.pos})
			return nil
		}
	}
	return l.MakeLexerError(1, startPos)
}

var numericValuePattern = regexp.MustCompile("[0-9]+.?[0-9]*")

// Read an numeric value. either an integer or a float value.
func (l *Lexer) readNumeric() error {
	startPos := l.pos
	value := numericValuePattern.FindString(string(l.Data[startPos:]))
	l.pos += len(value)
	l.Tokens = append(l.Tokens, Token{Tp: VALUE, StartPos: startPos, EndPos: l.pos})
	return nil
}

func (l *Lexer) String() string {
	var buf bytes.Buffer
	for _, token := range l.Tokens {
		buf.WriteString(string(l.Data[token.StartPos:token.EndPos]))
	}
	return buf.String()
}
