package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

// Alter statement can be alter table statement or alter database statement.
// Alter table statement is like:
// * alter [table] tb_name [
// add 	  [column] col_def |
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
// [[default] | character set = value] |
// [[default] | collate = value]
// ]
// Alter database statement can be:
// * alter {database | schema} db_name [[Default | character set = value] | [Default | collate = value]]

// Diff with mysql:
// Too many, doesn't show here.

func (parser *Parser) resolveAlterStm() (stm ast.Stm, err error) {
	if !parser.matchTokenTypes(false, lexer.ALTER) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if parser.matchTokenTypes(true, lexer.DATABASE) || parser.matchTokenTypes(true, lexer.SCHEMA) {
		return parser.parseAlterDatabaseStm()
	}
	parser.matchTokenTypes(true, lexer.TABLE)
	tableName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	t, ok := parser.NextToken()
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	switch t.Tp {
	case lexer.ENGINE:
		stm, err = parser.parseAlterEngineStm(string(tableName))
	case lexer.DEFAULT, lexer.CHARACTER, lexer.COLLATE:
		stm, err = parser.parseAlterCharsetCollateStm(string(tableName))
	default:
		stm, err = parser.parseAlterColumnOrIndexConstraintStm(t.Tp, string(tableName))
	}
	if !parser.matchTokenTypes(false, lexer.SEMICOLON) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return
}

// Alter database statement can be:
// * alter {database | schema} db_name [[Default | character set = value] | [Default | collate = value]]
func (parser *Parser) parseAlterDatabaseStm() (ast.Stm, error) {
	dbName, ok := parser.parseIdentOrWord(false)
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	charset, collate, ok := parser.parseCharsetAndCollate()
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &ast.AlterDatabaseStm{DatabaseName: string(dbName), Charset: charset, Collate: collate}, nil
}

// * alter [table] tb_name [
// add 	  [column] col_def |
// drop   [column] col_name |
// modify [column] col_def |
// change [column] old_col_name col_def |
// add {index|key} indexDef |
// add [constraint] primaryKeyDef |
// add [constraint] uniqueKeyDef |
// add [constraint] foreignKeyDef |
// drop {index|key} index_name |
// drop primary key |
// drop foreign key key_name
func (parser *Parser) parseAlterColumnOrIndexConstraintStm(alterTp lexer.TokenType, tableName string) (ast.Stm, error) {
	token, ok := parser.NextToken()
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	switch token.Tp {
	case lexer.INDEX, lexer.KEY, lexer.PRIMARY, lexer.UNIQUE, lexer.FOREIGN, lexer.CONSTRAINT:
		return parser.parseAlterIndexOrConstraintStm(alterTp, token.Tp, tableName)
	default:
		parser.UnReadToken()
		return parser.parseAlterColumnStm(alterTp, tableName)
	}
}

// add {index|key} indexDef |
// add [constraint] primaryKeyDef |
// add [constraint] uniqueKeyDef |
// add [constraint] foreignKeyDef |
// drop {index|key} index_name |
// drop primary key |
// drop foreign key key_name
func (parser *Parser) parseAlterIndexOrConstraintStm(alterTp, indexOrConstraintTp lexer.TokenType,
	tableName string) (ast.Stm, error) {
	switch alterTp {
	case lexer.ADD:
		switch indexOrConstraintTp {
		case lexer.INDEX, lexer.KEY:
			return parser.parseAlterTableAddIndexDefStm(tableName)
		default:
			parser.UnReadToken()
			return parser.parseAlterTableAddConstraintStm(tableName)
		}
	case lexer.DROP:
		switch indexOrConstraintTp {
		case lexer.INDEX, lexer.KEY:
			return parser.parseAlterTableDropIndexStm(tableName)
		case lexer.PRIMARY:
			return parser.parseAlterTableDropPrimaryKeyStm(tableName)
		case lexer.FOREIGN:
			return parser.parseAlterTableDropForeignKeyStm(tableName)
		}
	}
	return nil, parser.MakeSyntaxError(1, parser.pos)
}

func (parser *Parser) parseAlterTableAddIndexDefStm(tableName string) (*ast.AlterTableAddIndexOrConstraintStm, error) {
	parser.UnReadToken()
	indexDef, err := parser.parseIndexDef()
	if err != nil {
		return nil, err
	}
	return &ast.AlterTableAddIndexOrConstraintStm{Tp: ast.IsIndexTp, TableName: tableName, IndexOrConstraint: indexDef}, nil
}

func (parser *Parser) parseAlterTableAddConstraintStm(tableName string) (*ast.AlterTableAddIndexOrConstraintStm, error) {
	constraintDef, err := parser.parseConstraintDef()
	if err != nil {
		return nil, err
	}
	return &ast.AlterTableAddIndexOrConstraintStm{Tp: ast.IsConstraintTp, TableName: tableName, IndexOrConstraint: constraintDef}, nil
}

// drop {index|key} index_name |
func (parser *Parser) parseAlterTableDropForeignKeyStm(tableName string) (*ast.AlterTableDropIndexOrConstraintStm, error) {
	indexName, ok := parser.parseIdentOrWord(false)
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &ast.AlterTableDropIndexOrConstraintStm{Tp: ast.IndexTp, TableName: tableName, IndexOrKeyName: string(indexName)}, nil
}

// drop primary key
func (parser *Parser) parseAlterTableDropPrimaryKeyStm(tableName string) (*ast.AlterTableDropIndexOrConstraintStm, error) {
	if !parser.matchTokenTypes(false, lexer.KEY) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &ast.AlterTableDropIndexOrConstraintStm{Tp: ast.PrimaryKeyTp, TableName: tableName}, nil
}

// drop foreign key key_name
func (parser *Parser) parseAlterTableDropIndexStm(tableName string) (*ast.AlterTableDropIndexOrConstraintStm, error) {
	if !parser.matchTokenTypes(false, lexer.KEY) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	keyName, ok := parser.parseIdentOrWord(false)
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &ast.AlterTableDropIndexOrConstraintStm{Tp: ast.ForeignKeyTp, TableName: tableName, IndexOrKeyName: string(keyName)}, nil
}

// * alter [table] tb_name [
// add 	  [column] col_def |
// drop   [column] col_name |
// modify [column] col_def |
// change [column] old_col_name col_def
func (parser *Parser) parseAlterColumnStm(alterTp lexer.TokenType, tableName string) (*ast.AlterTableAlterColumnStm, error) {
	parser.matchTokenTypes(true, lexer.COLUMN)
	alterColumnTp := ast.AddColumnTp
	colDef := &ast.ColumnDefStm{}
	var err error
	var colName []byte
	ok := false
	switch alterTp {
	case lexer.ADD:
		colDef, err = parser.parseColumnDef()
	case lexer.DROP:
		alterColumnTp = ast.DropColumnTp
		colName, ok = parser.parseIdentOrWord(false)
		if !ok {
			err = parser.MakeSyntaxError(1, parser.pos-1)
		}
	case lexer.MODIFY:
		alterColumnTp = ast.ModifyColumnTp
		colDef, err = parser.parseColumnDef()
	case lexer.CHANGE:
		alterColumnTp = ast.ChangeColumnTp
		colName, ok = parser.parseIdentOrWord(false)
		if !ok {
			err = parser.MakeSyntaxError(1, parser.pos-1)
		}
		if err == nil {
			colDef, err = parser.parseColumnDef()
		}
	default:
		return nil, parser.MakeSyntaxError(1, parser.pos)
	}
	if err != nil {
		return nil, err
	}
	return &ast.AlterTableAlterColumnStm{
		TableName: tableName,
		Tp:        alterColumnTp,
		ColName:   string(colName),
		ColDef:    colDef,
	}, nil
}

// engine=value
func (parser *Parser) parseAlterEngineStm(tableName string) (*ast.AlterTableAlterEngineStm, error) {
	engine, ok := parser.parseIdentOrWord(false)
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &ast.AlterTableAlterEngineStm{TableName: tableName, Engine: string(engine)}, nil
}

// [[default] | character set = value] |
// [[default] | collate = value]
func (parser *Parser) parseAlterCharsetCollateStm(tableName string) (*ast.AlterTableCharsetCollateStm, error) {
	charset, collate, ok := parser.parseCharsetAndCollate()
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &ast.AlterTableCharsetCollateStm{TableName: tableName, Charset: charset, Collate: collate}, nil
}
