package parser

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

func (parser *Parser) resolveAlterStm() (stm Stm, err error) {
	if !parser.matchTokenTypes(false, ALTER) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	if parser.matchTokenTypes(true, DATABASE) || parser.matchTokenTypes(true, SCHEMA) {
		return parser.parseAlterDatabaseStm()
	}
	parser.matchTokenTypes(true, TABLE)
	tableName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	t, ok := parser.NextToken()
	if !ok {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	switch t.Tp {
	case ENGINE:
		stm, err = parser.parseAlterEngineStm(string(tableName))
	case DEFAULT, CHARACTER, COLLATE:
		stm, err = parser.parseAlterCharsetCollateStm(string(tableName))
	default:
		stm, err = parser.parseAlterColumnOrIndexConstraintStm(t.Tp, string(tableName))
	}
	if !parser.matchTokenTypes(false, SEMICOLON) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return
}

// Alter database statement can be:
// * alter {database | schema} db_name [[Default | character set = value] | [Default | collate = value]]
func (parser *Parser) parseAlterDatabaseStm() (Stm, error) {
	dbName, ok := parser.parseIdentOrWord(false)
	if !ok {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	charset, collate, err := parser.parseCharsetAndCollate()
	if !ok {
		return nil, err
	}
	return &AlterDatabaseStm{DatabaseName: string(dbName), Charset: charset, Collate: collate}, nil
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
func (parser *Parser) parseAlterColumnOrIndexConstraintStm(alterTp TokenType, tableName string) (Stm, error) {
	token, ok := parser.NextToken()
	if !ok {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	switch token.Tp {
	case INDEX, KEY, PRIMARY, UNIQUE, FOREIGN, CONSTRAINT:
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
func (parser *Parser) parseAlterIndexOrConstraintStm(alterTp, indexOrConstraintTp TokenType,
	tableName string) (Stm, error) {
	switch alterTp {
	case ADD:
		switch indexOrConstraintTp {
		case INDEX, KEY:
			return parser.parseAlterTableAddIndexDefStm(tableName)
		default:
			parser.UnReadToken()
			return parser.parseAlterTableAddConstraintStm(tableName)
		}
	case DROP:
		switch indexOrConstraintTp {
		case INDEX, KEY:
			return parser.parseAlterTableDropIndexStm(tableName)
		case PRIMARY:
			return parser.parseAlterTableDropPrimaryKeyStm(tableName)
		case FOREIGN:
			return parser.parseAlterTableDropForeignKeyStm(tableName)
		}
	}
	return nil, parser.MakeSyntaxError(parser.pos - 1)
}

func (parser *Parser) parseAlterTableAddIndexDefStm(tableName string) (*AlterTableAddIndexOrConstraintStm, error) {
	parser.UnReadToken()
	indexDef, err := parser.parseIndexDef()
	if err != nil {
		return nil, err
	}
	return &AlterTableAddIndexOrConstraintStm{Tp: IsIndexTp, TableName: tableName, IndexOrConstraint: indexDef}, nil
}

func (parser *Parser) parseAlterTableAddConstraintStm(tableName string) (*AlterTableAddIndexOrConstraintStm, error) {
	constraintDef, err := parser.parseConstraintDef()
	if err != nil {
		return nil, err
	}
	return &AlterTableAddIndexOrConstraintStm{Tp: IsConstraintTp, TableName: tableName, IndexOrConstraint: constraintDef}, nil
}

// drop {index|key} index_name |
func (parser *Parser) parseAlterTableDropForeignKeyStm(tableName string) (*AlterTableDropIndexOrConstraintStm, error) {
	indexName, ok := parser.parseIdentOrWord(false)
	if !ok {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return &AlterTableDropIndexOrConstraintStm{Tp: IndexTp, TableName: tableName, IndexOrKeyName: string(indexName)}, nil
}

// drop primary key
func (parser *Parser) parseAlterTableDropPrimaryKeyStm(tableName string) (*AlterTableDropIndexOrConstraintStm, error) {
	if !parser.matchTokenTypes(false, KEY) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return &AlterTableDropIndexOrConstraintStm{Tp: PrimaryKeyTp, TableName: tableName}, nil
}

// drop foreign key key_name
func (parser *Parser) parseAlterTableDropIndexStm(tableName string) (*AlterTableDropIndexOrConstraintStm, error) {
	if !parser.matchTokenTypes(false, KEY) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	keyName, ok := parser.parseIdentOrWord(false)
	if !ok {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return &AlterTableDropIndexOrConstraintStm{Tp: ForeignKeyTp, TableName: tableName, IndexOrKeyName: string(keyName)}, nil
}

// * alter [table] tb_name [
// add 	  [column] col_def |
// drop   [column] col_name |
// modify [column] col_def |
// change [column] old_col_name col_def
func (parser *Parser) parseAlterColumnStm(alterTp TokenType, tableName string) (*AlterTableAlterColumnStm, error) {
	parser.matchTokenTypes(true, COLUMN)
	alterColumnTp := AddColumnTp
	colDef := &ColumnDefStm{}
	var err error
	var colName []byte
	ok := false
	switch alterTp {
	case ADD:
		colDef, err = parser.parseColumnDef()
	case DROP:
		alterColumnTp = DropColumnTp
		colName, ok = parser.parseIdentOrWord(false)
		if !ok {
			err = parser.MakeSyntaxError(parser.pos - 1)
		}
	case MODIFY:
		alterColumnTp = ModifyColumnTp
		colDef, err = parser.parseColumnDef()
	case CHANGE:
		alterColumnTp = ChangeColumnTp
		colName, ok = parser.parseIdentOrWord(false)
		if !ok {
			err = parser.MakeSyntaxError(parser.pos - 1)
		}
		if err == nil {
			colDef, err = parser.parseColumnDef()
		}
	default:
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	if err != nil {
		return nil, err
	}
	return &AlterTableAlterColumnStm{
		TableName: tableName,
		Tp:        alterColumnTp,
		ColName:   string(colName),
		ColDef:    colDef,
	}, nil
}

// engine=value
func (parser *Parser) parseAlterEngineStm(tableName string) (*AlterTableAlterEngineStm, error) {
	engine, ok := parser.parseIdentOrWord(false)
	if !ok {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return &AlterTableAlterEngineStm{TableName: tableName, Engine: string(engine)}, nil
}

// [[default] | character set = value] |
// [[default] | collate = value]
func (parser *Parser) parseAlterCharsetCollateStm(tableName string) (*AlterTableCharsetCollateStm, error) {
	charset, collate, err := parser.parseCharsetAndCollate()
	if err != nil {
		return nil, err
	}
	return &AlterTableCharsetCollateStm{TableName: tableName, Charset: charset, Collate: collate}, nil
}
