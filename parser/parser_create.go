package parser

// Create Statement can be create table statement or create database statement.
// For create table statement, it supports:
// * create table [if not exist] tb_name like orig_tab_name;
// * create table [if not exist] tb_name2 (Column_Def..., Index_Def..., Constraint_Def...) [engine=value] [[Default | character set = value] | [Default | collate = value]];
// * create table [if not exist] as selectStatement;
// For create database statement, if supports:
// * create {database|schema} [if not exist] database_name [[Default | character set = value] | [Default | collate = value]];

// Diff with mysql:
// Create table statement:
// * Doesn't support temporary table.
// * Doesn't support ignore or replace.
// * Doesn't support spatial or fulltext index.
// * Doesn't support to check
// * Doesn't support column definition.
// * For column format:
//   * doesn't support comment.
//   * doesn't support column format, collate, storage.
//   * doesn't support reference.

func (parser *Parser) resolveCreateStm() (stm Stm, err error) {
	if !parser.matchTokenTypes(false, CREATE) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	token, ok := parser.NextToken()
	if !ok {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	switch token.Tp {
	case TABLE:
		stm, err = parser.parseCreateTableStm()
	case DATABASE, SCHEMA:
		stm, err = parser.parseCreateDatabaseStm()
	default:
		err = parser.MakeSyntaxError(parser.pos - 1)
	}
	if err != nil {
		return nil, err
	}
	if !parser.matchTokenTypes(true, SEMICOLON) {
		return nil, parser.MakeSyntaxError(parser.pos)
	}
	return stm, nil
}

// * create table [if not exist] tb_name like orig_tab_name;
// * create table [if not exist] tb_name2 (Column_Def..., Index_Def..., Constraint_Def...) [engine=value] [[Default | character set = value] | [Default | collate = value]];
// * create table [if not exist] as selectStatement;
func (parser *Parser) parseCreateTableStm() (stm Stm, err error) {
	ifNotExist := parser.matchTokenTypes(true, IF, NOT, EXIST)
	tableName, ret := parser.parseIdentOrWord(true)
	if !ret || isTableNameEmpty(tableName) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	token, ok := parser.NextToken()
	if !ok {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	switch token.Tp {
	case LIKE:
		stm, err = parser.parseCreateTableLikeStm(ifNotExist, tableName)
	case AS:
		stm, err = parser.parseCreateTableAsStm(ifNotExist, tableName)
	case LEFTBRACKET:
		stm, err = parser.parseClassicCreateTableStm(ifNotExist, tableName)
	default:
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return stm, err
}

func isTableNameEmpty(tableName []byte) bool {
	return len(tableName) == 0
}

// * create table [if not exist] tb_name like orig_tab_name;
func (parser *Parser) parseCreateTableLikeStm(ifNotExist bool, tableName []byte) (*CreateTableLikeStm, error) {
	origTableName, ret := parser.parseIdentOrWord(false)
	if !ret || isTableNameEmpty(origTableName) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return &CreateTableLikeStm{
		TableName:      string(tableName),
		IfNotExist:     ifNotExist,
		LikedTableName: string(origTableName),
	}, nil
}

// * create table [if not exist] tb_name2 (Column_Def..., Index_Def..., Constraint_Def...) [engine=value] [[Default | character set = value] | [Default | collate = value]];
func (parser *Parser) parseClassicCreateTableStm(ifNotExist bool, tableName []byte) (*CreateTableStm, error) {
	var constraints []*ConstraintDefStm
	var columns []*ColumnDefStm
	var indexes []*IndexDefStm
	var col *ColumnDefStm
	var index *IndexDefStm
	var constraint *ConstraintDefStm
	var err error
	for {
		token, ok := parser.NextToken()
		if !ok {
			return nil, parser.MakeSyntaxError(parser.pos - 1)
		}
		switch token.Tp {
		case WORD, IDENT:
			parser.UnReadToken()
			col, err = parser.parseColumnDef()
		case INDEX, KEY:
			parser.UnReadToken()
			index, err = parser.parseIndexDef()
		case CONSTRAINT, FOREIGN:
			parser.UnReadToken()
			constraint, err = parser.parseConstraintDef()
		default:
			return nil, parser.MakeSyntaxError(parser.pos - 1)
		}
		if err != nil {
			return nil, parser.MakeSyntaxError(parser.pos - 1)
		}
		if constraint != nil {
			constraints = append(constraints, constraint)
		}
		if col != nil {
			columns = append(columns, col)
		}
		if index != nil {
			indexes = append(indexes, index)
		}
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, RIGHTBRACKET) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	var engine []byte
	if parser.matchTokenTypes(true, ENGINE, EQUAL) {
		ret, ok := parser.parseValue(false)
		if !ok {
			return nil, parser.MakeSyntaxError(parser.pos - 1)
		}
		engine = ret
	}
	charset, collate, err := parser.parseCharsetAndCollate()
	if err != nil {
		return nil, err
	}
	return &CreateTableStm{
		TableName:   string(tableName),
		IfNotExist:  ifNotExist,
		Cols:        columns,
		Indexes:     indexes,
		Constraints: constraints,
		Engine:      string(engine),
		Charset:     charset,
		Collate:     collate,
	}, nil
}

// * create table [if not exist] as selectStatement;
func (parser *Parser) parseCreateTableAsStm(ifNotExist bool, tableName []byte) (*CreateTableAsSelectStm, error) {
	selectStm, err := parser.resolveSelectStm(false)
	if err != nil {
		return nil, err
	}
	return &CreateTableAsSelectStm{
		TableName:  string(tableName),
		IfNotExist: ifNotExist,
		Select:     selectStm.(*SelectStm),
	}, nil
}

// For create database statement, if supports:
// * create {database|schema} [if not exist] database_name [[Default | character set = value] | [Default | collate = value]];
func (parser *Parser) parseCreateDatabaseStm() (*CreateDatabaseStm, error) {
	ifNotExist := parser.matchTokenTypes(true, IF, NOT, EXIST)
	databaseName, ret := parser.parseIdentOrWord(false)
	if !ret || isTableNameEmpty(databaseName) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	charset, collate, err := parser.parseCharsetAndCollate()
	if err != nil {
		return nil, err
	}
	return &CreateDatabaseStm{
		DatabaseName: string(databaseName),
		IfNotExist:   ifNotExist,
		Charset:      charset,
		Collate:      collate,
	}, nil
}

func (parser *Parser) parseCharsetAndCollate() (charset CharacterSetTP, collate CollateTP, err error) {
	charset, err = parser.parseCharacterSet()
	if err != nil {
		return
	}
	collate, err = parser.parseCollate()
	return
}
