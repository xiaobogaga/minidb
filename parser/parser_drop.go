package parser

// Drop statement can be drop table statement or drop database statement.
// Drop database statement is like:
// * drop {database | schema} [if exists] db_name;
// Drop table statement is like:
// * drop table [if exists] tb_name[,tb_name...] [RESTRICT|CASCADE];

// parseDropStm parses a drop statement and return it.
func (parser *Parser) parseDropStm() (stm Stm, err error) {
	if !parser.matchTokenTypes(false, DROP) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	t, ok := parser.NextToken()
	if !ok {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	switch t.Tp {
	case TABLE:
		stm, err = parser.parseDropTableStm()
	case DATABASE, SCHEMA:
		stm, err = parser.parseDropDatabaseStm()
	default:
		err = parser.MakeSyntaxError(parser.pos - 1)
	}
	if err != nil {
		return nil, err
	}
	if !parser.matchTokenTypes(false, SEMICOLON) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return stm, nil
}

// Drop table statement is like:
// * drop table [if exists] tb_name[,tb_name...] [RESTRICT|CASCADE];
func (parser *Parser) parseDropTableStm() (*DropTableStm, error) {
	ifExist := parser.matchTokenTypes(true, IF, EXISTS)
	var tableNames []string
	for {
		name, ret := parser.parseIdentOrWord(false)
		if !ret {
			return nil, parser.MakeSyntaxError(parser.pos - 1)
		}
		tableNames = append(tableNames, string(name))
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	parser.matchTokenTypes(true, RESTRICT)
	parser.matchTokenTypes(true, CASCADE)
	return &DropTableStm{
		IfExists:   ifExist,
		TableNames: tableNames,
	}, nil
}

// Drop database statement is like:
// * drop {database | schema} [if exists] db_name;
func (parser *Parser) parseDropDatabaseStm() (*DropDatabaseStm, error) {
	ifExist := parser.matchTokenTypes(true, IF, EXISTS)
	name, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return &DropDatabaseStm{
		DatabaseName: string(name),
		IfExist:      ifExist,
	}, nil
}
