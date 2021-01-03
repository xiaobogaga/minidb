package parser

// Port from mysql.

// A table reference statement is like:
// table_factor | joined_table
// where table_factor can be:
// * {tb_name [[as] alias] | (table_subquery) as alias} | (tableRef)
// and joined_table is like:
// * table_factor { {left|right} [outer] join table_reference join_specification | inner join table_factor [join_specification] } *
// join_specification is like:
// on where_condition | using (col...)

// Diff with mysql
// * index_hint are not supported.
// * cross join, straight join and natural join keywords are not supported.

var (
	emptyTableRefStm            = TableReferenceStm{}
	emptyTableRefTableFactorStm = TableReferenceTableFactorStm{}
)

func (parser *Parser) parseTableReferenceStm() (stm TableReferenceStm, err error) {
	token, ok := parser.NextToken()
	if !ok {
		return emptyTableRefStm, parser.MakeSyntaxError(parser.pos - 1)
	}
	var tableFactorStm TableReferenceTableFactorStm
	switch token.Tp {
	case LEFTBRACKET:
		parser.UnReadToken()
		tableFactorStm, err = parser.parseSubTableRefOrTableSubQuery()
	case IDENT, WORD:
		parser.UnReadToken()
		tableFactorStm, err = parser.parseTableAsStm()
	default:
		return emptyTableRefStm, parser.MakeSyntaxError(parser.pos - 1)
	}

	// Also need to check join type, because maybe a joined_table reference.
	token, ok = parser.NextToken()
	switch token.Tp {
	case LEFT, RIGHT, INNER, JOIN:
		parser.UnReadToken()
		stm, err = parser.ParseJoinStm(tableFactorStm)
	default:
		// If not, unread this token.
		parser.UnReadToken()
		stm = TableReferenceStm{
			Tp:             TableReferenceTableFactorTp,
			TableReference: tableFactorStm,
		}
		return stm, err
	}
	return

}

func (parser *Parser) parseSubTableRefOrTableSubQuery() (stm TableReferenceTableFactorStm, err error) {
	if !parser.matchTokenTypes(false, LEFTBRACKET) {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(parser.pos - 1)
	}
	token, ok := parser.NextToken()
	if !ok {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(parser.pos - 1)
	}
	switch token.Tp {
	case SELECT:
		parser.UnReadToken()
		stm, err = parser.parseTableSubQuery()
	default:
		parser.UnReadToken()
		stm, err = parser.parseSubTableRefStm()
	}
	return stm, err
}

func (parser *Parser) parseSubTableRefStm() (stm TableReferenceTableFactorStm, err error) {
	if !parser.matchTokenTypes(false, LEFTBRACKET) {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(parser.pos - 1)
	}
	tableRef, err := parser.parseTableReferenceStm()
	if err != nil {
		return
	}
	if !parser.matchTokenTypes(false, RIGHTBRACKET) {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(parser.pos - 1)
	}
	return TableReferenceTableFactorStm{
		Tp:                   TableReferenceSubTableReferenceStmTP,
		TableFactorReference: tableRef,
	}, nil
}

func (parser *Parser) parseTableAsStm() (TableReferenceTableFactorStm, error) {
	tableName, ok := parser.parseIdentOrWord(false)
	if !ok {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(parser.pos - 1)
	}
	alias := ""
	if parser.matchTokenTypes(true, AS) {
		ret, ok := parser.parseIdentOrWord(false)
		if !ok {
			return emptyTableRefTableFactorStm, parser.MakeSyntaxError(parser.pos - 1)
		}
		alias = string(ret)
	} else {
		ident, isIdent := parser.parseIdentOrWord(true)
		if isIdent {
			alias = string(ident)
		}
	}
	return TableReferenceTableFactorStm{
		Tp: TableReferencePureTableNameTp,
		TableFactorReference: TableReferencePureTableRefStm{
			TableName: string(tableName),
			Alias:     alias,
		},
	}, nil
}

// * table_sub_query := (selectStm) as alias
func (parser *Parser) parseTableSubQuery() (TableReferenceTableFactorStm, error) {
	if !parser.matchTokenTypes(false, LEFTBRACKET) {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(parser.pos - 1)
	}
	selectStm, err := parser.resolveSelectStm(false)
	if err != nil {
		return emptyTableRefTableFactorStm, err
	}
	if parser.matchTokenTypes(false, AS) {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(parser.pos - 1)
	}
	alias, ok := parser.parseIdentOrWord(false)
	if !ok {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(parser.pos - 1)
	}
	if !parser.matchTokenTypes(false, RIGHTBRACKET) {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(parser.pos - 1)
	}
	return TableReferenceTableFactorStm{
		Tp: TableReferenceTableSubQueryTp,
		TableFactorReference: TableSubQueryStm{
			Select: selectStm.(*SelectStm),
			Alias:  string(alias),
		},
	}, nil
}

func (parser *Parser) parseJoinSpecification(joinType JoinType) (*JoinSpecification, error) {
	// Note: for inner join, it's okay if it doesn't have join specification.
	token, ok := parser.NextToken()
	if !ok && joinType != InnerJoin {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	if !ok {
		return nil, nil
	}
	switch token.Tp {
	case ON:
		parser.UnReadToken()
		return parser.parseOnJoinSpec()
	case USING:
		return parser.parseUsingJoinSpec()
	}
	if joinType == InnerJoin {
		parser.UnReadToken()
		return nil, nil
	}
	return nil, parser.MakeSyntaxError(parser.pos - 1)
}

func (parser *Parser) parseJoinType() (JoinType, error) {
	if parser.matchTokenTypes(true, JOIN) || parser.matchTokenTypes(true, INNER, JOIN) {
		return InnerJoin, nil
	}
	if parser.matchTokenTypes(true, LEFT, JOIN) || parser.matchTokenTypes(true, LEFT, OUTER, JOIN) {
		return LeftOuterJoin, nil
	}
	if parser.matchTokenTypes(true, RIGHT, JOIN) || parser.matchTokenTypes(true, RIGHT, OUTER, JOIN) {
		return RightOuterJoin, nil
	}
	return 0, parser.MakeSyntaxError(parser.pos)
}

func isJoinTypeToken(t Token) bool {
	tp := t.Tp
	return tp == LEFT || tp == RIGHT || tp == INNER || tp == JOIN
}

func (parser *Parser) ParseJoinFactors() (factors []JoinFactor, err error) {
	for {
		token, ok := parser.NextToken()
		if !ok || !isJoinTypeToken(token) {
			parser.UnReadToken()
			break
		}
		parser.UnReadToken()
		joinType, err := parser.parseJoinType()
		if err != nil {
			return nil, err
		}
		joinedTableRef, err := parser.parseTableReferenceStm()
		if err != nil {
			return nil, err
		}
		joinSpec, err := parser.parseJoinSpecification(joinType)
		if err != nil {
			return nil, err
		}
		joinFactor := JoinFactor{
			JoinTp:               joinType,
			JoinedTableReference: joinedTableRef,
			JoinSpec:             joinSpec,
		}
		factors = append(factors, joinFactor)
	}
	return
}

func (parser *Parser) ParseJoinStm(tableRef TableReferenceTableFactorStm) (TableReferenceStm, error) {
	joinFactos, err := parser.ParseJoinFactors()
	if err != nil {
		return emptyTableRefStm, err
	}
	return TableReferenceStm{
		Tp: TableReferenceJoinTableTp,
		TableReference: JoinedTableStm{
			TableFactor: tableRef,
			JoinFactors: joinFactos,
		},
	}, nil
}
