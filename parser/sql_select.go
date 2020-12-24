package parser

// Select statement is like:
// * select [all | distinct | distinctrow] select_expression... from table_reference... [WhereStm] [GroupByStm] [HavingStm]
// [OrderByStm] [LimitStm] [for update | lock in share mode]
// select_expression could be:
// expr [as] alias
// *

func (parser *Parser) resolveSelectStm(needCheckSemicolon bool) (stm Stm, err error) {
	if !parser.matchTokenTypes(false, SELECT) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	token, ok := parser.NextToken()
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	selectTp := SelectAllTp
	switch token.Tp {
	case ALL:
	case DISTINCT:
		selectTp = SelectDistinctTp
	case DISTINCTROW:
		selectTp = SelectDistinctRowTp
	default:
		parser.UnReadToken()
	}
	var selectExpressionStm *SelectExpressionStm
	if parser.matchTokenTypes(true, MUL) {
		selectExpressionStm, err = parser.parseStarSelectExpression()
	} else {
		selectExpressionStm, err = parser.parseExprSelectExpression()
	}
	if err != nil {
		return nil, err
	}
	stm, err = parser.resolveRemainingSelectStm(selectTp, selectExpressionStm, needCheckSemicolon)
	return
}

func (parser *Parser) parseStarSelectExpression() (*SelectExpressionStm, error) {
	return &SelectExpressionStm{
		Tp: StarSelectExpressionTp,
	}, nil
}

func (parser *Parser) parseExprSelectExpression() (*SelectExpressionStm, error) {
	var exprs []*SelectExpr
	for {
		expr, err := parser.resolveExpression()
		if err != nil {
			return nil, err
		}
		selectExpr := &SelectExpr{Expr: expr}
		alias := ""
		if parser.matchTokenTypes(true, AS) {
			ret, ok := parser.parseIdentOrWord(false)
			if !ok {
				return nil, parser.MakeSyntaxError(1, parser.pos-1)
			}
			alias = string(ret)
		}
		selectExpr.Alias = alias
		exprs = append(exprs, selectExpr)
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	return &SelectExpressionStm{
		Tp:   ExprSelectExpressionTp,
		Expr: exprs,
	}, nil
}

func (parser *Parser) resolveRemainingSelectStm(selectTp SelectTp, expr *SelectExpressionStm, needCheckSemicolon bool) (*SelectStm, error) {
	if !parser.matchTokenTypes(false, FROM) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var tables []TableReferenceStm
	for {
		tableRef, err := parser.parseTableReferenceStm()
		if err != nil {
			return nil, err
		}
		tables = append(tables, tableRef)
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	whereStm, err := parser.resolveWhereStm()
	if err != nil {
		return nil, err
	}
	groupByStm, err := parser.parseGroupByStm()
	if err != nil {
		return nil, err
	}
	havingStm, err := parser.parseHavingStm()
	if err != nil {
		return nil, err
	}
	orderByStm, err := parser.parseOrderByStm()
	if err != nil {
		return nil, err
	}
	limitStm, err := parser.parseLimit()
	if err != nil {
		return nil, err
	}
	if needCheckSemicolon && !parser.matchTokenTypes(false, SEMICOLON) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	lockTp := NoneLockTp
	if parser.matchTokenTypes(true, FOR, UPDATE) {
		lockTp = ForUpdateLockTp
	}
	if parser.matchTokenTypes(true, LOCK, IN, SHARE, MOD) {
		lockTp = LockInShareModeTp
	}
	return &SelectStm{
		Tp:                selectTp,
		SelectExpressions: expr,
		TableReferences:   tables,
		Where:             whereStm,
		OrderBy:           orderByStm,
		Groupby:           groupByStm,
		Having:            havingStm,
		LimitStm:          limitStm,
		LockTp:            lockTp,
	}, nil
}
