package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

// Select statement is like:
// * select [all | distinct | distinctrow] select_expression... from table_reference... [WhereStm] [GroupByStm] [HavingStm]
// [OrderByStm] [LimitStm] [for update | lock in share mode]
// select_expression could be:
// expr [as] alias
// *

func (parser *Parser) resolveSelectStm(needCheckSemicolon bool) (stm ast.Stm, err error) {
	if !parser.matchTokenTypes(false, lexer.SELECT) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	token, ok := parser.NextToken()
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	selectTp := ast.SelectAllTp
	switch token.Tp {
	case lexer.ALL:
	case lexer.DISTINCT:
		selectTp = ast.SelectDistinctTp
	case lexer.DISTINCTROW:
		selectTp = ast.SelectDistinctRowTp
	default:
		parser.UnReadToken()
	}
	var selectExpressionStm *ast.SelectExpressionStm
	if parser.matchTokenTypes(true, lexer.STAR) {
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

func (parser *Parser) parseStarSelectExpression() (*ast.SelectExpressionStm, error) {
	return &ast.SelectExpressionStm{
		Tp:   ast.StarSelectExpressionTp,
		Expr: lexer.STAR,
	}, nil
}

func (parser *Parser) parseExprSelectExpression() (*ast.SelectExpressionStm, error) {
	var exprs []*ast.ExpressionStm
	for {
		expr, err := parser.resolveExpression()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
		if !parser.matchTokenTypes(true, lexer.COMMA) {
			break
		}
	}
	return &ast.SelectExpressionStm{
		Tp:   ast.ExprSelectExpressionTp,
		Expr: exprs,
	}, nil
}

func (parser *Parser) resolveRemainingSelectStm(selectTp ast.SelectTp, expr *ast.SelectExpressionStm, needCheckSemicolon bool) (*ast.SelectStm, error) {
	if !parser.matchTokenTypes(false, lexer.FROM) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var tables []ast.TableReferenceStm
	for {
		tableRef, err := parser.parseTableReferenceStm()
		if err != nil {
			return nil, err
		}
		tables = append(tables, tableRef)
		if !parser.matchTokenTypes(true, lexer.COMMA) {
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
	if needCheckSemicolon && !parser.matchTokenTypes(false, lexer.SEMICOLON) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	lockTp := ast.NoneLockTp
	if parser.matchTokenTypes(true, lexer.FOR, lexer.UPDATE) {
		lockTp = ast.ForUpdateLockTp
	}
	if parser.matchTokenTypes(true, lexer.LOCK, lexer.IN, lexer.SHARE, lexer.MOD) {
		lockTp = ast.LockInShareModeTp
	}
	return &ast.SelectStm{
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
