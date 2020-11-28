package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

// Port from mysql.

// A table reference statement is like:
// table_factor | joined_table
// where table_factor can be:
// * {tb_name [as alias] | (table_subquery) as alias} | (tableRef)
// and joined_table is like:
// * table_factor { {left|right} [outer] join table_reference join_specification | inner join table_factor [join_specification] } *
// join_specification is like:
// on where_condition | using (col...)

// Diff with mysql
// * index_hint are not supported.
// * cross join, straight join and natural join keywords are not supported.

var (
	emptyTableRefStm            = ast.TableReferenceStm{}
	emptyTableRefTableFactorStm = ast.TableReferenceTableFactorStm{}
)

func (parser *Parser) parseTableReferenceStm() (stm ast.TableReferenceStm, err error) {
	token, ok := parser.NextToken()
	if !ok {
		return emptyTableRefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var tableFactorStm ast.TableReferenceTableFactorStm
	switch token.Tp {
	case lexer.LEFTBRACKET:
		parser.UnReadToken()
		tableFactorStm, err = parser.parseSubTableRefOrTableSubQuery()
	case lexer.IDENT, lexer.WORD:
		parser.UnReadToken()
		tableFactorStm, err = parser.parseTableAsStm()
	}

	for {
		// Also need to check join type, because maybe a joined_table reference.
		token, ok = parser.NextToken()
		switch token.Tp {
		case lexer.LEFT:
			stm, err = parser.parseLeftRightOuterJoinStm(tableFactorStm, ast.LeftOuterJoin)
		case lexer.RIGHT:
			stm, err = parser.parseLeftRightOuterJoinStm(tableFactorStm, ast.RightOuterJoin)
		case lexer.INNER:
			stm, err = parser.parseInnerJoinStm(tableFactorStm)
		default:
			// If not, unread this token.
			parser.UnReadToken()
			stm = ast.TableReferenceStm{
				Tp:             ast.TableReferenceTableFactorTp,
				TableReference: tableFactorStm,
			}
			break
		}
		if err != nil {
			return
		}
	}

	return stm, err
}

func (parser *Parser) parseSubTableRefOrTableSubQuery() (stm ast.TableReferenceTableFactorStm, err error) {
	if !parser.matchTokenTypes(false, lexer.LEFTBRACKET) {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	token, ok := parser.NextToken()
	if !ok {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(1, parser.pos)
	}
	switch token.Tp {
	case lexer.SELECT:
		parser.UnReadToken()
		stm, err = parser.parseTableSubQuery()
	default:
		parser.UnReadToken()
		stm, err = parser.parseSubTableRefStm()
	}
	return stm, err
}

func (parser *Parser) parseSubTableRefStm() (stm ast.TableReferenceTableFactorStm, err error) {
	if !parser.matchTokenTypes(false, lexer.LEFTBRACKET) {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	tableRef, err := parser.parseTableReferenceStm()
	if err != nil {
		return
	}
	if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET) {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return ast.TableReferenceTableFactorStm{
		Tp:                   ast.TableReferenceSubTableReferenceStmTP,
		TableFactorReference: tableRef,
	}, nil
}

func (parser *Parser) parseTableAsStm() (ast.TableReferenceTableFactorStm, error) {
	tableName, ok := parser.parseIdentOrWord(false)
	if !ok {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	alias := ""
	if parser.matchTokenTypes(true, lexer.AS) {
		ret, ok := parser.parseIdentOrWord(false)
		if !ok {
			return emptyTableRefTableFactorStm, parser.MakeSyntaxError(1, parser.pos-1)
		}
		alias = string(ret)
	}
	return ast.TableReferenceTableFactorStm{
		Tp: ast.TableReferencePureTableNameTp,
		TableFactorReference: ast.TableReferencePureTableRefStm{
			TableName: string(tableName),
			Alias:     alias,
		},
	}, nil
}

// * table_sub_query := (selectStm) as alias
func (parser *Parser) parseTableSubQuery() (ast.TableReferenceTableFactorStm, error) {
	if !parser.matchTokenTypes(false, lexer.LEFTBRACKET) {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	selectStm, err := parser.resolveSelectStm(false)
	if err != nil {
		return emptyTableRefTableFactorStm, err
	}
	if parser.matchTokenTypes(false, lexer.AS) {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	alias, ok := parser.parseIdentOrWord(false)
	if !ok {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET) {
		return emptyTableRefTableFactorStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return ast.TableReferenceTableFactorStm{
		Tp: ast.TableReferenceTableSubQueryTp,
		TableFactorReference: ast.TableSubQueryStm{
			Select: selectStm.(*ast.SelectStm),
			Alias:  string(alias),
		},
	}, nil
}

func (parser *Parser) parseLeftRightOuterJoinStm(tableRef ast.TableReferenceTableFactorStm, leftOrRight ast.JoinType) (ast.TableReferenceStm, error) {
	parser.matchTokenTypes(true, lexer.OUTER)
	if !parser.matchTokenTypes(false, lexer.JOIN) {
		return emptyTableRefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	joinedTableRef, err := parser.parseTableReferenceStm()
	if err != nil {
		return emptyTableRefStm, err
	}
	joinSpec, err := parser.parseJoinSpecification()
	if err != nil {
		return emptyTableRefStm, err
	}
	return ast.TableReferenceStm{
		Tp: ast.TableReferenceJoinTableTp,
		TableReference: ast.JoinedTableStm{
			TableReference:       tableRef,
			JoinTp:               leftOrRight,
			JoinedTableReference: joinedTableRef,
			JoinSpec:             joinSpec,
		},
	}, nil
}

func (parser *Parser) parseInnerJoinStm(tableRef ast.TableReferenceTableFactorStm) (ast.TableReferenceStm, error) {
	if !parser.matchTokenTypes(false, lexer.JOIN) {
		return emptyTableRefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	joinedTableRef, err := parser.parseTableReferenceStm()
	if err != nil {
		return emptyTableRefStm, err
	}
	joinSpec, err := parser.parseJoinSpecification()
	if err != nil {
		return emptyTableRefStm, err
	}
	return ast.TableReferenceStm{
		Tp: ast.TableReferenceJoinTableTp,
		TableReference: ast.JoinedTableStm{
			TableReference:       tableRef,
			JoinTp:               ast.InnerJoin,
			JoinedTableReference: joinedTableRef,
			JoinSpec:             joinSpec,
		},
	}, nil
}

var emptyJoinSepc = ast.JoinSpecification{}

func (parser *Parser) parseJoinSpecification() (ast.JoinSpecification, error) {
	token, ok := parser.NextToken()
	if !ok {
		return emptyJoinSepc, parser.MakeSyntaxError(1, parser.pos-1)
	}
	switch token.Tp {
	case lexer.ON:
		parser.UnReadToken()
		return parser.parseOnJoinSpec()
	case lexer.USING:
		parser.UnReadToken()
		return parser.parseUsingJoinSpec()
	default:
		return emptyJoinSepc, parser.MakeSyntaxError(1, parser.pos)
	}
}
