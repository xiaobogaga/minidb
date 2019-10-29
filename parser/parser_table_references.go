package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

// Port from mysql.

// A table reference statement is like:
// * {tb_name [as alias] | joined_table | (table_subquery) as alias} | (tableRef)
// where joined_table is like:
// * table_reference { {left|right} [outer] join table_reference join_specification | inner join table_factor [join_specification]}
// join_specification is like:
// on where_condition | using (col...)

// Diff with mysql
// * index_hint are not supported.
// * cross join, straight join and natural join keywords are not supported.

var emptyTableRefStm = ast.TableReferenceStm{}

func (parser *Parser) parseTableReferenceStm() (stm ast.TableReferenceStm, err error) {
	hasLeftBracket := parser.matchTokenTypes(true, lexer.LEFTBRACKET)
	if hasLeftBracket {
		stm, err = parser.parseTableReferenceStm()
	} else {
		token, ok := parser.NextToken()
		if !ok {
			return emptyTableRefStm, parser.MakeSyntaxError(1, parser.pos-1)
		}
		switch token.Tp {
		case lexer.SELECT:
			// Sub query
			parser.UnReadToken()
			stm, err = parser.parseTableSubQuery()
		case lexer.IDENT, lexer.WORD:
			// Table as
			parser.UnReadToken()
			stm, err = parser.parseTableAsStm()
		}
		// Also need to check join type, because maybe a joined_table reference.
		token, ok = parser.NextToken()
		switch token.Tp {
		case lexer.LEFT:
			stm, err = parser.parseLeftRightOuterJoinStm(stm, ast.LeftOuterJoin)
		case lexer.RIGHT:
			stm, err = parser.parseLeftRightOuterJoinStm(stm, ast.RightOuterJoin)
		case lexer.INNER:
			stm, err = parser.parseInnerJoinStm(stm)
		default:
			// If not, unread this token.
			parser.UnReadToken()
		}
	}
	if err != nil {
		return emptyTableRefStm, err
	}
	if hasLeftBracket && !parser.matchTokenTypes(true, lexer.RIGHTBRACKET) {
		return emptyTableRefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return stm, nil
}

func (parser *Parser) parseTableAsStm() (ast.TableReferenceStm, error) {
	tableName, ok := parser.parseIdentOrWord(false)
	if !ok {
		return emptyTableRefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	alias := ""
	if parser.matchTokenTypes(true, lexer.AS) {
		ret, ok := parser.parseIdentOrWord(false)
		if !ok {
			return emptyTableRefStm, parser.MakeSyntaxError(1, parser.pos-1)
		}
		alias = string(ret)
	}
	return ast.TableReferenceStm{
		Tp: ast.TableReferencePureTableNameTp,
		TableReference: ast.TableReferenceTblStm{
			TableName: string(tableName),
			Alias:     alias,
		},
	}, nil
}

// * table_sub_query := (selectStm) as alias
func (parser *Parser) parseTableSubQuery() (ast.TableReferenceStm, error) {
	if !parser.matchTokenTypes(false, lexer.LEFTBRACKET) {
		return emptyTableRefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	selectStm, err := parser.resolveSelectStm(false)
	if err != nil {
		return emptyTableRefStm, err
	}
	if parser.matchTokenTypes(false, lexer.AS) {
		return emptyTableRefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	alias, ok := parser.parseIdentOrWord(false)
	if !ok {
		return emptyTableRefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET) {
		return emptyTableRefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return ast.TableReferenceStm{
		Tp: ast.TableReferenceTableSubQueryTp,
		TableReference: ast.TableSubQueryStm{
			Select: selectStm,
			Alias:  string(alias),
		},
	}, nil
}

func (parser *Parser) parseLeftRightOuterJoinStm(tableRef ast.TableReferenceStm, leftOrRight ast.JoinType) (ast.TableReferenceStm, error) {
	parser.matchTokenTypes(true, lexer.OUTER)
	if !parser.matchTokenTypes(false, lexer.JOIN) {
		return emptyTableRefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	tableRef, err := parser.parseTableReferenceStm()
	if err != nil {
		return emptyTableRefStm, err
	}
	joinSpec, err := parser.parseJoinSpecification()
	if err != nil {
		return emptyTableRefStm, err
	}
	return ast.TableReferenceStm{
		Tp: ast.TableReferenceJoinedTableTp,
		TableReference: ast.JoinedTableStm{
			TableReference:       tableRef,
			JoinTp:               leftOrRight,
			JoinedTableReference: tableRef,
			JoinSpec:             joinSpec,
		},
	}, nil
}

func (parser *Parser) parseInnerJoinStm(tableRef ast.TableReferenceStm) (ast.TableReferenceStm, error) {
	if !parser.matchTokenTypes(false, lexer.JOIN) {
		return emptyTableRefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	tableRef, err := parser.parseTableReferenceStm()
	if err != nil {
		return emptyTableRefStm, err
	}
	joinSpec, err := parser.parseJoinSpecification()
	if err != nil {
		return emptyTableRefStm, err
	}
	return ast.TableReferenceStm{
		Tp: ast.TableReferenceJoinedTableTp,
		TableReference: ast.JoinedTableStm{
			TableReference:       tableRef,
			JoinTp:               ast.InnerJoin,
			JoinedTableReference: tableRef,
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
