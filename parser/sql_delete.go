package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

// Delete statement is like:
// * delete from tb_name [whereStm] [OrderByStm] [LimitStm]
// * delete tb1,... from table_references [WhereStm]

func (parser *Parser) resolveDeleteStm() (stm *ast.DeleteStm, err error) {
	if !parser.matchTokenTypes(false, lexer.DELETE) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if parser.matchTokenTypes(true, lexer.FROM) {
		return parser.parseDeleteSingleTableStm()
	}
	return parser.parseDeleteMultiTableStm()
}

func (parser *Parser) parseDeleteSingleTableStm() (stm *ast.DeleteStm, err error) {
	tableName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	whereStm, err := parser.resolveWhereStm()
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
	if !parser.matchTokenTypes(false, lexer.SEMICOLON) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &ast.DeleteStm{
		Tp: ast.SingleDeleteStmTp,
		Stm: ast.SingleDeleteStm{
			TableName: string(tableName),
			Where:     whereStm,
			OrderBy:   orderByStm,
			Limit:     limitStm,
		},
	}, nil
}

func (parser *Parser) parseDeleteMultiTableStm() (stm *ast.DeleteStm, err error) {
	var tableNames []string
	for {
		tableName, ok := parser.parseIdentOrWord(false)
		if !ok {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		tableNames = append(tableNames, string(tableName))
		if !parser.matchTokenTypes(true, lexer.COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, lexer.FROM) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var tableRefs []ast.TableReferenceStm
	for {
		tableRef, err := parser.parseTableReferenceStm()
		if err != nil {
			return nil, err
		}
		tableRefs = append(tableRefs, tableRef)
		if !parser.matchTokenTypes(true, lexer.COMMA) {
			break
		}
	}
	whereStm, err := parser.resolveWhereStm()
	if err != nil {
		return nil, err
	}
	if !parser.matchTokenTypes(false, lexer.SEMICOLON) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &ast.DeleteStm{
		Tp: ast.MultiDeleteStmTp,
		Stm: ast.MultiDeleteStm{
			TableNames:      tableNames,
			TableReferences: tableRefs,
			Where:           whereStm,
		},
	}, nil
}
