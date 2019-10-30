package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

// Update statement is like:
// * update table_reference set assignments... [WhereStm] [OrderByStm] [LimitStm]
// * update table_reference... set assignments... [WhereStm]

func (parser *Parser) resolveUpdateStm() (ast.Stm, error) {
	if !parser.matchTokenTypes(false, lexer.UPDATE) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var tableRefs []ast.TableReferenceStm
	for {
		tableRef, err := parser.parseTableReferenceStm()
		if err != nil {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		tableRefs = append(tableRefs, tableRef)
		if !parser.matchTokenTypes(false, lexer.COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, lexer.SET) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}

	var assignments []ast.AssignmentStm
	for {
		assignment, err := parser.parseAssignmentStm()
		if err != nil {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		assignments = append(assignments, assignment)
		if !parser.matchTokenTypes(true, lexer.COMMA) {
			break
		}
	}
	var order *ast.OrderByStm
	var limit *ast.LimitStm
	var err error
	where, _ := parser.resolveWhereStm()
	if len(tableRefs) > 1 {
		order, err = parser.parseOrderByStm()
		if err != nil {
			return nil, err
		}
		limit, err = parser.parseLimit()
		if err != nil {
			return nil, err
		}
	}
	if !parser.matchTokenTypes(false, lexer.SEMICOLON) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &ast.UpdateStm{
		TableRefs:   tableRefs,
		Assignments: assignments,
		Where:       where,
		OrderBy:     order,
		Limit:       limit,
	}, nil
}
