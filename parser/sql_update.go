package parser

// Update statement is like:
// * update table_reference set assignments... [WhereStm] [OrderByStm] [LimitStm]
// * update table_reference... set assignments... [WhereStm]

func (parser *Parser) resolveUpdateStm() (Stm, error) {
	if !parser.matchTokenTypes(false, UPDATE) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var tableRefs []TableReferenceStm
	for {
		tableRef, err := parser.parseTableReferenceStm()
		if err != nil {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		if tableRef.Tp != TableReferenceTableFactorTp || tableRef.TableReference.(TableReferenceTableFactorStm).Tp != TableReferencePureTableNameTp {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		tableRefs = append(tableRefs, tableRef)
		if !parser.matchTokenTypes(false, COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, SET) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}

	var assignments []AssignmentStm
	for {
		assignment, err := parser.parseAssignmentStm()
		if err != nil {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		assignments = append(assignments, assignment)
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	var order *OrderByStm
	var limit *LimitStm
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
	if !parser.matchTokenTypes(false, SEMICOLON) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &UpdateStm{
		TableRefs:   tableRefs,
		Assignments: assignments,
		Where:       where,
		OrderBy:     order,
		Limit:       limit,
	}, nil
}
