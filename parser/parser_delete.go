package parser

// Delete statement is like:
// * delete from tb_name [whereStm] [OrderByStm] [LimitStm]
// * delete tb1,... from table_references [WhereStm]

func (parser *Parser) resolveDeleteStm() (stm Stm, err error) {
	if !parser.matchTokenTypes(false, DELETE) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	if parser.matchTokenTypes(true, FROM) {
		return parser.parseDeleteSingleTableStm()
	}
	return parser.parseDeleteMultiTableStm()
}

func (parser *Parser) parseDeleteSingleTableStm() (stm *SingleDeleteStm, err error) {
	tableName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	whereStm, err := parser.ResolveWhereStm()
	if err != nil {
		return nil, err
	}
	orderByStm, err := parser.ParseOrderByStm()
	if err != nil {
		return nil, err
	}
	limitStm, err := parser.parseLimit()
	if err != nil {
		return nil, err
	}
	if !parser.matchTokenTypes(false, SEMICOLON) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return &SingleDeleteStm{
		TableRef: TableReferenceStm{
			Tp: TableReferenceTableFactorTp,
			TableReference: TableReferenceTableFactorStm{
				Tp: TableReferencePureTableNameTp,
				TableFactorReference: TableReferencePureTableRefStm{
					TableName: string(tableName),
				},
			},
		},
		Where:   whereStm,
		OrderBy: orderByStm,
		Limit:   limitStm,
	}, nil
}

func (parser *Parser) parseDeleteMultiTableStm() (stm *MultiDeleteStm, err error) {
	var tableNames []string
	for {
		tableName, ok := parser.parseIdentOrWord(false)
		if !ok {
			return nil, parser.MakeSyntaxError(parser.pos - 1)
		}
		tableNames = append(tableNames, string(tableName))
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, FROM) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	var tableRefs []TableReferenceStm
	for {
		tableRef, err := parser.parseTableReferenceStm()
		if err != nil {
			return nil, err
		}
		tableRefs = append(tableRefs, tableRef)
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	whereStm, err := parser.ResolveWhereStm()
	if err != nil {
		return nil, err
	}
	if !parser.matchTokenTypes(false, SEMICOLON) {
		return nil, parser.MakeSyntaxError(parser.pos - 1)
	}
	return &MultiDeleteStm{
		TableNames:      tableNames,
		TableReferences: tableRefs,
		Where:           whereStm,
	}, nil
}
