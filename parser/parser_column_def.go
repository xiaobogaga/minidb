package parser

// A columnDef statement is:
// col_name col_type [not null|null] [default default_value] [AUTO_INCREMENT] [unique [key]] [[primary] key]

// parseColumnDef parse a column definition statement and return it.

func (parser *Parser) parseColumnDef() (*ColumnDefStm, error) {
	columnName, ret := parser.parseIdentOrWord(false)
	if !ret {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	colType, success := parser.parseColumnType(false)
	if !success {
		return nil, parser.MakeSyntaxError(1, parser.pos)
	}
	col := &ColumnDefStm{ColName: string(columnName), ColumnType: colType}
	if parser.matchTokenTypes(true, NULL) {
		col.AllowNULL = true
	} else if parser.matchTokenTypes(true, NOT, NULL) {
		col.AllowNULL = false
	}
	if parser.matchTokenTypes(true, DEFAULT) {
		colValue, success := parser.parseValue(false)
		if !success {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		col.ColDefaultValue = colValue
	}
	if parser.matchTokenTypes(true, AUTO_INCREMENT) {
		col.AutoIncrement = true
	}
	if parser.matchTokenTypes(true, UNIQUE, KEY) || parser.matchTokenTypes(true, UNIQUE) {
		col.UniqueKey = true
	}
	if parser.matchTokenTypes(true, PRIMARY, KEY) || parser.matchTokenTypes(true, PRIMARY) {
		col.PrimaryKey = true
	}
	return col, nil
}
