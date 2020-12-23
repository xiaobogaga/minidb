package parser

// A constraint statement is like:
// * [Constraint] primary key (col_name [,col_name...)
// * [Constraint] unique {index|key} [index_name] (col_name [,col_name...)
// * [Constraint] foreign key [index_name] (col_name [,col_name...) references tb_name (key...) [on delete reference_option] [on update reference_option]
//    reference_option is like: {restrict | cascade | set null | no action | set default} and default is restrict
// Restrict is the default

// parseConstraintDef parse a constraint definition and return it.
func (parser *Parser) parseConstraintDef() (*ConstraintDefStm, error) {
	parser.matchTokenTypes(true, CONSTRAINT)
	token, ok := parser.NextToken()
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos)
	}
	switch token.Tp {
	case PRIMARY:
		return parser.parsePrimaryKeyDef()
	case UNIQUE:
		return parser.parseUniqueKeyDef()
	case FOREIGN:
		return parser.parseForeignKeyDef()
	}
	return nil, parser.MakeSyntaxError(1, parser.pos)
}

// * [Constraint] primary key (col_name [,col_name...)
func (parser *Parser) parsePrimaryKeyDef() (*ConstraintDefStm, error) {
	if !parser.matchTokenTypes(false, KEY) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if !parser.matchTokenTypes(false, LEFTBRACKET) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var colNames []string
	for {
		colName, ok := parser.parseIdentOrWord(false)
		if !ok {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		colNames = append(colNames, string(colName))
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, RIGHTBRACKET) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &ConstraintDefStm{
		Tp:         PrimaryKeyConstraintTp,
		Constraint: PrimaryKeyDefStm{ColNames: colNames},
	}, nil
}

// * [Constraint] unique {index|key} index_name (col_name [,col_name...)
func (parser *Parser) parseUniqueKeyDef() (*ConstraintDefStm, error) {
	if !parser.matchTokenTypes(true, INDEX) && !parser.matchTokenTypes(true, KEY) {
		return nil, parser.MakeSyntaxError(1, parser.pos)
	}
	indexName, _ := parser.parseIdentOrWord(true)
	if !parser.matchTokenTypes(false, LEFTBRACKET) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var colNames []string
	for {
		colName, ok := parser.parseIdentOrWord(false)
		if !ok {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		colNames = append(colNames, string(colName))
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, RIGHTBRACKET) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return &ConstraintDefStm{
		Tp:         UniqueKeyConstraintTp,
		Constraint: UniqueKeyDefStm{IndexName: string(indexName), ColNames: colNames},
	}, nil
}

// * [Constraint] foreign key [index_name] (col_name [,col_name...) references tb_name (key...) [on delete reference_option] [on update reference_option]
//   reference_option is like: {restrict | cascade | set null | no action | set default}, and restrict is default.
func (parser *Parser) parseForeignKeyDef() (*ConstraintDefStm, error) {
	if !parser.matchTokenTypes(true, KEY) {
		return nil, parser.MakeSyntaxError(1, parser.pos)
	}
	indexName, _ := parser.parseIdentOrWord(true)
	if !parser.matchTokenTypes(false, LEFTBRACKET) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var colNames []string
	for {
		colName, ok := parser.parseIdentOrWord(false)
		if !ok {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		colNames = append(colNames, string(colName))
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, RIGHTBRACKET) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if !parser.matchTokenTypes(false, REFERENCES) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	tableName, ok := parser.parseIdentOrWord(false)
	if !ok {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if !parser.matchTokenTypes(false, LEFTBRACKET) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var keyNames []string
	for {
		keyName, ok := parser.parseIdentOrWord(false)
		if !ok {
			return nil, parser.MakeSyntaxError(1, parser.pos-1)
		}
		keyNames = append(keyNames, string(keyName))
		if !parser.matchTokenTypes(true, COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, RIGHTBRACKET) {
		return nil, parser.MakeSyntaxError(1, parser.pos-1)
	}
	deleteRefOption, updateRefOption := RefOptionRestrict, RefOptionRestrict
	if parser.matchTokenTypes(true, ON, DELETE) {
		deleteRefOption = parser.parseReferenceOption()
	}
	if parser.matchTokenTypes(true, ON, UPDATE) {
		updateRefOption = parser.parseReferenceOption()
	}
	return &ConstraintDefStm{
		Tp: ForeignKeyConstraintTp,
		Constraint: ForeignKeyConstraintDefStm{
			IndexName:       string(indexName),
			Cols:            colNames,
			RefTableName:    string(tableName),
			RefKeys:         keyNames,
			DeleteRefOption: deleteRefOption,
			UpdateRefOption: updateRefOption,
		},
	}, nil
}

func (parser *Parser) parseReferenceOption() ReferenceOptionTp {
	if parser.matchTokenTypes(true, RESTRICT) {
		return RefOptionRestrict
	}
	if parser.matchTokenTypes(true, CASCADE) {
		return RefOptionCascade
	}
	if parser.matchTokenTypes(true, SET, NULL) {
		return RefOptionSetNull
	}
	if parser.matchTokenTypes(true, NO, ACTION) {
		return RefOptionNoAction
	}
	if parser.matchTokenTypes(true, SET, DEFAULT) {
		return RefOptionSetDefault
	}
	return RefOptionRestrict
}
