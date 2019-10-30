package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

var emptyConstraintDefStm = ast.ConstraintDefStm{}

// A constraint statement is like:
// * [Constraint] primary key (col_name [,col_name...)
// * [Constraint] unique {index|key} [index_name] (col_name [,col_name...)
// * [Constraint] foreign key [index_name] (col_name [,col_name...) references tb_name (key...) [on delete reference_option] [on update reference_option]
//    reference_option is like: {restrict | cascade | set null | no action | set default} and default is restrict
// Restrict is the default

// parseConstraintDef parse a constraint definition and return it.
func (parser *Parser) parseConstraintDef() (ast.ConstraintDefStm, error) {
	parser.matchTokenTypes(true, lexer.CONSTRAINT)
	token, ok := parser.NextToken()
	if !ok {
		return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos)
	}
	switch token.Tp {
	case lexer.PRIMARY:
		return parser.parsePrimaryKeyDef()
	case lexer.UNIQUE:
		return parser.parseUniqueKeyDef()
	case lexer.FOREIGN:
		return parser.parseForeignKeyDef()
	}
	return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos)
}

// * [Constraint] primary key (col_name [,col_name...)
func (parser *Parser) parsePrimaryKeyDef() (ast.ConstraintDefStm, error) {
	if !parser.matchTokenTypes(false, lexer.KEY) {
		return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if !parser.matchTokenTypes(false, lexer.LEFTBRACKET) {
		return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var colNames []string
	for {
		colName, ok := parser.parseIdentOrWord(false)
		if !ok {
			return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos-1)
		}
		colNames = append(colNames, string(colName))
		if !parser.matchTokenTypes(true, lexer.COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET) {
		return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return ast.ConstraintDefStm{
		Tp:         ast.PrimaryKeyConstraintTp,
		Constraint: ast.PrimaryKeyDefStm{ColNames: colNames},
	}, nil
}

// * [Constraint] unique {index|key} index_name (col_name [,col_name...)
func (parser *Parser) parseUniqueKeyDef() (ast.ConstraintDefStm, error) {
	if !parser.matchTokenTypes(true, lexer.INDEX) && !parser.matchTokenTypes(true, lexer.KEY) {
		return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos)
	}
	indexName, _ := parser.parseIdentOrWord(true)
	if !parser.matchTokenTypes(false, lexer.LEFTBRACKET) {
		return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var colNames []string
	for {
		colName, ok := parser.parseIdentOrWord(false)
		if !ok {
			return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos-1)
		}
		colNames = append(colNames, string(colName))
		if !parser.matchTokenTypes(true, lexer.COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET) {
		return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	return ast.ConstraintDefStm{
		Tp:         ast.UniqueKeyConstraintTp,
		Constraint: ast.UniqueKeyDefStm{IndexName: string(indexName), ColNames: colNames},
	}, nil
}

// * [Constraint] foreign key [index_name] (col_name [,col_name...) references tb_name (key...) [on delete reference_option] [on update reference_option]
//   reference_option is like: {restrict | cascade | set null | no action | set default}, and restrict is default.
func (parser *Parser) parseForeignKeyDef() (ast.ConstraintDefStm, error) {
	if !parser.matchTokenTypes(true, lexer.KEY) {
		return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos)
	}
	indexName, _ := parser.parseIdentOrWord(true)
	if !parser.matchTokenTypes(false, lexer.LEFTBRACKET) {
		return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var colNames []string
	for {
		colName, ok := parser.parseIdentOrWord(false)
		if !ok {
			return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos-1)
		}
		colNames = append(colNames, string(colName))
		if !parser.matchTokenTypes(true, lexer.COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET) {
		return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if !parser.matchTokenTypes(false, lexer.REFERENCES) {
		return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	tableName, ok := parser.parseIdentOrWord(false)
	if !ok {
		return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	if !parser.matchTokenTypes(false, lexer.LEFTBRACKET) {
		return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	var keyNames []string
	for {
		keyName, ok := parser.parseIdentOrWord(false)
		if !ok {
			return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos-1)
		}
		keyNames = append(keyNames, string(keyName))
		if !parser.matchTokenTypes(true, lexer.COMMA) {
			break
		}
	}
	if !parser.matchTokenTypes(false, lexer.RIGHTBRACKET) {
		return emptyConstraintDefStm, parser.MakeSyntaxError(1, parser.pos-1)
	}
	deleteRefOption, updateRefOption := ast.RefOptionRestrict, ast.RefOptionRestrict
	if parser.matchTokenTypes(true, lexer.ON, lexer.DELETE) {
		deleteRefOption = parser.parseReferenceOption()
	}
	if parser.matchTokenTypes(true, lexer.ON, lexer.UPDATE) {
		updateRefOption = parser.parseReferenceOption()
	}
	return ast.ConstraintDefStm{
		Tp: ast.ForeignKeyConstraintTp,
		Constraint: ast.ForeignKeyConstraintDefStm{
			IndexName:       string(indexName),
			Cols:            colNames,
			RefTableName:    string(tableName),
			RefKeys:         keyNames,
			DeleteRefOption: deleteRefOption,
			UpdateRefOption: updateRefOption,
		},
	}, nil
}

func (parser *Parser) parseReferenceOption() ast.ReferenceOptionTp {
	if parser.matchTokenTypes(true, lexer.RESTRICT) {
		return ast.RefOptionRestrict
	}
	if parser.matchTokenTypes(true, lexer.CASCADE) {
		return ast.RefOptionCascade
	}
	if parser.matchTokenTypes(true, lexer.SET, lexer.NULL) {
		return ast.RefOptionSetNull
	}
	if parser.matchTokenTypes(true, lexer.NO, lexer.ACTION) {
		return ast.RefOptionNoAction
	}
	if parser.matchTokenTypes(true, lexer.SET, lexer.DEFAULT) {
		return ast.RefOptionSetDefault
	}
	return ast.RefOptionRestrict
}
