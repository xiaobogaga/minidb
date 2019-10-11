package parser

import (
	"simpleDb/ast"
	"simpleDb/lexer"
)

const (
	WrongFuncationCallFormatErr = ParseError("Wrong Function Call Format")
	WrongOperatorFormatErr      = ParseError("wrong operator format err")
	WrongExpressionFormat       = ParseError("wrong expression format")
)

func (parser *Parser) resolveExpression(allowOneOperator bool) (ast.Stm, error) {
	// Expression is like [(expression)] | ope [Operation] [(Expression)|(ope)]
	// defer log.DebugF("resolveExpression: stm: %+v, err: %v, parser pos: %d\n", stm, err, parser.pos)
	stms := new(ast.ExpressionStm)
	for {
		if parser.matchTokenType(lexer.LEFTBRACKET, true) {
			stm, err := parser.resolveExpression(allowOneOperator)
			if err != nil {
				return nil, err
			}
			if !parser.matchTokenType(lexer.RIGHTBRACKET, false) {
				return nil, WrongExpressionFormat
			}
			stms.Append(stm)
			operation, ret := parser.resolveOperationStm(true)
			if !ret {
				break
			}
			stms.Append(&operation)
		} else {
			operator, err := parser.resolveOperator()
			if err != nil {
				break
			}
			stms.Append(operator)
			operation, ret := parser.resolveOperationStm(true)
			if !ret {
				break
			}
			stms.Append(&operation)
		}
	}
	if len(stms.Params) == 0 || (!allowOneOperator && len(stms.Params) == 1) {
		return nil, WrongExpressionFormat
	}
	if allowOneOperator && len(stms.Params) == 1 {
		return stms.Params[0], nil
	}
	return stms, nil
}

func (parser *Parser) resolveOperator() (ast.Stm, error) {
	// Its a Operator
	// defer log.DebugF("resolveOperator: ope: %+v, err: %v, parser pos: %d\n", ope, err, parser.pos)
	name, ret := parser.parseIdentOrWord(true)
	if !ret {
		valueStm, err := parser.parseValue(false)
		if err != nil {
			return nil, err
		}
		return &valueStm, nil
	}
	if parser.matchTokenType(lexer.LEFTBRACKET, true) {
		// Must be a function call
		parser.pos -= 2
		funcallStm, err := parser.resolveFunctionCall(false)
		if err != nil {
			return nil, WrongOperatorFormatErr
		}
		return funcallStm, nil
	}
	stm := ast.ColumnRefStm(name)
	return &stm, nil
}

func (parser *Parser) resolveFunctionCall(ifNotRollback bool) (*ast.FunctionCallStm, error) {
	// FunctionName(expression[,expression])
	// defer log.DebugF("resolveFunctionCall: stm: %+v, err: %v, parser pos: %d\n", stm, err, parser.pos)
	funcName, ret := parser.parseIdentOrWord(ifNotRollback)
	if !ret || !parser.matchTokenType(lexer.LEFTBRACKET, ifNotRollback) {
		return nil, WrongFuncationCallFormatErr
	}
	var params []ast.Stm
	for {
		paramExpression, err := parser.resolveExpression(true)
		if err != nil {
			return nil, err
		}
		params = append(params, paramExpression)
		if !parser.matchTokenType(lexer.COMMA, true) {
			break
		}
	}
	if !parser.matchTokenType(lexer.RIGHTBRACKET, false) {
		return nil, WrongFuncationCallFormatErr
	}
	stm := &ast.FunctionCallStm{FuncName: funcName, Params: params}
	return stm, nil
}

func (parser *Parser) resolveOperationStm(ifNotRollback bool) (ast.OperationStm, bool) {
	// defer log.DebugF("resolveOperationStm: tokenTp: %v, err: %v, parser pos: %d\n", t, ret, parser.pos)
	if !parser.hasNext() {
		if ifNotRollback {
			parser.pos--
		}
		return -1, false
	}
	token := parser.getToken()
	switch token.Tp {
	case lexer.ASSIGNEQUAL, lexer.CHECKEQUAL, lexer.NOTEQUAL, lexer.LESSEQUAL, lexer.LESS, lexer.GREATEQUAL, lexer.GREAT, lexer.ADD, lexer.MINUS,
		lexer.DIVIDE, lexer.STAR, lexer.AND, lexer.OR:
		return ast.OperationStm(token.Tp), true
	}
	if ifNotRollback {
		parser.pos--
	}
	return -1, false
}
