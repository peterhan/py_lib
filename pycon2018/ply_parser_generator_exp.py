from ply.lex import lex
tokens = 'ID,NUM,PLUS,TIMES,EQ,LPAREN,RPAREN,PRINT'.split(',')

t_ID=r'[A-z][A-z0-9_]*'
# t_NUM=r'[0-9]+'
t_PLUS=r'\+'
t_TIMES=r'\*'
t_EQ=r'\='
t_LPAREN=r'\('
t_RPAREN=r'\)'

def t_NUM(t):
    r'[0-9]+'
    t.value = int(t.value)
    return t

def t_error(t):
    print('Illegal character ',t.value[0])
    t.lexer.skip(1)
    
t_ignore=' \t'

lexer=lex()
lexer.input('a= 3 + 4 +5')
print '\n'.join([str(l) for l in lexer])
# print ''

from ply.yacc import yacc

def p_statements_multiple(p):
    '''
    statements : statements statement
    '''
    
def p_statement_single(p):
    '''
    statements : statement
    '''
    
def p_assignment_statement(p):
    '''
    statement : ID EQ expr
    '''
    print p
    
# def p_condition_statement(p):
    # '''
    # statement : ID EQ EQ expr
    # '''
    
def p_print_statement(p):
    '''
    statement : PRINT LPAREN expr RPAREN
    '''
    
def p_expr_binop(p):
    '''
    expr : expr PLUS expr
         | expr TIMES expr    
    '''
    
def p_expr_num_p(p):
    '''
    expr : NUM
    '''
    
def p_expr_name(p):
    '''
    expr : ID
    '''
    
def p_expr_group(p):
    '''
    expr : LPAREN expr RPAREN
    '''
    
parser = yacc()
parser.parse('a = 3*4+5')
parser.parse('a=3')