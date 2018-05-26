class Oper(object):
    def say_hi(self, name):
        print('Hello,', name)
        
    def say_bye(self, name):
        print('GoodBye,', name)
        
    def default(self, name):
        print('This operation is not supported')
        
if __name__ == '__main__':
    operation = Oper()
    command, argument = raw_input('>').split()
    getattr(operation, command, operation.default)(argument)
        
# f-string
# partition method
# youtube-dl        
# money class with different currency convert __str__,__add__ dunder
# agithub magic method functools partial base.py getattr alias
# context manager, feature flags __enter__,__exit__ contextlib 
# freezegun source
# NamedTuple