class _ErrorType():
    def __set_name__(self, parent, name):
        self.name = name

    def __call__(self, *args):
        err = _ErrorType()
        err.name = self.name
        err.args = args
        return err

    def __repr__(self):
        if not self.args or len(self.args) == 0: args = "()"
        elif len(self.args) == 1: args = f"({repr(self.args[0])})"
        else: args = self.args
        return f'{self.name}{args}'

    def __eq__(self, other):
        if not isinstance(other, _ErrorType): return False
        return self.name == other.name and self.args == other.args
    

class _Error:
    TypeError    = _ErrorType()
    RuntimeError = _ErrorType()
    ValueError   = _ErrorType()
    NotImplementedError = _ErrorType()
    BasicError = _ErrorType()

    def __call__(self, *args):
        return self.BasicError(*args)

    def __repr__(self):
        return f'Error'



Error = _Error()

