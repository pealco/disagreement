from functools import partial

class _compfunc(partial):
    def __add__(self, y):
        def composition(*args, **kwargs):
            return y(*self.func(*args, **kwargs))
        return _compfunc(composition)

def composable(f):
    return _compfunc(fail_gracefully(f))
    
def compose(functions):
    return reduce(lambda x, y: x + y, functions)

def fail_gracefully(func):
    def wrapper(*args):
        if args[0] == (0, False):
            return (0, False)
        else:
            return func(*args)
    return wrapper