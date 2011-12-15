from functools import partial

class _compfunc(partial):
    def __add__(self, y):
        f = lambda *args, **kwargs: y(self.func(*args, **kwargs)) 
        return _compfunc(f)

def composable(f):
    return _compfunc(fail_gracefully(f))
    
def compose(functions):
    return reduce(lambda x, y: x + y, functions)

def fail_gracefully(func):
    def wrapper(*args):
        if args[0] is False:
            return False
        else:
            return False
    return wrapper