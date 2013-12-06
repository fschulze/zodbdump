from ZODB import serialize
import pickle


class Unpickler(pickle.Unpickler):
    def load_reduce(self):
        stack = self.stack
        args = stack.pop()
        func = stack[-1]
        if args is None:
            value = func()
        else:
            value = func(*args)
        stack[-1] = value

    def find_class(self, modulename, name):
        return self.find_global(modulename, name)

Unpickler.dispatch[pickle.REDUCE] = Unpickler.load_reduce


def patch_zodb():
    if serialize.cPickle is not pickle:
        serialize.cPickle = pickle
    if pickle.Unpickler is not Unpickler:
        pickle.Unpickler = Unpickler
