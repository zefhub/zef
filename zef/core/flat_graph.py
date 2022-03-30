from ._ops import *
class FlatGraph:
    def __init__(self, *args):
        if args == ():
            self.key_dict = {}
            self.blobs = ()
        else:
            raise NotImplementedError("FlatGraph with args")

    def __repr__(self):
        kdict = "\n".join([f"({k}=>{v})" for k,v in self.key_dict.items()])
        blobs = "\n".join([str(e) for e in self.blobs])
        return f'FlatGraph(\n{kdict}\n-------\n{blobs}\n)'
    
    def __or__(self, other):
        return LazyValue(self) | other

    def __getitem__(self, key):
        return get(self, key)


class FlatRef:
    def __init__(self, fg, idx):
        self.fg = fg
        self.idx = idx
    def __repr__(self):
        return f'<FlatRef #{abs(self.idx)} {repr(self.fg.blobs[self.idx][1])}>'
    
    def __or__(self, other):
        return LazyValue(self) | other

    def __gt__(self, other):
        return LazyValue(self) > other

    def __lt__(self, other):
        return LazyValue(self) < other

    def __lshift__(self, other):
        return LazyValue(self) << other
    
    def __rshift__(self, other):
        return LazyValue(self) >> other

class FlatRefs:
    def __init__(self, fg, idxs):
        self.fg = fg
        self.idxs = idxs
    def __repr__(self):
        newline = '\n'
        return f"""<FlatRefs len={len(self.idxs)}> [
{["    " + repr(FlatRef(self.fg, i)) for i in self.idxs] | join[newline] | collect}
]"""
    
    def __or__(self, other):
        return LazyValue(self) | other

    def __iter__(self):
        return (FlatRef(self.fg, i) for i in self.idxs)

    def __gt__(self, other):
        return LazyValue(self) > other

    def __lt__(self, other):
        return LazyValue(self) < other

    def __lshift__(self, other):
        return LazyValue(self) << other
    
    def __rshift__(self, other):
        return LazyValue(self) >> other