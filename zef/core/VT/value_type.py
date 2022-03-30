class ValueType:
    def __init__(self, type_name: str, nesting_order: int, nested_types: tuple = ()):
        self.d = (type_name, nesting_order, nested_types) 
    
    def to_str(self, t: tuple)->str:
        """helper function for the repr: nice output. We want to 
        call recursively on a tuple, not a ValueType object"""
        nested_types = ",".join(self.to_str(n.d) for n in t[2])
        return f"{t[0]}{'' if t[2] == () else f'[{nested_types}]'}"
    
    def __repr__(self):
        return self.to_str(self.d)

    def nested(self):
        if len(self.d[2]) == 0:
            raise ValueError(f"{self.d[0]} doesn't have any nested values!")
        elif len(self.d[2]) == 1:
            return self.d[2][0]
        else:
            return self.d[2]
    
    def __getitem__(self, x):
        if self.d[1] == 0:
            raise ValueError(f"Nothing can be nested inside a ZefType \"{self.d[0]}\"")
        if self.d[1] == 1:
            return ValueType(self.d[0], self.d[1], (ValueType(*x.d), ))
        if self.d[1] == 2:
            return ValueType(self.d[0], self.d[1], (ValueType(*x[0].d), ValueType(*x[1].d)))

        raise NotImplementedError(f"Nesting level {self.d[1]} isn't handled in __getitem__")
        
    def __call__(self, *args, **kwargs):

        if self.d == ('String', 0, ()):
            assert len(args)==1
            return str(args[0])

        if self.d == ('Int', 0, ()):
            assert len(args)==1
            return int(args[0])

        if self.d == ('Float', 0, ()):
            assert len(args)==1
            return float(args[0])

        if self.d == ('Bool', 0, ()):
            assert len(args)==1
            # don't automatically determine truthiness, e.g. Bool(79) should return an Error
            if args[0] == 0: return False
            if args[0] == 1: return True
            raise TypeError(f"Bool({args[0]}) called. Only 0 and 1 are automatically converted to Bool in Zef. Please be more specific.")

        print(f">>> ValueType call triggered with {args=} {kwargs=}")
        print(f"{self.d=}")
        raise Exception(f"Cannot call ValueType().")
        

