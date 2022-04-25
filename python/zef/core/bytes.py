class Bytes:
    def __init__(self, data):
        import string
        if isinstance(data, bytes):
            self.data = data
            self.is_hex = False
        elif isinstance(data, str) and all(c in string.hexdigits for c in data):
            self.data = data
            self.is_hex = True
        else:
            raise NotImplementedError
    
    def __repr__(self):
        if self.is_hex: return f'Bytes("{self.data})"'
        else: return f'Bytes("{self.data.hex()}")'

    def __str__(self):
        if self.is_hex: return self.data
        else: return self.data.hex()