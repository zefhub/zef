# Copyright 2022 Synchronous Technologies Pte Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class Decimal_:
    def __init__(self, val, decimal_positions):
        if not isinstance(decimal_positions, int):
            raise ValueError(f"in constructing a decimal: decimal_positions must be an int. got {type(decimal_positions)}")
        
        if isinstance(val, float):
            self.val_int_encoded = round(val * 10**decimal_positions)
            self.decimal_positions = decimal_positions
        elif isinstance(val, str):
            pos = val.find('.')
            pre = val if pos==-1 else val[:pos]
            post = (f"{{:0<{decimal_positions}}}").format('' if pos==-1 else val[pos+1:pos+decimal_positions+1])
            # check whether we need to round up
            m = decimal_positions+2
            postpost = val[m:m+1]
            rounding_term = 1 if len(postpost)==1 and int(postpost)>4 else 0
            self.val_int_encoded = int(pre+post) + rounding_term
            self.decimal_positions = decimal_positions
        else:
            raise ValueError(f"Can't construct a Decimal from a {type(val)}")

    def __repr__(self):
        # correct for repr output if decimal starts with '0': pad str repr of int with zeros explicitly        
        s=(f"{{:0>{self.decimal_positions}}}").format(str(self.val_int_encoded))
        return f"Decimal({s[:-self.decimal_positions]}.{s[-self.decimal_positions:]}, {self.decimal_positions})"

    def __str__(self):
        s=(f"{{:0>{self.decimal_positions}}}").format(str(self.val_int_encoded))
        return f"{s[:-self.decimal_positions]}.{s[-self.decimal_positions:]}"

    def __float__(self):
        return float(self.val_int_encoded / 10**self.decimal_positions)
    
    def __add__(self, other):
        if isinstance(other, int):
            return Decimal_((self.val_int_encoded+ other * 10**self.decimal_positions)/10**self.decimal_positions , self.decimal_positions)
        elif isinstance(other, float):
            return Decimal_((self.val_int_encoded + round(other * 10**self.decimal_positions)) /10**self.decimal_positions, self.decimal_positions)
        elif isinstance(other, Decimal_):
            """
            Use the higher decimal precision of the two operands for the output
            """
            # pos_max = max(self.decimal_positions, other.decimal_positions)
            # pos_min = min(self.decimal_positions, other.decimal_positions)
            
            # return Decimal_((self.val_int_encoded + round(other.val_int_encoded * 10**self.decimal_positions)) 
            #  /10**pos_max, pos_max)
            raise NotImplementedError
        else:
            raise TypeError
    
    def __radd__(self, other):
        return self+other
    
    def __sub__(self, other):
        return self+ (-1*other)
    
    def __rsub__(self, other):
        # :(  very mutaty to avoid unnecessary multiplications and divisions
        tmp = Decimal_(0.0, self.decimal_positions)
        tmp.val_int_encoded = -self.val_int_encoded
        return tmp + other

    def __neg__(self):
        """
        allow writing "-my_decimal"
        """
        tmp = Decimal_(0.0, self.decimal_positions)
        tmp.val_int_encoded = -self.val_int_encoded
        return tmp

    def __pos__(self):
        """
        allow writing "+my_decimal"
        """
        return self
    
    def __mul__(self, x):
        if not isinstance(x, int):
            raise NotImplementedError('You can only multiply Decimal with integers for now')
        tmp = Decimal_(0.0, self.decimal_positions)
        tmp.val_int_encoded = x*self.val_int_encoded
        return tmp

    def __rmul__(self, x):
        return self*x

