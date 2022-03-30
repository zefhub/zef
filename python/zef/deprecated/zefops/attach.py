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

from ...core import *
from ...pyzef.zefops import assign_value, now, L, first

def auto_type_from_val(val):
    if type(val) == str:
        return AET.String
    elif type(val) == int:
        return AET.Int
    elif type(val) == bool:
        return AET.Bool
    elif type(val) == float:
        return AET.Float
    elif type(val) == ZefEnumValue:
        return getattr(AET.Enum, val.enum_type)
    elif type(val) == QuantityInt:
        return getattr(AET.QuantityInt, val.unit.enum_value)
    elif type(val) == QuantityFloat:
        return getattr(AET.QuantityFloat, val.unit.enum_value)
    elif type(val) == Time:
        return AET.Time
    elif type(val) == ZefRef or type(val) == EZefRef:
        return ZefRef
    else:
        raise TypeError(f"Unknown type for property: {type(val)}, {val}")

class Attach():    
    def __init__(self, args=()):
        # future API:
        #     my_machines | attach[RT.Age, [6,1,9,4]]                    # one ZefRef, multiple fields
        #     my_machine | attach[ [(RT.Age, 54), (RT.Color, 'red')] ]   # multiple args per ZefRef       

        # Note: fields is stored, after parsing, as a list of 3 items, rt,aet,val
        # If val is not given, then it is set to None.
        # As a special case, an aet of ZefRef (the type not an instance) means create a link only.

        if args==():
            self.fields = []
            return

        if type(args) == list:
            fields = args
        elif len(args) >= 2 and len(args) <= 3 and isinstance(args[0], RelationType):
            fields = [args]
            pass
        else:
            raise ValueError("Incorrect use of attach. Should be either an RT, or an RT/val pair, or a list.")

        parsed = []
        for field in fields:
            assert len(field) >= 2 and len(field) <= 3 and isinstance(field[0], RelationType)
            rt = field[0]

            def is_compatible_type(val, ae_type)->bool:            
                return auto_type_from_val(val) == ae_type
        
            # ----- two additional args -----
            if len(field) == 3:
                aet,val = field[1:]

                assert isinstance(aet, AtomicEntityType)
                if not auto_type_from_val(val) == aet:
                    raise TypeError(f'in attach: the type of the specified value "{args[1]}" does not match the specified type "{args[0]}"')
                parsed.append( (rt, aet, val) )
            else:
                # ----- one additional args -----
                item = field[1]
                if isinstance(item, AtomicEntityType):
                    parsed.append( (rt, item, None) )
                else:
                    parsed.append( (rt, auto_type_from_val(item), item) )
        self.fields = parsed

    def __getitem__(self, args):  
        # if we pass multiple args via [], they are always grouped into a single tuple assigned to the first arg after self        
        return type(self)(args)
    
    def __call__(self, z):
        g = Graph(z)
        with Transaction(g):
            for field in self.fields:
                rt,aet,val = field
                # Need "is" here to avoid failing to find a == method.
                if aet is ZefRef:
                    assert val is not None
                    instantiate(z, rt, val, g)            
                else:
                    my_ae = instantiate(aet, g)
                    if val is not None:
                        assign_value(my_ae, val)
                    instantiate(z, rt, my_ae, g)            
        return z | now   # TODO: confirm that projecting to the latest time slice makes sense here
    
    def __ror__(self, z):
        return self(z)

attach = Attach()


class FillOrAttach(Attach):
    """
    Check whether a field exists.
        a) if it does, assign the value to the target AET
        b) if it does not, instantiate the field and assign the value to the target AET
        
    smaple:
        z_person | fill_or_attach[RT.Age, 42]
    """
    def __init__(self, args=()):
        super().__init__(args)

        aet_rts = []
        for field in self.fields:
            rt,aet,val = field
            # Need "is" here to avoid failing to find a == method.
            if aet is not ZefRef:
                aet_rts.append(rt)

        import collections
        counts = collections.Counter(aet_rts)
        if any(count > 1 for count in counts.values()):
            raise TypeError("Can't have multiple RTs for an AET in fill_or_attach")
        
    def __call__(self, z):
        z = now(z)
        g = Graph(z)
        with Transaction(g):
            for field in self.fields:
                rt,aet,val = field
                # Need "is" here to avoid failing to find a == method.
                if aet is ZefRef:
                    # Always append new ZefRefs, these can never be filled.
                    assert val is not None
                    instantiate(z, rt, val, g)            
                else:
                    opts = z >> L[rt]
                    if len(opts) == 0:
                        my_ae = instantiate(aet, g)
                        if val is not None:
                            assign_value(my_ae, val)
                        instantiate(z, rt, my_ae, g)            
                    else:
                        if val is not None:
                            if len(opts) > 1:
                                raise Exception(f"More than one RT found for {rt} in fill_or_attach, going to assign to the first RT found only.")
                            assign_value(opts | first, val)
        return z | now   # TODO: confirm that projecting to the latest time slice makes sense here
    
    def __ror__(self, z):
        return self(z)

fill_or_attach = FillOrAttach()
