from ..pyzef.main import *
from ..pyzef import main, zefops, internals
from ._core import ET, AtomicEntityType

main.ZefRef.__hash__ = lambda self: hash(((index(self)), index(self | zefops.tx)))
main.EZefRef.__hash__ = lambda self: index(self)

main.QuantityFloat.__hash__ = lambda self: hash((self.value, self.unit))
main.QuantityInt.__hash__ = lambda self: hash((self.value, self.unit))
main.Time.__hash__ = lambda self: hash(self.seconds_since_1970)

internals.Delegate.__hash__ = lambda self: hash((self.order, self.item))
internals.DelegateEntity.__hash__ = lambda self: hash(self.et)
internals.DelegateAtomicEntity.__hash__ = lambda self: hash(self.aet)
internals.DelegateRelationGroup.__hash__ = lambda self: hash(self.rt)
internals.DelegateRelationTriple.__hash__ = lambda self: hash((self.rt, self.source, self.target))



# FIXME: why do we need the standard list of timezones in the docstring for Time? Should just refer people to the TZ database. I have removed it for now.

# ------------------------- patch Time constructor to allow create of zef Time objects both from time stamps and readable strings ------------------------
from datetime import datetime, timezone, timedelta
import dateparser
import pytz   # for time zones

def create_monkey_patched_Time_ctor(old_init_fct):
    def monkey_patched_Time_ctor(self, x, timezone: str = 'Asia/Singapore'):
        """ x could be a time stamp (float/int) or a string to be parsed
        Note: the python library date parser parses dates of the following form UNINTUITIVELY, i.e. it assumes the MDY convention:  
            '9.2.2015 13:59' is read as Sep. 2. 2015
            Hence: ALWAYS stick to the convention of transforming strings to the form Time('2019-07-15 13:59', timezone='Europe/London')

            Other formats work, e.g.
            Time('9. Feb 2015 13:59')
            Time('Martes 21 de Octubre de 2014 13:23:12')    # various languages
            Time('1 เดือนตุลาคม 2005, 1:00 AM')    # Thai
            Time('1 hour ago')         # relative dates
            Time('now')

        The time zone can be specified as a string: 
            t0 = Time('2019-07-15 13:59', timezone='Europe/Berlin')
        If no time zone is specified, Singapore is assumed as a default for now - this will be cleaned up in the future.
        

        Valid timezones: see TZ database.
        """
        if(isinstance(x, float)):
            self = old_init_fct(self, x)
        elif(isinstance(x, int)):
            self = old_init_fct(self, float(x))
        elif(isinstance(x, zefops.Now)):
            self = old_init_fct(self, x)
        elif(isinstance(x, str)):
            try:
                my_timestamp = datetime.timestamp(dateparser.parse(x, settings={'TIMEZONE': timezone, 'RETURN_AS_TIMEZONE_AWARE': True}))
                self = old_init_fct(self, my_timestamp)
            except:
                raise ValueError(f"Could not parse the following string as a valid datetime: \"{x}\"")
        else:
            raise ValueError('Invalid argument type passed to Time constructor')
    return monkey_patched_Time_ctor
main.Time.__init__ = create_monkey_patched_Time_ctor(Time.__init__)


def _to_date_time(self: Time, timezone:str='Asia/Singapore', str_format: str='%Y-%m-%d %H:%M:%S'):
    return datetime.fromtimestamp(self.seconds_since_1970)\
            .astimezone(pytz.timezone(timezone))\
            .strftime(str_format)

def _to_time(self: Time, timezone:str='Asia/Singapore'):
    return _to_date_time(self, timezone, str_format='%H:%M:%S')

def _to_date(self: Time, timezone:str='Asia/Singapore'):
    return _to_date_time(self, timezone, str_format='%Y-%m-%d')



main.Time.date_time = _to_date_time
main.Time.time = _to_time
main.Time.date = _to_date


def _repr_for_zef_time(self):
    return datetime.fromtimestamp(self.seconds_since_1970)\
                .astimezone(timezone(timedelta(hours=8)))\
                .strftime(f"<Time %Y-%m-%d %H:%M:%S (+0800)>")
def _str_for_zef_time(self):
    return datetime.fromtimestamp(self.seconds_since_1970)\
                .astimezone(timezone(timedelta(hours=8)))\
                .strftime(f"%Y-%m-%d %H:%M:%S (+0800)")
main.Time.__repr__ = _repr_for_zef_time
main.Time.__str__ = _str_for_zef_time


def help(arg):
    import builtins
    from . import ZefRef
    from ..zefops.base import L, value
    if isinstance(arg, ZefRef) and arg | BT == BT.ENTITY_NODE and arg | ET == ET.ZEF_Function:
        if len(arg >> L[RT.DocString]) == 1:
            print((arg >> RT.DocString) | value.String)
        else:
            print("This ZEF_Function doesn't have any DocString")
    else:
        print(builtins.help(arg))



# Temp fix fow now: these operators should be overloaded in C++ and return a new C++ type.
def _rae_get_item(self, n):
    """
    we want to write  RT.UsedBy['some internal id']. 
    Same for ETs, AETs. Monkeypatch that from Python for now.
    """
    from ._ops import instantiated
    # return {'RAE': self, 'capture': n}
    return instantiated[self][n]


main.RelationType.__getitem__ = _rae_get_item
main.EntityType.__getitem__ = _rae_get_item
internals.AtomicEntityType.__getitem__ = _rae_get_item
main.QuantityFloat.__getitem__ = _rae_get_item
main.QuantityInt.__getitem__ = _rae_get_item



# we want to allow for e.g. "AET.Float <= 42.1"
# return a ZefOp to use as a container here.
# We used a dict previously, but then we could not 
# catch the case "AET.Float['abc'] <= 42.1"
# For consistency, we use Lazy ZefOps as intermediate containers
def leq_monkey_patching_ae(self, other):
    from ._ops import assign_value, instantiated, LazyValue
    return LazyValue(instantiated[self]) | assign_value[other]
    
AtomicEntityType.__le__ = leq_monkey_patching_ae


# Pretty printing for ZefRefs
def pprint_ZefRefs(self, p, cycle):
    p.text(str(self))
    p.text(" [\n")

    N_max = 10
    if len(self) > N_max:
        # Split into first part and last part
        N = N_max // 2
        first = [self[i] for i in range(N)]
        last = [self[i] for i in range(len(self)-N, len(self)-1)]
        p.text('\n'.join("\t"+str(x) for x in first))
        p.text('\n\t...\n')
        p.text('\n'.join("\t"+str(x) for x in last))
    else:
        p.text('\n'.join("\t"+str(x) for x in self))
    p.text(']')

main.ZefRefs._repr_pretty_ = pprint_ZefRefs
main.EZefRefs._repr_pretty_ = pprint_ZefRefs

def convert_to_assign_value(self, value):
    from ._ops import assign_value
    return self | assign_value[value]
ZefRef.__le__ = convert_to_assign_value
EZefRef.__le__ = convert_to_assign_value

def add_internal_id(self, internal_id):
    from ._ops import merged
    return merged[self][internal_id]
ZefRef.__getitem__ = add_internal_id
EZefRef.__getitem__ = add_internal_id