from ..core._ops import value
from ..core import func

@func
def value_or(x, fallback):
    if x is None:
        return fallback
    else:
        return value(x)
maybe_value = value_or[None]
