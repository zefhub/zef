from .fx_types import _Effect_Class
from ...pyzef.internals import login, logout

def zefhub_login_handler(eff: _Effect_Class):
    login()
    return {}

def zefhub_logout_handler(eff: _Effect_Class):
    logout()
    print("Warning, after logging out all graphs will have been unloaded. Any accesses of a ZefRef referencing these old graphs will cause a segmentation fault!")
    return {}