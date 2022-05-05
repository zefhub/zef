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

__all__ = [
    "calc_time_from_date",
    "get_time_zone",
    "get_factory_default_time",
    "nice_date",
    "nice_time",
    "nice_duration",
    "time_in_graph_timezone",
]
    

import json
import re

from ...core import *
from ...ops import *

########################################################
# * Generic date->time 
#------------------------------------------------------
    
def get_time_zone(g):    
    return (g["Settings"] | now) >> RT.DefaultTimeZone | value


def get_factory_default_time(g):
    return (g["Settings"] | now) >> RT.FactoryDefaultTime | value


def calc_time_from_date(g, date):
    time_zone = get_time_zone(g)
    time_of_day = get_factory_default_time(g)
    m = re.match(r"(\d\d\d\d)(.)(\d\d)(\2)(\d\d)", date)
    if m is not None:
        date = '/'.join([m[1], m[3], m[5]])
        return Time(f"{date} {time_of_day}", time_zone)

    # TODO: This is assuming dates formatted as "DD-MM-YYYY" - not going to be true for every factory.
    assert time_zone in ["Asia/Jakarta", "Europe/London"]
    m = re.match(r"(\d\d)(.)(\d\d)(\2)(\d\d\d\d)", date)
    date = '/'.join([m[5], m[3], m[1]])
    return Time(f"{date} {time_of_day}", time_zone)
    



def nice_date(tt:Time):
    """ returns e.g. 'Mar 14' """
    d = {
        '01': 'Jan',
        '02': 'Feb',
        '03': 'Mar',
        '04': 'Apr',
        '05': 'May',
        '06': 'Jun',
        '07': 'Jul',
        '08': 'Aug',
        '09': 'Sep',
        '10': 'Oct',
        '11': 'Nov',
        '12': 'Dec',
    }
    spl = tt.date().split('-')
    return f"{d[spl[1]]} {spl[2]}"


def nice_time(tt:Time):
    """returns e.g. 23:10  (no seconds)"""
    return tt.time().rsplit(':', 1)[0]


def nice_duration(dur: QuantityFloat)->str:
    return str(int(dur.value / 3600)) + ' hrs   ' if dur > 2*hours else str(int(dur.value / 60)) + ' min   '


def time_in_graph_timezone(t : Time, g : Graph):
    tz_str = get_time_zone(g)
    import pytz
    tz = pytz.timezone(tz_str)
    from datetime import datetime
    return (datetime.fromtimestamp(t.seconds_since_1970)
            .astimezone(tz) \
            .strftime(f"%Y-%m-%d %H:%M:%S (%z)"))
