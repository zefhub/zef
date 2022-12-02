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


from .. import *
from .._ops import *
from ...ui import *


event = {"type": "Request", "path": "/orders", "time": "2022-11-05 14:12:03  +0800"}

def random_time():
    import datetime
    import random
    return (datetime.datetime(2022, 11, 6) + random.random() * datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")


def make_request_event(x):
    import random
    path = ["/orders", "/customers", "/gql"]
    return  {"type": "Request", "path": path[random.randint(0,2)], "time":random_time()}

range(100) | map[make_request_event] | collect



# a ascii historgam with a frame showing web server traffic. random data
s2 = """
┌── HTTP Server Traffic ─────────────────────────────────────────┐
│                                                                │
│                                                                │
|  /orders                                                       |
|  ▅▅▅▅▄▄▆▆▆▆▁▁▅▅▄▄▅▅▆▆▄▄▅▅▅▅▃▃▂▂▃▃▆▆▃▃▆▆▅▅▆▆▃▃██▅▅              |
|  /customers                                                    |
|  ▄▄▃▃▂▂▃▃▄▄▄▄▂▂▄▄▄▄▆▆▄▄██▁▁▃▃▄▄▃▃▄▄▅▅▄▄▂▂▄▄▂▂▃▃▇▇              |
|  /gql                                                          |
|  ▂▂▃▃▄▄▃▃▁▁▅▅▂▂▂▂▅▅▆▆▅▅▄▄▅▅▅▅▂▂▃▃▄▄▇▇▅▅██▃▃▅▅▄▄▄▄              |
|  🕰️ ──┬───────────────────┬────────────────────────┬──►        |
|  0        4        8        12        16        20             |
│                                                                │
│                                                                │
└────────────────────────────────────────────────────────────────┘
"""


# {"type": "Request", "path": "/orders", "time": "2022-11-05 14:12:03"},
events = range(1000) | map[make_request_event] | collect


# TODO fix time scale
@func
def histogram(data, group_key, x_key, x_range):
    spacing = 2
    histogram_blocks = ["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"]
    t_min, t_max = 0, len(histogram_blocks) - 1

    @func
    def normalize(m,r_min,r_max,t_min,t_max):
        return round((m-r_min)/(r_max-r_min)*(t_max-t_min)+t_min)

    key_to_size_groups = data | map[x_key] | group_by[group_key ] | map[lambda t: (t[0], len(t[1]))] | collect

    sorted_groups = key_to_size_groups | sort[second] | collect
    r_min, r_max = sorted_groups[0][1], sorted_groups[-1][1]
    
    key_to_size_d = dict(key_to_size_groups)

    normalized_blocks = x_range | map[lambda i_e: key_to_size_d.get(i_e, 0) | normalize[r_min][r_max][t_min][t_max] | collect] | map[lambda idx: histogram_blocks[idx] * spacing] | collect 
    return (normalized_blocks | join | collect)

@func
def process_request_events(events):
    grouped_by_path =  dict(events | group_by[get['path']] | collect)
    x_range = [i for i in range(0, 24)] 
    
    from datetime import datetime
    group_key = lambda s: datetime.strptime(s, "%Y-%m-%d %H:%M:%S").hour

    result = grouped_by_path | items | map[lambda kv: (kv[0], kv[1] | histogram[group_key][get['time']][x_range] | collect)] | concat | collect
    result += ["🕰️ ──┬───────────────────┬────────────────────────┬──►", '        '.join([str(x_range[i]) for i in range(0, len(x_range), 4)])]
    return result


from zef.core.op_implementations.yo_ascii import make_box
result = process_request_events(events)
http_box = make_box(result, "HTTP Server Traffic")

http_box2 =Frame(Text('\n'.join(result)), title= "HTTP Server Traffic", expand=False) 


# ┌── Effect Event Log ───────────────────────────────────────────────────────────────────────────────────┐
# │                                                                                                       │
# │                                                                                                       │
# |  ⚡ FX.Clipboard.CopyFrom           -                               12:14:02 - 6 mins ago     ✅        |
# |  ⚡ FX.HTTP.StartServer             Port 5002 is blocked            13:52:02 - 6 mins ago     ❌        |
# |  ⚡ FX.LocalFile.Load               file: zeyad.txt                 14:09:02 - 6 mins ago     ✅        |
# |  ⚡ FX.GraphQL.StartPlayground      -                               14:14:02 - 6 mins ago     ✅        |
# |  ⚡ FX.Graph.Load                   Loaded: worduel/main5           14:52:02 - 6 mins ago     ✅        |
# │                                                                                                       │
# │                                                                                                       │
# └───────────────────────────────────────────────────────────────────────────────────────────────────────┘


from datetime import datetime
events = [
    {"fx_type": FX.Graph.Load, "message": "Loaded: worduel/main5", "time": "2022-11-05 14:52:02  +0800", "is_error": False},
    {"fx_type": FX.HTTP.StartServer, "message": "Port 5002 is blocked", "time": "2022-11-05 13:52:02  +0800", "is_error": True},
    {"fx_type": FX.GraphQL.StartPlayground, "message": "-", "time": "2022-11-05 14:14:02  +0800", "is_error": False},
    {"fx_type": FX.Clipboard.CopyFrom, "message": "-", "time": "2022-11-05 12:14:02  +0800", "is_error": False},
    {"fx_type": FX.LocalFile.Load, "message": "file: zeyad.txt", "time": "2022-11-05 14:09:02  +0800", "is_error": False},
]

from zef.core.op_implementations.yo_ascii import make_box, pad_side
parse_time = lambda s: datetime.strptime(s, "%Y-%m-%d %H:%M:%S %z")

@func
def fx_event_parsing(event, longest_message, longest_fx_type):
    fx_type = str(event["fx_type"])
    message = event["message"]
    time = parse_time(event["time"]).time()
    is_error = event["is_error"]
    return f"💥 {pad_side(fx_type, 0, True, longest_fx_type)} {pad_side(message, 0, True, longest_message)} {time} - 6 mins ago     {'✅' if not is_error else '❌'}"


def fx_events_processing(events):
    events = events | sort[lambda x: parse_time(x['time'])] | collect
    longest_type = max(events | map[lambda d: d | get['fx_type']| func[str] | length | collect] | collect) + 5
    longest_message = max(events | map[lambda d: d | get['message']| func[str] | length | collect] | collect) + 10
    return events | map[fx_event_parsing[longest_message][longest_type]] | collect


from zef.core.op_implementations.yo_ascii import make_box

result = fx_events_processing(events)
# print(make_box(result, "Effect Event Log"))  
fx_log_box = make_box(result, "Effect Event Log")
fx_log_box2 =Frame(Text('\n'.join(result)), title= "Effect Event Log", expand=False) 


# ┌── Switches ──────────────────────────────────────────────┐
# │                                                          │
# │                                                          │
# |  ❌  Allow new RT type definitions      🆔 7988187e      |
# |  ✅  Allow new ET type definitions      🆔 37a4a100        |
# |  ❌  Allow new EN type definitions      🆔 a156e814        |
# |  ✅  Allow new EN type definitions      🆔 8bd8f2b1        |
# |  ❌  Quiet Mode                         🆔 2a6b3029        |
# |  ❌  Performance Mode                   🆔 c83d6b97        |
# |  ❌  Benchmark Mode                     🆔 e88aeb4b        |
# |  ✅  "yo" Condensed Output              🆔 80475a76        |
# │                                                          │
# │                                                          │
# └──────────────────────────────────────────────────────────┘

from zef.core.op_implementations.yo_ascii import make_box, pad_side

switches = [
    {"name": "Allow new RT type definitions", "is_on": False},
    {"name": "Allow new ET type definitions", "is_on": True},
    {"name": "Allow new EN type definitions", "is_on": False},
    {"name": "Allow new EN type definitions", "is_on": True},
    {"name": "Quiet Mode", "is_on": False},
    {"name": "Performance Mode", "is_on": False},
    {"name": "Benchmark Mode", "is_on": False},
    {"name": '"yo" Condensed Output', "is_on": True},
]

# a random uid generator
def make_uid():
    import uuid
    return str(uuid.uuid4())[:8]

@func
def switch_parsing(switch_t, longest_name):
    name, is_on =  switch_t
    return f"{'✅' if is_on == 'yes' else '❌'}  {pad_side(name, 0, True, longest_name)} 🆔 {make_uid()}"

def switches_processing(switches_dict):
    longest_name = max(switches_dict | items | map[lambda d: d[0] | length | collect] | collect) + 5
    return switches_dict | items | map[switch_parsing[longest_name]] | collect

result = switches_processing(zwitch.as_dict())
# print(make_box(result, "Switches"))

switches_box = make_box(result, "Switches")
switches_box2 =Frame(Text('\n'.join(result)), title= "Switches", expand=False) 

db  = Graph()
worduel_db = Graph("worduel/main5")

# ┌── Loaded Graphs ────────────────────────────────────────────────────────────────────────────────────────────────────┐
# │                                                                                                                     │
# │                                                                                                                     │
# |  Name            Blob Count         Size      UID                  Last Change                    Is Primary        |
# |  db              92 blobs           0MB       7fef825b4d77f2a3     2022-11-08 17:10:03 (+0800)    ✅                 |
# |  worduel_db      215973 blobs       3MB       3c723ac84a417f8f     2022-11-08 03:10:10 (+0800)    ❌                 |
# │                                                                                                                     │
# │                                                                                                                     │
# └─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

from zef.core.op_implementations.yo_ascii import make_box, pad_side

@func
def generate_graph_metrics_d(graph_t):
    g_name, g = graph_t
    return {
        "local_name": g_name,
        "uid": str(g.uid),
        "last_change": str(to_ezefref(g | all[TX] | last | collect) | time | collect),
        "blob_count": f"{g.graph_data.write_head} blobs",
        "size": f"{round((g.graph_data.write_head * 16) / 1E6)}MB",
        "is_primary_instance": g.graph_data.is_primary_instance,
    }


@func
def graph_metric_parsing(g_dict, longest_name, longest_blob, longest_size, longest_uid, longest_time):
    local_name = g_dict["local_name"]
    uid = g_dict["uid"]
    last_change = g_dict["last_change"]
    blob_count = g_dict["blob_count"]
    size = g_dict["size"]
    is_primary_instance = g_dict["is_primary_instance"]
    if isinstance(is_primary_instance, (int, bool)):
        is_primary_instance = '✅' if is_primary_instance else '❌'

    return f"{pad_side(local_name, 0, True, longest_name)} {pad_side(blob_count, 0, True, longest_blob)}  {pad_side(size, 0, True, longest_size)}   {pad_side(uid, 0, True, longest_uid)}  {pad_side(last_change, 0, True, longest_time)} {is_primary_instance}"


def graphs_processing():
    graphs = globals() | items | filter[lambda kv: is_a(kv[1], Graph)] | collect
    metrics_d = graphs | map[generate_graph_metrics_d] | collect

    headers = {
        "local_name": "Name",
        "uid": "UID",
        "last_change": "Last Change",
        "blob_count": "Blob Count",
        "size": "Size",
        "is_primary_instance": "Is Primary",
    }
    metrics_d = metrics_d | prepend[headers] | collect
    longest_name = max(metrics_d | map[lambda d: d | get['local_name']| func[str] | length | collect] | collect) + 5
    longest_blob = max(metrics_d | map[lambda d: d | get['blob_count']| func[str] | length | collect] | collect) + 5
    longest_size = max(metrics_d | map[lambda d: d | get['size']| func[str] | length | collect] | collect) + 3
    longest_uid = max(metrics_d | map[lambda d: d | get['uid']| func[str] | length | collect] | collect) + 3
    longest_time = max(metrics_d | map[lambda d: d | get['last_change']| func[str] | length | collect] | collect) + 3

    return metrics_d | map[graph_metric_parsing[longest_name][longest_blob][longest_size][longest_uid][longest_time]] | collect



result = graphs_processing()
graphs_box = make_box(result, "Loaded Graphs")
graphs_box2 = Frame(Text('\n'.join(result)), title= "Loaded Graphs", expand=False) 


hstack = HStack([ switches_box, http_box])
stack  = VStack([graphs_box, fx_log_box, hstack])




Frame(stack, title=Text("Zef Dashboard", color="#ffb3b3"), expand=False, box='ascii', padding=(2,2)) | show