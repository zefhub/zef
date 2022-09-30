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


from ..core import *
from ..ops import *

import asyncio

def load_graph(tag_or_uid):
    stream = {'type': FX.Stream.CreatePushableStream} | run
    return asyncio.run(load_graph_async(tag_or_uid, stream))

async def load_graph_async(tag_or_uid, stream):
    from rich.progress import Progress, TextColumn
    import rich.progress
    progress = Progress(
        # *Progress.get_default_columns(),
        TextColumn("[progress.description]{task.description}"),
        rich.progress.BarColumn(),
        rich.progress.DownloadColumn(),
        rich.progress.TransferSpeedColumn(),
        TextColumn("{task.fields[extra]}"),
    )
    # ptask = progress.add_task("...", total=100)
    # ptask_main = progress.add_task("...", start=False, total=None)
    # Unfortuantely need a completed and total otherwise this errors
    ptask_main = progress.add_task("...", completed=0, total=1, extra="")
    ptasks = None

    last_time = now()
    graph_uid = None

    async def update_main(msg, extra=""):
        nonlocal last_time
        last_time = now()
        progress.update(ptask_main, description=msg, extra=extra)

    queue = asyncio.Queue()
    async def react_worker(queue):
        while True:
            s = await queue.get()
            # progress.console.log(s)
            nonlocal ptasks, graph_uid
            if s.startswith("CHUNK:"):
                chunks = s[len("CHUNK:"):].split(",")[:-1]
                chunks = chunks | map[split['/']
                                      | apply_functions[str,int,int]
                                      ] | collect
                if ptasks is None:
                    ptasks = [progress.add_task(x[0], completed=x[1], total=x[2], extra="") for i,x in enumerate(chunks)]
                else:
                    for chunk,ptask in zip(chunks,ptasks):
                        progress.update(ptask, completed=chunk[1])
                await update_main(f"Transferring {graph_uid}")
            elif s.startswith("GRAPH UID:"):
                graph_uid = s[len("GRAPH UID:"):]
            else:
                await update_main(s)

    loop = asyncio.get_running_loop()
    sub = stream | subscribe[lambda x: queue.put_nowait(x)]
    react_worker_task = loop.create_task(react_worker(queue))

    async def long_wait():
        nonlocal last_time
        while True:
            await asyncio.sleep(1)
            wait_time =  now() - last_time
            if wait_time > 3*seconds:
                progress.update(ptask_main, extra=f"(taking long time, {wait_time})")
                
    long_wait_task = loop.create_task(long_wait())
    await update_main("Load graph starting")
    progress.start()

    # Doing this here because stream needs to be subscribed to first
    eff = {"type": FX.Graph.Load,
           "tag_or_uid": tag_or_uid,
           # "mem_style": internals.MMAP_STYLE_ANONYMOUS,
           "progress_stream": stream
           }
    # Because this is not async, need to run this in a separate thread to not cause blocking of asyncio queue
    # r = eff | run
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor() as pool:
        r = await loop.run_in_executor(pool, run, eff)

    # await asyncio.sleep(10)
    progress.stop()
    long_wait_task.cancel()
    react_worker_task.cancel()
    await update_main("Finished")

    return r["g"]