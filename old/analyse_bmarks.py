from zefdb import *
from zefdb.ops import *

with open("bmarks.dat") as file:
    raw = (
        LazyValue(file)
        | map[lambda x: x.strip()]
        | filter[lambda x: not x.startswith("#")]
        | map[split[" "] | func[lambda x: [float(x[0]), float(x[1]), *x[2:-1], float(x[-1])]]]
        | collect
    )


aggregates = raw | group_by[lambda x: tuple(x[:-1])] | map[
    lambda x: (x[0],
               mean(x[1]
                    | map[last]
                    | collect),
               len(x[1]),
               )
    
] | collect

compared = aggregates | group_by[lambda x: multiply(*x[0][:2])] | collect

print("n_entites, n_batches, time in ms, num_obs")
(compared
 | map[lambda x: f"Total {x[0]}:\n" + "\n".join(
     x[1]
     | map[lambda x: ','.join(x[0] | map[str] | collect) + ": " + f"{x[1]:.2f}   ({x[2]})"]
 ) | collect]
 | for_each[lambda x: print(x + "\n=====")]
 )