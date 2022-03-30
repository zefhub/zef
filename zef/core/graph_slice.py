from ._core import *
from ._ops import *

class GraphSlice:
    def __init__(self, *args):
        """
        Construct a GraphSlice, aka reference frame.

        Signature:
            ZefRef[TX] -> GraphSlice
            EZefRef[TX] -> GraphSlice
            (Time, Graph) -> GraphSlice
            (Graph, Time) -> GraphSlice
        """

        # ZefRef[TX] -> GraphSlice and EZefRef[TX] -> GraphSlice
        if len(args) == 1:
            z = args[0]
            if not (isinstance(z, ZefRef) or isinstance(z, EZefRef)):
                raise TypeError(f'When calling the GraphSlice constructor with a single arguments, this has to be a ZefRef[TX] / EZefRef[TX]. Called with: {args=}')
            self.tx = to_ezefref(z)
            return

        if len(args) == 2:
            if {type(args[0]), type(args[1])} != {Graph, Time}:
                raise TypeError(f'When calling the GraphSlice constructor with two args, this has to be with types (Time, Graph). Called with {args=}')

            t, g = args if isinstance(args[0], Time) else args[::-1]
            if t > ops.now():
                raise RuntimeError('One cannot ask for a GraphSlice at a fixed time in the future.')
            txs = g | ops.all[zefdb.TX] | ops.collect
            if t < ops.collect(txs | ops.first | ops.time):
                raise RuntimeError(f'In trying to determine a GraphSlice given a time: provided time {t} is before the graph instantiation time.')
            
            last_tx = txs | last
            self.tx = (
                last_tx                                 # treat the case of the 
                if t >= time(last_tx) else
                txs | filter[lambda x: time(x) >= t] | ops.first | ops.collect
            )
            return

        raise TypeError(f'The GraphSlice constructor can only be called with one or two args. Called with {args=}')
        
        
    def __repr__(self):
        return f"GraphSlice(graph_uid='{str(uid(Graph(self.tx)))}', time_slice={int(time_slice(self.tx))})"

    def __eq__(self, other):        
        return (
            self.tx == other.tx 
            if isinstance(other, GraphSlice)
            else False
            )
            
    def __int__(self):
        """
        Returns the time slice as an integer
        """
        return int(self.tx | time_slice | collect)

    def __hash__(self):
        return hash(self.tx)

    def __getitem__(self, thing):
        g = Graph(self.tx)
        zr = g[thing]
        return zr | in_frame[GraphSlice(self.tx)] | collect

