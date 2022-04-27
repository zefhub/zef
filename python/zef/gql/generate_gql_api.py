from ariadne import make_executable_schema, graphql_sync
from . import schema_graph_to_file as file_generator
from . import schema_file_to_graph as graph_generator
import os
from zef import Graph, RT
from zef.ops import *
import json


def generate_resolvers_fcts(schema_root, resolvers_destination):
    """ Generates ariadne resolvers from the generate_ariadne_resolvers.py
        cog file. And outputs generated_resolvers.py

        We pass a global dict with the root node of the schema graph that cog
        uses to generate the resolvers.
    """
    from cogapp import Cog
    cog = Cog()
    cog.options.bReplace = True

    z = schema_root >> O[RT.DefaultResolversList] | maybe_value | collect
    if z is None:
        default_resolvers_list = []
    else:
        default_resolvers_list = json.loads(z)
        
        
    z = schema_root >> O[RT.CustomerSpecificResolvers] | collect
    if z is None:
        customer_specific_resolvers = lambda *args,**kwds: ("return None #This means that no resolver is defined!", ["z", "ctx"])
    else:
        customer_specific_func_inner = z | value | collect
        d = {}
        exec(customer_specific_func_inner, d)
        customer_specific_resolvers = d["customer_specific_resolvers"]

    additional_exec = schema_root >> O[RT.AdditionalExec] | value_or[""] | collect

    global_dict = {"schema_root": schema_root,
                   "default_resolvers_list": default_resolvers_list,
                   "customer_specific_resolvers": customer_specific_resolvers,
                   "additional_exec": additional_exec}

    try:
        path = os.path.dirname(os.path.realpath(__file__))
        cog.processFile(os.path.join(path, "generate_ariadne_resolvers.py"),
                        os.path.join(resolvers_destination, "ariadne_resolvers.py"), globals=global_dict)
    except Exception as exc:
        print(f'An exception was raised when processing file {exc}')


def generate_api_schema_file(schema_root, destination):
    """
    Creates a variable that holds the schema file which is represented by the passed schema graph.
    """
    file_generator.generate_api_schema_file(schema_root, destination)


def generate_graph_from_file(schema_root, g):
    """
    Creates a graph from a given schema.
    """
    return graph_generator.parse_schema(g, schema_root)


def make_api(schema_root, schema_destination=None, resolvers_destination=None):
    """ This function takes a zefref to the root of the schema graph.
    It then generates the schema file by traversing the graph that is passed.
    From the same graph, the resolver functions for the schema file are generated.
    The schema is then checked for syntax errors and the resolvers are checked for all present
    GQL types in the schema and if this is all successful an API is then created and returned.
    """
    import tempfile
    if schema_destination is None:
        fd,schema_destination = tempfile.mkstemp(suffix=".py", prefix="schema_")
    if resolvers_destination is None:
        resolvers_destination = tempfile.mkdtemp()

    generate_api_schema_file(schema_root, schema_destination)
    generate_resolvers_fcts(schema_root, resolvers_destination)

    resolvers = load_module_from_abs_path("generated_resolvers", os.path.join(resolvers_destination, "ariadne_resolvers.py"))
    schema = load_module_from_abs_path("schema", schema_destination)

    # Need to inject the graph into the resolvers module
    resolvers.g = Graph(schema_root)
    schema = make_executable_schema(schema.generated_schema, resolvers.object_types)
    return schema


triggered_callbacks = []
def create_subscription(payload, schema, g, sng_callback):
    # parsing
    from graphql import parse
    ast = parse(payload['query'])
    name = ast.definitions[0].selection_set.selections[0].name.value

    # register callback
    if name == "onValueChange":
        if 'variables' in payload:
            uid = payload['variables']['uid']
        else:
            # Will throw an error
            uid = ast.definitions[0].selection_set.selections[0].arguments[0].value.value

        def new_value_callback(z):
            response = graphql_sync(schema, payload, context_value={"tx": z | tx, "g": g})
            triggered_callbacks.append((sng_callback, response[1]))

        sub = g[uid] | now | subscribe[on_value_assignment][new_value_callback]
    else:
        sub = None

    return sub

def load_module_from_abs_path(name, path):
    # Why is this so complicated?
    import importlib.util
    spec = importlib.util.spec_from_file_location(name, path)
    foo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(foo)
    return foo