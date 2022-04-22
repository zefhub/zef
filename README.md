<p align="center">
<img width="350px" src="https://github.com/zefhub/zefhub-web-assets/blob/main/zef_logo_white_alt.png">
</p>

<p align="center">
A toolkit for data-oriented pipelines and backends
</p>

<p align="center">
<em>versioned graphs + streams + query using Python + GraphQL</em>
</p>

## Description

Zef is an open source toolkit of modules for building data-oriented pipelines and backends. Zef consists of an immutable, in-memory data structure, versioned graphs, data streams, composable lazy operators, effects handling, and GraphQL support. You can pick and choose the modules you need for your project.

If any of these apply to you, Zef might help:

- I need a graph data model that's more powerful than NetworkX but easier than Neo4j
- I need a GraphQL API that's easy to spin up and close to my data model
- I like Datomic but prefer something open source that feels like working with local data structures
- I need to "time travel" and access past states easily
- I don't want to learn a new query language like Cypher or GSQL (I just want to use Python)
- I want an easy way to build data pipelines and subscribe to data streams

## Features

- in-memory, immutable data structure
- fully versioned graphs
- work with your data like local data structures
- no separate query language
- no ORM
- query and transform data using Python with composable lazy operators
- data streams and subscriptions
- GraphQL API with low impedance mismatch to data model
- automatically store, sync, distribute graphs with ZefHub

## Status

Zef can currently handle up to ~100 million nodes.

## Installation

Check out our [installation doc](https://zef.zefhub.io/introduction/installation) for more details.

```bash
pip install zef
```

## Using Zef

Here's some quick points to get going. Check out our [Quick Start](https://zef.zefhub.io/introduction/quick-start) and docs for more details.

### Get started

```python
from zef import *          # you'll see with our functional operators why we violate PEP in the first line
from zef.ops import *

g = Graph()                # create an empty graph
```

### Add some data

```python
p1 = ET.Person | g | run                  # add an entity to the graph

(p1, RT.FirstName, "Yolandi") | g | run   # add "fields" via relations triples: (source, relation, target)
```

### Traverse the graph

```python
p1 | Out[RT.FirstName]         # one hop: step onto the relation

p1 | out_rel[RT.FirstName]     # two hops: step onto the target
```

### Time travel

```python
p1 | time_travel[-2]                                           # move reference frame back two time slices

p1 | time_travel[Time('2021 December 4 15:31:00 (+0100)')]     # move to a specific date and time
```

### Share with other users (via ZefHub)

```python
g | sync[True] | run                            # save and sync all future changes on ZefHub

# ---------------- Python Session A (You) -----------------
g | uid | to_clipboard | run                    # uid is used to retrieve a graph uid

# ---------------- Python Session B (Friend) -----------------
graph_uid: str = '...'                          # uid copied from Slack/WhatsApp/email/etc
g = Graph(graph_uid)
g | now | all[ET] | collect                     # see all entities in the latest graph slice
```

### Intro to Zef

Give our [basic tutorial](https://zef.zefhub.io/tutorials/basic/employee-database) a go to get a broad overview of Zef.

### Choose your own adventure

- [Import data from CSV](https://zef.zefhub.io/how-to/import-csv)
- [Import data from NetworkX](https://zef.zefhub.io/how-to/import-graph-formats)
- [Set up a GraphQL API](https://zef.zefhub.io/how-to/graphql-basic)
- [Use Zef graphs in NetworkX](https://zef.zefhub.io/how-to/use-zef-networkx)
- [Add Zef to a project and use the functional operators](https://zef.zefhub.io/zef-ops) (doc not yet complete)

### A note on ZefHub

Zef is designed so you can use it locally and drop it into any existing project. You have the option of syncing your graphs with ZefHub, a service that stores, syncs, and distributes graphs automatically (and the company behind Zef). ZefHub makes it possible to [share graphs with other users and see changes live](https://zef.zefhub.io/how-to/share-graphs), by memory mapping across machines in real-time!

You can create a ZefHub account for free and you'll automatically be on the free tier (we will let you know far in advance if you ever need to pay). We will always have a free ZefHub tier available. For full transparency, our long-term hope is that many users will get value from Zef or Zef + ZefHub for free, while ZefHub power users can pay a fee for added features and services.

## Roadmap

We want to make it incredibly easy for developers to build fully distributed, reactive systems with consistent data and cross-language (Python, C++, Julia) support. If there's sufficient interest, we'd be happy to share a public board of items we're working on.

## Contributing

Thank you for considering contributing to Zef! We know your time is valuable and your input makes Zef better for all current and future users.

[Raise bugs or issues](https://github.com/zefhub/zef/issues)

[Suggest features or ideas](https://github.com/zefhub/zef/discussions)

Please refer to our [CONTRIBUTING file](https://github.com/zefhub/zef/blob/master/CONTRIBUTING.md) for more details.

## Community

No question, issue, or feedback is too small or insignificant, so please reach out!

Join our community chat: [https://zef.chat](https://zef.chat)

Follow us on Twitter: [@zefhub](https://twitter.com/zefhub)

Please refer to our [CODE_OF_CONDUCT file](https://github.com/zefhub/zef/blob/master/CODE_OF_CONDUCT.md) for more details.

## License
