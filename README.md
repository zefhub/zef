<div align="center">
    <img width="300px" src="https://github.com/zefhub/zefhub-web-assets/blob/main/zef_logo_white.png#gh-dark-mode-only">
    <img width="300px" src="https://github.com/zefhub/zefhub-web-assets/blob/main/zef_logo_black.png#gh-light-mode-only">
</div>

<p align="center">
    A toolkit for data-oriented systems
</p>

<p align="center">
    <em>versioned graphs + streams + query using Python + GraphQL</em>
</p>

<div align="center">
    <a href="https://github.com/zefhub/zef/blob/master/LICENSE">
        <img src="https://img.shields.io/badge/license-Apache%202.0-teal" />
    </a>
    <br />
    <br />
    <a href="https://zef.zefhub.io/">Docs</a>
    <span>&nbsp;&nbsp;|&nbsp;&nbsp;</span>
    <a href="https://zef.zefhub.io/blog">Blog</a>
    <span>&nbsp;&nbsp;|&nbsp;&nbsp;</span>
    <a href="https://zef.chat/">Chat</a>
    <span>&nbsp;&nbsp;|&nbsp;&nbsp;</span>
    <a href="https://www.zefhub.io/">ZefHub</a>
</div>

<br />
<br />

## Description

Zef is an open source toolkit of modules for building data-oriented systems. These systems can include backends, graph projects, or pipelines. Zef consists of an immutable, in-memory data structure, versioned graphs, data streams, composable lazy operators, effects handling, and GraphQL support. You can pick and choose the modules you need for your project.

If any of these apply to you, Zef might help:

- I need a graph data model that's more powerful than NetworkX but easier than Neo4j
- I need a GraphQL API that's easy to spin up and close to my data model
- I like Datomic but prefer something open source that feels like working with local data structures
- I need to "time travel" and access past states easily
- I don't want to learn a new query language like Cypher or GSQL (I just want to use Python)
- I want an easy way to build data pipelines and subscribe to data streams

<br />
<br />

## Features

- in-memory, immutable data structure
- fully versioned graphs
- work with your data like local data structures
- no separate query language
- no ORM
- query and transform data using Python with composable lazy operators
- data streams and subscriptions
- GraphQL API with low impedance mismatch to data model
- automatically store, sync, distribute graphs securely with ZefHub

<br />
<br />

## Status

Zef is currently in Public Alpha.

- [x] Private Alpha: Testing Zef internally and with a closed group of users.
- [x] Public Alpha: Anyone can use Zef but please be patient with very large graphs!
- [ ] Public Beta: Stable enough for most non-enterprise use cases.
- [ ] Public: Stable for all production use cases.

<br />
<br />

## Installation

The platforms we currently support are 64-bit Linux and MacOS. The latest version can be installed via the PyPI repository using:

```bash
pip install zef
```

This will attempt to install a wheel if supported by your system and compile from source otherwise. See INSTALL for more details if compiling from source.

Check out our [installation doc](https://zef.zefhub.io/introduction/installation) for more details about getting up and running once installed.

<br />
<br />

## Using Zef

Here's some quick points to get going. Check out our [Quick Start](https://zef.zefhub.io/introduction/quick-start) and docs for more details.

<br />

<div align="center">
    <h3>💆 Get started 💆</h3>
</div>

```python
from zef import *          # you'll see with our functional operators why we violate PEP in the first line
from zef.ops import *

g = Graph()                # create an empty graph
```

<br />

<div align="center">
    <h3>🌱 Add some data 🌱</h3> 
</div>

```python
p1 = ET.Person | g | run                  # add an entity to the graph

(p1, RT.FirstName, "Yolandi") | g | run   # add "fields" via relations triples: (source, relation, target)
```

<br />

<div align="center">
    <h3>🐾 Traverse the graph 🐾</h3>
</div>

```python
p1 | Out[RT.FirstName]         # one hop: step onto the relation

p1 | out_rel[RT.FirstName]     # two hops: step onto the target
```

<br />

<div align="center">
    <h3>⏳ Time travel ⌛</h3>
</div>

```python
p1 | time_travel[-2]                                           # move reference frame back two time slices

p1 | time_travel[Time('2021 December 4 15:31:00 (+0100)')]     # move to a specific date and time
```

<br />

<div align="center">
    <h3>👐 Share with other users (via ZefHub) 👐</h3>
</div>

```python
g | sync[True] | run                            # save and sync all future changes on ZefHub

# ---------------- Python Session A (You) -----------------
g | uid | to_clipboard | run                    # uid is used to retrieve a graph uid

# ---------------- Python Session B (Friend) -----------------
graph_uid: str = '...'                          # uid copied from Slack/WhatsApp/email/etc
g = Graph(graph_uid)
g | now | all[ET] | collect                     # see all entities in the latest graph slice
```

<br />

<div align="center">
    <h3>🚣 Choose your own adventure 🚣</h3>
</div>

- [Basic tutorial of Zef](https://zef.zefhub.io/tutorials/basic/employee-database)
- [Import data from CSV](https://zef.zefhub.io/how-to/import-csv)
- [Import data from NetworkX](https://zef.zefhub.io/how-to/import-graph-formats)
- [Set up a GraphQL API](https://zef.zefhub.io/how-to/graphql-basic)
- [Use Zef graphs in NetworkX](https://zef.zefhub.io/how-to/use-zef-networkx)
- [Add Zef to a project and use the functional operators](https://zef.zefhub.io/zef-ops) (doc not yet complete)

<br />

<div align="center">
    <h3>📌 A note on ZefHub 📌</h3>
</div>

Zef is designed so you can use it locally and drop it into any existing project. You have the option of syncing your graphs with ZefHub, a service that stores, syncs, and distributes graphs automatically (and the company behind Zef). ZefHub makes it possible to [share graphs with other users and see changes live](https://zef.zefhub.io/how-to/share-graphs), by memory mapping across machines in real-time!

You can create a ZefHub account for free which gives you full access to storing and sharing graphs forever. For full transparency, our long-term hope is that many users will get value from Zef or Zef + ZefHub for free, while ZefHub power users will pay a fee for added features and services.

<br />
<br />

## Roadmap

We want to make it incredibly easy for developers to build fully distributed, reactive systems with consistent data and cross-language (Python, C++, Julia) support. If there's sufficient interest, we'd be happy to share a public board of items we're working on.

<br />
<br />

## Contributing

Thank you for considering contributing to Zef! We know your time is valuable and your input makes Zef better for all current and future users.

To optimize for feedback speed, please raise bugs or suggest features directly in our community chat [https://zef.chat](https://zef.chat).

Please refer to our [CONTRIBUTING file](https://github.com/zefhub/zef/blob/master/CONTRIBUTING.md) and [CODE_OF_CONDUCT file](https://github.com/zefhub/zef/blob/master/CODE_OF_CONDUCT.md) for more details.

<br />
<br />

## License

Zef is licensed under the Apache License, Version 2.0 (the "License"). You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)
