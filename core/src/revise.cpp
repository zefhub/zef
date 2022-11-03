// Copyright 2022 Synchronous Technologies Pte Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "revise.h"
#include "ops_imperative.h"
#include "graph.h"

namespace zefDB {
    Graph copy_graph_slice(EZefRef ctx) {
        using namespace imperative;

        Graph old_g = Graph(ctx);

        // No writes while this is going on!
        GraphData & old_gd = old_g.my_graph_data();
        LockGraphData lock{&old_gd};

        Graph new_g(false);

        // Simply copy entities and relations as we come across them. This
        // should mean that any relation comes after its source/target.

        EZefRefs raes = all_raes(old_g);
        EZefRefs current_raes = filter<EZefRef>(
            raes,
            [ctx](EZefRef x) {
                return exists_at(x, ctx);
            }
        );

        std::vector<EZefRef> todo;
        todo.reserve(length(current_raes));
        for(auto it : current_raes)
            todo.push_back(it);

        Transaction t{new_g};
        Time last = imperative::now();
        while(true) {
            std::vector<EZefRef> new_todo;

            for(auto it : todo) {
                if(imperative::now() - last > 5*seconds) {
                    std::cerr << "Up to index " << index(it) << "/" << old_gd.read_head.load() << std::endl;
                    last = imperative::now();
                }
                if(BT(it) == BT.VALUE_NODE)
                    continue;
                EternalUID euid = origin_uid(it);
                if(BT(it) == BT.ENTITY_NODE) {
                    internals::merge_entity_(new_g, ET(it), euid.blob_uid, euid.graph_uid);
                } else if(BT(it) == BT.RELATION_EDGE) {
                    if(!new_g.contains(origin_uid(source(it))) || !new_g.contains(origin_uid(target(it)))) {
                        new_todo.push_back(it);
                        continue;
                    }
                    EZefRef src = new_g[origin_uid(source(it))];
                    src = target(traverse_in_node(src, BT.ORIGIN_RAE_EDGE));
                    EZefRef trg = new_g[origin_uid(target(it))];
                    trg = target(traverse_in_node(trg, BT.ORIGIN_RAE_EDGE));
                    internals::merge_relation_(new_g, RT(it), src, trg, euid.blob_uid, euid.graph_uid);
                } else if(BT(it) == BT.ATTRIBUTE_ENTITY_NODE) {
                    EZefRef new_ae = internals::merge_atomic_entity_(new_g, AET(it), euid.blob_uid, euid.graph_uid);
                    auto maybe_value = value(to_frame(it,ctx));
                    if(maybe_value)
                        assign_value(new_ae, *maybe_value);
                }
            }

            if(new_todo.size() == 0)
                break;
            if(new_todo.size() == todo.size())
                throw std::runtime_error("Cycle detected, can't clone");

            todo = new_todo;
            new_todo.clear();
            std::cerr << "Relooping in copy_graph_slice" << std::endl;
        }

        return new_g;
    }
}