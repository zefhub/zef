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

#include "tools.h"

namespace zefDB {
	tools::ZefLog zeflog("bug_log");

namespace tools {


	void ZefLog::write_to_file() {
		using namespace std;
		ofstream myfile;
		str fname = filename_base + str("_") + to_str(all_records_to_date.size()) + ".zeflog";
		myfile.open(fname);
		myfile << "[\n";
		for (const auto& ro : all_records_to_date)
			std::visit(
				[&myfile](auto x)->void {myfile << x << std::endl; }
				, ro);
		myfile << "]";



		if (bool(g_maybe)) {
		//if (true) {
			myfile << "\n\n\n ----------------------- graph state at this point in time -----------------------------\n\n";
			for (auto uzr : blobs(*g_maybe))
				myfile << uzr << '\n';

			myfile << "\n\n                  ---- key_dict ----\n";
            throw std::runtime_error("Needs reimplementing!");
			// for (auto p : (*g_maybe).key_dict())
			// 	myfile << p.first << ": " << p.second << "\n";
		}

		myfile.close();
	}
} //tools



Graph deep_copy(Graph g) {
    using namespace internals;
    Messages::UpdatePayload payload;
    {
        auto & gd = g.my_graph_data();
        LockGraphData gd_lock(&gd);

        if (g.my_graph_data().number_of_open_tx_sessions != 0)
            throw std::runtime_error("One cannot clone a graph with open Transactions! Close these first.");

        payload = graph_as_UpdatePayload(gd);
    }

    // As part of creating a graph, the uid_bytes needs to contain that graph's
    // UID. So overwrite now before sending it to the butler.
    blobs_ns::ROOT_NODE * ptr = (blobs_ns::ROOT_NODE*)payload.rest[0].data();
    ptr->uid = make_random_uid();

    auto g_new = Graph::create_from_bytes(std::move(payload));	

    GraphData & gd = g_new.my_graph_data();
    LockGraphData gd_lock(&gd);

    // Redo all of the other UIDs (skipping the first which is the graph UID)
    for (auto uzr : blobs(g_new)) {
        if(BT(uzr) == BT.ROOT_NODE)
            continue;
		if (has_uid(uzr)) {
			assign_uid(uzr, make_random_uid());
		}
	}

    // Manually reseting the caches
    //.. this is not possible with these structs! Need to instead remove them from the payload I think.
    throw std::runtime_error("Not possible currently - DANNY FIX!");
	// gd.key_dict->clear();
    // blob_index blob_index_lo = constants::ROOT_NODE_blob_index;
    // blob_index blob_index_hi = gd.write_head;
	// apply_actions_to_blob_range(g_new, blob_index_lo, blob_index_hi, false, false);

    // gd.is_primary_instance = true;
	// return g_new;
}














Graph revision_graph(Graph g) {

	throw std::runtime_error("not implemented");
	// create new graph with deferred edge lists absorbed into local lists
	auto g_new = Graph();
	auto& gd_new = g.my_graph_data();
	auto old_blbs = blobs(g);
	std::vector<blob_index> new_index_given_old(0, length(old_blbs));   // for each old blob: take its index as position in this vector and save equivalent blob in the new graph
	
	// in the first iteration go through and copy all blobs that are not deferred edge lists. Immediately after allocating each blob, assign the right
	// size for the edge list: the total number of edge list indexes. Don't set the edge indexes yet, since we don't know the new index of each blob in the new layout yet
	for (auto bl : old_blbs) {
		AllEdgeIndexes(bl).begin();


		//new_index_given_old[index(bl)] = index(new_blob);
		internals::move_head_forward(gd_new);
	}

	for (auto& old_uzr : old_blbs) {
		auto new_uzr = EZefRef(new_index_given_old[index(old_uzr)], gd_new);

		blob_index* first_ind_ptr = &(*AllEdgeIndexes(new_uzr).begin());
		int count = 0;
		for (auto ind : AllEdgeIndexes(old_uzr))
			*(first_ind_ptr+(count++)) = ind > 0 ? new_index_given_old[ind] : -new_index_given_old[-ind];
	}
	


	return g_new;
}



















struct Contains {
    std::optional<ZefRef> value_to_search_for = {};
    Contains operator[](ZefRef z) const { return Contains{ z }; }

	bool operator() (ZefRefs list_to_search) {
		if (!value_to_search_for.has_value()) 
			throw std::runtime_error("'contains' zefop called on ZefRefs, but no value to search for was curried in. Use as 'some_uzrs | contains[my_zr_to_search_for]'.");
		for (const auto& el : list_to_search) {
			if (el == (*value_to_search_for))
				return true;
		}
		return false; // value was not found in list
	}
};
const Contains contains_zefop;
bool operator| (ZefRefs list_to_search, Contains op) { return op(list_to_search); }




bool is_logic_edge(ZefRef ed) {
	auto edge_type = ed | RT;
	return
		edge_type == RT.If ||
		edge_type == RT.Previous ||
		edge_type == RT.Next ||
		edge_type == RT.Value;
}

bool is_value_edge(ZefRef ed) {
	auto edge_type = ed | RT;
	return  
		edge_type == RT.Value ||
		edge_type == RT.Previous ||
		edge_type == RT.Next;
}























} //zefDB
