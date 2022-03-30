#include "verification.h"

// This is only needed for the DOCTEST
#include "high_level_api.h"

#include <doctest/doctest.h>

namespace zefDB {
	namespace verification {	

        using namespace internals;


		// make go to the both the src and trg blobs: there check that indx of start blob is in the edge_list exactly once. Else throw.
		bool verify_that_my_index_is_in_source_target_node_edge_list(EZefRef uzr) {
			using namespace ranges;
			blob_index my_indx = index(uzr);
			blob_index src_indx = source_node_index(uzr);
			blob_index trg_indx = target_node_index(uzr);
			
			// assert src and trg have edge_list
			EZefRef src_uzr(src_indx, *graph_data(uzr));
			EZefRef trg_uzr(trg_indx, *graph_data(uzr));
			if (!has_edge_list(src_uzr))
				throw std::runtime_error("expected source blob to have an edge_list, but it did not. Graph uid: " + to_str(Graph(uzr) | uid));
			if (!has_edge_list(trg_uzr))
				throw std::runtime_error("expected target blob to have an edge_list, but it did not. Graph uid: " + to_str(Graph(uzr) | uid));

			int counter = 0;
			for (auto ind : AllEdgeIndexes(src_uzr)) {
				counter += int(ind == my_indx);
			}		
			if (counter != 1) {
				throw std::runtime_error("In verify_that_my_index_is_in_source_target_node_edge_list: index of a source node did not appear exactly once in edge_list of blob it refers to. Graph uid: " 
					+ to_str(Graph(uzr) | uid)
					+ "\n problem with edge indexes on "
					+ to_str(src_uzr)
					+ "\nexpeted index "
					+ to_str(my_indx)
				);
			}
			counter = 0;
			for (auto ind : AllEdgeIndexes(trg_uzr)) {
				counter += int(ind == -my_indx);
			}
			if (counter != 1)
				throw std::runtime_error("In verify_that_my_index_is_in_source_target_node_edge_list: index of a target node did not appear exactly once in edge_list of blob it refers to. Graph uid: "
					+ to_str(Graph(uzr) | uid)
					+ "\n problem with edge indexes on "
					+ to_str(trg_uzr)
					+ "\nexpeted index "
					+ to_str(-my_indx)
				);			
			return true;
		}
		

		bool verify_that_all_uzrs_in_my_edgelist_refer_to_me(EZefRef uzr) {
			using namespace internals;
			using namespace ranges;
			blob_index my_indx = index(uzr);
			for (auto edge_indx : AllEdgeIndexes(uzr)) {
				if (edge_indx > 0) {
					EZefRef edge_uzr(edge_indx, *graph_data(uzr));
					if (source_node_index(edge_uzr) != my_indx) {
						throw std::runtime_error("In verify_that_all_uzrs_in_my_edgelist_refer_to_me: Went to edge blob looking whether its source_index agrees with the starting blob, but it did not. Graph uid: " 
							+ to_str(Graph(uzr) | uid) 
							+ "\nsource of "
							+ to_str(edge_uzr)
							+ "\ndoes not agree with " 
							+ to_str(my_indx)
						);
					}
				}
			}
			for (auto edge_indx : AllEdgeIndexes(uzr)) {
				if (edge_indx < 0) {
					EZefRef edge_uzr(-edge_indx, *graph_data(uzr));
					if (target_node_index(edge_uzr) != my_indx) {
						throw std::runtime_error("In verify_that_all_uzrs_in_my_edgelist_refer_to_me: Went to edge blob looking whether its tarindex agrees with the starting blob, but it did not. Graph uid: "
							+ to_str(Graph(uzr) | uid)
							+ "\target of "
							+ to_str(edge_uzr)
							+ "\ndoes not agree with "
							+ to_str(my_indx)
						);
					}
				}
			}
			return true;
		}


		// check low level graph that double linking of node / edges with indexes is consistent
		bool verify_graph_double_linking(Graph& g){
			GraphData& gd = g.my_graph_data();
            // Need to use indices to avoid running off the edge of memory.
            blob_index cur_index = internals::root_node_blob_index();
			while (cur_index < gd.write_head) {
                EZefRef uzr{cur_index,gd};
				if (internals::has_source_target_node(uzr)) {
					verify_that_my_index_is_in_source_target_node_edge_list(uzr);
				}
				// deferred edge lists are the exception here: blobs in indexes appearing here will point to the true source blob and not the deferred_edge_list
				if (internals::has_edge_list(uzr) && get<BlobType>(uzr) != BlobType::DEFERRED_EDGE_LIST_NODE) {
					verify_that_all_uzrs_in_my_edgelist_refer_to_me(uzr);
				}
                cur_index += blob_index_size(uzr);
			}
			return true;
		}

	}
}
