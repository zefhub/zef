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


#include "ops_imperative.h"

namespace zefDB {
    namespace imperative {
        //////////////////////////////
        // * retire

		void retire(EZefRef uzr) {
            // This is only for the delegates and is deliberately separate from
            // termination of instances in order to make this harder to
            // accidentally do.


            // We are also going to allow retiring delegates that no longer have
            // any alive instances, or higher-order instances. This means any
            // relation connected to the delegate (e.g. metadata) must be
            // terminated before this is called.

            if(!internals::is_delegate(uzr))
                throw std::runtime_error("Can only retire delegates not" + to_str(uzr) + ".");

            Graph g(uzr);
            // All of these checks must happen while we have write role so that we don't check against mutating data.
            auto & gd = g.my_graph_data();
            LockGraphData lock{&gd};

            if(!exists_at_now(uzr))
                throw std::runtime_error("Delegate is already retired");
            
            for(auto & parent : traverse_out_node_multi(uzr, BT.TO_DELEGATE_EDGE)) {
                if(exists_at_now(parent))
                    throw std::runtime_error("Can't retire a delegate when it has a parent higher-order delegate.");
            }

            for(auto & rel : ins_and_outs(uzr)) {
                if(BT(rel) == BT.RELATION_EDGE && exists_at_now(rel))
                    throw std::runtime_error("Can't retire a delegate when it is the source/target of another instance/delegate.");
            }

            for(auto & instance : traverse_out_node_multi((traverse_in_edge(uzr, BT.TO_DELEGATE_EDGE)), BT.RAE_INSTANCE_EDGE)) {
                if(exists_at_now(instance))
                    throw std::runtime_error("Can't retire a delegate when it has existing instances.");
            }

            // Finally we can retire this delegate!
            Transaction transaction{gd};
            EZefRef tx_node = internals::get_or_create_and_get_tx(gd);
            auto retire_uzr = internals::instantiate(tx_node, BT.DELEGATE_RETIREMENT_EDGE, traverse_in_edge(uzr, BT.TO_DELEGATE_EDGE), g);
        }
        //////////////////////////////
        // * exists_at

		bool exists_at(EZefRef uzr, TimeSlice ts) {
            if(internals::is_delegate(uzr)) {
                // Although the node itself might have some information, we
                // should check the sequence of DELEGATE_INSTANTIATION_EDGE and
                // DELEGATE_RETIREMENT_EDGE to determine if we are in a valid
                // timeslice for the delegate.

                EZefRef to_del = traverse_in_edge(uzr, BT.TO_DELEGATE_EDGE);

                TimeSlice dummy(-1);
                TimeSlice latest_instantiation_edge = dummy;
                TimeSlice latest_retirement_edge = dummy;

                for(auto & it : traverse_in_node_multi(to_del, BT.DELEGATE_INSTANTIATION_EDGE)) {
                    // `it` should always be a TX.
                    TimeSlice this_ts(it);
                    if(this_ts <= ts && this_ts > latest_instantiation_edge)
                        latest_instantiation_edge = this_ts;
                }

                for(auto & it : traverse_in_node_multi(to_del, BT.DELEGATE_RETIREMENT_EDGE)) {
                    // `it` should always be a TX.
                    TimeSlice this_ts(it);
                    if(this_ts <= ts && this_ts > latest_retirement_edge)
                        latest_retirement_edge = this_ts;
                }

                if(latest_instantiation_edge == dummy)
                    return false;
                if(latest_retirement_edge == dummy)
                    return true;
                return (latest_instantiation_edge > latest_retirement_edge);
            }

            // TODO: This needs to be updated with the instantiation/retirement of RAEs
            switch (get<BlobType>(uzr)) {
            case BlobType::RELATION_EDGE: {
                blobs_ns::RELATION_EDGE& x = get<blobs_ns::RELATION_EDGE>(uzr);
                return ts >= x.instantiation_time_slice
                    && (x.termination_time_slice.value == 0 || ts < x.termination_time_slice);
            }
            case BlobType::ENTITY_NODE: {
                blobs_ns::ENTITY_NODE& x = get<blobs_ns::ENTITY_NODE>(uzr);
                return ts >= x.instantiation_time_slice
                    && (x.termination_time_slice.value == 0 || ts < x.termination_time_slice);
            }
            case BlobType::ATTRIBUTE_ENTITY_NODE: {
                blobs_ns::ATTRIBUTE_ENTITY_NODE& x = get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(uzr);
                return ts >= x.instantiation_time_slice
                    && (x.termination_time_slice.value == 0 || ts < x.termination_time_slice);
            }
            case BlobType::VALUE_NODE: {
                auto ez_tx = traverse_in_node(traverse_in_edge(uzr, BT.RAE_INSTANCE_EDGE), BT.INSTANTIATION_EDGE);
                return exists_at(ez_tx, ts);
            }
            case BlobType::TX_EVENT_NODE: {
                return ts >= get<blobs_ns::TX_EVENT_NODE>(uzr).time_slice;
            }
            case BlobType::ROOT_NODE: {
                return true;
            }
            default: {throw std::runtime_error("ExistsAt() called on a EZefRef (" + to_str(uzr) + ") that cannot be promoted to a ZefRef and where asking this question makes no sense."); return false; }
            }
        };
        bool exists_at(EZefRef uzr, EZefRef tx) {
            if (get<BlobType>(tx) != BlobType::TX_EVENT_NODE)
                throw std::runtime_error("ExistsAt() called with a tx that is not a TX_EVENT_NODE.");
            return exists_at(uzr, get<blobs_ns::TX_EVENT_NODE>(tx).time_slice);
        }
        bool exists_at(EZefRef uzr, ZefRef tx) {
            return exists_at(uzr, tx.blob_uzr);
        }
		bool exists_at(ZefRef zr, TimeSlice ts) {
            return exists_at(zr.blob_uzr, ts);
        }
		bool exists_at(ZefRef zr, EZefRef tx) {
            return exists_at(zr.blob_uzr, tx);
        }
		bool exists_at(ZefRef zr, ZefRef tx) {
            return exists_at(zr.blob_uzr, tx);
        }

        //////////////////////////////
        // * exists_at_now

        // This is an optimised version that uses only the latest edge in the
        // RAE_INSTANCE_EDGE or TO_DELEGATE_EDGE in order to determine whether
        // the RAE/delegate is alive.
        //
        // This relies on the instantiations/terminations being sorted chronologically.
		bool exists_at_now(EZefRef uzr) {
            if(internals::is_delegate(uzr)) {
                EZefRef to_del = internals::get_TO_DELEGATE_EDGE(uzr);
                EZefRef last_edge(internals::last_set_edge_index(to_del), *graph_data(uzr));

                if (get<BlobType>(last_edge) == BlobType::DELEGATE_RETIREMENT_EDGE) return false;
                assert(get<BlobType>(last_edge) == BlobType::DELEGATE_INSTANTIATION_EDGE
                       || get<BlobType>(last_edge) == BlobType::RAE_INSTANCE_EDGE);
                return true;
            }

            if(get<BlobType>(uzr) == BlobType::ROOT_NODE)
                return true;

            if(get<BlobType>(uzr) == BlobType::TX_EVENT_NODE)
                return true;

            if(get<BlobType>(uzr) == BlobType::VALUE_NODE)
                return true;

            // For RAEs
            internals::assert_is_this_a_rae(uzr);
			EZefRef this_re_ent_inst = internals::get_RAE_INSTANCE_EDGE(uzr);
			blob_index last_index = internals::last_set_edge_index(this_re_ent_inst);
			EZefRef last_in_edge_on_scenario_node(last_index, *graph_data(uzr));

			if (get<BlobType>(last_in_edge_on_scenario_node) == BlobType::TERMINATION_EDGE) return false;
            assert(get<BlobType>(last_in_edge_on_scenario_node) == BlobType::INSTANTIATION_EDGE
                   || get<BlobType>(last_in_edge_on_scenario_node) == BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE
                   || get<BlobType>(last_in_edge_on_scenario_node) == BlobType::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE
                   || get<BlobType>(last_in_edge_on_scenario_node) == BlobType::ORIGIN_RAE_EDGE
                   || get<BlobType>(last_in_edge_on_scenario_node) == BlobType::ASSIGN_TAG_NAME_EDGE);
            return true;
        }

        //////////////////////////////
        // * now

        ZefRef now(const GraphData& gd) {
            EZefRef tx = (gd.number_of_open_tx_sessions > 0 && gd.open_tx_thread == std::this_thread::get_id()) ?
                EZefRef(gd.index_of_open_tx_node, gd) :
                EZefRef(gd.latest_complete_tx, gd);
            return ZefRef(tx,tx);
        }

        ZefRef now(EZefRef uzr, bool allow_terminated) {
            auto & gd = *graph_data(uzr);

            // We do the exists_at_now check in here as it is more efficient
            // than the general check in to_frame.
            if (!allow_terminated && !exists_at_now(uzr))
                throw std::runtime_error("'now(EZefRef)' called on a EZefRef that does not exist at the latest time slice.");

            // We pass in allow_terminated as true to bypass the exists_at check
            // of to_frame.
            return to_frame(uzr, now(gd), true);
        }

        ZefRef now(ZefRef zr, bool allow_terminated) {
            return now(zr.blob_uzr, allow_terminated);
        }

        Time now() {
            return Time{ std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count() * 1E-6 };
        }

        //////////////////////////////
        // * to_frame

        ZefRef to_frame(EZefRef uzr, EZefRef tx, bool allow_terminated) {
            if (!is_promotable_to_zefref(uzr, tx))
                throw std::runtime_error("'to_frame' called on EZefRef that cannot be promoted to a ZefRef.");
            if (!allow_terminated && !exists_at(uzr, tx))
                throw std::runtime_error("to_frame called to promote a EZefRef that does not exist at the time slice specified.");
            return ZefRef{ uzr, tx };
        }
        ZefRef to_frame(ZefRef zr, EZefRef tx, bool allow_terminated) {
            return to_frame(zr.blob_uzr, tx, allow_terminated);
        }
        ZefRef to_frame(EZefRef uzr, ZefRef tx, bool allow_terminated) {
            return to_frame(uzr, tx.blob_uzr, allow_terminated);
        }
        ZefRef to_frame(ZefRef zr, ZefRef tx, bool allow_terminated) {
            return to_frame(zr.blob_uzr, tx.blob_uzr, allow_terminated);
        }

        ZefRefs to_frame(EZefRefs uzrs, EZefRef tx, bool allow_terminated) {
            if(!allow_terminated) {
                for(const auto & uzr : uzrs) {
                    if(!exists_at(uzr, tx))
                        throw std::runtime_error("to_frame called to promote a EZefRef that does not exist at the time slice specified.");
                }
            }
            auto res = ZefRefs(uzrs.len, tx);
            // even in a ZefRefs struct, the various elements are stored as a
            // contiguous list of EZefRefs. The reference tx is stored only once
            // TODO: THIS SEEMS WRONG IF DEFFERRED LISTS ARE USED
            // TODO: THIS SEEMS WRONG IF DEFFERRED LISTS ARE USED
            // TODO: THIS SEEMS WRONG IF DEFFERRED LISTS ARE USED
            // TODO: THIS SEEMS WRONG IF DEFFERRED LISTS ARE USED
            // TODO: THIS SEEMS WRONG IF DEFFERRED LISTS ARE USED
            std::memcpy(
                    res._get_array_begin(),
                    uzrs._get_array_begin_const(),
                    uzrs.len * sizeof(EZefRef)
            );
            return res;
        }
        // ZefRefs to_frame(ZefRefs zrs, EZefRef tx, bool allow_terminated) {
        //     return to_frame(to_ezefref(zrs), tx, allow_terminated);
        // }
        // ZefRefs to_frame(EZefRefs uzrs, ZefRef tx, bool allow_terminated) {
        //     return to_frame(uzrs, tx.blob_uzr, allow_terminated);
        // }
        // ZefRefs to_frame(ZefRefs zrs, ZefRef tx, bool allow_terminated) {
        //     return to_frame(to_ezefref(zrs), tx.blob_uzr, allow_terminated);
        // }

        //////////////////////////////
        // * target


        EZefRef target(EZefRef uzr) {
            if(!internals::has_source_target_node(uzr))
                throw std::runtime_error(" 'target(my_uzef_ref)' called for a uzr where a target node is not defined.");
            blob_index target_index = internals::target_node_index(uzr);
            return EZefRef(target_index, *graph_data(uzr));
        }
        ZefRef target(ZefRef zr) {
			return to_frame(target(EZefRef(zr)), zr.tx, true);
        }

        EZefRefs target(const EZefRefs& uzrs) {
            auto res = EZefRefs(
                    uzrs.len,
                    graph_data(uzrs)
            );
            std::transform(
                    uzrs._get_array_begin_const(),
                    uzrs._get_array_begin_const() + uzrs.len,
                    res._get_array_begin(),
                    [](EZefRef uzr) { return target(uzr); }
            );
            return res;
        }

        ZefRefs target(const ZefRefs& zrs) {
            auto res = ZefRefs(
                    zrs.len,
                    zrs.reference_frame_tx
            );
            std::transform(
                    zrs._get_array_begin_const(),
                    zrs._get_array_begin_const() + zrs.len,
                    res._get_array_begin(),
                    [](EZefRef uzr) { return target(uzr); }
            );
            return res;
        }

        //////////////////////////////
        // * source


        EZefRef source(EZefRef uzr) {
            if(!internals::has_source_target_node(uzr))
                throw std::runtime_error(" 'source(my_uzef_ref)' called for a uzr where a source node is not defined.");
            blob_index src_index = internals::source_node_index(uzr);
            return EZefRef(src_index, *graph_data(uzr));
        }
        ZefRef source(ZefRef zr) {
            return to_frame(source(EZefRef(zr)), zr.tx, true);
        }

        EZefRefs source(const EZefRefs& uzrs) {
            auto res = EZefRefs(
                    uzrs.len,
                    graph_data(uzrs)
            );
            std::transform(
                    uzrs._get_array_begin_const(),
                    uzrs._get_array_begin_const() + uzrs.len,
                    res._get_array_begin(),
                    [](EZefRef uzr) { return source(uzr); }
            );
            return res;
        }

        ZefRefs source(const ZefRefs& zrs) {
            auto res = ZefRefs(
                    zrs.len,
                    zrs.reference_frame_tx
            );
            std::transform(
                    zrs._get_array_begin_const(),
                    zrs._get_array_begin_const() + zrs.len,
                    res._get_array_begin(),
                    [](EZefRef uzr) { return source(uzr); }
            );
            return res;
        }

        //////////////////////////////
        // * ins and outs

		ZefRefs _filter_and_promote(EZefRefs ezrs, EZefRef tx) {
            auto temp = filter(ezrs,
                               [tx](EZefRef x) {
                                   return BT(x) == BT.RELATION_EDGE && exists_at(x, tx);
                                       });
            return ZefRefs{map<ZefRef>(temp, [tx](EZefRef x) { return ZefRef{x, tx}; }),
                false,
                tx};
		}

        EZefRefs ins_and_outs(const EZefRef uzr) {
            GraphData* gd = graph_data(uzr);
            // allocate enough space: this may be more than the total number of edges
            // and specifically more than the in edges only
            if (!internals::has_edges(uzr))
                throw std::runtime_error("ins_and_outs called on a EZefRef that does not have incoming or outgoing low level edges.");

            auto res = EZefRefs(
                    internals::total_edge_index_list_size_upper_limit(uzr),
                    gd
            );
            EZefRef* pos_to_write_to = res._get_array_begin();
            int counter = 0;
            for (blob_index ind : AllEdgeIndexes(uzr)) {
                if (ind != 0) { *(pos_to_write_to++) = EZefRef(ind > 0 ? ind : -ind, *gd); counter++; }
            }
            res.len = counter;
            if (res.delegate_ptr != nullptr) res.delegate_ptr->len = counter;
            return res;
        }

		ZefRefs ins_and_outs(const ZefRef zr) {
            return _filter_and_promote(ins_and_outs(EZefRef(zr)),
                                       zr.tx);
        }

        EZefRefs ins(const EZefRef uzr) {
            GraphData* gd = graph_data(uzr);
            // allocate enough space: this may be more than the total number of edges
            // and specifically more than the in edges only
            if (!internals::has_edges(uzr))
                throw std::runtime_error("ins called on a EZefRef that does not have incoming or outgoing low level edges.");

            auto res = EZefRefs(
                    internals::total_edge_index_list_size_upper_limit(uzr),
                    gd
            );
            EZefRef* pos_to_write_to = res._get_array_begin();
            int counter = 0;
            for (blob_index ind : AllEdgeIndexes(uzr)) {
                if (ind < 0) { *(pos_to_write_to++) = EZefRef(-ind, *gd); counter++; }
            }
            res.len = counter;
            if (res.delegate_ptr != nullptr) res.delegate_ptr->len = counter;
            return res;
        }

		ZefRefs ins(const ZefRef zr) {
            return _filter_and_promote(ins(EZefRef(zr)),
                                       zr.tx);
        }

        EZefRefs outs(const EZefRef uzr) {
            GraphData* gd = graph_data(uzr);
            // allocate enough space: this may be more than the total number of edges
            // and specifically more than the in edges only
            if (!internals::has_edges(uzr))
                throw std::runtime_error("outs called on a EZefRef that does not have incoming or outgoing low level edges.");

            auto res = EZefRefs(
                    internals::total_edge_index_list_size_upper_limit(uzr),
                    gd
            );
            EZefRef* pos_to_write_to = res._get_array_begin();
            int counter = 0;
            for (blob_index ind : AllEdgeIndexes(uzr)) {
                if (ind > 0) { *(pos_to_write_to++) = EZefRef(ind, *gd); counter++; }
            }
            res.len = counter;
            if (res.delegate_ptr != nullptr) res.delegate_ptr->len = counter;
            return res;
        }

		ZefRefs outs(const ZefRef zr) {
            return _filter_and_promote(outs(EZefRef(zr)),
                                       zr.tx);
        }

        //////////////////////////////////////////
        // * has_in and has_out

        bool has_in(const EZefRef ezr, const RelationType rt) {
            return length(traverse_in_edge_multi(ezr, rt)) >= 1;
        }
        bool has_in(const EZefRef ezr, const BlobType bt) {
            return length(traverse_in_edge_multi(ezr, bt)) >= 1;
        }
        bool has_in(const ZefRef zr, const RelationType rt) {
            return length(traverse_in_edge_multi(zr, rt)) >= 1;
        }
        // bool has_in(const ZefRef zr, const BlobType bt) {
        //     return length(traverse_in_edge_multi(zr, bt)) >= 1;
        // }

        bool has_out(const EZefRef ezr, const RelationType rt) {
            return length(traverse_out_edge_multi(ezr, rt)) >= 1;
        }
        bool has_out(const EZefRef ezr, const BlobType bt) {
            return length(traverse_out_edge_multi(ezr, bt)) >= 1;
        }
        bool has_out(const ZefRef zr, const RelationType rt) {
            return length(traverse_out_edge_multi(zr, rt)) >= 1;
        }
        // bool has_out(const ZefRef zr, const BlobType bt) {
        //     return length(traverse_out_edge_multi(zr, bt)) >= 1;
        // }


        //////////////////////////////
        // * Traversal

        std::optional<EZefRef> make_optional(EZefRefs zrs) {
            if (length(zrs) == 0)
                return std::optional<EZefRef>{};
            else if (length(zrs) == 1)
                return std::optional<EZefRef>(only(zrs));
            else
                throw std::runtime_error("More than one item found for O_Class");
        }
        std::optional<ZefRef> make_optional(ZefRefs zrs) {
            if (length(zrs) == 0)
                return std::optional<ZefRef>{};
            else if (length(zrs) == 1)
                return std::optional<ZefRef>(only(zrs));
            else
                throw std::runtime_error("More than one item found for O_Class");
        }
        EZefRef traverse_out_edge(EZefRef node, BlobType bt_or_rt) {
            try {
                return only(filter(outs(node), bt_or_rt));
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_edge " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRef traverse_out_node(EZefRef node, BlobType bt_or_rt) {
            try {
                return only(target(filter(outs(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_node " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRef traverse_in_edge(EZefRef node, BlobType bt_or_rt) {
            try {
                return only(filter(ins(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_edge " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRef traverse_in_node(EZefRef node, BlobType bt_or_rt) {
            try {
                return only(source(filter(ins(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_node " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRefs traverse_out_edge_multi(EZefRef node, BlobType bt_or_rt) {
            try {
                return filter(outs(node), bt_or_rt); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_edge_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRefs traverse_out_node_multi(EZefRef node, BlobType bt_or_rt) {
            try {
                return target(filter(outs(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_node_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRefs traverse_in_edge_multi(EZefRef node, BlobType bt_or_rt) {
            try {
                return filter(ins(node), bt_or_rt); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_edge_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRefs traverse_in_node_multi(EZefRef node, BlobType bt_or_rt) {
            try {
                return source(filter(ins(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_node_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<EZefRef> traverse_out_edge_optional(EZefRef node, BlobType bt_or_rt) {
            try {
                return make_optional(filter(outs(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_edge_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<EZefRef> traverse_out_node_optional(EZefRef node, BlobType bt_or_rt) {
            try {
                return make_optional(target(filter(outs(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_node_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<EZefRef> traverse_in_edge_optional(EZefRef node, BlobType bt_or_rt) {
            try {
                return make_optional(filter(ins(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_edge_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<EZefRef> traverse_in_node_optional(EZefRef node, BlobType bt_or_rt) {
            try {
                return make_optional(source(filter(ins(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_node_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }

        EZefRef traverse_out_edge(EZefRef node, RelationType bt_or_rt) {
            try {
                return only(filter(outs(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_edge " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRef traverse_out_node(EZefRef node, RelationType bt_or_rt) {
            try {
                return only(target(filter(outs(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_node " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRef traverse_in_edge(EZefRef node, RelationType bt_or_rt) {
            try {
                return only(filter(ins(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_edge " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRef traverse_in_node(EZefRef node, RelationType bt_or_rt) {
            try {
                return only(source(filter(ins(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_node " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRefs traverse_out_edge_multi(EZefRef node, RelationType bt_or_rt) {
            try {
                return filter(outs(node), bt_or_rt); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_edge_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRefs traverse_out_node_multi(EZefRef node, RelationType bt_or_rt) {
            try {
                return target(filter(outs(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_node_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRefs traverse_in_edge_multi(EZefRef node, RelationType bt_or_rt) {
            try {
                return filter(ins(node), bt_or_rt); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_edge_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRefs traverse_in_node_multi(EZefRef node, RelationType bt_or_rt) {
            try {
                return source(filter(ins(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_node_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<EZefRef> traverse_out_edge_optional(EZefRef node, RelationType bt_or_rt) {
            try {
                return make_optional(filter(outs(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_edge_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<EZefRef> traverse_out_node_optional(EZefRef node, RelationType bt_or_rt) {
            try {
                return make_optional(target(filter(outs(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_node_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<EZefRef> traverse_in_edge_optional(EZefRef node, RelationType bt_or_rt) {
            try {
                return make_optional(filter(ins(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_edge_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<EZefRef> traverse_in_node_optional(EZefRef node, RelationType bt_or_rt) {
            try {
                return make_optional(source(filter(ins(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_node_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }

        ZefRef traverse_out_edge(ZefRef node, RelationType rt) {
            ZefRefs this_outs = filter(outs(node), rt); 
            if(length(this_outs) == 0) {
                int this_time_slice = TimeSlice(node.tx);
                EZefRefs all_outs = filter(outs(to_ezefref(node)), rt);
                if(length(all_outs) > 0) {
                    throw std::runtime_error("There are no " + to_str(rt) + " found in the outgoing relations on node " + to_str(node) + " in time slice " + to_str(this_time_slice) + " however there were outgoing " + to_str(rt) + " found in other time slices.\nHint: you may have wanted to change the reference frame via `z | now` or `z | in_frame[...]`.");
                }
                ZefRefs all_ins = filter(ins(node), rt);
                if(length(all_ins) > 0) {
                    throw std::runtime_error("There was no " + to_str(rt) + " found in the outgoing relations on node " + to_str(node) + " in time slice " + to_str(this_time_slice) + " however there were incoming " + to_str(rt) + " found.\nHint: you may have wanted to use < or << instead.");
                }
                throw std::runtime_error("There was no " + to_str(rt) + " found in the outgoing relations on node " + to_str(node) + ".");
            } else if(length(this_outs) >= 2) {
                int this_time_slice = TimeSlice(node.tx);
                throw std::runtime_error("There are more than one " + to_str(rt) + " found in the outgoing relations on node " + to_str(node) + " in time slice " + to_str(this_time_slice) + ".\nHint: you may have wanted to use > L[" + to_str(rt) + "] or >> L[" + to_str(rt) + "] instead.");
            }

            try {
                return only(this_outs);
            } catch (const std::runtime_error& error) {
                throw std::runtime_error("Unexpected error while traversing outwards along " + to_str(rt) + " of " + to_str(node) + ":\n" + error.what()); 
            }
        }
        ZefRef traverse_out_node(ZefRef node, RelationType rt) {
            try {
                return target(traverse_out_edge(node, rt));
            } catch (const std::runtime_error& error) {
                throw std::runtime_error("Unexpected error while traversing outwards along " + to_str(rt) + " of " + to_str(node) + ":\n" + error.what()); 
            }
        }
        ZefRef traverse_in_edge(ZefRef node, RelationType rt) {
            ZefRefs this_ins = filter(ins(node), rt); 
            if(length(this_ins) == 0) {
                int this_time_slice = TimeSlice(node.tx);
                EZefRefs all_ins = filter(ins(to_ezefref(node)), rt);
                if(length(all_ins) > 0) {
                    throw std::runtime_error("There are no " + to_str(rt) + " found in the incoming relations on node " + to_str(node) + " in time slice " + to_str(this_time_slice) + " however there were incoming " + to_str(rt) + " found in other time slices.\nHint: you may have wanted to change the reference frame via `z | now` or `z | in_frame[...]`.");
                }
                ZefRefs all_outs = filter(outs(node), rt);
                if(length(all_outs) > 0) {
                    throw std::runtime_error("There was no " + to_str(rt) + " found in the incoming relations on node " + to_str(node) + " in time slice " + to_str(this_time_slice) + " however there were outgoing " + to_str(rt) + " found.\nHint: you may have wanted to use > or >> instead.");
                }
                throw std::runtime_error("There was no " + to_str(rt) + " found in the incoming relations on node " + to_str(node) + ".");
            } else if(length(this_ins) >= 2) {
                int this_time_slice = TimeSlice(node.tx);
                throw std::runtime_error("There are more than one " + to_str(rt) + " found in the incoming relations on node " + to_str(node) + " in time slice " + to_str(this_time_slice) + ".\nHint: you may have wanted to use < L[" + to_str(rt) + "] or << L[" + to_str(rt) + "] instead.");
            }

            try {
                return only(this_ins);
            } catch (const std::runtime_error& error) {
                throw std::runtime_error("Unexpected error while traversing inwards along " + to_str(rt) + " of " + to_str(node) + ":\n" + error.what()); 
            }
        }
        ZefRef traverse_in_node(ZefRef node, RelationType rt) {
            try {
                return source(traverse_in_edge(node, rt));
            } catch (const std::runtime_error& error) {
                throw std::runtime_error("Unexpected error while traversing inwards along " + to_str(rt) + " of " + to_str(node) + ":\n" + error.what()); 
            }
        }
        ZefRefs traverse_out_edge_multi(ZefRef node, RelationType bt_or_rt) {
            try {
                return filter(outs(node), bt_or_rt); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_edge_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        ZefRefs traverse_out_node_multi(ZefRef node, RelationType bt_or_rt) {
            try {
                return target(filter(outs(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_node_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        ZefRefs traverse_in_edge_multi(ZefRef node, RelationType bt_or_rt) {
            try {
                return filter(ins(node), bt_or_rt); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_edge_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        ZefRefs traverse_in_node_multi(ZefRef node, RelationType bt_or_rt) {
            try {
                return source(filter(ins(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_node_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<ZefRef> traverse_out_edge_optional(ZefRef node, RelationType bt_or_rt) {
            try {
                return make_optional(filter(outs(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_edge_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<ZefRef> traverse_out_node_optional(ZefRef node, RelationType bt_or_rt) {
            try {
                return make_optional(target(filter(outs(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_node_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<ZefRef> traverse_in_edge_optional(ZefRef node, RelationType bt_or_rt) {
            try {
                return make_optional(filter(ins(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_edge_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<ZefRef> traverse_in_node_optional(ZefRef node, RelationType bt_or_rt) {
            try {
                return make_optional(source(filter(ins(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_node_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }

        EZefRefs traverse_out_edge(const EZefRefs& nodes, BlobType bt_or_rt) {
            EZefRefs res(nodes);
            for (auto& el : res)
                el = traverse_out_edge(el, bt_or_rt);
            return res; 
        }
        EZefRefss traverse_out_edge_multi(const EZefRefs& uzrs, BlobType bt_or_rt) {
            EZefRefss res(length(uzrs));
            for (auto el : uzrs)
                res.v.emplace_back(traverse_out_edge_multi(el, bt_or_rt));
            return res; 
        }

        EZefRefs traverse_out_node(const EZefRefs& nodes, BlobType bt_or_rt) {
            EZefRefs res(nodes);
            for (auto& el : res)
                el = traverse_out_node(el, bt_or_rt);
            return res; 
        }
        EZefRefss traverse_out_node_multi(const EZefRefs& uzrs, BlobType bt_or_rt) {
            EZefRefss res(length(uzrs));
            for (auto el : uzrs)
                res.v.emplace_back(traverse_out_node_multi(el, bt_or_rt));
            return res; 
        }

        EZefRefs traverse_in_edge(const EZefRefs& nodes, BlobType bt_or_rt) {
            EZefRefs res(nodes);
            for (auto& el : res)
                el = traverse_in_edge(el, bt_or_rt);
            return res; 
        }
        EZefRefss traverse_in_edge_multi(const EZefRefs& uzrs, BlobType bt_or_rt) {
            EZefRefss res(length(uzrs));
            for (auto el : uzrs)
                res.v.emplace_back(traverse_in_edge_multi(el, bt_or_rt));
            return res; 
        }

        EZefRefs traverse_in_node(const EZefRefs& nodes, BlobType bt_or_rt) {
            EZefRefs res(nodes);
            for (auto& el : res)
                el = traverse_in_node(el, bt_or_rt);
            return res; 
        }
        EZefRefss traverse_in_node_multi(const EZefRefs& uzrs, BlobType bt_or_rt) {
            EZefRefss res(length(uzrs));
            for (auto el : uzrs)
                res.v.emplace_back(traverse_in_node_multi(el, bt_or_rt));
            return res; 
        }

        EZefRefs traverse_out_edge(const EZefRefs& nodes, RelationType bt_or_rt) {
            EZefRefs res(nodes);
            for (auto& el : res)
                el = traverse_out_edge(el, bt_or_rt);
            return res; 
        }
        EZefRefss traverse_out_edge_multi(const EZefRefs& uzrs, RelationType bt_or_rt) {
            EZefRefss res(length(uzrs));
            for (auto el : uzrs)
                res.v.emplace_back(traverse_out_edge_multi(el, bt_or_rt));
            return res; 
        }

        EZefRefs traverse_out_node(const EZefRefs& nodes, RelationType bt_or_rt) {
            EZefRefs res(nodes);
            for (auto& el : res)
                el = traverse_out_node(el, bt_or_rt);
            return res; 
        }
        EZefRefss traverse_out_node_multi(const EZefRefs& uzrs, RelationType bt_or_rt) {
            EZefRefss res(length(uzrs));
            for (auto el : uzrs)
                res.v.emplace_back(traverse_out_node_multi(el, bt_or_rt));
            return res; 
        }

        EZefRefs traverse_in_edge(const EZefRefs& nodes, RelationType bt_or_rt) {
            EZefRefs res(nodes);
            for (auto& el : res)
                el = traverse_in_edge(el, bt_or_rt);
            return res; 
        }
        EZefRefss traverse_in_edge_multi(const EZefRefs& uzrs, RelationType bt_or_rt) {
            EZefRefss res(length(uzrs));
            for (auto el : uzrs)
                res.v.emplace_back(traverse_in_edge_multi(el, bt_or_rt));
            return res; 
        }

        EZefRefs traverse_in_node(const EZefRefs& nodes, RelationType bt_or_rt) {
            EZefRefs res(nodes);
            for (auto& el : res)
                el = traverse_in_node(el, bt_or_rt);
            return res; 
        }
        EZefRefss traverse_in_node_multi(const EZefRefs& uzrs, RelationType bt_or_rt) {
            EZefRefss res(length(uzrs));
            for (auto el : uzrs)
                res.v.emplace_back(traverse_in_node_multi(el, bt_or_rt));
            return res; 
        }

        ZefRefs traverse_out_edge(const ZefRefs& zrs, RelationType rt) {
            auto res = ZefRefs(zrs.len, zrs.reference_frame_tx);
            std::transform( zrs._get_array_begin_const(), zrs._get_array_begin_const() + zrs.len, res._get_array_begin(),
                            [&](const EZefRef& uzr) {
                                return traverse_out_edge(ZefRef{uzr, zrs.reference_frame_tx} , rt).blob_uzr; 
                            });
            return res; 
        }
        ZefRefss traverse_out_edge_multi(const ZefRefs& zrs, RelationType rt) {
            ZefRefss res(length(zrs));
            for (auto el : zrs)
                res.v.emplace_back(traverse_out_edge_multi(el, rt));
            res.reference_frame_tx = zrs.reference_frame_tx;
            return res; 
        }

        ZefRefs traverse_out_node(const ZefRefs& zrs, RelationType rt) {
            auto res = ZefRefs(zrs.len, zrs.reference_frame_tx);
            std::transform( zrs._get_array_begin_const(), zrs._get_array_begin_const() + zrs.len, res._get_array_begin(),
                            [&](const EZefRef& uzr) {
                                return traverse_out_node(ZefRef{uzr, zrs.reference_frame_tx} , rt).blob_uzr; 
                            });
            return res; 
        }
        ZefRefss traverse_out_node_multi(const ZefRefs& zrs, RelationType rt) {
            ZefRefss res(length(zrs));
            for (auto el : zrs)
                res.v.emplace_back(traverse_out_node_multi(el, rt));
            res.reference_frame_tx = zrs.reference_frame_tx;
            return res; 
        }

        ZefRefs traverse_in_edge(const ZefRefs& zrs, RelationType rt) {
            auto res = ZefRefs(zrs.len, zrs.reference_frame_tx);
            std::transform( zrs._get_array_begin_const(), zrs._get_array_begin_const() + zrs.len, res._get_array_begin(),
                            [&](const EZefRef& uzr) {
                                return traverse_in_edge(ZefRef{uzr, zrs.reference_frame_tx} , rt).blob_uzr; 
                            });
            return res; 
        }
        ZefRefss traverse_in_edge_multi(const ZefRefs& zrs, RelationType rt) {
            ZefRefss res(length(zrs));
            for (auto el : zrs)
                res.v.emplace_back(traverse_in_edge_multi(el, rt));
            res.reference_frame_tx = zrs.reference_frame_tx;
            return res; 
        }

        ZefRefs traverse_in_node(const ZefRefs& zrs, RelationType rt) {
            auto res = ZefRefs(zrs.len, zrs.reference_frame_tx);
            std::transform( zrs._get_array_begin_const(), zrs._get_array_begin_const() + zrs.len, res._get_array_begin(),
                            [&](const EZefRef& uzr) {
                                return traverse_in_node(ZefRef{uzr, zrs.reference_frame_tx} , rt).blob_uzr; 
                            });
            return res; 
        }
        ZefRefss traverse_in_node_multi(const ZefRefs& zrs, RelationType rt) {
            ZefRefss res(length(zrs));
            for (auto el : zrs)
                res.v.emplace_back(traverse_in_node_multi(el, rt));
            res.reference_frame_tx = zrs.reference_frame_tx;
            return res; 
        }




        // -------------------------------------------------------------------------------
        //////////////////////////////
        // * filter

        EZefRefs filter(const EZefRefs& input, const std::function<bool(const EZefRef &)>& pred) {
            // std::cerr << "Filtering using input length of: " << input.len << std::endl;
            EZefRefs res(
                length(input),
                // graph_data_ptr(input)
                graph_data(input)
            );
            const EZefRef* read_it = input._get_array_begin_const();
            auto write_it = res._get_array_begin();
            int count = 0;
            for (auto it = read_it; it != read_it + input.len; ++it) {
                if (pred(*it)) {    // this is the same function as xor (predicate, not_in_activated_flag)
                    *(write_it++) = *it;
                    count++;
                }
            }
            res.len = count;				
            if (res.delegate_ptr != nullptr)
                res.delegate_ptr->len = count;
            return res;
        }
        ZefRefs filter(const ZefRefs& input, const std::function<bool(const ZefRef &)>& pred) {
            auto res = ZefRefs(
                    input.len,
                    input.reference_frame_tx
            );

            const EZefRef* read_it = input._get_array_begin_const();
            EZefRef* write_it = res._get_array_begin();
            int count = 0;
            for (auto it = read_it; it != read_it + input.len; ++it) {
                if (pred(ZefRef(*it, input.reference_frame_tx))) {
                    *(write_it++) = *it;
                    count++;
                }
            }
            res.len = count;
            if (res.delegate_ptr != nullptr)
                res.delegate_ptr->len = count;
            return res;
        }


        ZefRefs filter(const ZefRefs& zrs, EntityType et) {
            return filter(zrs, [&et](ZefRef zr) {
                return is_zef_subtype(zr, et);
            });
        }
        ZefRefs filter(const ZefRefs& zrs, BlobType bt) {
            return filter(zrs, [&bt](ZefRef zr) {
                return is_zef_subtype(zr, bt);
            });
        }
        ZefRefs filter(const ZefRefs& zrs, RelationType rt) {
            return filter(zrs, [&rt](ZefRef zr) {
                return is_zef_subtype(zr, rt);
            });
        }
        ZefRefs filter(const ZefRefs& zrs, ValueRepType vrt) {
            return filter(zrs, [&vrt](ZefRef zr) {
                return is_zef_subtype(zr, vrt);
            });
        }

        EZefRefs filter(const EZefRefs& uzrs, EntityType et) {
            return filter(uzrs, [&et](EZefRef uzr) {
                return is_zef_subtype(uzr, et);
            });
        }
        EZefRefs filter(const EZefRefs& uzrs, BlobType bt) {
            return filter(uzrs, [&bt](EZefRef uzr) {
                return is_zef_subtype(uzr, bt);
            });
        }
        EZefRefs filter(const EZefRefs& uzrs, RelationType rt) {
            return filter(uzrs, [&rt](EZefRef uzr) {
                return is_zef_subtype(uzr, rt);
            });
        }
        EZefRefs filter(const EZefRefs& uzrs, ValueRepType vrt) {
            return filter(uzrs, [&vrt](EZefRef uzr) {
                return is_zef_subtype(uzr, vrt);
            });
        }

        //////////////////////////////
        // * terminate

    
        // terminate any entity or relation
        void terminate(EZefRef my_rel_ent) {
            GraphData& gd = *graph_data(my_rel_ent);
            if (!gd.is_primary_instance)
                throw std::runtime_error("'terminate' called for a graph which is not a primary instance. This is not allowed. Shame on you!");
            if (internals::is_delegate(my_rel_ent))
                throw std::runtime_error("Terminate called on a delegate. This is not allowed. At most, delegates may be tagged as 'disabled' in the future.");

            // tasks::apply_immediate_updates_from_zm();
            // TODO: check whether locally owned!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            using namespace internals;
            assert_is_this_a_rae(my_rel_ent);
            if (is_terminated(my_rel_ent))
                throw std::runtime_error("Terminate called on already terminated entity or relation.");

            auto my_tx_to_keep_this_open_in_this_fct = Transaction(gd);		
            EZefRef tx_node = get_or_create_and_get_tx(gd);

            // execute zefhook before the rel_ent is terminated
            ZefRef my_rel_ent_now = to_frame(my_rel_ent, tx_node);

            EZefRef RAE_INSTANCE_EDGE = get_RAE_INSTANCE_EDGE(my_rel_ent);
            blobs_ns::TERMINATION_EDGE& my_termination_edge = get_next_free_writable_blob<blobs_ns::TERMINATION_EDGE>(gd);
            MMap::ensure_or_alloc_range(&my_termination_edge, blobs_ns::max_basic_blob_size);
            my_termination_edge.this_BlobType = BlobType::TERMINATION_EDGE;
            switch (get<BlobType>(my_rel_ent)) {
            case BlobType::ENTITY_NODE: {get<blobs_ns::ENTITY_NODE>(my_rel_ent).termination_time_slice = get<blobs_ns::TX_EVENT_NODE>(tx_node).time_slice; break; }
            case BlobType::ATTRIBUTE_ENTITY_NODE: {get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(my_rel_ent).termination_time_slice = get<blobs_ns::TX_EVENT_NODE>(tx_node).time_slice; break; }
            case BlobType::RELATION_EDGE: {get<blobs_ns::RELATION_EDGE>(my_rel_ent).termination_time_slice = get<blobs_ns::TX_EVENT_NODE>(tx_node).time_slice; break; }
            default: {throw std::runtime_error("termiate called on a EZefRef pointing to a blob type where the concept of termination makes no sense."); }
            }
            move_head_forward(gd);
            my_termination_edge.source_node_index = index(tx_node);
            my_termination_edge.target_node_index = index(RAE_INSTANCE_EDGE);
            blob_index this_termination_edge_index = index(EZefRef((void*)&my_termination_edge));
            append_edge_index(tx_node, this_termination_edge_index);
            append_edge_index(RAE_INSTANCE_EDGE, -this_termination_edge_index);
            //terminate all edges that have not been terminated yet
            for (EZefRef ed : ins_and_outs(my_rel_ent))
                // Note: this can't be done in a filter until filters are lazily evaluated.
                if(is_promotable_to_zefref(ed) && exists_at(ed, tx_node))
                    terminate(ed);
        }
        void terminate(ZefRef my_rel_ent) {
            terminate(EZefRef(my_rel_ent));
        }

        // void terminate(EZefRefs uzrs) {
        //     // This will check before terminating each EZefRef whether it currently
        //     // exists. e.g. a problem can occur if terminating an entity also
        //     // terminates a relation that is in the list.
        //     GraphData * gd_ptr = graph_data(uzrs);
        //     if(!gd_ptr)
        //         return;
        //     GraphData& gd = *gd_ptr;
        //     auto my_tx_to_keep_this_open_in_this_fct = Transaction(gd);		
        //     EZefRef tx_node = internals::get_or_create_and_get_tx(gd);

        //     for (EZefRef uzr : uzrs)
        //         // Note: this can't be done in a filter until filters are lazily evaluated.
        //         if(exists_at(uzr, tx_node))
        //             terminate(uzr);
        // }
        // void terminate(ZefRefs zrs) {
        //     terminate(to_ezefref(zrs));
        // }

        //////////////////////////////
        // * delegate

        EZefRef delegate(EZefRef uzr) {
            // return the delegate RelEnt for a given instance
            return target(traverse_in_node(uzr, BT.RAE_INSTANCE_EDGE));
        }
        EZefRef delegate(ZefRef zr) {
            return delegate(to_ezefref(zr));
        }
        std::optional<EZefRef> delegate(const Graph & g, EntityType et) {
            GraphData& gd = g.my_graph_data();
            EZefRef root_ezr{constants::ROOT_NODE_blob_index, gd};
            auto temp = filter(traverse_out_node_multi(root_ezr, BT.TO_DELEGATE_EDGE), et);
            if(length(temp) == 0)
                return {};
            return only(temp);
        }

        std::optional<EZefRef> delegate(const Graph & g, ValueRepType vrt) {
            GraphData& gd = g.my_graph_data();
            EZefRef root_ezr{constants::ROOT_NODE_blob_index, gd};
            auto temp = filter(traverse_out_node_multi(root_ezr, BT.TO_DELEGATE_EDGE), vrt);
            if(length(temp) == 0)
                return {};
            return only(temp);
        }

        std::optional<EZefRef> delegate_to_ezr(const EntityType & et, int order, Graph g, bool create) {
            auto & gd = g.my_graph_data();
            EZefRef root{constants::ROOT_NODE_blob_index, gd};
            EZefRef z = root;
            for(int i = 0 ; i < order ; i++) {
                EZefRefs opts = filter(traverse_out_node_multi(z, BT.TO_DELEGATE_EDGE), et);
                if(length(opts) == 0) {
                    if(create) {
                        EZefRef tx = internals::get_or_create_and_get_tx(gd);
                        EZefRef new_z = internals::instantiate(BT.ENTITY_NODE, gd);
                        get<blobs_ns::ENTITY_NODE>(new_z).entity_type = et;
                        get<blobs_ns::ENTITY_NODE>(new_z).instantiation_time_slice = get<blobs_ns::TX_EVENT_NODE>(tx).time_slice;
                        EZefRef new_to_delegate_edge = internals::instantiate(z, BT.TO_DELEGATE_EDGE, new_z, gd);
                        EZefRef new_del_inst_edge = internals::instantiate(tx, BT.DELEGATE_INSTANTIATION_EDGE, new_to_delegate_edge, gd);
                        internals::apply_action_ENTITY_NODE(gd, new_z, true);
                        internals::apply_action_DELEGATE_INSTANTIATION_EDGE(gd, new_del_inst_edge, true);
                        internals::apply_action_TO_DELEGATE_EDGE(gd, new_to_delegate_edge, true);
                        z = new_z;
                    } else
                        return {};
                } else {
                    z = only(opts);
                }
            }

            return z;
        }

        std::optional<EZefRef> delegate_to_ezr(const ValueRepType & vrt, int order, Graph g, bool create) {
            auto & gd = g.my_graph_data();
            EZefRef root{constants::ROOT_NODE_blob_index, gd};
            EZefRef z = root;
            for(int i = 0 ; i < order ; i++) {
                EZefRefs opts = filter(traverse_out_node_multi(z, BT.TO_DELEGATE_EDGE), vrt);
                if(length(opts) == 0) {
                    if(create) {
                        EZefRef tx = internals::get_or_create_and_get_tx(gd);
                        EZefRef new_z = internals::instantiate(BT.ATTRIBUTE_ENTITY_NODE, gd);
                        get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(new_z).primitive_type = vrt;
                        get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(new_z).instantiation_time_slice = get<blobs_ns::TX_EVENT_NODE>(tx).time_slice;
                        EZefRef new_to_delegate_edge = internals::instantiate(z, BT.TO_DELEGATE_EDGE, new_z, gd);
                        EZefRef new_del_inst_edge = internals::instantiate(tx, BT.DELEGATE_INSTANTIATION_EDGE, new_to_delegate_edge, gd);
                        internals::apply_action_ATTRIBUTE_ENTITY_NODE(gd, new_z, true);
                        internals::apply_action_DELEGATE_INSTANTIATION_EDGE(gd, new_del_inst_edge, true);
                        internals::apply_action_TO_DELEGATE_EDGE(gd, new_to_delegate_edge, true);
                        z = new_z;
                    } else
                        return {};
                } else {
                    z = only(opts);
                }
            }

            return z;
        }

        std::optional<EZefRef> delegate_to_ezr(const RelationType & rt, int order, Graph g, bool create) {
            auto & gd = g.my_graph_data();
            EZefRef root{constants::ROOT_NODE_blob_index, gd};
            EZefRef z = root;
            for(int i = 0 ; i < order ; i++) {
                EZefRefs opts = filter(traverse_out_node_multi(z, BT.TO_DELEGATE_EDGE), rt);
                opts = filter(opts, internals::is_delegate_relation_group);
                if(length(opts) == 0) {
                    if(create) {
                        EZefRef tx = internals::get_or_create_and_get_tx(gd);
                        EZefRef new_z = internals::instantiate(BT.RELATION_EDGE, gd);
                        get<blobs_ns::RELATION_EDGE>(new_z).relation_type = rt;
                        get<blobs_ns::RELATION_EDGE>(new_z).instantiation_time_slice = get<blobs_ns::TX_EVENT_NODE>(tx).time_slice;
                        get<blobs_ns::RELATION_EDGE>(new_z).source_node_index = index(new_z);
                        get<blobs_ns::RELATION_EDGE>(new_z).target_node_index = index(new_z);
                        internals::append_edge_index(new_z, index(new_z));
                        internals::append_edge_index(new_z, -index(new_z));
                        EZefRef new_to_delegate_edge = internals::instantiate(z, BT.TO_DELEGATE_EDGE, new_z, gd);
                        EZefRef new_del_inst_edge = internals::instantiate(tx, BT.DELEGATE_INSTANTIATION_EDGE, new_to_delegate_edge, gd);
                        internals::apply_action_RELATION_EDGE(gd, new_z, true);
                        internals::apply_action_TO_DELEGATE_EDGE(gd, new_to_delegate_edge, true);
                        internals::apply_action_DELEGATE_INSTANTIATION_EDGE(gd, new_del_inst_edge, true);
                        z = new_z;
                    } else
                        return {};
                } else {
                    z = only(opts);
                }
            }

            return z;
        }

        std::optional<EZefRef> delegate_to_ezr(const DelegateRelationTriple & d, int order, Graph g, bool create) {
            auto & gd = g.my_graph_data();
            EZefRef root{constants::ROOT_NODE_blob_index, gd};

            std::optional<EZefRef> d_src = delegate_to_ezr(*(d.source), g, create, 1);
            std::optional<EZefRef> d_trg = delegate_to_ezr(*(d.target), g, create, 1);

            auto rel_group = delegate_to_ezr(d.rt, 1, g, create);
            if(!rel_group)
                return {};
            EZefRef z = *rel_group;
            for(int i = 0 ; i < order ; i++) {
                if(!d_src || !d_trg)
                    return {};
                EZefRefs opts = filter(traverse_out_node_multi(z, BT.TO_DELEGATE_EDGE), d.rt);
                opts = filter(opts, [&](EZefRef z) { return source(z) == *d_src && target(z) == *d_trg; });
                if(length(opts) == 0) {
                    if(create) {
                        EZefRef tx = internals::get_or_create_and_get_tx(gd);
                        EZefRef new_z = internals::instantiate(*d_src, BT.RELATION_EDGE, *d_trg, gd);
                        get<blobs_ns::RELATION_EDGE>(new_z).relation_type = d.rt;
                        get<blobs_ns::RELATION_EDGE>(new_z).instantiation_time_slice = get<blobs_ns::TX_EVENT_NODE>(tx).time_slice;
                        EZefRef new_to_delegate_edge = internals::instantiate(z, BT.TO_DELEGATE_EDGE, new_z, gd);
                        EZefRef new_del_inst_edge = internals::instantiate(tx, BT.DELEGATE_INSTANTIATION_EDGE, new_to_delegate_edge, gd);
                        internals::apply_action_RELATION_EDGE(gd, new_z, true);
                        internals::apply_action_TO_DELEGATE_EDGE(gd, new_to_delegate_edge, true);
                        internals::apply_action_DELEGATE_INSTANTIATION_EDGE(gd, new_del_inst_edge, true);
                        z = new_z;
                    } else
                        return {};
                } else {
                    z = only(opts);
                }
                // Don't create these on the last loop, as that's unnecessary
                if(i < order-1) {
                    d_src = delegate_to_ezr(delegate_of(*d_src), g, create);
                    d_trg = delegate_to_ezr(delegate_of(*d_trg), g, create);
                }
            }

            return z;
        }

        std::optional<EZefRef> delegate_to_ezr(const DelegateTX & d, int order, Graph g, bool create) {
            auto & gd = g.my_graph_data();
            EZefRef root{constants::ROOT_NODE_blob_index, gd};

            EZefRef z = root;
            for(int i = 0 ; i < order ; i++) {
                EZefRefs opts = filter(traverse_out_node_multi(root, BT.TO_DELEGATE_EDGE),
                                       [&](EZefRef z) { return BT(z) == BT.TX_EVENT_NODE; });
                if(length(opts) == 0) {
                    if(create) {
                        EZefRef tx = internals::get_or_create_and_get_tx(gd);
                        EZefRef new_z = internals::instantiate(BT.TX_EVENT_NODE, gd);
                        EZefRef new_to_delegate_edge = internals::instantiate(z, BT.TO_DELEGATE_EDGE, new_z, gd);
                        EZefRef new_del_inst_edge = internals::instantiate(tx, BT.DELEGATE_INSTANTIATION_EDGE, new_to_delegate_edge, gd);
                        internals::apply_action_TX_EVENT_NODE(gd, new_z, true);
                        internals::apply_action_TO_DELEGATE_EDGE(gd, new_to_delegate_edge, true);
                        internals::apply_action_DELEGATE_INSTANTIATION_EDGE(gd, new_del_inst_edge, true);
                        z = new_z;
                    } else
                        return {};
                } else {
                    z = only(opts);
                }
            }

            return z;
        }

        std::optional<EZefRef> delegate_to_ezr(const DelegateRoot & d, int order, Graph g, bool create) {
            auto & gd = g.my_graph_data();
            EZefRef root{constants::ROOT_NODE_blob_index, gd};

            EZefRef z = root;
            for(int i = 0 ; i < order ; i++) {
                EZefRefs opts = filter(traverse_out_node_multi(root, BT.TO_DELEGATE_EDGE),
                                       [&](EZefRef z) { return BT(z) == BT.ROOT_NODE; });
                if(length(opts) == 0) {
                    if(create) {
                        EZefRef tx = internals::get_or_create_and_get_tx(gd);
                        EZefRef new_z = internals::instantiate(BT.ROOT_NODE, gd);
                        EZefRef new_to_delegate_edge = internals::instantiate(z, BT.TO_DELEGATE_EDGE, new_z, gd);
                        EZefRef new_del_inst_edge = internals::instantiate(tx, BT.DELEGATE_INSTANTIATION_EDGE, new_to_delegate_edge, gd);
                        internals::apply_action_ROOT_NODE(gd, new_z, true);
                        internals::apply_action_TO_DELEGATE_EDGE(gd, new_to_delegate_edge, true);
                        internals::apply_action_DELEGATE_INSTANTIATION_EDGE(gd, new_del_inst_edge, true);
                        z = new_z;
                    } else
                        return {};
                } else {
                    z = only(opts);
                }
            }

            return z;
        }

        std::optional<EZefRef> delegate_to_ezr(const Delegate & d, Graph g, bool create, int order_diff) {
            auto & gd = g.my_graph_data();

            int actual_order = d.order + order_diff;
            
            if(actual_order == 0) {
                throw std::runtime_error("Can't obtain EZefRef of a delegate of order 0");
            }
            // To try and combine all additions into one transaction. Will do two attempts in the case of creating.
            std::optional<EZefRef> res =  std::visit([&](auto & x) { return delegate_to_ezr(x, actual_order, g, false); },
                                                     d.item);
            if(!res) {
                if(create) {
                    auto tx = Transaction(gd);
                    res =  std::visit([&](auto & x) { return delegate_to_ezr(x, actual_order, g, true); },
                                      d.item);
                    return res;
                } else 
                    return {};
            } else {
                if(!exists_at_now(*res)) {
                    if(create) {
                        // Assign new instantiation edges all the way down.
                        EZefRef tx = internals::get_or_create_and_get_tx(gd);
                        EZefRef cur_d = *res;
                        while(!exists_at_now(cur_d)) {
                            internals::instantiate(tx, BT.DELEGATE_INSTANTIATION_EDGE, traverse_in_edge(cur_d, BT.TO_DELEGATE_EDGE), gd);
                            cur_d = traverse_in_node(cur_d, BT.TO_DELEGATE_EDGE);
                        }
                    } else {
                        return {};
                    }
                }
                return res;
            }
        }

        Delegate delegate_rep(ZefRef zr) {
            return delegate_rep(to_ezefref(zr));
        }

        Delegate delegate_rep(EZefRef ezr) {
            if(is_zef_subtype(ezr, ET)
               || is_zef_subtype(ezr, VRT)
               || is_delegate_relation_group(ezr)
               || BT(ezr) == BT.TX_EVENT_NODE
               || BT(ezr) == BT.ROOT_NODE) {
                int order = 0;
                EZefRef cur = ezr;
                while(has_in(cur, BT.TO_DELEGATE_EDGE)) {
                    order++;
                    cur = traverse_in_node(cur, BT.TO_DELEGATE_EDGE);
                }
                // Note: we may end up with order 0 at the end here - this is
                // acceptable, and means this is an instance!
                if(is_zef_subtype(ezr, ET))
                    return Delegate{order, ET(ezr)};
                if(is_zef_subtype(ezr, VRT))
                    return Delegate{order, VRT(ezr)};
                if(is_zef_subtype(ezr, RT))
                    return Delegate{order, RT(ezr)};
                if(BT(ezr) == BT.TX_EVENT_NODE)
                    return Delegate{order, DelegateTX{}};
                if(BT(ezr) == BT.ROOT_NODE)
                    return Delegate{order, DelegateRoot{}};
            }

            if(is_zef_subtype(ezr, RT)) {
                // A relation but not a delegate group
                Delegate src = delegate_rep(source(ezr));
                Delegate trg = delegate_rep(target(ezr));

                int order = 0;
                EZefRef cur = ezr;
                while(has_in(cur, BT.TO_DELEGATE_EDGE) && !is_delegate_relation_group(cur)) {
                    order++;
                    src.order--;
                    trg.order--;
                    cur = traverse_in_node(cur, BT.TO_DELEGATE_EDGE);
                }

                return Delegate{order, DelegateRelationTriple{RT(ezr), src, trg}};
            }

            throw std::runtime_error("Don't know how to get delegate from " + to_str(ezr));
        }
    }

    namespace imperative {

        value_ret_t value(ZefRef z) {
            if (get<BlobType>(z.blob_uzr) == BlobType::ATTRIBUTE_ENTITY_NODE) {
                auto & ae = get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(z.blob_uzr);
                auto vrt = ae.primitive_type.value;
                switch (vrt) {
                case VRT.Float.value: { return value_from_ae<double>(z); }
                case VRT.Int.value: { return value_from_ae<int>(z); }
                case VRT.Bool.value: { return value_from_ae<bool>(z); }
                case VRT.String.value: { return value_from_ae<std::string>(z); }
                case VRT.Time.value: { return value_from_ae<Time>(z); }
                case VRT.Serialized.value: { return value_from_ae<SerializedValue>(z); }
                case VRT.Any.value: { return value_from_ae<value_variant_t>(z); }
                case VRT.Type.value: { return value_from_ae<AttributeEntityType>(z); }
                default: {
                    switch (vrt % 16) {
                    case 1: { return value_from_ae<ZefEnumValue>(z); }
                    case 2: { return value_from_ae<QuantityFloat>(z); }
                    case 3: { return value_from_ae<QuantityInt>(z); }
                    default: throw std::runtime_error("Return type not implemented.");
                    }
                }
                }
            } else if (get<BlobType>(z.blob_uzr) == BlobType::VALUE_NODE) {
                auto & ent = get<blobs_ns::VALUE_NODE>(z.blob_uzr);
                return internals::value_from_node<value_variant_t>(ent);
            } else {
            throw std::runtime_error("'value(zefref)' called for a zefref which is not an atomic entity.");
            }
        }
        value_ret_t value(EZefRef z, EZefRef tx) {
            return value(to_frame(z, tx));
        }
        value_ret_t value(ZefRef z, EZefRef tx) {
            return value(to_frame(z, tx));
        }
        value_ret_t value(EZefRef z, ZefRef tx) {
            return value(to_frame(z, tx));
        }
        value_ret_t value(ZefRef z, ZefRef tx) {
            return value(to_frame(z, tx));
        }

        value_ret_t value(EZefRef z) {
            if (get<BlobType>(z) == BlobType::ATTRIBUTE_ENTITY_NODE) {
                throw std::runtime_error("Need a graph slice to extract an AE's value.");
            } else if (get<BlobType>(z) == BlobType::VALUE_NODE) {
                auto & ent = get<blobs_ns::VALUE_NODE>(z);
                return internals::value_from_node<value_variant_t>(ent);
            } else {
                throw std::runtime_error("'value(zefref)' called for a zefref which is not an atomic entity.");
            }
        }

        // std::vector<value_ret_t> value(ZefRefs zrs) {
        //     std::vector<value_ret_t> res;
        //     res.reserve(length(zrs));
        //     std::transform( 
        //         zrs.begin(),
        //         zrs.end(),
        //         std::back_inserter(res),
        //         [](const ZefRef& zr) { return value(zr); }
        //     );
        //     return res;
        // }

        // std::vector<value_ret_t> value(EZefRefs uzrs, EZefRef tx) {
        //     return value(to_frame(uzrs, tx));
        // }
        // std::vector<value_ret_t> value(ZefRefs zrs, EZefRef tx) {
        //     return value(to_frame(zrs, tx));
        // }
        // std::vector<value_ret_t> value(EZefRefs uzrs, ZefRef tx) {
        //     return value(to_frame(uzrs, tx));
        // }
        // std::vector<value_ret_t> value(ZefRefs zrs, ZefRef tx) {
        //     return value(to_frame(zrs, tx));
        // }

        //////////////////////////////
        // * to_ezefref

        EZefRef to_ezefref(ZefRef zr) {
            return zr.blob_uzr; 
        }
        EZefRef to_ezefref(EZefRef zr) {
            return zr;
        }

        //////////////////////////////
        // * origin_uid

        EternalUID origin_uid(EZefRef ezr) {
            if(!internals::has_uid(ezr))
                throw std::runtime_error("origin_uid can't take the uid of a " + to_str(BT(ezr)));
            if(BT(ezr) == BT.TX_EVENT_NODE ||
               BT(ezr) == BT.ROOT_NODE)
                return uid(ezr);

            EZefRefs origin_candidates = traverse_out_node_multi(
                                        traverse_in_edge(ezr, BT.RAE_INSTANCE_EDGE),
                                        BT.ORIGIN_RAE_EDGE);
            if(length(origin_candidates) == 0) {
                // z itself is the origin
                return uid(ezr);
            }
            EZefRef z_or = only(origin_candidates);
            return uid(z_or);
        }

        EternalUID origin_uid(ZefRef zr) {
            return origin_uid(zr.blob_uzr);
        }
       
        //////////////////////////////
        // * uid

        EternalUID uid(const EZefRef uzr) {
            // Special handling for foreign RAEs. We want invertibility for
            // g[uid(z)] to be true, which means returning the foreign
            // EternalUID in that case.
            if(internals::is_foreign_rae_blob(BT(uzr))) {
                if(BT(uzr) == BT.FOREIGN_GRAPH_NODE)
                    return EternalUID(internals::get_blob_uid(uzr),
                                      internals::get_blob_uid(uzr));
                else
                    return EternalUID(internals::get_blob_uid(uzr),
                                      internals::get_blob_uid(traverse_out_node(uzr, BT.ORIGIN_GRAPH_EDGE)));
            }
            return EternalUID(internals::get_blob_uid(uzr),
                              internals::get_graph_uid(uzr));
        }
        ZefRefUID uid(const ZefRef zr) {
            // Note that it makes no sense to have a foreign blob as a
            // ZefRef. In fact it is downright confusing - does the graph
            // UID belong to the current graph or the foreign graph? So
            // reject this outright.
            if(internals::is_foreign_rae_blob(BT(zr)))
                throw std::runtime_error("Cannot get the ZefRefUID of a ZefRef pointing at a foreign RAE blob. You should convert to an EZefRef first.");
            return ZefRefUID(internals::get_blob_uid(to_ezefref(zr)),
                             internals::get_blob_uid(to_ezefref(zr.tx)),
                             internals::get_graph_uid(to_ezefref(zr)));
        }

    }

}