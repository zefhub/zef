/*
 * Copyright 2022 Synchronous Technologies Pte Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "export_statement.h"

#include "zefref.h"
#include "high_level_api.h"

namespace zefDB {
    namespace imperative {

		LIBZEF_DLL_EXPORTED bool exists_at(EZefRef, TimeSlice);
		LIBZEF_DLL_EXPORTED bool exists_at(EZefRef, EZefRef tx);
		LIBZEF_DLL_EXPORTED bool exists_at(EZefRef, ZefRef tx);
		LIBZEF_DLL_EXPORTED bool exists_at(ZefRef, TimeSlice);
		LIBZEF_DLL_EXPORTED bool exists_at(ZefRef, EZefRef tx);
		LIBZEF_DLL_EXPORTED bool exists_at(ZefRef, ZefRef tx);

		LIBZEF_DLL_EXPORTED bool exists_at_now(EZefRef);

        LIBZEF_DLL_EXPORTED ZefRef to_frame(EZefRef uzr, EZefRef tx, bool allow_terminated=false);
        LIBZEF_DLL_EXPORTED ZefRef to_frame(ZefRef uzr, EZefRef tx, bool allow_terminated=false);
        LIBZEF_DLL_EXPORTED ZefRef to_frame(EZefRef uzr, ZefRef tx, bool allow_terminated=false);
        LIBZEF_DLL_EXPORTED ZefRef to_frame(ZefRef uzr, ZefRef tx, bool allow_terminated=false);

        LIBZEF_DLL_EXPORTED ZefRef now(EZefRef ezr, bool allow_terminated=false);
        LIBZEF_DLL_EXPORTED ZefRef now(ZefRef zr, bool allow_terminated=false);
        LIBZEF_DLL_EXPORTED ZefRef now(const GraphData & gd);
        LIBZEF_DLL_EXPORTED Time now();

        /* LIBZEF_DLL_EXPORTED ZefRefs to_frame(EZefRefs uzr, EZefRef tx, bool allow_terminated=false);
         * LIBZEF_DLL_EXPORTED ZefRefs to_frame(ZefRefs uzr, EZefRef tx, bool allow_terminated=false);
         * LIBZEF_DLL_EXPORTED ZefRefs to_frame(EZefRefs uzr, ZefRef tx, bool allow_terminated=false);
         * LIBZEF_DLL_EXPORTED ZefRefs to_frame(ZefRefs uzr, ZefRef tx, bool allow_terminated=false); */

        LIBZEF_DLL_EXPORTED EZefRef target(EZefRef uzr);
        LIBZEF_DLL_EXPORTED EZefRefs target(const EZefRefs& uzrs);
        LIBZEF_DLL_EXPORTED ZefRef target(ZefRef zr);
        LIBZEF_DLL_EXPORTED ZefRefs target(const ZefRefs& zrs);

        LIBZEF_DLL_EXPORTED EZefRef source(EZefRef uzr);
        LIBZEF_DLL_EXPORTED EZefRefs source(const EZefRefs& uzrs);
        LIBZEF_DLL_EXPORTED ZefRef source(ZefRef zr);
        LIBZEF_DLL_EXPORTED ZefRefs source(const ZefRefs& zrs);

        LIBZEF_DLL_EXPORTED EZefRefs ins_and_outs(const EZefRef uzr);
        LIBZEF_DLL_EXPORTED EZefRefs ins(const EZefRef uzr);
        LIBZEF_DLL_EXPORTED EZefRefs outs(const EZefRef uzr);
        LIBZEF_DLL_EXPORTED ZefRefs ins_and_outs(const ZefRef zr);
        LIBZEF_DLL_EXPORTED ZefRefs ins(const ZefRef uzr);
        LIBZEF_DLL_EXPORTED ZefRefs outs(const ZefRef uzr);

        LIBZEF_DLL_EXPORTED bool has_in(const EZefRef uzr, const RelationType rt);
        LIBZEF_DLL_EXPORTED bool has_in(const EZefRef uzr, const BlobType bt);
        LIBZEF_DLL_EXPORTED bool has_in(const ZefRef uzr, const RelationType rt);
        /* LIBZEF_DLL_EXPORTED bool has_in(const ZefRef uzr, const BlobType bt); */

        LIBZEF_DLL_EXPORTED bool has_out(const EZefRef uzr, const RelationType rt);
        LIBZEF_DLL_EXPORTED bool has_out(const EZefRef uzr, const BlobType bt);
        LIBZEF_DLL_EXPORTED bool has_out(const ZefRef uzr, const RelationType rt);
        /* LIBZEF_DLL_EXPORTED bool has_out(const ZefRef uzr, const BlobType bt); */

        LIBZEF_DLL_EXPORTED EZefRef traverse_out_edge(EZefRef, BlobType);
        LIBZEF_DLL_EXPORTED EZefRef traverse_out_node(EZefRef, BlobType);
        LIBZEF_DLL_EXPORTED EZefRef traverse_in_edge(EZefRef, BlobType);
        LIBZEF_DLL_EXPORTED EZefRef traverse_in_node(EZefRef, BlobType);
        LIBZEF_DLL_EXPORTED EZefRefs traverse_out_edge_multi(EZefRef, BlobType);
        LIBZEF_DLL_EXPORTED EZefRefs traverse_out_node_multi(EZefRef, BlobType);
        LIBZEF_DLL_EXPORTED EZefRefs traverse_in_edge_multi(EZefRef, BlobType);
        LIBZEF_DLL_EXPORTED EZefRefs traverse_in_node_multi(EZefRef, BlobType);
        LIBZEF_DLL_EXPORTED std::optional<EZefRef> traverse_out_edge_optional(EZefRef, BlobType);
        LIBZEF_DLL_EXPORTED std::optional<EZefRef> traverse_out_node_optional(EZefRef, BlobType);
        LIBZEF_DLL_EXPORTED std::optional<EZefRef> traverse_in_edge_optional(EZefRef, BlobType);
        LIBZEF_DLL_EXPORTED std::optional<EZefRef> traverse_in_node_optional(EZefRef, BlobType);

        LIBZEF_DLL_EXPORTED EZefRef traverse_out_edge(EZefRef, RelationType);
        LIBZEF_DLL_EXPORTED EZefRef traverse_out_node(EZefRef, RelationType);
        LIBZEF_DLL_EXPORTED EZefRef traverse_in_edge(EZefRef, RelationType);
        LIBZEF_DLL_EXPORTED EZefRef traverse_in_node(EZefRef, RelationType);
        LIBZEF_DLL_EXPORTED EZefRefs traverse_out_edge_multi(EZefRef, RelationType);
        LIBZEF_DLL_EXPORTED EZefRefs traverse_out_node_multi(EZefRef, RelationType);
        LIBZEF_DLL_EXPORTED EZefRefs traverse_in_edge_multi(EZefRef, RelationType);
        LIBZEF_DLL_EXPORTED EZefRefs traverse_in_node_multi(EZefRef, RelationType);
        LIBZEF_DLL_EXPORTED std::optional<EZefRef> traverse_out_edge_optional(EZefRef, RelationType);
        LIBZEF_DLL_EXPORTED std::optional<EZefRef> traverse_out_node_optional(EZefRef, RelationType);
        LIBZEF_DLL_EXPORTED std::optional<EZefRef> traverse_in_edge_optional(EZefRef, RelationType);
        LIBZEF_DLL_EXPORTED std::optional<EZefRef> traverse_in_node_optional(EZefRef, RelationType);

        LIBZEF_DLL_EXPORTED ZefRef traverse_out_edge(ZefRef, RelationType);
        LIBZEF_DLL_EXPORTED ZefRef traverse_out_node(ZefRef, RelationType);
        LIBZEF_DLL_EXPORTED ZefRef traverse_in_edge(ZefRef, RelationType);
        LIBZEF_DLL_EXPORTED ZefRef traverse_in_node(ZefRef, RelationType);
        LIBZEF_DLL_EXPORTED ZefRefs traverse_out_edge_multi(ZefRef, RelationType);
        LIBZEF_DLL_EXPORTED ZefRefs traverse_out_node_multi(ZefRef, RelationType);
        LIBZEF_DLL_EXPORTED ZefRefs traverse_in_edge_multi(ZefRef, RelationType);
        LIBZEF_DLL_EXPORTED ZefRefs traverse_in_node_multi(ZefRef, RelationType);
        LIBZEF_DLL_EXPORTED std::optional<ZefRef> traverse_out_edge_optional(ZefRef, RelationType);
        LIBZEF_DLL_EXPORTED std::optional<ZefRef> traverse_out_node_optional(ZefRef, RelationType);
        LIBZEF_DLL_EXPORTED std::optional<ZefRef> traverse_in_edge_optional(ZefRef, RelationType);
        LIBZEF_DLL_EXPORTED std::optional<ZefRef> traverse_in_node_optional(ZefRef, RelationType);

        LIBZEF_DLL_EXPORTED EZefRefs traverse_out_edge(const EZefRefs&, BlobType);
        LIBZEF_DLL_EXPORTED EZefRefs traverse_out_node(const EZefRefs&, BlobType);
        LIBZEF_DLL_EXPORTED EZefRefs traverse_in_edge(const EZefRefs&, BlobType);
        LIBZEF_DLL_EXPORTED EZefRefs traverse_in_node(const EZefRefs&, BlobType);

        LIBZEF_DLL_EXPORTED EZefRefss traverse_out_edge_multi(const EZefRefs&, BlobType);
        LIBZEF_DLL_EXPORTED EZefRefss traverse_out_node_multi(const EZefRefs&, BlobType);
        LIBZEF_DLL_EXPORTED EZefRefss traverse_in_edge_multi(const EZefRefs&, BlobType);
        LIBZEF_DLL_EXPORTED EZefRefss traverse_in_node_multi(const EZefRefs&, BlobType);

        LIBZEF_DLL_EXPORTED EZefRefs traverse_out_edge(const EZefRefs&, RelationType);
        LIBZEF_DLL_EXPORTED EZefRefs traverse_out_node(const EZefRefs&, RelationType);
        LIBZEF_DLL_EXPORTED EZefRefs traverse_in_edge(const EZefRefs&, RelationType);
        LIBZEF_DLL_EXPORTED EZefRefs traverse_in_node(const EZefRefs&, RelationType);

        LIBZEF_DLL_EXPORTED EZefRefss traverse_out_edge_multi(const EZefRefs&, RelationType);
        LIBZEF_DLL_EXPORTED EZefRefss traverse_out_node_multi(const EZefRefs&, RelationType);
        LIBZEF_DLL_EXPORTED EZefRefss traverse_in_edge_multi(const EZefRefs&, RelationType);
        LIBZEF_DLL_EXPORTED EZefRefss traverse_in_node_multi(const EZefRefs&, RelationType);

        LIBZEF_DLL_EXPORTED ZefRefs traverse_out_edge(const ZefRefs&, RelationType);
        LIBZEF_DLL_EXPORTED ZefRefs traverse_out_node(const ZefRefs&, RelationType);
        LIBZEF_DLL_EXPORTED ZefRefs traverse_in_edge(const ZefRefs&, RelationType);
        LIBZEF_DLL_EXPORTED ZefRefs traverse_in_node(const ZefRefs&, RelationType);

        LIBZEF_DLL_EXPORTED ZefRefss traverse_out_edge_multi(const ZefRefs&, RelationType);
        LIBZEF_DLL_EXPORTED ZefRefss traverse_out_node_multi(const ZefRefs&, RelationType);
        LIBZEF_DLL_EXPORTED ZefRefss traverse_in_edge_multi(const ZefRefs&, RelationType);
        LIBZEF_DLL_EXPORTED ZefRefss traverse_in_node_multi(const ZefRefs&, RelationType);




        // This is temporary and will be replaced with type comparison when that
        // moves from python into C++.
        template<class ELEM, class ITR>
        std::vector<ELEM> filter(const ITR & iterable, std::function<bool(const ELEM &)> f) {
            std::vector<ELEM> out_vec;
            std::copy_if(iterable.begin(), iterable.end(), std::back_inserter(out_vec), f);
            return out_vec;
        }
        LIBZEF_DLL_EXPORTED EZefRefs filter(const EZefRefs& uzrs, const std::function<bool(const EZefRef &)> & pred);
        LIBZEF_DLL_EXPORTED ZefRefs filter(const ZefRefs& zrs, const std::function<bool(const ZefRef &)> & pred);

        // Convenience versions
        LIBZEF_DLL_EXPORTED ZefRefs filter(const ZefRefs& zrs, EntityType et);
        LIBZEF_DLL_EXPORTED ZefRefs filter(const ZefRefs& zrs, BlobType bt);
        LIBZEF_DLL_EXPORTED ZefRefs filter(const ZefRefs& zrs, RelationType rt);
        LIBZEF_DLL_EXPORTED ZefRefs filter(const ZefRefs& zrs, ValueRepType vrt);

        LIBZEF_DLL_EXPORTED EZefRefs filter(const EZefRefs& uzrs, EntityType et);
        LIBZEF_DLL_EXPORTED EZefRefs filter(const EZefRefs& uzrs, BlobType bt);
        LIBZEF_DLL_EXPORTED EZefRefs filter(const EZefRefs& uzrs, RelationType rt);
        LIBZEF_DLL_EXPORTED EZefRefs filter(const EZefRefs& uzrs, ValueRepType vrt);

        // terminate any entity or relation
        LIBZEF_DLL_EXPORTED void terminate(EZefRef my_rel_ent);
        LIBZEF_DLL_EXPORTED void terminate(ZefRef my_rel_ent);
        /* LIBZEF_DLL_EXPORTED void terminate(EZefRefs uzrs);
         * LIBZEF_DLL_EXPORTED void terminate(ZefRefs zrs); */

        LIBZEF_DLL_EXPORTED void retire(EZefRef my_rel_ent);

        LIBZEF_DLL_EXPORTED EZefRef delegate(EZefRef uzr);
        LIBZEF_DLL_EXPORTED EZefRef delegate(ZefRef uzr);
        LIBZEF_DLL_EXPORTED std::optional<EZefRef> delegate(const Graph & g, EntityType et);
        LIBZEF_DLL_EXPORTED std::optional<EZefRef> delegate(const Graph & g, ValueRepType aet);

        LIBZEF_DLL_EXPORTED std::optional<EZefRef> delegate_to_ezr(const Delegate & d, Graph g, bool create, int order_diff=0);
        LIBZEF_DLL_EXPORTED Delegate delegate_rep(EZefRef ezr);
        LIBZEF_DLL_EXPORTED Delegate delegate_rep(ZefRef zr);

        using zefDB::assign_value;

        using value_ret_t = std::optional<value_variant_t>;

        LIBZEF_DLL_EXPORTED value_ret_t value(ZefRef ae);
        LIBZEF_DLL_EXPORTED value_ret_t value(EZefRef ae, EZefRef tx);
        LIBZEF_DLL_EXPORTED value_ret_t value(ZefRef ae, EZefRef tx);
        LIBZEF_DLL_EXPORTED value_ret_t value(EZefRef ae, ZefRef tx);
        LIBZEF_DLL_EXPORTED value_ret_t value(ZefRef ae, ZefRef tx);

        // This one only works on value nodes
        LIBZEF_DLL_EXPORTED value_ret_t value(EZefRef avn);

        /* LIBZEF_DLL_EXPORTED std::vector<value_ret_t> value(ZefRefs zrs);
         * LIBZEF_DLL_EXPORTED std::vector<value_ret_t> value(EZefRefs uzrs, EZefRef tx);
         * LIBZEF_DLL_EXPORTED std::vector<value_ret_t> value(ZefRefs zrs, EZefRef tx);
         * LIBZEF_DLL_EXPORTED std::vector<value_ret_t> value(EZefRefs uzrs, ZefRef tx);
         * LIBZEF_DLL_EXPORTED std::vector<value_ret_t> value(ZefRefs zrs, ZefRef tx); */

        LIBZEF_DLL_EXPORTED EZefRef to_ezefref(ZefRef zr);
        LIBZEF_DLL_EXPORTED EZefRef to_ezefref(EZefRef zr);

        LIBZEF_DLL_EXPORTED EternalUID origin_uid(EZefRef ezr);
        LIBZEF_DLL_EXPORTED EternalUID origin_uid(ZefRef zr);

        LIBZEF_DLL_EXPORTED EternalUID uid(const EZefRef uzr);
        LIBZEF_DLL_EXPORTED ZefRefUID uid(const ZefRef zr);

        ////////////////////////////////////
        // * templated tools

        template<class ELEM, class FUNC, class ITR>
        std::vector<ELEM> map(const ITR & iterable, FUNC f) {
            std::vector<ELEM> out_vec;
            std::transform(iterable.begin(), iterable.end(), std::back_inserter(out_vec), f);
            return out_vec;
        }

        template<class ITR>
            auto only(const ITR & iterable) {
            if(iterable.size() != 1) 
                throw std::runtime_error("only(...) but length was " + to_str(iterable.size()));
            return *iterable.begin();
        }
    }
}