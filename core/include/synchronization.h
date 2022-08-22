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

#include "fwd_declarations.h"
#include "scalars.h"
#include "zefref.h"
#include "graph.h"

namespace zefDB {

	namespace internals {
        // Apply action blob now uses a blob_index to refer to "from where should I update double-linking".
        // But we keep the bool version for backwards compatibility.

        void apply_action_blob(GraphData& gd, EZefRef uzr_to_blob, bool fill_caches);
        void apply_action_lookup(GraphData& gd, EZefRef uzr, bool fill_key_dict);

        void apply_action_ROOT_NODE(GraphData& gd, EZefRef uzr_to_blob, bool fill_key_dict);

        void apply_action_ATTRIBUTE_ENTITY_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        void apply_action_ENTITY_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict) ;										 
        void apply_action_RELATION_EDGE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict) ;								 
        void apply_action_TX_EVENT_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        // void apply_action_DEFERRED_EDGE_LIST_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict, blob_index latest_blob_to_double_link, std::unordered_map<blob_index,blob_index> & latest_deferred);
        void apply_action_DEFERRED_EDGE_LIST_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        void apply_action_ASSIGN_TAG_NAME_EDGE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        void apply_action_FOREIGN_GRAPH_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        void apply_action_FOREIGN_ENTITY_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        void apply_action_FOREIGN_ATTRIBUTE_ENTITY_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        void apply_action_FOREIGN_RELATION_EDGE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        void apply_action_TERMINATION_EDGE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        LIBZEF_DLL_EXPORTED void apply_action_ATOMIC_VALUE_ASSIGNMENT_EDGE(GraphData & gd, EZefRef uzr, bool fill_key_dict);
        void apply_action_VALUE_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        void apply_action_TO_DELEGATE_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches);
        void apply_action_DELEGATE_INSTANTIATION_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches);

        void unapply_action_blob(GraphData& gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_ROOT_NODE(GraphData& gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_ATTRIBUTE_ENTITY_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_ENTITY_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_RELATION_EDGE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_TX_EVENT_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_DEFERRED_EDGE_LIST_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_ASSIGN_TAG_NAME_EDGE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_FOREIGN_GRAPH_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_FOREIGN_ENTITY_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_FOREIGN_ATTRIBUTE_ENTITY_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_FOREIGN_RELATION_EDGE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_TERMINATION_EDGE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_ATOMIC_VALUE_ASSIGNMENT_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches);
        void unapply_action_VALUE_NODE(GraphData & gd, EZefRef uzr, bool fill_caches);
        void unapply_action_TO_DELEGATE_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches);
        void unapply_action_DELEGATE_INSTANTIATION_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches);


        LIBZEF_DLL_EXPORTED void apply_double_linking(GraphData& gd, blob_index start_index, blob_index end_index);
        LIBZEF_DLL_EXPORTED void undo_double_linking(GraphData& gd, blob_index start_index, blob_index end_index);
        LIBZEF_DLL_EXPORTED void apply_actions_to_blob_range_only_key_dict(GraphData& gd, blob_index blob_index_lo, blob_index blob_index_hi);

	}
}
