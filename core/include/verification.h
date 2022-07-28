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
#include "zefDB_utils.h"
#include "scalars.h"
#include "zefref.h"
#include "blobs.h"
/* #include "zef_script.h" */
#include "graph.h"

#include "low_level_api.h"

namespace zefDB {
    namespace verification {	



        // make go to the both the src and trg blobs: there check that indx of start blob is in the edge_list exactly once. Else throw.
        LIBZEF_DLL_EXPORTED bool verify_that_my_index_is_in_source_target_node_edge_list(EZefRef uzr);
        

        LIBZEF_DLL_EXPORTED bool verify_that_all_uzrs_in_my_edgelist_refer_to_me(EZefRef uzr);

        // check low level graph that double linking of node / edges with indexes is consistent
        LIBZEF_DLL_EXPORTED bool verify_graph_double_linking(Graph& g);
        LIBZEF_DLL_EXPORTED bool verify_chronological_instantiation_order(Graph g);
        LIBZEF_DLL_EXPORTED void break_graph(Graph&g, blob_index index, int style);

        inline bool verify_graph(Graph&g) {
            try {
            return (verify_graph_double_linking(g)
                    && verify_chronological_instantiation_order(g));
            } catch(const std::exception & e) {
                std::cerr << "Verification failed with: " << e.what() << std::endl;
                return false;
            }
        }
	}
}
