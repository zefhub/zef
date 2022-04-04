#pragma once

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
        LIBZEF_DLL_EXPORTED void break_graph(Graph&g, blob_index index, int style);

	}






}
