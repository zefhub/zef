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

#include "graph.h"
#include "zefref.h"
#include "low_level_api.h"

namespace zefDB {

//                              _                                  _   _                       _                                         
//                             | |_ _ __ __ _ _ __  ___  __ _  ___| |_(_) ___  _ __        ___| | __ _ ___ ___                           
//    _____ _____ _____ _____  | __| '__/ _` | '_ \/ __|/ _` |/ __| __| |/ _ \| '_ \      / __| |/ _` / __/ __|  _____ _____ _____ _____ 
//   |_____|_____|_____|_____| | |_| | | (_| | | | \__ \ (_| | (__| |_| | (_) | | | |    | (__| | (_| \__ \__ \ |_____|_____|_____|_____|
//                              \__|_|  \__,_|_| |_|___/\__,_|\___|\__|_|\___/|_| |_|     \___|_|\__,_|___/___/                          
//                       

    LIBZEF_DLL_EXPORTED void StartTransaction(GraphData& gd) ;
    inline void StartTransaction(Graph & g) { StartTransaction(g.my_graph_data()); }

    LIBZEF_DLL_EXPORTED ZefRef StartTransactionReturnTx(GraphData& gd) ;
    inline ZefRef StartTransactionReturnTx(Graph & g) { return StartTransactionReturnTx(g.my_graph_data()); }

    LIBZEF_DLL_EXPORTED void FinishTransaction(GraphData& gd, bool wait, bool rollback_empty_tx, bool check_schema);
    LIBZEF_DLL_EXPORTED void FinishTransaction(GraphData& gd, bool wait, bool rollback_empty_tx);
    LIBZEF_DLL_EXPORTED void FinishTransaction(GraphData& gd, bool wait);
    LIBZEF_DLL_EXPORTED void FinishTransaction(GraphData& gd) ;
    inline void FinishTransaction(Graph & g, bool wait, bool rollback_empty_tx, bool check_schema) { FinishTransaction(g.my_graph_data(), wait, rollback_empty_tx, check_schema); }
    inline void FinishTransaction(Graph & g, bool wait, bool rollback_empty_tx) { FinishTransaction(g.my_graph_data(), wait, rollback_empty_tx); }
    inline void FinishTransaction(Graph & g, bool wait) { FinishTransaction(g.my_graph_data(), wait); }
    inline void FinishTransaction(Graph & g) { FinishTransaction(g.my_graph_data()); }

    LIBZEF_DLL_EXPORTED void AbortTransaction(GraphData& gd);
    inline void AbortTransaction(Graph& g) { AbortTransaction(g.my_graph_data()); }

	// lightweight object with no data. Only role is to give a zefDB graph access to the number of open transactions
	// at any time. If >0 transactions are open, any changes / appending to the graph are grouped under one tx_node.
	struct LIBZEF_DLL_EXPORTED Transaction {
		// if we use a ZefRef, the reassignment to a new memory pool in case of a reassignment can be taken care of by the ZefRef mechanics
		EZefRef graph_data_that_i_am_marking;
        // Danny: I can't see where this is used so I'm commenting it out for now.
		// bool this_transaction_already_unregistered_from_graph = false;
        bool wait;
        bool rollback_empty;
        bool check_schema;
			
		Transaction() = delete;
		Transaction(const Transaction&) = delete;
		Transaction(Transaction&&) = delete;  // this could be weakened and implemented if needed
        // The default behaviour is based on zwitch so we have to do things the long-winded way here.
		Transaction(GraphData& gd);
		Transaction(GraphData& gd, bool wait);
		Transaction(GraphData& gd, bool wait, bool rollback_empty);
		Transaction(GraphData& gd, bool wait, bool rollback_empty, bool check_schema) :
        wait(wait),
        rollback_empty(rollback_empty),
        check_schema(check_schema) {
            StartTransaction(gd);
            graph_data_that_i_am_marking.blob_ptr = &gd;
		}
		Transaction(Graph& g) : Transaction(g.my_graph_data()) {}
		Transaction(Graph& g, bool wait) : Transaction(g.my_graph_data(), wait) {}
		Transaction(Graph& g, bool wait, bool rollback_empty) : Transaction(g.my_graph_data(), wait, rollback_empty) {}
		Transaction(Graph& g, bool wait, bool rollback_empty, bool check_schema) : Transaction(g.my_graph_data(), wait, rollback_empty, check_schema) {}
		~Transaction() {
            GraphData& gd = *(GraphData*)graph_data_that_i_am_marking.blob_ptr;
			try {
				FinishTransaction(gd, wait, rollback_empty, check_schema);
			}
			catch (const std::exception& exc) {
				std::cerr << "Error occurred on Transaction closing: " << exc.what() << "\n";
			}
		}
	};

}