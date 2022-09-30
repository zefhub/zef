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

#include "transaction.h"
#include "zwitch.h"
#include "external_handlers.h"
#include "zefops.h"

namespace zefDB {

    void StartTransaction(GraphData& gd) {
        if (!gd.is_primary_instance)
            throw std::runtime_error("attempted opening a transaction for a graph which is not a primary instance. This is not allowed. Shame on you!");

        if(gd.error_state != GraphData::ErrorState::OK)
            throw std::runtime_error("The graph is in a bad state - not allowing new additions.");

        // Mutually exclusive access to transactions, based on thread id.
        // std::thread::id() works as the "unset" value in this case.
        auto this_id = std::this_thread::get_id();
        if(this_id == gd.sync_thread_id) {
            // If we are here, then we are the manager running subscriptions. We
            // only need to take the write role.
            update_when_ready(gd.open_tx_thread_locker,
                            gd.open_tx_thread,
                            std::thread::id(),
                            this_id);
        } else {
            // We are a client, we need to wait for the manager to have caught
            // up and no-one else is writing.

            // Note: the two things we check are actually accessed using
            // different locks. This is a little weird... and might be
            // problematic?
            while(gd.latest_complete_tx != gd.manager_tx_head.load()
                || gd.open_tx_thread != this_id) {
                // Give up the open_tx_thread if we stole it before the manager can catch up.
                if(gd.open_tx_thread == this_id)
                    update(gd.open_tx_thread_locker, gd.open_tx_thread, std::thread::id());
                wait_pred(gd.heads_locker, [&]() { return gd.latest_complete_tx.load() == gd.manager_tx_head; });
                update_when_ready(gd.open_tx_thread_locker,
                                gd.open_tx_thread,
                                std::thread::id(),
                                this_id);
            }
        }
                          
		if (gd.number_of_open_tx_sessions == 0) // make sure a tx has been instantiated, e.g. for the case that someone does my_ent | now and expects a ZR within this very time slice
			internals::get_or_create_and_get_tx(gd);  // do not open a separate tx inside, this would lead to an endless loop
        else if(gd.index_of_open_tx_node == 0)
            throw std::runtime_error("Trying to open a new transaction when it was already aborted. Don't do this!");
        gd.number_of_open_tx_sessions++;

        // This is a check for someone trying to open a nested tx after aborting the tx.
    }

    ZefRef StartTransactionReturnTx(GraphData& gd) {
		StartTransaction(gd);
		EZefRef tx = internals::get_or_create_and_get_tx(gd);		
        return ZefRef(tx,tx);
    }


	// namespace internals {

	// 	void execute_queued_fcts(GraphData& gd){
	// 		// factor this out, it is used both in FinishTransaction and q_function_on_graph()
	// 		auto there_is_a_fct_in_q = [](GraphData& gd)->bool {
	// 			return bool(gd.q_fcts_to_execute_when_txs_close);	// we have to make sure that this is ony not nullopt if the vector also non-empty
	// 		};

	// 		auto clean_up_q = [](GraphData& gd)->void {
	// 			// remove all nullopt elements from back of vector. If empty, set the entire q to nullopt
	// 			auto& v = *(gd.q_fcts_to_execute_when_txs_close);
	// 			while (!v.empty() && !bool(v.back())) {	// second expr: is the element set to nullopt?
	// 				v.pop_back();
	// 			}
	// 			if (v.empty()) {
	// 				gd.q_fcts_to_execute_when_txs_close = std::nullopt;   // if the entire q is empty, set the optional q to nullopt
	// 			}
	// 		};
			
	// 		auto pop_fct_with_highest_priority_off_q = [&clean_up_q](GraphData& gd)->std::function<void(Graph)> {
				
	// 			// if the remaining vector is empty: set it to std::nullopt
	// 			// find the highest priority element  ----  do this old-school imperatively :(
	// 			double highest_priority_to_date = -std::numeric_limits<double>::infinity();
	// 			int indx_with_highest_priority = -1;
	// 			int ind = 0;
	// 			for (auto& el : *gd.q_fcts_to_execute_when_txs_close) {
	// 				if (bool(el) && el->priority > highest_priority_to_date) {
	// 					indx_with_highest_priority = ind;
	// 					highest_priority_to_date = el->priority;
	// 				}
	// 				ind++;
	// 			}
	// 			auto fct = (*(*(gd.q_fcts_to_execute_when_txs_close))[indx_with_highest_priority]).fct;
	// 			(*(gd.q_fcts_to_execute_when_txs_close))[indx_with_highest_priority] = std::nullopt;
	// 			clean_up_q(gd);
	// 			return fct;
	// 		};


	// 		// Execute all queued functions, each in their own transaction, here. 
	// 		// Don't do this recursively to prevent the stack from overflowing.
	// 		// Execute subscriptions after each individual q'ed fct execution.
	// 		// The q'ed fcts may append new q'ed fcts themselves.
	// 		// Do this with a while loop until there are no more queued fcts
	// 		int q_execution_ct = 0;
	// 		while (there_is_a_fct_in_q(gd)) {
	// 			if (q_execution_ct >= constants::max_qd_fct_execution_number_for_one_explicit_tx_closing) {
	// 				throw std::runtime_error("Max number of allowed Q executions exceeded!");
	// 			}
	// 			auto fct = pop_fct_with_highest_priority_off_q(gd);
	// 			try {
	// 				q_execution_ct++;
	// 				StartTransaction(gd);
	// 				fct(Graph(gd));
	// 				FinishTransaction(gd);
	// 			}
	// 			catch (const std::exception& exc) {
	// 				std::cerr << "Error executing a function in the graph's Q! \nError:    " << exc.what() << "\n";
	// 			}
	// 		}
	// 	}



	// }


    void FinishTransaction(GraphData& gd) {
        FinishTransaction(gd, zwitch.default_wait_for_tx_finish());
    }
    void FinishTransaction(GraphData& gd, bool wait) {
        FinishTransaction(gd, wait, zwitch.default_rollback_empty_tx());
    }

    void FinishTransaction(GraphData& gd, bool wait, bool rollback_empty_tx) {
        FinishTransaction(gd, wait, rollback_empty_tx, true);
    }

    void FinishTransaction(GraphData& gd, bool wait, bool rollback_empty_tx, bool check_schema) {
        gd.number_of_open_tx_sessions--;
        // in case this was the last transaction that is closed, we want to mark the 
        // transcation node as complete: any write mod to the graph will trigger a new tx hereafter
        if (gd.number_of_open_tx_sessions == 0) {
            blob_index manager_tx = 0;
            {
                RAII_CallAtEnd call_at_end([&]() {
                    update(gd.open_tx_thread_locker, gd.open_tx_thread, std::thread::id());
                });
                
                if(check_schema && gd.index_of_open_tx_node != 0) {
                    EZefRef ezr_tx{gd.index_of_open_tx_node, gd};
                    ZefRef ctx{ezr_tx, ezr_tx};
                    // We fake that we have the transaction still open just for AbortTransaction
                    gd.number_of_open_tx_sessions++;
                    try {
                        internals::pass_to_schema_validator(ctx);
                    } catch(const std::exception & e) {
                        std::cerr << "Exception in schema_validator: " << e.what() << std::endl;
                        AbortTransaction(Graph(ctx));
                        gd.number_of_open_tx_sessions--;
                        throw std::runtime_error(std::string("Schema validation failed: ") + e.what());
                    }
                    gd.number_of_open_tx_sessions--;
                }

                if(rollback_empty_tx && gd.index_of_open_tx_node != 0) {
                    // The transaction is empty if the tx node is the last thing before the write head.
                    blob_index next_node = gd.index_of_open_tx_node + blob_index_size(EZefRef{gd.index_of_open_tx_node, gd});
                    if(next_node == gd.write_head.load()) {
                        // We fake that we have the transaction still open just for AbortTransaction
                        gd.number_of_open_tx_sessions++;
                        AbortTransaction(gd);
                        gd.number_of_open_tx_sessions--;
                    }
                }

                // If we have been aborted, then don't continue for the rest of the logic
                if(gd.index_of_open_tx_node == 0)
                    return;


                // TODO: This might not be the right place.
                auto & info = MMap::info_from_blobs(&gd);
                MMap::flush_mmap(info, gd.write_head);

                // Unlike the write_head, we need to inform any listeners if the read_head changes.
                // update(gd.heads_locker, gd.read_head, gd.write_head.load());  // the zefscription manager can send out updates up to this pointer (not including)		
                update(gd.heads_locker, [&]() {
                    gd.read_head = gd.write_head.load();
                    gd.latest_complete_tx = gd.index_of_open_tx_node;
                    gd.index_of_open_tx_node = 0;
                    manager_tx = gd.manager_tx_head;
                });
            }
            // Let's check in this thread - here at least we should be able to see the next tx edge
            // EZefRef debug_tx{manager_tx, gd};
            // if(!(debug_tx | has_out[BT.NEXT_TX_EDGE])) {
            //     std::cerr << "guid: " << uid(gd) << std::endl;
            //     std::cerr << "CAN'T SEE NEXT_TX_EDGE EVEN FROM WITHIN FINISH TRANSACTION!!!!" << std::endl;
            // }

            // Note: we have to give up the lock on the thread by this point, as
            // we could block waiting for the msg_queue of the graph manager in
            // the next lines.

            auto butler = Butler::get_butler();
            // False is because we don't want to wait for response
            butler->msg_push(Messages::NewTransactionCreated{Graph(gd), gd.latest_complete_tx}, false);

            // Wait if requested and we aren't running subscriptions.
            if(std::this_thread::get_id() != gd.sync_thread_id) {
                if(wait) {
                    wait_pred(gd.heads_locker, [&]() { return gd.latest_complete_tx.load() == gd.manager_tx_head; });
                }
            }
        }
    }

    void AbortTransaction(GraphData& gd) {
        // This breaks the chain of StartTransaction/FinishTransaction and rolls
        // back any changes to the read_head.
        
        if(gd.number_of_open_tx_sessions == 0)
            throw std::runtime_error("Can't abort a transaction when there are no open sessions.");
        if(gd.index_of_open_tx_node == 0)
            throw std::runtime_error("Don't know which tx node is open - have you already aborted this transaction?");

        gd.index_of_open_tx_node = 0;

        // Move back to the read head
        roll_back_to(gd, gd.read_head, true);
    }


    Transaction::Transaction(GraphData & gd) : Transaction(gd, zwitch.default_wait_for_tx_finish()) {}
    Transaction::Transaction(GraphData & gd, bool wait) : Transaction(gd, wait, zwitch.default_rollback_empty_tx()) {}
    Transaction::Transaction(GraphData & gd, bool wait, bool rollback_empty) : Transaction(gd, wait, rollback_empty, true) {}

    void run_subscriptions(GraphData & gd, EZefRef transaction_uzr) {
        if(!gd.observables)
            return;

        std::shared_ptr<ZefObservables> obs = gd.observables;
        auto g = Graph(gd);
        Graph& g_subs = *obs->g_observables;  // the subscription graph
        EZefRefs outgoing_from_tx = transaction_uzr | outs;
        // Use a consistent frame of reference throughout this function, even though
        // subscribes/unsubscribes may be happening. Of course, we can't use the
        // functions that have been unsubscribed, so we will have to be careful
        // there too.
        auto nowish = to_zefref[g_subs|now][allow_terminated_relent_promotion];
        auto maybe_run_callback = [&](BaseUID sub_uid, auto... args) {
            // Grab the callback, incrementing the ref count while we have it and run
            // We use find to return an iterator, that locks the dictionary for us
            // while we are in here. Just until we can increment the reference count
            auto sub = try_get_subscription(obs, sub_uid);
            if(!sub)
                return;

            // While we have the subscription, we can call away knowing the callback won't disappear on us.
            obs->callbacks_and_refcount[sub_uid].callback(args...);   // execute the callback: use the latest time slice as reference frame
        };

        // ----------------------------------------- AE value updates ------------------------------------
        for (auto z : outgoing_from_tx | filter[BT.ATOMIC_VALUE_ASSIGNMENT_EDGE, BT.ATTRIBUTE_VALUE_ASSIGNMENT_EDGE]) {
            EZefRef my_ae = z | target | target;
            auto my_ae_uid = uid(my_ae);
            if (obs->g_observables->contains(my_ae_uid)) {		// if there is a subscription to z, its uid is definitely in the subscription graph (it's the uid of the cloning edge)
                // look in this time slice
                ZefRefs val_assignment_grouping_node = (internals::local_entity(g_subs[my_ae_uid]) | nowish) >> L[RT.OnValueAssignment];   // take the latest time slice of the subscription graph (this was a bug before where transaction_uzr was passed in as ref frame)
                if (length(val_assignment_grouping_node) == 1) {
                    for (auto z_subsc : (val_assignment_grouping_node | first) >> L[RT.ListElement]) {
                        BaseUID callback_uid = uid(z_subsc|to_ezefref).blob_uid;
                        try {
                            // set_open_tx_thread();
                            LockGraphData gd_lock(&gd);
                            // obs.callbacks_and_refcount[callback_uid].callback(my_ae | to_zefref[transaction_uzr]);   // execute the callback: use the latest time slice as reference frame														
                            maybe_run_callback(callback_uid, my_ae | to_zefref[transaction_uzr]);   // execute the callback: use the latest time slice as reference frame														
                        } catch(const std::exception& exc) {
                            std::cerr << "Error in value assignment for uid " << uid(my_ae) << " callback - ignoring. Error: " << exc.what() << std::endl;
                        }
                    }
                }
            }
        }			





        // ---------------------------------------- structural updates ---------------------------------------
        // the common parts of the following code are not factored out, although they could be. DRY does not win here

        if (g_subs.contains("MonitoredRelInstantiations")) {  // exit early if no lists are monitored
            EZefRefs all_instantiated_rels = outgoing_from_tx | filter[BT.INSTANTIATION_EDGE] | target | target | filter[BT.RELATION_EDGE];
            for (auto rel : all_instantiated_rels) {  // rel lives on the 'data graph'
                for (auto is_out_rel : { true, false }) {
                    EZefRef subject = is_out_rel ? (rel | source) : (rel | target);
                    std::size_t composite_hash = internals::make_hash(subject, RT(rel), is_out_rel, true);
                    auto key = TagString("CallbackList." + to_str(composite_hash));
                    if (g_subs.contains(key)) {
                        for (auto cb : (g_subs[key] | nowish) >> L[RT.ListElement]) {
                            try {
                                // set_open_tx_thread();
                                LockGraphData gd_lock(&gd);
                                // obs.callbacks_and_refcount[UID(cb | to_ezefref)].callback(rel | to_zefref[transaction_uzr]);   // execute the callback: use the latest time slice as reference frame														
                                maybe_run_callback(uid(cb | to_ezefref).blob_uid, rel | to_zefref[transaction_uzr]);   // execute the callback: use the latest time slice as reference frame														
                            } catch(const std::exception& exc) {
                                std::cerr << "Error in instantiation callback for uid " << uid(rel) << " - ignoring. Error: " << exc.what() << std::endl;
                            }
                        }
                    }
                }
            }
        }

        if (g_subs.contains("MonitoredRelTerminations")) {  // exit early if no lists are monitored
            EZefRefs all_terminated_rels = outgoing_from_tx | filter[BT.TERMINATION_EDGE] | target | target | filter[BT.RELATION_EDGE];
            for (auto rel : all_terminated_rels) {  // rel lives on the 'data graph'
                for (auto is_out_rel : { true, false }) {
                    EZefRef subject = is_out_rel ? (rel | source) : (rel | target);
                    std::size_t composite_hash = internals::make_hash(subject, RT(rel), is_out_rel, false);
                    auto key = TagString("CallbackList." + to_str(composite_hash));
                    if (g_subs.contains(key)) {
                        for (auto cb : (g_subs[key] | nowish) >> L[RT.ListElement]) {
                            try {
                                // set_open_tx_thread();
                                LockGraphData gd_lock(&gd);
                                // obs.callbacks_and_refcount[UID(cb | to_ezefref)].callback(rel | to_zefref[allow_terminated_relent_promotion][transaction_uzr]);   // execute the callback: use the latest time slice as reference frame														
                                maybe_run_callback(uid(cb | to_ezefref).blob_uid, rel | to_zefref[allow_terminated_relent_promotion][transaction_uzr]);   // execute the callback: use the latest time slice as reference frame														
                            } catch(const std::exception& exc) {
                                std::cerr << "Error in termination callback for uid " << uid(rel) << " - ignoring. Error: " << exc.what() << std::endl;
                            }
                        }
                    }
                }
            }
        }



        // ---------------------------------------- general graph callbacks: executed on every Transaction closing ---------------------------------------
        if (g_subs.contains("GraphSubscriptions")) {  // exit early if no general graph callbacks are monitored
            for (auto z : (g_subs[TagString("GraphSubscriptions")] | nowish) >> L[RT.ListElement]) {
                try {
                    // set_open_tx_thread();
                    LockGraphData gd_lock(&gd);
                    // obs.callbacks_and_refcount[UID(z | to_ezefref)].callback(g[constants::ROOT_NODE_blob_index] | to_zefref[transaction_uzr]);   // the callback functions are of of uniform type signature: void(ZefRef). By default, pass in the current graph, rerpresented as its versioned root node
                    maybe_run_callback(uid(z | to_ezefref).blob_uid, g[constants::ROOT_NODE_blob_index] | to_zefref[transaction_uzr]);   // the callback functions are of of uniform type signature: void(ZefRef). By default, pass in the current graph, rerpresented as its versioned root node
                }
                catch (const std::exception& exc) {
                    std::cerr << "An exception occurred executing the GraphSubscription callback with subscription uid = " << uid(z | to_ezefref) << "\nError:    " << exc.what() << "\n";
                }
            }			
        }

        // TODO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        // possibly perform cleanup of subscriptions triggered by relents that were terminated. For value updates this brings no performance gain, 
        // since the entry point is the dict uid lookup directly. Graph updates are also not affected: the entire graph can't be terminated.
        // Only structural updates are affected if the source/target relent of the monitored rlation type is terminated. When should this cleanup be performed?
        // If it is checked each time the callback loop runs, the checking of all terminated entities may be more costly than the subscription not being termianted
        // on the zefscription graph
    }

}