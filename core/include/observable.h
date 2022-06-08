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

#include <vector>
#include <unordered_map>
#include <functional>
#include <memory>
#include "fwd_declarations.h"
#include "blobs.h"
#include "zefDB_utils.h"

namespace zefDB {










	// be very specific and pragmatic for now with observables, the implementation may change later: only allow subscriptions to 
	// a) AE  value assignments 
	// b) relents monitoring for structural changes on in/out relations

	// the following struct is attached as a unique ptr to a GraphData struct once required
	struct LIBZEF_DLL_EXPORTED ZefObservables {
		struct DictElement {
			// keep in addition to the graph: mutable ref_counts and callback fcts are stored here
			std::function<void(ZefRef)> callback;
			int ref_count = 0;    // how many 'subscription' objects are currently alive.
            bool keep_alive = false;
		};
		ZefObservables();   // ctor implemented in cpp file

		std::unique_ptr<Graph> g_observables;
		thread_safe_unordered_map<BaseUID, DictElement> callbacks_and_refcount;
	};

	inline std::ostream& operator<< (std::ostream& os, const ZefObservables& x) {
		os << "ZefObservables( #(AE callbacks: ) .. TODO...)";
		return os;
	}




	// handle object to keep track and call to unsubscribe.
	//    ------------ sample usage -----------
	// my_aet | subscribe[on_value_assignment, my_callback]
	// my_subscription.unsubscribe()
	// Is this really required? Maybe track all subscriptions on a zefscription graph
	struct LIBZEF_DLL_EXPORTED Subscription {
		//FIXME: if tokolosh revisions graph and maps it into a new mem area, should we put double linking here to update the ptr?
        std::weak_ptr<ZefObservables> zef_observables_ptr;
		BaseUID uid;

		Subscription(std::weak_ptr<ZefObservables> zef_observables_ptr, BaseUID uid) :
			zef_observables_ptr(zef_observables_ptr),
			uid(uid)
		{}

        void _remove_subscription(bool force=false);
        void unsubscribe();


		// ------------- rule of five -------------
		Subscription(const Subscription& sub); // copy ctor
		Subscription(Subscription&& sub) noexcept; // move ctor
		Subscription& operator= (const Subscription& sub);  //copy assignment
		Subscription& operator= (Subscription&& sub) noexcept;  //move assignment
		~Subscription();
	};

	inline std::ostream& operator<< (std::ostream& os, const Subscription& x) {
		// os << "Subscription(" << x.uid << ", keep_alive=" << (x.keep_alive ? "true" : "false") << ")";
		os << "Subscription(" << x.uid << ")";
		return os;
	}

    inline std::optional<Subscription> try_get_subscription(std::shared_ptr<ZefObservables> zef_observables, BaseUID uid) {
        // This doesn't ensure that we get a subscription but fails gracefully.
        auto & dict = zef_observables->callbacks_and_refcount;
        // std::lock_guard lock(dict.m);
        // auto item = dict.map.find(uid);
        // if(item == dict.map.end()) {
        //     // The subscription has been cancelled.
        //     return {};
        // }

        // item->second.ref_count++;
        // return Subscription(&zef_observables, uid);

        // This version relies on parallel-hashmap features
        bool modified = dict.modify_if(uid, [](auto & item) {
            item.ref_count++;
        });
        if(modified)
            return Subscription(zef_observables, uid);
        return {};
    }

}
