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

#include "graph.h"
#include "observable.h"
#include "high_level_api.h"
#include "ops_imperative.h"

namespace zefDB {

	ZefObservables::ZefObservables() {
		g_observables = std::make_unique<Graph>(false, MMap::MMAP_STYLE_ANONYMOUS);
	};


	Subscription::Subscription(const Subscription& sub) :  //copy ctor
		zef_observables_ptr(sub.zef_observables_ptr),
		uid(sub.uid)
	{ 
        // Need to use an iterator so that the dictionary doesn't change
        if(auto ptr = zef_observables_ptr.lock()) { 
            auto item = ptr->callbacks_and_refcount.find(uid);
            item->second.ref_count++;
        }
	}


	Subscription::Subscription(Subscription&& sub) noexcept :  //move ctor
		zef_observables_ptr(std::move(sub.zef_observables_ptr)),
		uid(sub.uid)
	{}


	Subscription& Subscription::operator= (const Subscription& sub) {  //copy assignment
		zef_observables_ptr = sub.zef_observables_ptr;
		uid = sub.uid;

        // Need to use an iterator so that the dictionary doesn't change
        if(auto ptr = zef_observables_ptr.lock()) { 
            auto item = ptr->callbacks_and_refcount.find(uid);
            item->second.ref_count++;
        }
        return *this;
    }


	Subscription& Subscription::operator= (Subscription&& sub) noexcept {  //move assignment
		zef_observables_ptr = std::move(sub.zef_observables_ptr);
		uid = sub.uid;
		return *this;
	}

    void Subscription::_remove_subscription(bool force) {
        // With threads, just because we get here doesn't mean we are guaranteed
        // of this item having a zero ref count. Instead, we require write
        // access to the dictionary to ensure that it is zero before we take it
        // out, and then we can take it out without anyone else incrementing the
        // reference count.
        if(auto ptr = zef_observables_ptr.lock()) { 
            // Using parallel-hashmap features
            bool was_erased = ptr->callbacks_and_refcount.erase_if(uid, [force](auto & item) {
                return force || (item.ref_count == 0 && !item.keep_alive);
            });
            Graph& g = *ptr->g_observables;
            auto uzr = g[uid];
            imperative::terminate(uzr);
        }

        zef_observables_ptr.reset();
    }

    void Subscription::unsubscribe() {
        // In this variant, we ignore keep_alives or anything else and forcibly stop this subscription
        
        if(auto ptr = zef_observables_ptr.lock()) { 
            // Using parallel-hashmap features
            ptr->callbacks_and_refcount.modify_if(uid, [](auto & item) {
                item.ref_count = 0;
            });
            _remove_subscription(true);
            zef_observables_ptr.reset();
		}
    }

	Subscription::~Subscription() {
        if(auto ptr = zef_observables_ptr.lock()) { 
            bool should_remove = false;

            // Using parallel-hashmap features
            ptr->callbacks_and_refcount.modify_if(uid, [&should_remove](auto & item) {
                item.ref_count--;
                if (item.ref_count == 0 && !item.keep_alive)
                    should_remove = true;
            });

            // This is outside, so the dict lock introduced by the iterator is removed
            if(should_remove)
                _remove_subscription();
            zef_observables_ptr.reset();
		}
    }

}
