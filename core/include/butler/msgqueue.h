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
#include "graph.h"
#include "butler/locking.h"
#include "zwitch.h"

#include <string>
#include <variant>
#include <thread>
#include <memory>
#include <atomic>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <future>
#include <shared_mutex>

#include <nlohmann/json.hpp>

namespace zefDB {
    
    namespace Butler {
        using json = nlohmann::json;

        // Any adding to the queue will block when this is hit.
        //
        // A good idea for the butler, is to not put a new message on the queue
        // when a thread is yet to process an existing message, and watch the
        // num_messages value for changes.
        //
        // This is not a traditional queue, where the readers pop an item off
        // (typically FIFO). In one use-case, the butler can pop any item off,
        // and multiple guests can write. In the the other use-case, the butler
        // writes and each guest should find the item that corresponds to them.
        // Perhaps this could be implemented with two separate classes, but we
        // should cross that bridge only if it is necessary.
        //
        // Actually it seems like a message queue is only necessary in the one
        // direction (guests to butler) but in the other direction, it is always
        // synchronous per guest, so we shouldn't need to ever handle more than
        // one message. This is really a callback pattern.
        template <typename T>
        struct MessageQueue {
            std::atomic_int num_messages = 0;
            constexpr static int max_messages = 20;
            // This is used to handle race conditions when stopping a queue.
            std::atomic_bool _closed = false;

            // This is just for debugging
            std::string who_am_i = "unset";
            std::string last_popped = "";

            using ptr = std::shared_ptr<T>;
            // Warning: the standard mentions every access to an atomic shared
            // pointer should be using atomics. I think this is implicitly taken
            // care of using our model, that the pointer will not be accessed in
            // two threads simultaneously anyway. The use of shared_ptr here is
            // only a convenience, that we allow for the reuse of memory.
            //
            // Weirdly atomic<shared_ptr<T>> seems to fail even though it should be allowed.
            // using ptr = typename std::add_pointer<T>::type;

            // TODO: COME BACK TO THIS - I THINK I FINALLY KNOW WHAT"S GOING ON.
            // In C++20 there is std::atomic<std::shared_ptr>, with member
            // functions. But prior to that, there are overloads of the
            // non-member functions for std::shared_ptr (no explicit atomic
            // word) which can be used.

            //using slot = std::atomic<ptr>;
            // using slot = std::atomic<ptr*>;
            using slot = ptr;

            std::array<slot,max_messages> slots;

            // Will block until a message is available.
            void push(ptr && message, bool ignore_closed=false);
            // Returns false/true if an item was actually popped or not.
            bool pop_any(ptr &out);
            bool pop_loop(ptr &out);
            void set_closed(bool warn_if_already_closed=true);

            AtomicLockWrapper locker;

            MessageQueue() {
                for(auto & slot : slots)
                    slot = nullptr;
            }
        };

        template <typename T>
        void MessageQueue<T>::push(MessageQueue<T>::ptr && message, bool ignore_closed) {
            if(_closed) {
                if(ignore_closed)
                    return;
                throw std::runtime_error("Throwing because of closed queue, because it's the simplest thing to do");
            }
            // This function puts its message onto the queue. If there isn't
            // space it will block until space becomes available and it has
            // successfully been able to place the message onto the queue.
            if (zwitch.developer_output())
                std::cerr << "Pushing a message of type " << msgqueue_to_str(*message) << " onto queue " << who_am_i << std::endl;
            if(num_messages.load() == max_messages)
                std::cerr << "A message queue (" + who_am_i + ") has filled up!!!" << std::endl;
            // ptr* item = new ptr(std::move(message));
            while(true) {
                wait_diff(locker, num_messages, max_messages);
                // There is a chance we can get a message through now
                for (auto & slot : slots) {
                    // ptr* expected = nullptr;
                    // if(slot.compare_exchange_strong(expected, item))
                    //     goto finished;
                    ptr expected = nullptr;
                    if(std::atomic_compare_exchange_strong(&slot, &expected, message))
                        goto finished;
                }
            }
        finished:
            update(locker, [&]() { num_messages++; });
        }

        // I struggled with making this overload the to_str function. This did
        // not seem to trigger it, which I thought was something to do with the
        // order of definitions of the to_str functions. In the end I didn't
        // want to waste more time, so this is defined as a workaround.

        struct RequestWrapper;
        std::string msgqueue_to_str(const RequestWrapper & wrapper);

        template <typename T>
        bool MessageQueue<T>::pop_any(MessageQueue<T>::ptr &out) {
            // This function returns false if it wasn't able to get an item
            // before the queue says there are no more items left. Otherwise it
            // returns true and sets the output.
            
            wait_diff(locker, num_messages, 0);
            // Use a while loop as we may fail if there are multiple readers.
            //
            // Note: not likely to be multiple readers in our case, but future
            // proofing anyway to avoid nasty surprises..
            while(num_messages > 0) {
                for (auto & slot : slots) {
                    // The goal here is to check if the pointer is not null and
                    // then fill it in with null. We achieve this by trying to
                    // place null in everywhere, and if it was already null, we
                    // see that in the return of exchange.
                    // ptr* this_slot = slot.exchange(nullptr);
                    out = std::atomic_exchange(&slot, ptr());
                    // if (this_slot) {
                    if (out) {
                        // out = std::move(*this_slot);
                        last_popped = msgqueue_to_str(*out);
                        // delete this_slot;
                        update(locker, [this]() { num_messages--; });
                        return true;
                    }
                }

                // The "closed" counts as a fake message
                if (_closed && num_messages == 1)
                    return false;
            }
            return false;
        }

        template <typename T>
        bool MessageQueue<T>::pop_loop(MessageQueue<T>::ptr &out) {
            // Keep trying to pop messages. Returns after each message or when
            // closed (potentially leaving some messages on the queue).
            
            while(true) {
                bool popped = pop_any(out);
                if (_closed) {
                    // Set the return appropriately so the caller can make
                    // decisions. If we didn't pop a message we need to make
                    // sure they know about that.
                    if(!popped)
                        out.reset();
                    return false;
                }

                if(popped)
                    return true;
            }
        }

        template <typename T>
        void MessageQueue<T>::set_closed(bool warn_if_already_closed) {
            if(_closed) {
                if(warn_if_already_closed)
                std::cerr << "Warning, trying to close a queue (" + who_am_i + ") that is already closed." << std::endl;
                return;
            }
                
            _closed = true;
            // We must increment the message count, as the closed pretends to be
            // a message itself. This is so that anything waiting on
            // "num_message > 0" will fire.
            update(locker, [this]() { num_messages++; });
        }
    }
} // namespace zefDB
