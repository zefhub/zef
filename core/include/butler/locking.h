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

#include <atomic>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>

// TODO: Wrap this so it only mentions it once for all c++ files.
/* #if __cplusplus == 202002L
 * #pragma message "Using C++20 atomics for locking."
 * #else
 * #pragma message "Using C++11 CVs for locking."
 * #endif */

// Note: rough comparison of times with test script: atomics were ~0.5s and CVs were ~6s

// TODO: 
// This version is an aux part for an atomic
#if __cplusplus == 202002L
// This can just be a stub, because the atomics do everything themselves
struct AtomicLockWrapper {};
template <typename T>
void wait_diff(AtomicLockWrapper & locker, std::atomic<T> & atom, T old_value) {
    atom.wait(old_value);
}

template <typename T>
void wait_same(AtomicLockWrapper & locker, std::atomic<T> & atom, T desired) {
    T cur = atom.load();
    while(cur != desired) {
        atom.wait(cur);
        cur = atom.load();
    }
}

template <typename T>
void wait(AtomicLockWrapper & locker, std::atomic<T> & atom, T old_value, std::chrono::duration<double> timeout) {
    throw std::runtime_error("Need to implement this");
    // TODO: This probably requires a check, followed by setting up a timeout in a separate thread, because there is no overload atomic_wait with a timeout. Sounds expensive...
}

template <typename T, typename F>
void update(AtomicLockWrapper & locker, T & atom, F new_value) {
    update_func();
    atom.notify_all();
}
#else
// C++17 requires CVs and mutexs to enact a lock.
struct AtomicLockWrapper {
    std::mutex mutex;
    std::condition_variable cv;
};

// Note: all the below use a manually written-out early bail-out to avoid locking the mutex.

// Should I forgoe the manual implementation of wait_diff/wait_same and just
// make them convenience functions to call wait_pred?
template <typename T>
void wait_diff(AtomicLockWrapper & locker, std::atomic<T> & atom, T old_value) {
    // Early bail out
    if(atom.load() != old_value)
        return;
    std::unique_lock lock(locker.mutex);
    locker.cv.wait(lock, [&]() { return atom != old_value; });
}

template <typename T>
void wait_same(AtomicLockWrapper & locker, std::atomic<T> & atom, T old_value) {
    // Early bail out
    if(atom.load() == old_value)
        return;
    std::unique_lock lock(locker.mutex);
    locker.cv.wait(lock, [&]() { return atom == old_value; });
}

template <typename T>
bool wait_diff(AtomicLockWrapper & locker, std::atomic<T> & atom, T old_value, std::chrono::duration<double> timeout) {
    // Early bail out
    if(atom.load() != old_value)
        return true;
    std::unique_lock lock(locker.mutex);
    return locker.cv.wait_for(lock, timeout, [&]() { return atom != old_value; });
}

template <typename T>
bool wait_same(AtomicLockWrapper & locker, std::atomic<T> & atom, T old_value, std::chrono::duration<double> timeout) {
    // Early bail out
    if(atom.load() == old_value)
        return true;
    std::unique_lock lock(locker.mutex);
    return locker.cv.wait_for(lock, timeout, [&]() { return atom == old_value; });
}

inline void wait_pred(AtomicLockWrapper & locker, std::function<bool()> func) {
    // Early bail out
    if(func())
        return;
    std::unique_lock lock(locker.mutex);
    locker.cv.wait(lock, func);
}
inline bool wait_pred(AtomicLockWrapper & locker, std::function<bool()> func, std::chrono::duration<double> timeout) {
    // Early bail out
    if(func())
        return true;
    std::unique_lock lock(locker.mutex);
    return locker.cv.wait_for(lock, timeout, func);
}

inline void wait_pred_poll(AtomicLockWrapper & locker, std::function<bool()> func, std::chrono::duration<double> timeout) {
    while(!func()) {
        wait_pred(locker, func, timeout);
    }
}

inline void update(AtomicLockWrapper & locker, std::function<void()> update_func) {
    std::lock_guard lock(locker.mutex);
    update_func();
    locker.cv.notify_all();
}

template <typename T>
void update(AtomicLockWrapper & locker, std::atomic<T> & atom, T update_val) {
    update(locker, [&atom,&update_val]() {atom = update_val;});
}

// This is a convenience form
template <typename T>
void update(AtomicLockWrapper & locker, std::atomic<T> & atom, std::atomic<T> update_atom) {
    update(locker, atom, update_atom.load());
}

inline void wake(AtomicLockWrapper & locker) {
    locker.cv.notify_all();
}

#endif


template <typename T>
void update_when_ready(AtomicLockWrapper & locker, std::atomic<T> & atom, T ready_value, T new_value) {
    while(atom != new_value) {
        // A custom wait - waiting for either the ready_value or the new_value.
        {
            std::unique_lock lock(locker.mutex);
            locker.cv.wait(lock, [&]() { return atom == ready_value || atom == new_value; });
        }
        T expected = atom.load();
        if(expected != ready_value && expected != new_value)
            continue;
        std::atomic_compare_exchange_weak(&atom,
                                            &expected,
                                            new_value);
    }
}

template<typename T>
bool is_future_ready(const std::future<T> & future) {
    return future.wait_for(std::chrono::seconds(0)) != std::future_status::timeout;
}
