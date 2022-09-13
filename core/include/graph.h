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

#include <iostream>
// #include <unordered_map>
#include <variant>
#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <sstream> 
#include <thread>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <future>
#include <unordered_set>
// #include <chrono>         // std::chrono::seconds
#include "range/v3/all.hpp"

#include "fwd_declarations.h"
#include "zefDB_utils.h"
#include "scalars.h"
#include "observable.h"
#include "tokens.h"
#include "xxhash64.h"
#include "butler/locking.h"
#include "butler/threadsafe_map.h"

#include "append_structures.h"

namespace zefDB {
    // Need a forward declaration of this, as the butler depends on knowing what a Graph is.
    namespace Butler {
        struct Butler;
        struct UpdateHeads;
    }

    // Need a forward declaration of this, so that messages.h can refer to Graphs freely.
    namespace Messages {
        struct UpdatePayload;
    }

    // Need a forward declaration of ZefRef
    struct EZefRef;
    struct ZefRef;
}

namespace zefDB {



    //                                        _                  __                                                                                           
	//                   __ _ _ __ __ _ _ __ | |__     _ __ ___ / _| ___ _ __ ___ _ __   ___ ___    _ __ ___   __ _ _ __   __ _  __ _  ___ _ __               
	//    _____ _____   / _` | '__/ _` | '_ \| '_ \   | '__/ _ | |_ / _ | '__/ _ | '_ \ / __/ _ \  | '_ ` _ \ / _` | '_ \ / _` |/ _` |/ _ | '__|  _____ _____ 
	//   |_____|_____| | (_| | | | (_| | |_) | | | |  | | |  __|  _|  __| | |  __| | | | (_|  __/  | | | | | | (_| | | | | (_| | (_| |  __| |    |_____|_____|
	//                  \__, |_|  \__,_| .__/|_| |_|  |_|  \___|_|  \___|_|  \___|_| |_|\___\___|  |_| |_| |_|\__,_|_| |_|\__,_|\__, |\___|_|                 
	//                  |___/          |_|                                                                                      |___/                  
    // This is to distinguish between "dispatching strings" vs "definitely a tag"
    struct LIBZEF_DLL_EXPORTED TagString {
        std::string s;
        TagString(const std::string &s) : s(s) {}
        bool operator==(const TagString & other) const {
            return s == other.s;
        }
        operator str() const {
            return s;
        }
    };
    inline std::ostream & operator<<(std::ostream & o, const TagString & tag) {
        o << "TagString(\"" << tag.s << "\")";
        return o;
    }
} // namespace zefDB
namespace std {
    template<>
    struct hash<zefDB::TagString> {
        size_t operator()(const zefDB::TagString & s) {
            return std::hash<std::string>{}(s.s);
        }
    };
}

namespace zefDB {

    // using UID_or_string = std::variant<BaseUID,EternalUID,std::string>;
    using UID_or_string = std::variant<BaseUID,EternalUID,TagString>;

    struct LIBZEF_DLL_EXPORTED GraphData {
		// the GraphData needs to have the info where the actual pool that was allocated starts. Why? The active_graph_data_tracker has access to 
		// all the GraphData objects only (not Graphs object). It also needs to return e.g. a vector of all active graphs.
		// On the last destructor being called, the full mempool needs to be deleted, i.e. the Graph object needs access this ptr to call delete. 
		// It therefore needs to be stored here and not in the Graph object
		// char* raw_mem_pool_begin = nullptr;  
		
        // each graph object referring to this increases the count by 1.
        // Destruct if it reaches zero when a Graph destructor is called
        //
        // Thread notes: mostly this will be managed by the butler through
        // LoadGraph/DoneWithGraph messages from the guests. However, if any
        // guest makes a copy of a Graph object, it can increment this, so we
        // still need atomics.
        //
        // Note, however, that every destructor of a Graph must send a
        // DoneWithGraph message. In some way, we're just implementing an
        // optimised version of the LoadGraph message for simple copies.
        std::atomic_int reference_count = 0;
        // A generic flag for error checking. Should probably change this out
        // for the C++ STL error codes.
        enum class ErrorState : unsigned char {
            OK,
            UNSPECIFIED_ERROR,
        } error_state = ErrorState::OK;
        // This is used to "disable" ~Graph inside of the final cleanup in the butler.
        bool started_destructing = false;
        // Only allow one thread at a time to have an open transaction. 
        AtomicLockWrapper open_tx_thread_locker;
        std::atomic<std::thread::id> open_tx_thread;
		int number_of_open_tx_sessions = 0;	    // any TX_session object created increases this counter by 1, the destructor reduces it		

        // These IDs allow us to shortcut through blocking checks in, e.g.
        // loading a page or running subscriptions.
        std::thread::id managing_thread_id;
        std::thread::id sync_thread_id;
		
		// this is set to 0 initially and in the unregister method of the last transaction object: signal to the next transaction object to open a new tx
		blob_index index_of_open_tx_node = 0;

        // Internally, read_head is the point up to where we can be sure that
        // accessing the graph is allowed. The write_head is used for adding
        // onto the graph with primary role, or when handling updates when
        // without primary role.
        AtomicLockWrapper heads_locker;
        // TODO: Need to try these without atomics... it might be me
        // misunderstanding the memory fencing introduced by the CV.
        std::atomic<blob_index> write_head = constants::ROOT_NODE_blob_index;
        std::atomic<blob_index> read_head = constants::ROOT_NODE_blob_index;
        // which temporal slice of the graph is the latest one?
        std::atomic<blob_index> latest_complete_tx = constants::ROOT_NODE_blob_index;
        std::atomic<blob_index> last_run_subscriptions = constants::ROOT_NODE_blob_index;
        // Where is the manager up to? This points to a tx node
        std::atomic<blob_index> manager_tx_head = constants::ROOT_NODE_blob_index;

        // In sync is for checking if any additions to the graph have been
        // pushed upstream. It is not for checking if upstream has updates to
        // apply.
        //
        // Note: sync_head==0 means upstream doesn't know about this.
        std::atomic<blob_index> sync_head = 0;
        std::atomic<bool> currently_subscribed = false;
        bool in_sync() { return sync_head == read_head && read_head == write_head; }

        // Revisions start at 1. Zero is reserved for "unknown revision"
        int revision = 0;
        // TODO: Notes for future use of revisions follow here.
        //
        // A revision can correspond to different things, but represents a
        // change in the blob layout of the graph.
        //
        // Index lookups are not compatible between graph revisions, but UZR
        // uids are.
        //
        // The primary use of revisions is to optimise the graph blobs e.g. a)
        // continguous access, b) allocating extra room for edges, c) "hiding"
        // terminated edges, etc...
        //
        // Zefhub may automatically perform this "defrag" (even on graphs which
        // it doesn't own) such that users should not be aware of it happening.
        //
        // For a graph which is currently checked out as a primary instance,
        // Zefhub could signal the client that it has a new revision. The client
        // could then accept/reject this revision, depending on whether its
        // write head agrees. On rejection, Zefhub may try again later at a time
        // it estimates the graph will be "free" for defragging.

		bool is_primary_instance = true;
        std::atomic<bool> should_sync = true;  // only relevant to the behavior of the owning process: register and send updates to zefhub?
        std::filesystem::path local_path = "";

        // using key_map = thread_safe_unordered_map<std::string, blob_index>;
        using key_map = phmap::parallel_flat_hash_map<
            UID_or_string, blob_index,
            // UID_or_string::HashFct,
            std::hash<UID_or_string>,
            std::equal_to<UID_or_string>,
            std::allocator<std::pair<const UID_or_string, blob_index>>,
            4,
            // 1,
            std::mutex
            >;
        // using key_map = phmap::flat_hash_map<UID_or_string, blob_index>;
        
		// std::unordered_map<std::string, blob_index> key_dict;   // e.g. to get the blob_indx from a uid. Also used for other string ids referring to specific blobs_ns
        // std::unique_ptr<key_map> key_dict;
        // std::atomic<bool> key_dict_initialized = false;

        std::unique_ptr<MMap::WholeFileMapping<AppendOnlySet<token_value_t>>> ETs_used;
        std::unique_ptr<MMap::WholeFileMapping<AppendOnlySet<token_value_t>>> RTs_used;
        std::unique_ptr<MMap::WholeFileMapping<AppendOnlySet<enum_indx>>> ENs_used;

        // std::unique_ptr<MMap::WholeFileMapping<AppendOnlyDictFixed<BaseUID,blob_index>>> uid_lookup;
        // std::unique_ptr<MMap::WholeFileMapping<AppendOnlyDictFixed<EternalUID,blob_index>>> euid_lookup;
        std::unique_ptr<MMap::WholeFileMapping<AppendOnlyBinaryTree<BaseUID,blob_index>>> uid_lookup;
        std::unique_ptr<MMap::WholeFileMapping<AppendOnlyBinaryTree<EternalUID,blob_index>>> euid_lookup;
        std::unique_ptr<MMap::WholeFileMapping<AppendOnlyDictVariable<VariableString,VariableBlobIndex>>> tag_lookup;
        std::unique_ptr<MMap::WholeFileMapping<AppendOnlyCollisionHashMap<value_hash_t,blob_index>>> av_hash_lookup;

        // std::unique_ptr<TokenStore> local_tokens;

        std::vector<std::string> tag_list; // list of tags last received from zefhub
		// std::optional<ZefObservables> observables = {};
        std::shared_ptr<ZefObservables> observables = {};

		// make the matrix elements optional<> : completed tasks can be set tol null. Instead of popping and rearranging the vector contents
		// put this into an optional that the bit that stores whether any(!) fct is to be executed after a tx is closing is directly in the graphdata memory

		// easy way to get access to the associated graph from any object in the mempool, 
		// which gets this GraphData struct froma bit of memory arithmetics (relative to its own address)
		EZefRef get_ROOT_node();
		std::uintptr_t ptr_to_write_head_location() { return (std::uintptr_t(this) + constants::blob_indx_step_in_bytes * write_head); }
		uint64_t hash(blob_index blob_index_lo, blob_index blob_index_hi, uint64_t seed, std::string working_layout) const;
		uint64_t hash_partial(blob_index blob_index_hi, uint64_t seed, std::string working_layout) const;

		// GraphData() { get_all_active_graph_data_tracker().register_graph_data(this); }
		GraphData(MMap::FileGraph * fg, std::optional<BaseUID> maybe_uid, bool generate_root);

		// C++ rule of three: copy ctor, copy assignement, destructor
		GraphData(const GraphData&) = delete;
		GraphData& operator=(const GraphData&) = delete;
        ~GraphData();
    };

    LIBZEF_DLL_EXPORTED std::ostream& operator << (std::ostream& o, const GraphData& gd);

    GraphData * create_GraphData(int mem_style, MMap::FileGraph * fg, std::optional<BaseUID> uid, bool generate_root);
    LIBZEF_DLL_EXPORTED void save_local(GraphData & gd);
    LIBZEF_DLL_EXPORTED void roll_back_using_only_existing(GraphData& gd);
    LIBZEF_DLL_EXPORTED void roll_back_to(GraphData& gd, blob_index index_hi, bool fill_caches);

	namespace internals {
		LIBZEF_DLL_EXPORTED EZefRef get_latest_complete_tx_node(GraphData& gd, blob_index latest_complete_tx_hint = constants::ROOT_NODE_blob_index);
	}



//                                                                ____                 _                                                                
//                                                               / ___|_ __ __ _ _ __ | |__                                                             
//    _____ _____ _____ _____ _____ _____ _____ _____ _____     | |  _| '__/ _` | '_ \| '_ \      _____ _____ _____ _____ _____ _____ _____ _____ _____ 
//   |_____|_____|_____|_____|_____|_____|_____|_____|_____|    | |_| | | | (_| | |_) | | | |    |_____|_____|_____|_____|_____|_____|_____|_____|_____|
//                                                               \____|_|  \__,_| .__/|_| |_|                                                           
//                                                                              |_|                                                           
    struct LIBZEF_DLL_EXPORTED GraphRef {
        BaseUID uid;

        GraphRef(BaseUID uid) : uid(uid) {}
        GraphRef(Graph g);
    };

	LIBZEF_DLL_EXPORTED std::ostream& operator << (std::ostream& o, GraphRef& g);

    struct _InternalEmptyGraph {};
	struct LIBZEF_DLL_EXPORTED Graph {
		// this is the parent object that owns and manages all the memory. The memory pool should be copyable with memcopy
		
		uintptr_t mem_pool = 0; //pointer to the beginning of the used memory pool for nodes and edges. First element is the GraphData. precalc this for lookups
        std::weak_ptr<Butler::Butler> butler_weak;
		//blob_id_to_index_map index_from_id;
		GraphData& my_graph_data() const { return *(GraphData*)mem_pool; }
		operator GraphData& () const { return *(GraphData*)mem_pool; }

        // Deleting the no-arguments ctor to better handle creating Graph objects and then initialising them.
        Graph() = delete;
        explicit Graph(_InternalEmptyGraph) : mem_pool(0) {};
        // This version is for creating a new local graph.
        explicit Graph(bool sync, int mem_style=MMap::MMAP_STYLE_AUTO, bool internal_use_only=false);
        // This version is about preparing a graph for other jobs.
		explicit Graph(int mem_style, MMap::FileGraph * fg = nullptr, std::optional<BaseUID> uid = {}) ;
        explicit Graph(const std::filesystem::path & directory);

		explicit Graph(const GraphData& gd) ;
		explicit Graph(EZefRef uzr) ;		
		explicit Graph(ZefRef zr) ;
        // Stop the implicit cast from GraphData* ptr to bool
        explicit Graph(void * ptr) = delete;
        explicit Graph(const GraphData * ptr, bool stealing_reference);

		explicit Graph(const std::string& graph_uid_or_tag_or_file, int mem_style = MMap::MMAP_STYLE_AUTO) ;
        explicit Graph(const char * graph_uid_or_tag, int mem_style = MMap::MMAP_STYLE_AUTO) : Graph(std::string(graph_uid_or_tag), mem_style) {}
		explicit Graph(const BaseUID& graph_uid, int mem_style = MMap::MMAP_STYLE_AUTO) : Graph(str(graph_uid), mem_style) {};

        static Graph create_from_bytes(Messages::UpdatePayload && payload, int mem_style=MMap::MMAP_STYLE_AUTO, bool internal_use_only = false);
        Graph(const GraphRef& ref) : Graph(ref.uid) {};

        // copy ctor
        Graph(const Graph& g);

		// copy assignment operator
		Graph& operator=(const Graph& g) ;

		// move ctor
		Graph(Graph&& g);
		
		// move assignment operator 
		Graph& operator=(Graph&& g);

		// destructor: if no more Graph objects refer to the underlying GraphData, the last one calls the dtor and frees the memory
		~Graph() ;

        // This delete is only useful for manual control of freeing up the
        // reference to the GraphData, when we can't wait for things like
        // garbage collection in python.
        void delete_graphdata(void);


		uint64_t hash(blob_index blob_index_lo, blob_index blob_index_hi, uint64_t seed, std::string working_layout) const;
        // GraphData::key_map & key_dict();
		bool contains(const std::string&) const;  // check if a key is contained
		bool contains(const TagString&) const;
		bool contains(const BaseUID&) const;
		bool contains(const EternalUID&) const;
		bool contains(const ZefRefUID&) const;

        void sync(bool val=true);

        std::variant<EZefRef,ZefRef> operator[] (const std::string& key) const ;
		EZefRef operator[] (const TagString&) const;
		EZefRef operator[] (const BaseUID&) const;
		EZefRef operator[] (const EternalUID&) const;
		ZefRef operator[] (const ZefRefUID&) const;

		EZefRef operator[] (blob_index index_key) const ;

		bool operator== (const Graph& g2) const;
	};

	LIBZEF_DLL_EXPORTED std::ostream& operator << (std::ostream& o, Graph& g);

    LIBZEF_DLL_EXPORTED Graph create_partial_graph(Graph old_g, blob_index index_hi);
    LIBZEF_DLL_EXPORTED uint64_t partial_hash(Graph g, blob_index index_hi, uint64_t seed, std::string working_layout);

    inline void save_local(Graph & g) {
        save_local(g.my_graph_data());
    }

    // This is related to a transaction, but is more low-level
    struct LockGraphData {
        GraphData * gd;
        // TODO: I was thinking about making this reset to open_tx_thread... might revist later.
        bool was_already_set;

        LockGraphData(GraphData * gd) : gd(gd) {
            if(gd->open_tx_thread == std::this_thread::get_id())
                was_already_set = true;
            else {
                was_already_set = false;
                update_when_ready(gd->open_tx_thread_locker,
                                  gd->open_tx_thread,
                                  std::thread::id(),
                                  std::this_thread::get_id());
            }
        }
        ~LockGraphData() {
            if(was_already_set)
                return;
            // If is for safety - someone lower down may have unlocked already.
            if(gd->open_tx_thread == std::this_thread::get_id())
                update(gd->open_tx_thread_locker, gd->open_tx_thread, std::thread::id());
        }
    };


	namespace internals {

		constexpr blob_index root_node_blob_index() {
			return constants::ROOT_NODE_blob_index;
        }
	
		// exposed to python to get access to the serialized form
        LIBZEF_DLL_EXPORTED std::string get_blobs_as_bytes(GraphData& gd, blob_index start_index, blob_index end_index);
        LIBZEF_DLL_EXPORTED Butler::UpdateHeads full_graph_heads(const GraphData & gd);
		LIBZEF_DLL_EXPORTED Messages::UpdatePayload graph_as_UpdatePayload(GraphData& gd, std::string target_layout="");


		// Blob_and_uid_bytes is assumed to be of size m*2*constants::blob_indx_step_in_bytes, where m is integer.
		// The first half is the blob data, the second the uids.
		LIBZEF_DLL_EXPORTED void set_byte_range(GraphData& gd, blob_index start_index, blob_index end_index, const std::string& blob_bytes);
		LIBZEF_DLL_EXPORTED void include_new_blobs(GraphData& gd, blob_index start_index, blob_index end_index, const std::string& blob_bytes, bool double_link = true, bool fill_caches = true);

		LIBZEF_DLL_EXPORTED void set_data_layout_version_info(const str& new_val, GraphData& gd);
		LIBZEF_DLL_EXPORTED str get_data_layout_version_info(const GraphData& gd);
		LIBZEF_DLL_EXPORTED void set_graph_revision_info(const str& new_val, GraphData& gd);
		LIBZEF_DLL_EXPORTED str get_graph_revision_info(GraphData& gd);

        uint64_t hash_memory_range(const void * lo_ptr, size_t len, uint64_t seed=0);


        LIBZEF_DLL_EXPORTED Messages::UpdatePayload payload_from_local_file(std::filesystem::path path);
        LIBZEF_DLL_EXPORTED void save_payload_to_local_file(const BaseUID & uid, const Messages::UpdatePayload & payload, std::filesystem::path path);

	} //internals

    LIBZEF_DLL_EXPORTED void run_subscriptions(GraphData & gd, EZefRef transaction_uzr);

	namespace internals {
		LIBZEF_DLL_EXPORTED size_t make_hash(EZefRef z_for_uid, RelationType rt, bool is_out_rel, bool is_instantiation);

	}

}

