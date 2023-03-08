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
#include "high_level_api.h"
#include "synchronization.h"
#include "verification.h"
#include "zefops.h"
#include "external_handlers.h"
#include "conversions.h"
#include "tar_file.h"
#include "transaction.h"

#include <doctest/doctest.h>

// TODO: It would be nice to be able to remove this.
// #include <pybind11/pybind11.h>
// namespace py = pybind11;

namespace zefDB {

	namespace internals {
		EZefRef get_latest_complete_tx_node(GraphData& gd, blob_index latest_complete_tx_hint) {
			auto get_next_tx = [](EZefRef tx_uzr)->std::optional<EZefRef> {
				auto tmp = tx_uzr >> L[BT.NEXT_TX_EDGE];
				if (length(tmp) > 1)
					throw std::runtime_error("There is a max of one BT.NEXT_TX_EDGE allowed to come out of any given tx or root blob. This was violated");
				return length(tmp) == 0 ? std::nullopt : std::optional<EZefRef>(tmp | first);
			};

            std::optional<EZefRef> tmp = EZefRef{constants::ROOT_NODE_blob_index, gd};
			try {
				// EZefRef candidate{latest_complete_tx_hint, gd};
                // Can't use a EZefRef in case we go beyond the end.
                void * candidate = ptr_from_blob_index(latest_complete_tx_hint, gd);

                if(latest_complete_tx_hint >= constants::ROOT_NODE_blob_index
                   && latest_complete_tx_hint < gd.write_head) {
                    Butler::ensure_or_get_range(candidate, 1);
                    if (*(BlobType*)candidate == BT.TX_EVENT_NODE) {
                        tmp = EZefRef{candidate};
                    }
                }
			}
			catch (...) {}
			
			while(true) {
				auto last_tx = tmp;
				tmp = get_next_tx(*last_tx);
				if (!bool(tmp))
					return *last_tx;
                if(*tmp == last_tx)
                    throw std::runtime_error("Never going to find the latest complete tx node!");
			}
		}


		// enum_indx generate_unused_random_number_in_enum_reserved_range(const zef_enum_bidirectional_map& enum_map) {
		// 	static std::random_device rd;  //Will be used to obtain a seed for the random number engine
		// 	static std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
		// 	static std::uniform_int_distribution<enum_indx> dis(constants::compiled_aet_types_max_indx, std::numeric_limits<enum_indx>::max());
		// 	enum_indx ran_initial = dis(gen);
		// 	enum_indx indx_candidate = ran_initial - (ran_initial % 16);
		// 	return contains(enum_map.indx_to_string_pair, indx_candidate) ? generate_unused_random_number_in_enum_reserved_range(enum_map) : indx_candidate;
		// }



		size_t make_hash(EZefRef z_for_uid, RelationType rt, bool is_out_rel, bool is_instantiation) {
			static_assert(constants::blob_uid_size_in_bytes >= sizeof(std::size_t));
			return std::hash<token_value_t>{}(rt.relation_type_indx) ^
				std::hash<int>{}(int(is_out_rel) + 2 * int(is_instantiation)) ^  // offset by two: all combinations unique
				// *(std::size_t*)(std::uintptr_t(z_for_uid.blob_ptr) + constants::main_mem_pool_size_in_bytes);   // no need to hash the uid, this is random. Use the first 64bits only
                                    *(size_t*)&blob_uid_ref(z_for_uid);   // no need to hash the uid, this is random. Use the first 64bits only
		}


	}



	std::ostream& operator << (std::ostream& o, const GraphData& gd) {
		o << "<GraphData:";
		o << "\n    reference_count=" << gd.reference_count.load();
		o << "\n    is_primary_instance=" << gd.is_primary_instance;
		o << "\n    number_of_open_tx_sessions=" << gd.number_of_open_tx_sessions;
		o << "\n    should_sync=" << gd.should_sync.load();
		o << "\n    latest_complete_tx=" << gd.latest_complete_tx.load();
		o << "\n    index_of_open_tx_node=" << gd.index_of_open_tx_node;
		// o << "\n    zefscription_head_was_sent_out_head=" << gd.zefscription_head_was_sent_out_head;
		// o << "\n    zefscription_head_can_send_out_head=" << gd.zefscription_head_can_send_out_head;
		o << "\n    write_head=" << gd.write_head.load();
		o << "\n    read_head=" << gd.read_head.load();
		o << "\n    sync_head=" << gd.sync_head.load();
		// o << "\n    global_updates_available_for_primary_thread=" << gd.global_updates_available_for_primary_thread;
		// o << "\n    some_view_graph_of_this_graph_has_an_update=" << gd.some_view_graph_of_this_graph_has_an_update;
		o << "\n    tag_list=[";
        for (const auto & tag : gd.tag_list)
            o << tag << ",";
        o << "]";
		o << "\n >";
		return o;
	}











    GraphData::~GraphData() { 
        // TODO: This check should be replaced with something more obvious - is this combination equivalent to "is subscribed"?
        if (should_sync && is_primary_instance && !Butler::butler_is_master) {
            if(!in_sync()) {
                std::cerr << "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" << std::endl;
                std::cerr << "WARNING: Graph was not fully sent out before being cleaned up." << std::endl;
                std::cerr << "WARNING: Graph was not fully sent out before being cleaned up." << std::endl;
                std::cerr << "WARNING: Graph was not fully sent out before being cleaned up." << std::endl;
                std::cerr << "WARNING: Graph was not fully sent out before being cleaned up." << std::endl;
                std::cerr << "WARNING: Graph was not fully sent out before being cleaned up." << std::endl;
                std::cerr << "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" << std::endl;
            }
        }
    }

 





    GraphData::GraphData(MMap::FileGraph * fg, std::optional<BaseUID> given_uid, bool generate_root) {
        static_assert(constants::ROOT_NODE_blob_index * constants::blob_indx_step_in_bytes > sizeof(GraphData));  // make sure 42 is a sufficiently large index that gd fits in before
        // initialize ROOT_NODE as the first node at ROOT_NODE_blob_index
        reference_count = 0;
        managing_thread_id = std::this_thread::get_id();

        // Prepare the additional structures that are file backed.
        // We reserve 120kB-ish for the key_dict to attempt to make it go into
        // mmap-ed space instead of a memory arena.
        // key_dict.reserve(3*1024);
        // This is here only for testing
        // mallopt(M_MMAP_THRESHOLD, 100*1024);
        // mallopt(M_ARENA_MAX, 8);
        // key_dict = std::make_unique<key_map>();
        // key_dict->reserve(3*1024);
        if(fg == nullptr) {
            // To save typing out lots of things but otherwise hideous...
#define MAKE_UNIQUE(x) x = std::make_unique<decltype(x)::element_type>(MMap::Anonymous{});
            MAKE_UNIQUE(ETs_used);
            MAKE_UNIQUE(RTs_used);
            MAKE_UNIQUE(ENs_used);

            MAKE_UNIQUE(uid_lookup);
            MAKE_UNIQUE(euid_lookup);
            MAKE_UNIQUE(tag_lookup);
            MAKE_UNIQUE(av_hash_lookup);
#undef MAKE_UNIQUE
        } else {
            if(fg->get_version() <= 2)
                throw std::runtime_error("Too old a graph");

            auto prefix = fg->get_prefix();
#define MAKE_UNIQUE2(x,y) x = std::make_unique<decltype(x)::element_type>(*fg, prefix->y);
#define MAKE_UNIQUE(x) MAKE_UNIQUE2(x,x)
            MAKE_UNIQUE2(ETs_used, tokens_ET);
            MAKE_UNIQUE2(RTs_used, tokens_RT);
            MAKE_UNIQUE2(ENs_used, tokens_EN);

            MAKE_UNIQUE(uid_lookup);
            MAKE_UNIQUE(euid_lookup);
            MAKE_UNIQUE(tag_lookup);
            MAKE_UNIQUE(av_hash_lookup);
#undef MAKE_UNIQUE
#undef MAKE_UNIQUE2
        }

        should_sync = false;
        is_primary_instance = true;

        bool preexisting_fg = false;
        if(fg != nullptr) {
            // This is how we tell the file graph has only just been created and has no useful data.
            if(fg->get_latest_blob_index() != 0)
                preexisting_fg = true;
        }

        if(generate_root && preexisting_fg)
            throw std::runtime_error("Something weird going on here! Trying to generate a new graph on top of a preexisting file graph");

        if (generate_root) {
            auto root_uzr = internals::instantiate(BT.ROOT_NODE, *this);
            auto root_uid = given_uid ? *given_uid : make_random_uid();
            auto root_indx = constants::ROOT_NODE_blob_index;
            internals::assign_uid(root_uzr, root_uid);
            move_read_heads_to_write_heads();
            // (*key_dict)[root_uid] = root_indx;
            internals::apply_action_ROOT_NODE(*this, root_uzr, true);
            latest_complete_tx = root_indx;
            // if a new graph is instantiated locally: put in the data layout
            // version stamp of the running zefDB version
            internals::set_data_layout_version_info(data_layout_version, *this);
            // if a new graph is instantiated locally: put in the data layout
            // version stamp of the running zefDB version
            internals::set_graph_revision_info("0", *this);

            // ---------------- create delegate[tx_node] --------------------
            auto delegate_tx_uzr = internals::instantiate(BT.TX_EVENT_NODE, *this);     // the delegate for TXs

            auto to_del_edge_uzr = internals::instantiate(root_uzr, BT.TO_DELEGATE_EDGE, delegate_tx_uzr, *this);
            auto del_inst_uzr = internals::instantiate(root_uzr, BT.DELEGATE_INSTANTIATION_EDGE, to_del_edge_uzr, *this);

            // Every graph starts with an empty transaction
            auto my_tx = Transaction(*this, false, false, false);

            auto & info = MMap::info_from_blobs(this);
            MMap::flush_mmap(info, write_head);
        } else if(preexisting_fg) {
            if(given_uid && internals::get_graph_uid(*this) != given_uid) {
                std::cerr << *given_uid << std::endl;
                std::cerr << internals::get_graph_uid(*this) << std::endl;
                throw std::runtime_error("File graph doesn't have the same uid as passed in!");
            }
            
            write_head = fg->get_latest_blob_index();
            read_head = fg->get_latest_blob_index();
            latest_complete_tx = index(internals::get_latest_complete_tx_node(*this, 0));

        } else {
            // Nothing to do here - just waiting for caller to fill in the graph
        }

    }



    GraphData * create_GraphData(int mem_style, MMap::FileGraph * fg, std::optional<BaseUID> uid, bool generate_root) {  
        Butler::maybe_show_early_tokens();

        static_assert(sizeof(GraphData) < constants::blob_indx_step_in_bytes * constants::ROOT_NODE_blob_index);

        void * mem_pool = MMap::create_mmap(mem_style, fg);
        assert(((uintptr_t)mem_pool % MMap::ZEF_UID_SHIFT) == 0);

        // Have to prep the front part of the blobs range
#ifdef ZEFDB_TEST_NO_MMAP_CHECKS
        // Hacking a direct 10MB alloc once at the beginning
        std::cerr << "WARNING: graph is alloced with only 100MB and no mmap ensure page checks will be done." << std::endl;
        std::cerr << "WARNING: graph is alloced with only 100MB and no mmap ensure page checks will be done." << std::endl;
        std::cerr << "WARNING: graph is alloced with only 100MB and no mmap ensure page checks will be done." << std::endl;
        std::cerr << "WARNING: graph is alloced with only 100MB and no mmap ensure page checks will be done." << std::endl;
        std::cerr << "WARNING: graph is alloced with only 100MB and no mmap ensure page checks will be done." << std::endl;
        MMap::ensure_or_alloc_range_direct(mem_pool, 1000*1000*100);
#else
        MMap::ensure_or_alloc_range(mem_pool, constants::ROOT_NODE_blob_index * constants::blob_indx_step_in_bytes + blobs_ns::max_basic_blob_size);
#endif

        // call the constructor explicitly with the object fixed to this
        // memory location: |new(&var_name)(var_type)|
        new(mem_pool) GraphData(fg, uid, generate_root);

        return (GraphData*)mem_pool;
    }

    GraphDataWrapper::GraphDataWrapper(GraphData * ptr) {
        gd = std::shared_ptr<GraphData>(ptr, [](GraphData * gd) {
            MMap::destroy_mmap((void*)gd);
        });
    }



//                                                                ____                 _                                                                
//                                                               / ___|_ __ __ _ _ __ | |__                                                             
//    _____ _____ _____ _____ _____ _____ _____ _____ _____     | |  _| '__/ _` | '_ \| '_ \      _____ _____ _____ _____ _____ _____ _____ _____ _____ 
//   |_____|_____|_____|_____|_____|_____|_____|_____|_____|    | |_| | | | (_| | |_) | | | |    |_____|_____|_____|_____|_____|_____|_____|_____|_____|
//                                                               \____|_|  \__,_| .__/|_| |_|

    GraphRef::GraphRef(Graph g)
    : uid(internals::get_graph_uid(g)),
      g_cache(g) {}

	std::ostream& operator << (std::ostream& o, GraphRef& g) {
        o << "GraphRef(" << str(g.uid) << ")";
        return o;
    }

    // This version is for creating a new local graph.
	Graph::Graph(bool sync, int mem_style, bool internal_use_only) {
        auto butler = Butler::get_butler();
        butler_weak = butler;
        auto response = butler->msg_push<Messages::GraphLoaded>(Butler::NewGraph{mem_style, internal_use_only});
        if(!response.generic.success)
            throw std::runtime_error("Unable to create new graph: " + response.generic.reason);
        *this = std::move(*response.g);
        // std::cerr << "New graph Graph(), ref_count = " << my_graph_data().reference_count.load() << std::endl;
        
        // After here, we should always destruct if anything fails
        try {
            if(sync)
                this->sync();
        } catch(...) {
            delete_graphdata();
            throw;
        }
	}

	Graph Graph::create_from_bytes(Messages::UpdatePayload && payload, int mem_style, bool internal_use_only) {
        auto butler = Butler::get_butler();
        auto response = butler->msg_push<Messages::GraphLoaded>(Butler::NewGraph{mem_style, std::move(payload), internal_use_only});
        if(!response.generic.success)
            throw std::runtime_error("Unable to create new graph: " + response.generic.reason);

        return *response.g;
	}

    Graph::Graph(const GraphData * ptr, bool stealing_reference) {
        if(ptr == nullptr) {
            mem_pool = 0;
            return;
        }
        butler_weak = Butler::get_butler();
        // TODO: Introduce a check here to determine if the graph has been unloaded, so that we don't segfault.
        mem_pool = (uintptr_t)ptr;
        if (!stealing_reference)
            my_graph_data().reference_count++;
    }
        
    Graph::Graph(const GraphData& gd) : Graph(&gd, false)
        // create a graph struct referring to an existing GraphData struct
    {}

    Graph::Graph(EZefRef uzr) : Graph(graph_data(uzr), false)
        // create a graph struct from a EZefRef
    {}
        
    Graph::Graph(ZefRef zr) : Graph(zr.blob_uzr)
    {}


    Graph::Graph(const std::string & graph_uid_or_tag_or_file, int mem_style, bool create) {
        auto butler = Butler::get_butler();
        butler_weak = butler;
        auto response = butler->msg_push<Messages::GraphLoaded>(Butler::LoadGraph{graph_uid_or_tag_or_file, mem_style, {}, create});
        if(!response.generic.success)
            throw std::runtime_error("Unable to load graph: " + response.generic.reason);

        *this = *response.g;
    }


        // copy ctor
    Graph::Graph(const Graph& g) : mem_pool(g.mem_pool), butler_weak(g.butler_weak) {
            if(mem_pool != 0)
                my_graph_data().reference_count++;
            // std::cerr << "Copy ctor, ref_count = " << my_graph_data().reference_count.load() << std::endl;
		}


		// copy assignment operator
    Graph& Graph::operator=(const Graph& g) {
        butler_weak = g.butler_weak;
			mem_pool = g.mem_pool;
            if(mem_pool != 0)
                my_graph_data().reference_count++;
            // std::cerr << "Copy assign, ref_count = " << my_graph_data().reference_count.load() << std::endl;
			return *this;
		}

		// move ctor
    Graph::Graph(Graph&& g) :
        mem_pool(g.mem_pool), butler_weak(std::move(g.butler_weak))
		{
            g.mem_pool = 0;
            // std::cerr << "Move ctor, ref_count = " << my_graph_data().reference_count.load() << std::endl;
		}

    Graph::Graph(GraphRef& ref) {
        if(ref.g_cache.mem_pool == 0)
            ref.g_cache = Graph{ref.uid};
        *this = ref.g_cache;
    }

        
        // move assignment operator 
    Graph& Graph::operator=(Graph&& g) {
        mem_pool = g.mem_pool;
        g.mem_pool = 0;
        butler_weak = std::move(g.butler_weak);
        return *this;
    }

    void Graph::delete_graphdata() {
        if(mem_pool == 0)
            return;

        // We might get here after the butler has been manually stopped. Check for this first.
        auto butler = butler_weak.lock();
        if(!butler || butler->should_stop)
            // Getting here, we have no guarantee that the underlying GraphData hasn't already been freed. So ignore this for now.
            return;
        
        GraphData & gd = my_graph_data();
        if(gd.started_destructing)
            // Note: we should only ever hit this if we are inside the graph manager for this graph.
            return;
        gd.reference_count--;
        // std::cerr << "At deconstruct, ref count was: " << my_graph_data().reference_count.load() << std::endl;
        if (gd.reference_count == 0) {
            // The final false,true here is to indicate that we don't
            // want to throw if the msgqueue is closed and don't want to
            // wait for a response.
            butler->msg_push(Butler::DoneWithGraph{&gd}, false, true);
        }
        mem_pool = 0;
    }
        // destructor: if no more Graph objects refer to the underlying GraphData, the last one calls the dtor and frees the memory
    Graph::~Graph() {
        delete_graphdata();
    }

    std::ostream& operator << (std::ostream& o, Graph& g) {
        auto& gd = g.my_graph_data();
        o << "Graph(";
        o << '"' << str(uid(g)) << '"';
        if(gd.local_path != "")
            o << ", local: " << gd.local_path.string();
        o << ")";
        return o;
    }


    EZefRef GraphData::get_ROOT_node() { return EZefRef(constants::ROOT_NODE_blob_index ,*this); }

    uint64_t Graph::hash(blob_index blob_index_lo, blob_index blob_index_hi, uint64_t seed, std::string target_layout_version) const {
        return my_graph_data().hash(blob_index_lo, blob_index_hi, seed, target_layout_version);
    }

    uint64_t GraphData::hash(blob_index blob_index_lo, blob_index blob_index_hi, uint64_t seed, std::string target_layout_version) const {
        if(target_layout_version == "")
            target_layout_version = "0.3.0";

        char * lo_ptr = (char*)this + blob_index_lo * constants::blob_indx_step_in_bytes;
        size_t len = (blob_index_hi - blob_index_lo)*constants::blob_indx_step_in_bytes;
        Butler::ensure_or_get_range(lo_ptr, len);

        if (blob_index_lo < 0 ||
            blob_index_lo > blob_index_hi ||
            blob_index_hi > write_head
            ) throw std::runtime_error("invalid blob range to hash");

        if(target_layout_version == "0.3.0")
            return internals::hash_memory_range((void*)(lo_ptr), len, seed);
        else if(target_layout_version == "0.2.0")
            return conversions::hash_0_3_0_as_if_0_2_0((void*)(lo_ptr), len, seed);
        else
            throw std::runtime_error("Can't hash for layout of " + target_layout_version);
    }

    void GraphData::move_read_heads_to_write_heads(bool atomic_update) {
        if(atomic_update) {
            update(heads_locker, [&]() {
                this->move_read_heads_to_write_heads(false);
            });
            return;
        }
            
        read_head = write_head.load();

#define GEN_CACHE(x, y) {                       \
            auto ptr = this->y->get_writer();   \
            ptr->move_read_to_write(); \
        }

            GEN_CACHE("_ETs_used", ETs_used)
            GEN_CACHE("_RTs_used", RTs_used)
            GEN_CACHE("_ENs_used", ENs_used)
            GEN_CACHE("_uid_lookup", uid_lookup)
            GEN_CACHE("_euid_lookup", euid_lookup)
            GEN_CACHE("_tag_lookup", tag_lookup)
            GEN_CACHE("_av_hash_lookup", av_hash_lookup)
#undef GEN_CACHE
    }

    bool GraphData::this_thread_has_write() {
        return is_primary_instance && open_tx_thread == std::this_thread::get_id();
    }

    uint64_t partial_hash(Graph g, blob_index index_hi, uint64_t seed, std::string target_layout_version) {
        // // Optimised common case
        GraphData & gd = g.my_graph_data();
        if(index_hi == gd.write_head)
            return gd.hash(constants::ROOT_NODE_blob_index, index_hi, seed, target_layout_version);

        GraphDataWrapper old_gdw = create_partial_graph(g.my_graph_data(), index_hi);
        // return old_g.hash(constants::ROOT_NODE_blob_index, index_hi, seed, target_layout_version);
        return old_gdw->hash(constants::ROOT_NODE_blob_index, index_hi, seed, target_layout_version);
    }

    GraphDataWrapper create_GraphDataWrapper(const Messages::UpdatePayload & payload) {
        GraphData * gd = create_GraphData(MMap::MMAP_STYLE_ANONYMOUS, nullptr, {}, false);

        Butler::apply_update_with_caches(*gd, payload, false, false);

        return GraphDataWrapper(gd);
    }

    GraphDataWrapper create_partial_graph(GraphData & cur_gd, blob_index index_hi) {
        blob_index index_lo = constants::ROOT_NODE_blob_index;
        if(index_hi > cur_gd.read_head)
            throw std::runtime_error("in create_partial_graph: index_hi is larger than current graph");
        if(index_hi < index_lo)
            throw std::runtime_error("in create_partial_graph: index_hi (" + to_str(index_hi) + ") is before the root node!");

        // We create a proper graph here so that we can access it like normal.
        // The only potential issue is that the graph uid will no longer match
        // what's inside the root blob after we copy it over.

        // Create a graph with "internal_use" turned on.
        // Graph g{false, MMap::MMAP_STYLE_ANONYMOUS, true};
        // GraphData & gd = g.my_graph_data();
        GraphData * gd = create_GraphData(MMap::MMAP_STYLE_ANONYMOUS, nullptr, {}, false);
        // Graph g{gd, false};
        // LockGraphData lock(&gd);

        // std::cerr << "Created temporary internal graph with uid: " << uid(g) << std::endl;

        {
            LockGraphData cur_lock(&cur_gd);
            // Note: even though we hold a lock on the GraphData, this doesn't
            // mean that a transaction isn't open. Instead, we can be sure that
            // our thread is the only one allowed to write to the graph, so the
            // data will be stable while we are in here.
            //
            // The effect of this is that we must use write_head below, as we
            // need to rewind everything affected by blobs past the read_head
            // too.

            auto fixed_write_head = cur_gd.write_head.load();
            // // The above is no longer true, as the lock has been removed. The
            // // new behaviour with updating graph heads under the lock should
            // // make this possible, but lots of testing needs to be done.
            // //
            // // The danger is that `roll_back_to` requires a consistent view and
            // // a way to at least eliminate anything that might be beyond its
            // // knowledge, *without* having to call unapply on each blob.

            // In the end, I re-enabled locking to avoid this. The better way forward is to allow only a partial graph to be created from:
            // a) a graph with a lock on (like above) - can use roll back.
            // b) a set of UpdateHeads, which should be consistent with one another. Can use <= index logic (roll_back_to_using_only_existing but will need cache logic added).
            //    In fact, we may need to throw away the caches and regenerate them, as the logic is too tightly bound otherwise.
            //
            // A partial graph could then be created from a write-in-progress
            // graph at an arbitrary tx by first creating a partial graph from
            // the latest read heads, then rolling back to the given index.

            char * lo_ptr = (char*)gd + index_lo * constants::blob_indx_step_in_bytes;
            // Note we copy the whole lot across, so that roll back can unapply the caches properly
            size_t len = (fixed_write_head - index_lo)*constants::blob_indx_step_in_bytes;
            MMap::ensure_or_alloc_range(lo_ptr, len);

            char * cur_lo_ptr = (char*)&cur_gd + index_lo * constants::blob_indx_step_in_bytes;
            Butler::ensure_or_get_range(cur_lo_ptr, len);
            std::memcpy(lo_ptr, cur_lo_ptr, len);
            // gd.write_head = index_hi;
            gd->write_head = fixed_write_head;
            gd->latest_complete_tx = cur_gd.latest_complete_tx.load();

#define GEN_CACHE(x, y) {                                               \
                auto ptr = gd->y->get_writer();                          \
                auto cur_ptr = cur_gd.y->get();                         \
                auto diff = cur_ptr->create_diff(0, cur_ptr->size());   \
                ptr->apply_diff(diff, ptr.ensure_func());               \
            }

            GEN_CACHE("_ETs_used", ETs_used)
            GEN_CACHE("_RTs_used", RTs_used)
            GEN_CACHE("_ENs_used", ENs_used)
            GEN_CACHE("_uid_lookup", uid_lookup)
            GEN_CACHE("_euid_lookup", euid_lookup)
            GEN_CACHE("_tag_lookup", tag_lookup)
            GEN_CACHE("_av_hash_lookup", av_hash_lookup)
#undef GEN_CACHE
        }

        // roll_back_using_only_existing(gd);
        roll_back_to(*gd, index_hi, true);

        gd->move_read_heads_to_write_heads();

        return GraphDataWrapper(gd);
    }

    // void roll_back_using_only_existing(GraphData & gd) {
    //     // This assumes we have been given (up to write_head) a set of blobs
    //     // which may refer to things beyond the write_head. We update these
    //     // existing blobs so as to forget anything that lies outside their
    //     // range.

    //     LockGraphData lock(&gd);

    //     blob_index index_hi = gd.write_head;

    //     std::unordered_set<blob_index> updated_last_blobs;

    //     TimeSlice latest_time_slice;
    //     blob_index latest_complete_tx = 0;

    //     blob_index cur_index = constants::ROOT_NODE_blob_index;
    //     while(cur_index < index_hi) {
    //         EZefRef ezr{cur_index, gd};
    //         int this_size = size_of_blob(ezr);
    //         int new_last_blob = -1;
    //         bool is_start_of_edges = false;
    //         if(internals::has_edge_list(ezr)) {
    //             visit_blob_with_edges([&](auto & edges) {
    //                 int this_last_blob_offset = -1;
    //                 for(int i = 0; i < edges.local_capacity ; i++) {
    //                     if(abs(edges.indices[i]) >= index_hi) {
    //                         edges.indices[i] = 0;
    //                         if(this_last_blob_offset == -1)
    //                             this_last_blob_offset = i;
    //                     }
    //                 }

    //                 if(edges.indices[edges.local_capacity] >= index_hi) {
    //                     edges.indices[edges.local_capacity] = blobs_ns::sentinel_subsequent_index;
    //                     if(this_last_blob_offset == -1)
    //                         this_last_blob_offset = edges.local_capacity;
    //                 }

    //                 if(this_last_blob_offset != -1) {
    //                     if(this_last_blob_offset == 0)
    //                         new_last_blob = 0;
    //                     else {
    //                         uintptr_t direct_ptr = (uintptr_t)&edges.indices[this_last_blob_offset];
    //                         blob_index * ptr = (blob_index*)(direct_ptr - (direct_ptr % constants::blob_indx_step_in_bytes));
    //                         new_last_blob = blob_index_from_ptr(ptr);
    //                     }
    //                 }
    //             }, ezr);

    //             if(new_last_blob != -1) {
    //                 if(get<BlobType>(ezr) == BlobType::DEFERRED_EDGE_LIST_NODE) {
    //                     auto def = (blobs_ns::DEFERRED_EDGE_LIST_NODE*)ezr.blob_ptr;
    //                     // We only update if the source blob wasn't previously updated.
    //                     if(updated_last_blobs.count(def->first_blob) == 0) {
    //                         EZefRef orig_ezr{def->first_blob, gd};
    //                         *internals::last_edge_holding_blob(orig_ezr) = new_last_blob;
    //                         updated_last_blobs.insert(def->first_blob);
    //                     } else {
    //                         // If we get here something is weird - we are
    //                         // removing items from a deferred edge list that
    //                         // *must* be beyond the end of the index_hi.
    //                         throw std::runtime_error("We should never get here!");
    //                     }
    //                 } else {
    //                     *internals::last_edge_holding_blob(ezr) = new_last_blob;
    //                 }

    //                 updated_last_blobs.insert(cur_index);
    //             }
    //         }

    //         if(get<BlobType>(ezr) == BlobType::TX_EVENT_NODE) {
    //             auto tx = (blobs_ns::TX_EVENT_NODE*)ezr.blob_ptr;
    //             if(tx->time_slice > latest_time_slice) {
    //                 latest_time_slice = tx->time_slice;
    //                 latest_complete_tx = cur_index;
    //             }
    //         }

    //         cur_index += blob_index_size(ezr);
    //     }

    //     // We also need to update terminated time slices. Unfortunately we can't
    //     // do this until we know what the latest time slice was, hence why this
    //     // occurs outside of the loop above.
        
    //     cur_index = constants::ROOT_NODE_blob_index;
    //     while(cur_index < index_hi) {
    //         EZefRef ezr{cur_index, gd};
    //         if(get<BlobType>(ezr) == BlobType::ENTITY_NODE) {
    //             auto rae = (blobs_ns::ENTITY_NODE*)ezr.blob_ptr;
    //             if(rae->termination_time_slice > latest_time_slice)
    //                 rae->termination_time_slice = TimeSlice();
    //         } else if(get<BlobType>(ezr) == BlobType::ATTRIBUTE_ENTITY_NODE) {
    //             auto rae = (blobs_ns::ATTRIBUTE_ENTITY_NODE*)ezr.blob_ptr;
    //             if(rae->termination_time_slice > latest_time_slice)
    //                 rae->termination_time_slice = TimeSlice();
    //         } else if(get<BlobType>(ezr) == BlobType::RELATION_EDGE) {
    //             auto rae = (blobs_ns::RELATION_EDGE*)ezr.blob_ptr;
    //             if(rae->termination_time_slice > latest_time_slice)
    //                 rae->termination_time_slice = TimeSlice();
    //         }
    //         cur_index += blob_index_size(ezr);
    //     }
    //     gd.latest_complete_tx = latest_complete_tx;
    // }

    void roll_back_to(GraphData & gd, blob_index index_hi, bool roll_back_caches) {
        LockGraphData lock(&gd);

        if(index_hi > gd.write_head)
            throw std::runtime_error("in roll_back_to: index_hi is larger than old graph");
        if(index_hi < gd.read_head)
            throw std::runtime_error("in roll_back_to: index_hi (" + to_str(index_hi) + ") is before the read_head!");

        // First run all unapplies. Since we can only traverse forwards in
        // indices, build a list and reverse it to unapply things in FILO order
        // we need.
        std::vector<blob_index> all_indices;
        blob_index cur_index = index_hi;
        while(cur_index < gd.write_head) {
            all_indices.push_back(cur_index);
            EZefRef ezr{cur_index, gd};
            cur_index += blob_index_size(ezr);
        }

        EZefRef earliest_tx(nullptr);
        
        for(auto it = all_indices.rbegin(); it != all_indices.rend(); it++) {
            EZefRef ezr{*it, gd};
            internals::unapply_action_blob(gd, ezr, roll_back_caches);
            if(BT(ezr) == BT.TX_EVENT_NODE) {
                earliest_tx = ezr;
            }
        }

        if(earliest_tx.blob_ptr != nullptr) {
            gd.latest_complete_tx = index(earliest_tx << BT.NEXT_TX_EDGE);
        }

        // Note: we could immediately move the write_head to index_hi in order
        // to avoid any complication with other threads accessing edges.
        // However, the contract is that other threads should only read and use
        // blobs/edges up to read_head anyway so this would be redundant.

        if(false) {
            // TODO: Change this to be the inverse of apply_double_linking
            std::unordered_set<blob_index> updated_last_blobs;

            cur_index = constants::ROOT_NODE_blob_index;
            while(cur_index < index_hi) {
                EZefRef ezr{cur_index, gd};
                int this_size = size_of_blob(ezr);
                int new_last_blob = -1;
                bool is_start_of_edges = false;
                if(internals::has_edge_list(ezr)) {
                    visit_blob_with_edges([&](auto & edges) {
                        int this_last_blob_offset = -1;
                        for(int i = 0; i < edges.local_capacity ; i++) {
                            if(abs(edges.indices[i]) >= index_hi) {
                                edges.indices[i] = 0;
                                if(this_last_blob_offset == -1)
                                    this_last_blob_offset = i;
                            }
                        }

                        if(edges.indices[edges.local_capacity] >= index_hi) {
                            edges.indices[edges.local_capacity] = blobs_ns::sentinel_subsequent_index;
                            if(this_last_blob_offset == -1)
                                this_last_blob_offset = edges.local_capacity;
                        }

                        if(this_last_blob_offset != -1) {
                            if(this_last_blob_offset == 0)
                                new_last_blob = 0;
                            else {
                                uintptr_t direct_ptr = (uintptr_t)&edges.indices[this_last_blob_offset];
                                blob_index * ptr = (blob_index*)(direct_ptr - (direct_ptr % constants::blob_indx_step_in_bytes));
                                new_last_blob = blob_index_from_ptr(ptr);
                            }
                        }
                    }, ezr);

                    if(new_last_blob != -1) {
                        if(get<BlobType>(ezr) == BlobType::DEFERRED_EDGE_LIST_NODE) {
                            auto def = (blobs_ns::DEFERRED_EDGE_LIST_NODE*)ezr.blob_ptr;
                            // We only update if the source blob wasn't previously updated.
                            if(updated_last_blobs.count(def->first_blob) == 0) {
                                EZefRef orig_ezr{def->first_blob, gd};
                                *internals::last_edge_holding_blob(orig_ezr) = new_last_blob;
                                updated_last_blobs.insert(def->first_blob);
                            } else {
                                // If we get here something is weird - we are
                                // removing items from a deferred edge list that
                                // *must* be beyond the end of the index_hi.
                                throw std::runtime_error("We should never get here!");
                            }
                        } else {
                            *internals::last_edge_holding_blob(ezr) = new_last_blob;
                        }

                        updated_last_blobs.insert(cur_index);
                    }
                }

                cur_index += blob_index_size(ezr);
            }
        } else {
            internals::undo_double_linking(gd, index_hi, gd.write_head.load());
        }

        // Now blank out the memory above
        memset(ptr_from_blob_index(index_hi, gd), 0, (gd.write_head.load() - index_hi)*constants::blob_indx_step_in_bytes);
        gd.write_head = index_hi;
    }

    void save_local(GraphData & gd) {
        if(gd.local_path == "")
            throw std::runtime_error("Graph is not a local file, cannot save it.");

        if(gd.sync_head == gd.read_head.load()) {
            // No need to save, should be the same.
            std::cerr << "Not saving, graph hasn't changed since it was loaded." << std::endl;
            return;
        }

        Messages::UpdatePayload payload = internals::graph_as_UpdatePayload(gd, "");
        internals::save_payload_to_local_file(internals::get_graph_uid(gd), payload, gd.local_path);
        gd.sync_head = gd.read_head.load();
        std::cerr << "Wrote graph to: '" << gd.local_path << "'" << std::endl;
    }

	// // thread_safe_unordered_map<std::string, blob_index>& Graph::key_dict() {
    // GraphData::key_map& Graph::key_dict() {
    //     return *my_graph_data().key_dict;
	// }

    using keydict_options = std::variant<TagString, BaseUID, EternalUID, ZefRefUID>;
    keydict_options convert_uid(const std::string & key) {
        auto maybe_uid = to_uid(key);
        return std::visit(overloaded {
                [&key](std::monostate &) -> keydict_options {
                    // This is not a UID. Look it up as a string then.
                    return TagString{key};
                },
                [](auto & x) -> keydict_options {
                    // This is any of the UIDs, BaseUID, EternalUID or ZefRefUID.
                    return x;
                },
            }, maybe_uid);
    }

    bool Graph::contains(const std::string& key) const {
        auto& gd = my_graph_data();
        // wait_same(gd.heads_locker, gd.key_dict_initialized, true);
        // Dispatch depending on if we identify this as a uid or not
        // std::visit(gd.key_dict->contains, maybe_convert_uid(key));
        return std::visit([&](auto && x) { return contains(x); },
                          convert_uid(key));
    }

    bool Graph::contains(const TagString& key) const {
        auto& gd = my_graph_data();
        // wait_same(gd.heads_locker, gd.key_dict_initialized, true);
        return gd.tag_lookup->get()->contains(gd.this_thread_has_write(), key.s);
    }
    bool Graph::contains(const BaseUID& key) const {
        auto& gd = my_graph_data();
        // wait_same(gd.heads_locker, gd.key_dict_initialized, true);
        return gd.uid_lookup->get()->contains(gd.this_thread_has_write(), key);
    }
    bool Graph::contains(const EternalUID& key) const {
        auto& gd = my_graph_data();
        // wait_same(gd.heads_locker, gd.key_dict_initialized, true);
        // If this belongs to this graph, we should use the abbreviation.
        if(key.graph_uid == uid(*this))
            return contains(key.blob_uid);
        else
            return gd.euid_lookup->get()->contains(gd.this_thread_has_write(), key);
    }
    bool Graph::contains(const ZefRefUID& key) const {
        auto& gd = my_graph_data();
        // wait_same(gd.heads_locker, gd.key_dict_initialized, true);
        // Lookup both the blob and the tx
        return (contains(EternalUID(key.blob_uid, key.graph_uid))
                && contains(EternalUID(key.tx_uid, key.graph_uid)));
    }

    void Graph::sync(bool val) {
        auto butler = butler_weak.lock();
        auto response = butler->msg_push<Messages::GenericResponse>(Messages::NotifySync{*this, val});
        if(!response.success)
            throw std::runtime_error("Unable to sync graph: " + response.reason);
    }

    std::variant<EZefRef,ZefRef> Graph::operator[] (const std::string& key) const {
        auto& gd = my_graph_data();
        // wait_same(gd.heads_locker, gd.key_dict_initialized, true);
        return std::visit([&](auto && x) -> std::variant<EZefRef,ZefRef> {
                return (*this)[x];
            }, convert_uid(key));
    }

    EZefRef Graph::operator[] (const TagString& key) const {
        auto& gd = my_graph_data();
        // wait_same(gd.heads_locker, gd.key_dict_initialized, true);
        return operator[](gd.tag_lookup->get()->at(gd.this_thread_has_write(), key.s));
    }

    EZefRef Graph::operator[] (const BaseUID& key) const {
        auto& gd = my_graph_data();
        // wait_same(gd.heads_locker, gd.key_dict_initialized, true);
        return operator[](gd.uid_lookup->get()->at(gd.this_thread_has_write(), key));
    }

    EZefRef Graph::operator[] (const EternalUID& key) const {
        auto& gd = my_graph_data();
        // wait_same(gd.heads_locker, gd.key_dict_initialized, true);
        // If this belongs to this graph, we should use the abbreviation.
        if(key.graph_uid == uid(*this))
            return operator[](key.blob_uid);
        else
            return operator[](gd.euid_lookup->get()->at(gd.this_thread_has_write(), key));
    }

    ZefRef Graph::operator[] (const ZefRefUID& key) const {
        auto& gd = my_graph_data();
        // wait_same(gd.heads_locker, gd.key_dict_initialized, true);
        return ZefRef(operator[](EternalUID(key.blob_uid, key.graph_uid)),
                      operator[](EternalUID(key.tx_uid, key.graph_uid)));
    }

    EZefRef Graph::operator[] (blob_index index_key) const {
			auto& gd = my_graph_data();
			if (index_key < constants::ROOT_NODE_blob_index || index_key>gd.write_head)
				throw std::runtime_error("int index out of range for valid blobs when calling 'g[some_blob_index]': " + to_str(index_key));
			//TODO: perferom more thorough check that the index_key actually refers to a valid blob starting point 
			return EZefRef(index_key, gd);  // throws if key not found
		}

	bool Graph::operator== (const Graph& g2) const {
		return &(my_graph_data()) == &(g2.my_graph_data());  // do they point to the same GraphData struct?
	}

    LockGraphData::LockGraphData(GraphData * gd, bool block) : gd(gd) {
        if(gd->open_tx_thread == std::this_thread::get_id()) {
            was_already_set = true;
            acquired = true;
        } else {
            // We only get the guid if the graph has been initialized.
            BaseUID guid;
            if(gd->write_head > constants::ROOT_NODE_blob_index)
                guid = uid(*gd);
            internals::pass_to_pre_lock_hook(guid);
            was_already_set = false;
            if(block) {
                update_when_ready(gd->open_tx_thread_locker,
                                gd->open_tx_thread,
                                std::thread::id(),
                                std::this_thread::get_id());
                acquired = true;
            } else {
                std::unique_lock lock(gd->open_tx_thread_locker.mutex, std::try_to_lock);
                if(!lock.owns_lock()) {
                    acquired = false;
                } else {
                    auto expected = std::thread::id();
                    // I don't think it is necessary to use atomic exchange,
                    // but will include for safety.
                    std::atomic_compare_exchange_weak(
                        &gd->open_tx_thread,
                        &expected,
                        std::this_thread::get_id()
                    );
                    if(expected != std::thread::id())
                        acquired = false;
                    else
                        acquired = true;
                }
            }
        }
    }
    LockGraphData::~LockGraphData() {
        if(was_already_set || !acquired)
            return;
        // If is for safety - someone lower down may have unlocked already.
        if(gd->open_tx_thread == std::this_thread::get_id())
            update(gd->open_tx_thread_locker, gd->open_tx_thread, std::thread::id());
    }

	namespace internals {
		// // exposed to python to get access to the serialized form
		std::string get_blobs_as_bytes(GraphData& gd, blob_index start_index, blob_index end_index) {
            // Need to ensure we have the range in order to allow a view on it.
            void * blob_ptr = EZefRef{start_index, gd}.blob_ptr;
            Butler::ensure_or_get_range(blob_ptr, (end_index - start_index)*constants::blob_indx_step_in_bytes);

			// memory range returned does not(!) contain the blob at the actual end index.
			return std::string(
				(char*)(&gd) + start_index * constants::blob_indx_step_in_bytes,
				(end_index - start_index) * constants::blob_indx_step_in_bytes
			);
		}	

        Butler::UpdateHeads full_graph_heads(GraphData & gd) {
            return with_lock(gd.heads_locker, [&gd]() {
                Butler::UpdateHeads heads{
                    {constants::ROOT_NODE_blob_index, gd.read_head}
                };

    #define GEN_CACHE(x,y) {                                     \
                    auto ptr = gd.y->get();                      \
                    heads.caches.push_back({x, 0, ptr->read_size()}); \
                }

                GEN_CACHE("_ETs_used", ETs_used)
                GEN_CACHE("_RTs_used", RTs_used)
                GEN_CACHE("_ENs_used", ENs_used)

                GEN_CACHE("_uid_lookup", uid_lookup)
                GEN_CACHE("_euid_lookup", euid_lookup)
                GEN_CACHE("_tag_lookup", tag_lookup)
                GEN_CACHE("_av_hash_lookup", av_hash_lookup)
    #undef GEN_CACHE

                return heads;
            });
        }

        Butler::UpdatePayload graph_as_UpdatePayload(GraphData& gd, std::string target_layout) {
            return Butler::create_update_payload(gd, full_graph_heads(gd), target_layout);
        }

		// Blob_and_uid_bytes is assumed to be of size m*2*constants::blob_indx_step_in_bytes, where m is integer.
		// The first half is the blob data, the second the uids.
		void set_byte_range(GraphData& gd, blob_index start_index, blob_index end_index, const std::string& blob_bytes) {
            // This function only makes sense as a low-level action. But because
            // it modifies the graph, we should make sure the caller grabbed
            // write access first.
            if(gd.open_tx_thread != std::this_thread::get_id())
                throw std::runtime_error("Need write lock to set blobs!");

            void* blobs_start = (void*)((char*)&gd + start_index * constants::blob_indx_step_in_bytes);
            size_t length = (end_index-start_index) * constants::blob_indx_step_in_bytes;

			if (length != blob_bytes.size())
				throw std::runtime_error("size of blob_bytes (" + to_str(blob_bytes.size()) + ") passed to set_byte_range have to be equal.");
            // We pad this by the basic blob size, as this is used when looking
            // up EZefRefs currently. TODO: This is bad! We shouldn't have to
            // include this here, but it is a quick and dirty fix for the zefhub
            // woes.
            MMap::ensure_or_alloc_range(blobs_start, length + blobs_ns::max_basic_blob_size);

            memcpy(blobs_start, blob_bytes.data(), length);
		}

        void include_new_blobs(GraphData& gd, blob_index start_index, blob_index end_index, const std::string& blob_bytes, bool double_link, bool fill_caches) {

            if(gd.open_tx_thread != std::this_thread::get_id())
                throw std::runtime_error("Need write lock to update blobs!");

            set_byte_range(gd, start_index, end_index, blob_bytes);
            if(gd.write_head < end_index)
                gd.write_head = end_index;

            // Note: we can't use EZefRefs here as we will eventually run off
            // the end and try to ensure memory which is not alloced. Need to
            // work with indices first.

			blob_index cur_index = start_index;
			while (cur_index < end_index) {
                EZefRef uzr(cur_index, gd);
				apply_action_blob(gd, uzr, fill_caches);
				cur_index += blob_index_size(uzr);
			}

            if(double_link) {
                apply_double_linking(gd, start_index, end_index);
            } else {
                Graph g(gd);
                // TODO: Change this to partial check only
                verification::verify_graph_double_linking(g);
            }

            auto & info = MMap::info_from_blobs(&gd);
            MMap::flush_mmap(info, gd.write_head);
        }

		void set_data_layout_version_info(const str& new_val, GraphData& gd) {
			if (new_val.size() > constants::data_layout_version_info_size)
				throw std::runtime_error("The max string size that can be assigned to the 'data_layout_version_info' (saved in the graph's root node) is " + to_str(constants::data_layout_version_info_size));
			auto& root_blob = get<blobs_ns::ROOT_NODE>(EZefRef{constants::ROOT_NODE_blob_index, gd});
			root_blob.actual_written_data_layout_version_info_size = new_val.size();
			memcpy(
				root_blob.data_layout_version_info,
				new_val.c_str(),
				new_val.size()
			);
		}
		
		str get_data_layout_version_info(const GraphData& gd) {
			auto& root_blob = get<blobs_ns::ROOT_NODE>(EZefRef{constants::ROOT_NODE_blob_index, gd});
			return std::string(root_blob.data_layout_version_info, root_blob.actual_written_data_layout_version_info_size);
		}

		void set_graph_revision_info(const str& new_val, GraphData& gd) {
			if (new_val.size() > constants::graph_revision_info_size)
				throw std::runtime_error("The max string size that can be assigned to the 'graph_revision_info' (saved in the graph's root node) is " + to_str(constants::graph_revision_info_size));
			auto& root_blob = get<blobs_ns::ROOT_NODE>(EZefRef{constants::ROOT_NODE_blob_index, gd});
			root_blob.actual_written_graph_revision_info_size = new_val.size();
			memcpy(
				root_blob.graph_revision_info,
				new_val.c_str(),
				new_val.size()
			);
		}
		
		str get_graph_revision_info(GraphData& gd) {
			auto& root_blob = get<blobs_ns::ROOT_NODE>(EZefRef{constants::ROOT_NODE_blob_index, gd});
			return std::string(root_blob.graph_revision_info, root_blob.actual_written_graph_revision_info_size);
		}

        uint64_t hash_memory_range(const void * lo_ptr, size_t len, uint64_t seed) {
            // This is just so we can adjust it in the future.
            return XXHash64::hash(lo_ptr, len, seed);
        }


        Messages::UpdatePayload payload_from_local_file(std::filesystem::path path) {
            FileGroup file_group = load_tar_into_memory(path);

            json j;
            std::vector<std::string> rest;

            auto & encoded_file = file_group.find_file("graph.zefgraph");

            std::tie(j,rest) = Communication::parse_ZH_message(encoded_file.contents);

            return Messages::UpdatePayload{j,rest};
        }

        void save_payload_to_local_file(const BaseUID & uid, const Messages::UpdatePayload & payload, std::filesystem::path path) {
            std::string contents = Communication::prepare_ZH_message(payload.j, payload.rest);
            FileInMemory file_data{"graph.zefgraph", std::move(contents)};

            FileInMemory file_uid{"graph.uid", str(uid)};

            FileGroup file_group({file_uid, file_data});
            save_filegroup_to_tar(file_group, path);
        }
	} //internals
}

