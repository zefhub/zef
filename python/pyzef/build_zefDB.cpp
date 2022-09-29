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

#include "common.h"

PYBIND11_MODULE(pyzef, toplevel_module) {
	using namespace zefDB;
	toplevel_module.doc() = "zefdb";

	// --------------------------------- internals submodule --------------------------------------------------------
	py::module main_module = toplevel_module.def_submodule("main", "Where all of the toplevel functions lie");
	py::module internals_submodule = toplevel_module.def_submodule("internals", "The internals namespace of zefDB to access the internal, technical API");  //create submodule
	py::module verification_submodule = toplevel_module.def_submodule("verification", "Tools to verify consistency of graphs and specs");  //create submodule
	// py::module zefops_submodule = toplevel_module.def_submodule("zefops", "operators to be used in conjunction with (U)ZefRef(s)(s)");  //create submodule
	py::module admins_submodule = toplevel_module.def_submodule("admins", "For admin rights operations on zefhub");  //create submodule

    main_module.def("data_layout_version", []() {return zefDB::data_layout_version;});


    main_module.def("rae_type", [](ZefRef zr) { return rae_type(zr); } );
    main_module.def("rae_type", [](EZefRef uzr) { return rae_type(uzr); } );

    py::class_<zefDB::Graph>(main_module, "Graph", py::buffer_protocol())
		//.def("__init__", [](const Graph& self, bool sync) {self.my_graph_data().sync = sync; return self; }, py::arg("self"), py::arg("sync") = true, py::return_value_policy::take_ownership)
		.def(py::init<bool,int>(), py::arg("sync") = false, py::arg("mem_style") = MMap::MMAP_STYLE_AUTO, py::call_guard<py::gil_scoped_release>(), "Graph constructor: sync=False does not register this graph on zefhub. mem_style should be chosen from MMAP_STYLE_AUTO, MMAP_STYLE_ANONYMOUS, MMAP_STYLE_FILE_BACKED.")
		// .def(py::init<py::bytes, py::bytes, bool, int, bool>(), py::arg("blob_bytes"), py::arg("uid_bytes"), py::arg("is_master_graph") = 0, py::arg("index_of_latest_complete_tx_node_hint") = 0, py::arg("sync") = true, "Graph constructor from blob and uid bytes")
		.def(py::init<std::string,int>(), py::arg("tag_or_uid"), py::arg("mem_style") = MMap::MMAP_STYLE_AUTO, py::call_guard<py::gil_scoped_release>(), "Graph constructor from graph uid or tag")
		.def(py::init<BaseUID,int>(), py::arg("uid"), py::arg("mem_style") = MMap::MMAP_STYLE_AUTO, py::call_guard<py::gil_scoped_release>(), "Graph constructor from graph uid")   // TODO: move this into the fct? Is the gil put back on if the constructor throws?
		.def(py::init<GraphData&>(), "Graph constructor from graph uid")
		.def(py::init<EZefRef>(), "Graph constructor from EZefRef")
		.def(py::init<ZefRef>(), "Graph constructor from ZefRef: returns the graph that owns the zefref data, not the reference frame graph")
		.def_property_readonly("graph_data", [](Graph& g)->GraphData& { return g.my_graph_data(); }, py::return_value_policy::reference)  // the mem policy return_value_policy::reference_internal is used here by default: ties lifetime of property returned to lifetime of parent (also stops parent from being destroyed while this is alive)
		.def_property_readonly("uid", [](Graph& g)->BaseUID { return uid(g); })
		.def("hash", &Graph::hash, py::arg("blob_index_lo"), py::arg("blob_index_hi"), py::arg("seed")=0, py::arg("working_layout")="", "calculate the xxhash of the data within the specified blob range. This is non-cryptographic hash fct.")
		.def("hash", [](Graph& g, std::string working_layout) { return g.hash(constants::ROOT_NODE_blob_index, g.my_graph_data().read_head, 0, working_layout); }, py::arg("working_layout")="")
		.def("__repr__", [](const Graph& self) {
            auto& gd = self.my_graph_data();
            if(gd.local_path == "")
                return std::string("Graph('") + str(uid(self)) + "')";
            else
                return std::string("Graph('file://") + gd.local_path.string() + "')";
        })
        .def("__str__", [](const Graph& self) { return to_str(self); })
		.def("__getitem__", [](const Graph& self, const std::string& key)->std::variant<EZefRef,ZefRef> {try { return self[key]; } catch (...) { throw py::key_error{ "key \"" + to_str(key) + "\" not found in graph with uid " + to_str(self | uid)  }; } }, "key lookup in blob key dictionary for this graph returning a EZefRef")
		.def("__getitem__", [](const Graph& self, blob_index key_index)->EZefRef {return self[key_index]; }, "blob index lookup for this graph returning a EZefRef")
		.def("__getitem__", [](const Graph& self, BaseUID key) {return self[key]; }, "key lookup returning EZefRef")
		.def("__getitem__", [](const Graph& self, EternalUID key) {return self[key]; }, "key lookup returning EZefRef")
		.def("__getitem__", [](const Graph& self, ZefRefUID key) {return self[key]; }, "key lookup returning ZefRef")
		.def("__contains__", [](const Graph& self, str key)->bool { return self.contains(key); }, "check whether a key is contained in the graph's key_dict")
		.def("__contains__", [](const Graph& self, BaseUID key)->bool { return self.contains(key); }, "check whether a key is contained in the graph's key_dict")
		.def("__contains__", [](const Graph& self, EternalUID key)->bool { return self.contains(key); }, "check whether a key is contained in the graph's key_dict")
		.def("__contains__", [](const Graph& self, ZefRefUID key)->bool { return self.contains(key); }, "check whether a key is contained in the graph's key_dict")
		.def("__hash__", [](const Graph& self)->int { return int(self.mem_pool); }, "if graph is used as a key in a python dict or set: consider two graphs equal if they refer to the same graph data object")
		.def("__eq__", [](const Graph& self, const Graph& other)->bool { return self.mem_pool==other.mem_pool; }, "if graph is used as a key in a python dict or set: consider two graphs equal if they refer to the same graph data object", py::is_operator())
		.def_property_readonly("key_dict", [](const Graph& self)->std::unordered_map<std::string, blob_index> {
                // Make a copy of the unordered map under a lock
                // auto & gd = self.my_graph_data();
                // std::lock_guard lock(gd.key_dict.m);
                // return gd.key_dict.map;

                // Can't return the threadsafe dicts directly, so need to make a
                // copy. But this is not as simple with a custom dict, as pybind
                // doesn't have automatic conversions for us. Since this really
                // shouldn't be done for now, let's ignore it.
                //
                // If really necessary in the future, probably exposing only the list of keys would be better.
                throw std::runtime_error("No longer allowed to get the key_dict directly. Will reintroduce this later if necessary.");
            })
        .def("list_of_ETs", [](const Graph& self) {
            return self.my_graph_data().ETs_used->get()->as_vector();
        })
        .def("list_of_RTs", [](const Graph& self) {
            return self.my_graph_data().RTs_used->get()->as_vector();
        })
        .def("list_of_ENs", [](const Graph& self) {
            return self.my_graph_data().ENs_used->get()->as_vector();
        })
        .def("uid_cache", [](const Graph& self) {
            auto temp = self.my_graph_data().uid_lookup->get()->as_vector();
            std::vector<std::pair<BaseUID, blob_index>> out;
            std::transform(temp.begin(), temp.end(), std::back_inserter(out),
                           [&](auto & x) { return std::make_pair(x.key, x.val); });
            return out;
        })
        .def("euid_cache", [](const Graph& self) {
            auto temp = self.my_graph_data().euid_lookup->get()->as_vector();
            std::vector<std::pair<EternalUID, blob_index>> out;
            std::transform(temp.begin(), temp.end(), std::back_inserter(out),
                           [&](auto & x) { return std::make_pair(x.key, x.val); });
            return out;
        })
        .def("tag_cache", [](const Graph& self) {
            return self.my_graph_data().tag_lookup->get()->as_vector();
        })
        .def("avn_cache", [](const Graph& self) {
            auto temp = self.my_graph_data().av_hash_lookup->get()->as_vector();
            std::vector<std::pair<value_hash_t, blob_index>> out;
            std::transform(temp.begin(), temp.end(), std::back_inserter(out),
                           [&](auto & x) { return std::make_pair(x.key, x.val); });
            return out;
        })
		.def_property_readonly("tags", [](const Graph& self) { return self.my_graph_data().tag_list; } )
		.def_property_readonly("_raw_ptr", [](const Graph& self) {
            auto ctypes = py::module_::import("ctypes");
            auto c_void_p = ctypes.attr("c_void_p");
            return c_void_p(self.mem_pool);
        })
		.def_property_readonly("subscription_graph", [](const Graph& self)->std::optional<Graph> {
			return bool(self.my_graph_data().observables) ? std::optional<Graph>(*(*self.my_graph_data().observables).g_observables) : std::optional<Graph>({}) ;
		})
		;

    main_module.def("load_graph",
                    // &effect_load_graph,
                    [](std::string tag_or_uid, int mem_style, std::optional<Messages::load_graph_callback_t> callback) {
                        auto butler = Butler::get_butler();
                        auto response = butler->msg_push<Messages::GraphLoaded>(Butler::LoadGraph{tag_or_uid, mem_style, callback});
                        if(!response.generic.success)
                            throw std::runtime_error("Unable to load graph: " + response.generic.reason);
                        return response.g;
                    },
                    py::arg("tag_or_uid"), py::arg("mem_style") = MMap::MMAP_STYLE_AUTO, py::arg("callback") = std::nullopt,
                    py::call_guard<py::gil_scoped_release>(), "Graph constructor from graph uid or tag");

	py::class_<zefDB::Zwitch>(main_module, "Zwitch", py::buffer_protocol())
		.def(py::init<>())
		.def("__repr__", [](const Zwitch& self) { return to_str(self); })
        .def("allow_dynamic_entity_type_definitions", py::overload_cast<>(&Zwitch::allow_dynamic_entity_type_definitions, py::const_))
        .def("allow_dynamic_entity_type_definitions", py::overload_cast<bool>(&Zwitch::allow_dynamic_entity_type_definitions))
        .def("allow_dynamic_relation_type_definitions", py::overload_cast<>(&Zwitch::allow_dynamic_relation_type_definitions, py::const_))
        .def("allow_dynamic_relation_type_definitions", py::overload_cast<bool>(&Zwitch::allow_dynamic_relation_type_definitions))
        .def("allow_dynamic_enum_type_definitions", py::overload_cast<>(&Zwitch::allow_dynamic_enum_type_definitions, py::const_))
        .def("allow_dynamic_enum_type_definitions", py::overload_cast<bool>(&Zwitch::allow_dynamic_enum_type_definitions))
        .def("allow_dynamic_keyword_definitions", py::overload_cast<>(&Zwitch::allow_dynamic_keyword_definitions, py::const_))
        .def("allow_dynamic_keyword_definitions", py::overload_cast<bool>(&Zwitch::allow_dynamic_keyword_definitions))
        .def("allow_dynamic_type_definitions", py::overload_cast<bool>(&Zwitch::allow_dynamic_type_definitions))

        .def("short_output", py::overload_cast<>(&Zwitch::short_output, py::const_))
        .def("short_output", py::overload_cast<bool>(&Zwitch::short_output))
		.def("zefhub_communication_output", py::overload_cast<>(&Zwitch::zefhub_communication_output, py::const_))
		.def("zefhub_communication_output", py::overload_cast<bool>(&Zwitch::zefhub_communication_output))
        .def("graph_event_output", py::overload_cast<>(&Zwitch::graph_event_output, py::const_))
        .def("graph_event_output", py::overload_cast<bool>(&Zwitch::graph_event_output))
        .def("developer_output", py::overload_cast<>(&Zwitch::developer_output, py::const_))
        .def("developer_output", py::overload_cast<bool>(&Zwitch::developer_output))
        .def("debug_zefhub_json_output", py::overload_cast<>(&Zwitch::debug_zefhub_json_output, py::const_))
        .def("debug_zefhub_json_output", py::overload_cast<bool>(&Zwitch::debug_zefhub_json_output))
        .def("debug_allow_unknown_tokens", py::overload_cast<>(&Zwitch::debug_allow_unknown_tokens, py::const_))
        .def("debug_allow_unknown_tokens", py::overload_cast<bool>(&Zwitch::debug_allow_unknown_tokens))
        .def("throw_on_zefrefs_no_tx", py::overload_cast<>(&Zwitch::throw_on_zefrefs_no_tx, py::const_))
        .def("throw_on_zefrefs_no_tx", py::overload_cast<bool>(&Zwitch::throw_on_zefrefs_no_tx))
        .def("default_wait_for_tx_finish", py::overload_cast<>(&Zwitch::default_wait_for_tx_finish, py::const_))
        .def("default_wait_for_tx_finish", py::overload_cast<bool>(&Zwitch::default_wait_for_tx_finish))
        .def("default_rollback_empty_tx", py::overload_cast<>(&Zwitch::default_rollback_empty_tx, py::const_))
        .def("default_rollback_empty_tx", py::overload_cast<bool>(&Zwitch::default_rollback_empty_tx))
		.def("as_dict", &Zwitch::as_dict)
		;
	main_module.attr("zwitch") = &zwitch;  // expose this singleton


	main_module.def("currently_open_tx", [](Graph& g)->EZefRef {
		if (g.my_graph_data().number_of_open_tx_sessions == 0)
			throw std::runtime_error("currently_open_tx(g) called, but there are currently no open tx's.");
		return internals::get_or_create_and_get_tx(g.my_graph_data()); 
		});

    main_module.def("save_local", py::overload_cast<Graph &>(&save_local), py::call_guard<py::gil_scoped_release>());
		

	/*
	Allow dynamic attributes on this class: each instance has the regular __dict__ attached 
	into which key values can be added on the fly. See
	https://pybind11.readthedocs.io/en/stable/classes.html#dynamic-attributes

	Why are we doing this? We want to be able to absorb Zef Values in entity types, e.g.
	ET.Foo['a'] should be self-contained. Using dynamic attributes is not ideal
	when designing this from the ground up, but at the point of writing this struct is used all
	over the place and when writing `ET.Foo['a']` returns a ZefOp 
	`instantiated[ET.Foo]['a']`, which grew out of the very first use case and is causing confusion
	all over the place.
	*/
	py::class_<zefDB::EntityType>(main_module, "EntityType", py::buffer_protocol(), py::dynamic_attr())		
        .def(py::init<token_value_t>())
        .def_readonly("value", &EntityType::entity_type_indx)
		.def("__repr__", [](const EntityType& self)->std::string { return to_str(self); })
		.def("__str__", [](const EntityType& self)->std::string { return str(self); })
		.def("__eq__", [](const EntityType& self, const EntityType& other)->bool {return self==other; }, py::is_operator())
		.def("__hash__", [](const EntityType& self) {return get_hash(self); })  // similar to the python hash for a python int: just use the int itself as the hash
		.def("__int__", [](const EntityType& self)->int {return self.entity_type_indx; })  // similar to the python hash for a python int: just use the int itself as the hash
		.def("__copy__", [](const EntityType& self)->EntityType {return self.entity_type_indx; })  // similar to the python hash for a python int: just use the int itself as the hash
		;
			
	py::class_<zefDB::RelationType>(main_module, "RelationType", py::buffer_protocol(), py::dynamic_attr())
        .def(py::init<token_value_t>())
        .def_readonly("value", &RelationType::relation_type_indx)
		.def("__repr__", [](const RelationType& self)->std::string { return to_str(self); })
		.def("__str__", [](const RelationType& self)->std::string {return str(self); })
		.def("__eq__", [](const RelationType& self, const RelationType& other)->bool {return self.relation_type_indx == other.relation_type_indx; }, py::is_operator())
		.def("__hash__", [](const RelationType& self) {return get_hash(self); })
		.def("__copy__", [](const RelationType& self)->RelationType {return self; })
		;

	py::class_<zefDB::Keyword>(main_module, "Keyword", py::buffer_protocol(), py::dynamic_attr())
        .def(py::init<token_value_t>())
        .def_readonly("value", &Keyword::indx)
		.def("__repr__", [](const Keyword& self)->std::string { return to_str(self); })
		.def("__str__", [](const Keyword& self)->std::string {return str(self); })
		.def("__eq__", [](const Keyword& self, const Keyword& other)->bool {return self.indx == other.indx; }, py::is_operator())
		.def("__hash__", [](const Keyword& self) {return get_hash(self); })
		.def("__copy__", [](const Keyword& self)->Keyword {return self; })
		;


	py::class_<zefDB::ZefEnumValue>(main_module, "ZefEnumValue", py::buffer_protocol())
        .def(py::init<enum_indx>())
		.def_readonly("value", &ZefEnumValue::value)
		.def("__repr__", [](const ZefEnumValue& self)->std::string { return to_str(self); })		
		.def("__eq__", [](const ZefEnumValue& self, const ZefEnumValue& other)->bool {return self.value == other.value; }, py::is_operator())
		.def("__hash__", [](const ZefEnumValue& self) {return get_hash(self); })
        .def_property_readonly("enum_type", [](ZefEnumValue& self)->std::string { return self.enum_type(); })
        .def_property_readonly("enum_value", [](ZefEnumValue& self)->std::string { return self.enum_value(); })
		;

	// main_module.def("type_name", py::overload_cast<EZefRef>(&zefDB::type_name), "given a delegate rel_ent (of arbitrary order, recursively generate its type name: used in the key_dict)");
	// main_module.def("type_name", py::overload_cast<ZefRef>(&zefDB::type_name), "given a delegate rel_ent (of arbitrary order, recursively generate its type name: used in the key_dict)");
	
	
	
	
	
	





	py::class_<zefDB::QuantityFloat>(main_module, "QuantityFloat", py::buffer_protocol())
		.def(py::init<py::float_, ZefEnumValue>(), "Constructor for QuantityFloat from a float and a unit")
		.def(py::init([](int x, ZefEnumValue en) { return new QuantityFloat(double(x), en); }), "Constructor for QuantityFloat from a float and a unit")
		.def_readonly("value", &QuantityFloat::value)
		.def_readonly("unit", &QuantityFloat::unit)

		.def("__add__", [](const QuantityFloat& self, const QuantityFloat& other)->QuantityFloat { return self + other; })
		.def("__add__", [](const QuantityFloat& self, const Time& other)->Time { return self + other; })
		.def("__add__", [](const QuantityFloat& self, const QuantityInt& other)->QuantityFloat { return self + other; })
		.def("__sub__", [](const QuantityFloat& self, const QuantityFloat& other)->QuantityFloat { return self - other; })
		.def("__sub__", [](const QuantityFloat& self, const QuantityInt& other)->QuantityFloat { return self - other; })
		.def("__neg__", [](const QuantityFloat& self)->QuantityFloat { return -self; })

		.def("__mul__", [](const QuantityFloat& self, double x)->QuantityFloat { return self * x; }, py::is_operator())
		.def("__mul__", [](const QuantityFloat& self, int x)->QuantityFloat { return self * x; }, py::is_operator())
		.def("__rmul__", [](const QuantityFloat& self, double x)->QuantityFloat { return x * self; }, py::is_operator())
		.def("__rmul__", [](const QuantityFloat& self, int x)->QuantityFloat { return x * self; }, py::is_operator())

		.def("__truediv__", [](const QuantityFloat& self, double x)->QuantityFloat { return self / x; }, py::is_operator())
		.def("__truediv__", [](const QuantityFloat& self, int x)->QuantityFloat { return self / x; }, py::is_operator())

		.def("__repr__", [](const QuantityFloat& self)->std::string { std::stringstream ss; ss << "<QuantityFloat: " << self << ">"; return ss.str(); })
		.def("__eq__", [](const QuantityFloat& self, const QuantityFloat& other)->bool { return self == other; }, py::is_operator())
		.def("__ne__", [](const QuantityFloat& self, const QuantityFloat& other)->bool { return self != other; }, py::is_operator())
		.def("__gt__", [](const QuantityFloat& self, const QuantityFloat& other)->bool { return self > other; }, py::is_operator())
		.def("__lt__", [](const QuantityFloat& self, const QuantityFloat& other)->bool { return self < other; }, py::is_operator())
		.def("__ge__", [](const QuantityFloat& self, const QuantityFloat& other)->bool { return self >= other; }, py::is_operator())
		.def("__le__", [](const QuantityFloat& self, const QuantityFloat& other)->bool { return self <= other; }, py::is_operator())
		;
	
	py::class_<zefDB::QuantityInt>(main_module, "QuantityInt", py::buffer_protocol())
		.def(py::init<py::int_, ZefEnumValue>(), "Constructor for QuantityInt from a int and a unit")
		.def_readonly("value", &QuantityInt::value)
		.def_readonly("unit", &QuantityInt::unit)

		.def("__add__", [](const QuantityInt& self, const QuantityInt& other)->QuantityInt { return self + other; })
		.def("__add__", [](const QuantityInt& self, const Time& other)->Time { return self + other; })
		.def("__add__", [](const QuantityInt& self, const QuantityFloat& other)->QuantityFloat { return self + other; })
		.def("__sub__", [](const QuantityInt& self, const QuantityInt& other)->QuantityInt { return self - other; })
		.def("__sub__", [](const QuantityInt& self, const QuantityFloat& other)->QuantityFloat { return self - other; })
		.def("__neg__", [](const QuantityInt& self)->QuantityInt { return -self; })

		.def("__mul__", [](const QuantityInt& self, int x)->QuantityInt { return self * x; }, py::is_operator())
		.def("__mul__", [](const QuantityInt& self, double x)->QuantityFloat { return self * x; }, py::is_operator())
		.def("__rmul__", [](const QuantityInt& self, int x)->QuantityInt { return x * self; }, py::is_operator())
		.def("__rmul__", [](const QuantityInt& self, double x)->QuantityFloat { return x * self; }, py::is_operator())

		.def("__truediv__", [](const QuantityInt& self, double x)->QuantityFloat { return self / x; }, py::is_operator())
		.def("__truediv__", [](const QuantityInt& self, int x)->QuantityInt { return self / x; }, py::is_operator())

		.def("__repr__", [](const QuantityInt& self)->std::string { std::stringstream ss; ss << "<QuantityInt: " << self << ">"; return ss.str(); })
		.def("__eq__", [](const QuantityInt& self, const QuantityInt& other)->bool { return self == other; }, py::is_operator())
		.def("__ne__", [](const QuantityInt& self, const QuantityInt& other)->bool { return self != other; }, py::is_operator())
		.def("__gt__", [](const QuantityInt& self, const QuantityInt& other)->bool { return self > other; }, py::is_operator())
		.def("__lt__", [](const QuantityInt& self, const QuantityInt& other)->bool { return self < other; }, py::is_operator())
		.def("__ge__", [](const QuantityInt& self, const QuantityInt& other)->bool { return self >= other; }, py::is_operator())
		.def("__le__", [](const QuantityInt& self, const QuantityInt& other)->bool { return self <= other; }, py::is_operator())
		;



	py::class_<zefDB::Time>(main_module, "Time", py::buffer_protocol())
		.def(py::init<py::float_>(), "Constructor from a unix timestamp")
		//.def(py::init<ZefRef>(), "Constructor from a ZefRef using the time from its reference frame tx")
		.def("__add__", [](Time self, const QuantityFloat& other)->Time { return self + other; })
		.def("__add__", [](Time self, const QuantityInt& other)->Time { return self + other; })
		.def("__sub__", [](Time self, const QuantityFloat& other)->Time { return self - other; })
		.def("__sub__", [](Time self, const QuantityInt& other)->Time { return self - other; })
		.def("__sub__", [](Time self, Time other)->QuantityFloat { return self - other; })
		
		.def("__eq__", [](Time self, Time other)->bool { return self == other; }, py::is_operator())
		.def("__ne__", [](Time self, Time other)->bool { return self != other; }, py::is_operator())
		.def("__lt__", [](Time self, Time other)->bool { return self < other; }, py::is_operator())
		.def("__le__", [](Time self, Time other)->bool { return self <= other; }, py::is_operator())
		.def("__gt__", [](Time self, Time other)->bool { return self > other; }, py::is_operator())
		.def("__ge__", [](Time self, Time other)->bool { return self >= other; }, py::is_operator())
		
		.def("__float__", [](Time self)->double { return self.seconds_since_1970; })			// whne casting to float in python return unix time:   float(my_time)
		
		.def_readonly("seconds_since_1970", &Time::seconds_since_1970 )
		.def("__repr__", [](Time self)->std::string { std::stringstream ss; ss << "<Time in unix time:" << self.seconds_since_1970 << ">"; return ss.str(); })
		;

	main_module.attr("seconds") = seconds;  // expose this singleton
	main_module.attr("minutes") = minutes;
	main_module.attr("hours") = hours;
	main_module.attr("days") = days;
	main_module.attr("weeks") = weeks;
	main_module.attr("months") = months;
	main_module.attr("years") = years;



//                           _   _ _____     __ ____       __                          
//                          | | | |__  /___ / _|  _ \ ___ / _|___                      
//      _____ _____ _____   | | | | / // _ \ |_| |_) / _ \ |_/ __|   _____ _____ _____ 
//     |_____|_____|_____|  | |_| |/ /|  __/  _|  _ <  __/  _\__ \  |_____|_____|_____|
//                           \___//____\___|_| |_| \_\___|_| |___/                     
//                                                                                   
	

	py::class_<zefDB::EZefRef>(main_module, "EZefRef", py::buffer_protocol())
		.def(py::init<>())
		.def(py::init<blob_index, GraphData&>())
		.def(py::init<ZefRef>())
		// equivalent to assigning to 'self', but the recommended way of doing this in pybind11. See https://github.com/pybind/pybind11/blob/master/docs/upgrade.rst
		.def(py::init([](blob_index ind, Graph& g) { return EZefRef(ind, g.my_graph_data()); }))  
        .def("__repr__", [](const EZefRef& self)->std::string {  
            return to_str(self); }, "short output")
        .def("__str__", [](const EZefRef& uzr)->std::string { 
            return to_str(uzr);
        }, "readable output of EZefRef, called with print(...)")
        .def("__eq__", [](EZefRef self, EZefRef other) {return self == other; }, py::is_operator())
		.def("__ne__", [](EZefRef self, EZefRef other) {return self != other; }, py::is_operator())
		;
	

	// py::class_<zefDB::EZefRefs>(main_module, "EZefRefs", py::buffer_protocol())
	// 	.def(py::init([](const std::vector<EZefRef>& v_init) { return new(v_init.size()) EZefRefs(v_init, true); }))
	// 	.def("__repr__", [](const EZefRefs& self)->std::string { std::stringstream ss;
	// 		ss << "<EZefRefs len=" << self.len << ">";
	// 	return ss.str(); }, "short output")
	// 	.def("__iter__", [](EZefRefs& self) { return EZefRefs::PyIterator { self.begin(), self.end() }; },
	// 	// 0 referes to returns val, 1 to the first arg. 
	// 	// The first arg in the template below is the Nurse (the PyIterator returned), the second is the Patient (the Parent EZefRefs object)
	// 	// What this specifies: keep the Patient alive at least during the lifetime of the Nurse
	// 	py::keep_alive<0, 1>())
	// 	.def("__len__", [](const EZefRefs& self) { return self.len; })
	// 	.def("__getitem__", [](EZefRefs& self, int index) {
	// 	if (index < 0 || index >= self.len) throw std::runtime_error("index out of bounds accessing EZefRefs[...]");
	// 	return *(self._get_array_begin() + index);
	// 		})
    //     .def("__add__", [](EZefRefs& self, EZefRefs& other) {
    //         return concatenate(self, other);
    //     })
	// 	;

	

	// py::class_<zefDB::EZefRefs::PyIterator>(main_module, "EZefRefs_PyIterator", py::buffer_protocol())		
	// 	.def("__repr__", [](const zefDB::EZefRefs::PyIterator& self)->std::string { std::stringstream ss;
	// 		ss << "<EZefRefs_PyIterator: ";			
	// 		ss << ">";
	// 		return ss.str(); }, "short output")
	// 	.def("__iter__", [](const zefDB::EZefRefs::PyIterator& self) { return self; })
	// 	.def("__next__", [](zefDB::EZefRefs::PyIterator& self)->EZefRef {
	// 		if(self.main_it.ptr_to_current_uzr == self.end_it.ptr_to_current_uzr)
	// 			throw py::stop_iteration();
	// 		return EZefRef(*(self.main_it++));
	// 	})
	// 	;


	
//                           _____     __ ____       __                          
//                          |__  /___ / _|  _ \ ___ / _|___                      
//      _____ _____ _____     / // _ \ |_| |_) / _ \ |_/ __|   _____ _____ _____ 
//     |_____|_____|_____|   / /|  __/  _|  _ <  __/  _\__ \  |_____|_____|_____|
//                          /____\___|_| |_| \_\___|_| |___/                     
//                                                                         
	
	py::class_<zefDB::ZefRef>(main_module, "ZefRef", py::buffer_protocol())		
		.def(py::init<EZefRef, EZefRef>())
		.def(py::init<blob_index, GraphData&, EZefRef>())
		//.def_readonly("reference_frame_tx", &ZefRefs::reference_frame_tx)
		.def("__repr__", [](const ZefRef& zr)->std::string {
			// std::stringstream ss;
			// ss << "<Zefref  :\n   pointing to uzr=";
			// ss << zr.blob_uzr << "\n    viewed from reference frame tx=" << zr.tx << "\n>";
			// return ss.str();
            std::stringstream ss;
            ss << zr;
            return ss.str();
		})
		.def("__str__", [](const ZefRef& self)->std::string { std::stringstream ss;
                ss << self;
                return ss.str(); })
		.def("__eq__", [](ZefRef self, ZefRef other) {return self == other; }, py::is_operator())
		.def("__ne__", [](ZefRef self, ZefRef other) {return self != other; }, py::is_operator())
		;


		
	// // Python objects are always allocated on the heap. Pybind's wrapping ctor calls the 'new' on a given object by default.
	// // For ZefRefs, the new operator requires extra arguments: we need a custom init fct here, that explicitly passes these
	// // arguments to 'new'. Also, since we are dynamically allocating the ZefRefs struct, we pass 'I_am_allowed_to_overflow=true' along
	// py::class_<zefDB::ZefRefs>(main_module, "ZefRefs", py::buffer_protocol())		
	// 	.def(py::init([](const std::vector<ZefRef>& v_init) { 
    //         return new(v_init.size()) ZefRefs(v_init, true); 
	// 		}
	// 	))
	// 	.def(py::init([](const std::vector<ZefRef>& v_init, const EZefRef& ctx) { 
	// 		return new(v_init.size(), graph_data(ctx)) ZefRefs(v_init, true, ctx); 
	// 		}
	// 	))
	// 	.def(py::init([](const std::vector<ZefRef>& v_init, const ZefRef& ctx) { 
	// 		return new(v_init.size(), graph_data(ctx)) ZefRefs(v_init, true, ctx|to_ezefref); 
	// 		}
	// 	))
	// 	.def("__repr__", [](const ZefRefs& self)->std::string { std::stringstream ss;
    //             ss << "<ZefRefs";
    //             ss << " len=" << self.len;
    //             ss << " slice=" << time_slice(self.reference_frame_tx);
    //             ss << ">";		
	// 	return ss.str(); }, "short output")
	// 	.def("__iter__", [](ZefRefs& self) { return ZefRefs::PyIterator { self.begin(), self.end() }; },
	// 	 //0 refers to returns val, 1 to the first arg. 
	// 	 //The first arg in the template below is the Nurse (the PyIterator returned), the second is the Patient (the Parent ZefRefs object)
	// 	 //What this specifies: keep the Patient alive at least during the lifetime of the Nurse
	// 	py::keep_alive<0, 1>()  
	// 	)
	// 	.def("__len__", [](const ZefRefs& self) { return self.len; })
	// 	.def("__getitem__", [](ZefRefs& self, int index) {
	// 		if (index < 0 || index >= self.len) throw std::runtime_error("index out of bounds accessing ZefRefs[...]");
	// 		return ZefRef{ *(self._get_array_begin() + index), self.reference_frame_tx};
	// 	})

    //     .def("__add__", [](ZefRefs& self, ZefRefs& other) {
    //         return concatenate(self, other);
    //     })
	// 	;

	//main_module.def("length", py::overload_cast<EZefRef>([](EZefRefs zz) {return zz.len; }));
	// internals_submodule.def("length", [](EZefRefs zz) {return zz.len; });
	// internals_submodule.def("length", [](ZefRefs zz) {return zz.len; });

	// py::class_<zefDB::ZefRefs::PyIterator>(main_module, "ZefRefs_PyIterator", py::buffer_protocol())
	// 	.def("__repr__", [](const zefDB::ZefRefs::PyIterator& self)->std::string { std::stringstream ss;
	// ss << "<ZefRefs_PyIterator: ";
	// ss << ">";
	// return ss.str(); }, "short output")
	// 	.def("__iter__", [](const zefDB::ZefRefs::PyIterator& self) { return self; })
	// 	.def("__next__", [](zefDB::ZefRefs::PyIterator& self)->ZefRef {
	// 		if (self.main_it.ptr_to_current_uzr == self.end_it.ptr_to_current_uzr)
	// 			throw py::stop_iteration();
	// 		return ZefRef { *(self.main_it.ptr_to_current_uzr++), self.main_it.reference_frame_tx };
	// 	})		
	// 	;








//                         _   _ _____     __ ____       __                              
//                        | | | |__  /___ / _|  _ \ ___ / _|___ ___                      
//    _____ _____ _____   | | | | / // _ \ |_| |_) / _ \ |_/ __/ __|   _____ _____ _____ 
//   |_____|_____|_____|  | |_| |/ /|  __/  _|  _ <  __/  _\__ \__ \  |_____|_____|_____|
//                         \___//____\___|_| |_| \_\___|_| |___/___/                     
//                                                                                       
// 	py::class_<zefDB::EZefRefss>(main_module, "EZefRefss", py::buffer_protocol())
// 		.def(py::init<std::vector<EZefRefs>>())
// 		.def("__repr__", [](const EZefRefss& self)->std::string { std::stringstream ss;
// 	ss << "<EZefRefss at " << &self << " of length=" << self.len() << ">"; return ss.str(); })
// 		.def("__len__", [](const EZefRefss& self)->size_t { return self.len(); })
// 		.def("__getitem__", [](const EZefRefss& self, int index) {
// 		if (index < 0 || size_t(index) >= self.len()) throw std::runtime_error("index out of bounds accessing EZefRefss[...]");
// 		return self.v[index];
// 			})
// 		.def("__iter__", [](EZefRefss& self) { return EZefRefss::PyIterator{ self.begin(), self.end() }; },
// 			// 0 referes to returns val, 1 to the first arg. 
// 				// The first arg in the template below is the Nurse (the PyIterator returned), the second is the Patient (the Parent EZefRefs object)
// 				// What this specifies: keep the Patient alive at least during the lifetime of the Nurse
// 			py::keep_alive<0, 1>()
// 			)
// 		;


// 	py::class_<zefDB::EZefRefss::PyIterator>(main_module, "EZefRefss_PyIterator", py::buffer_protocol())
// 		.def("__repr__", [](const zefDB::EZefRefss::PyIterator& self)->std::string { std::stringstream ss;
// 	ss << "<EZefRefss_PyIterator: ";
// 	ss << ">";
// 	return ss.str(); }, "short output")
// 		.def("__iter__", [](const zefDB::EZefRefss::PyIterator& self) { return self; })
// 		.def("__next__", [](zefDB::EZefRefss::PyIterator& self)->EZefRefs {
// 		if (self.main_it == self.end_it)
// 			throw py::stop_iteration();
// 		return *self.main_it++;
// 			})
// 		;




// //                           _____     __ ____       __                              
// //                          |__  /___ / _|  _ \ ___ / _|___ ___                      
// //      _____ _____ _____     / // _ \ |_| |_) / _ \ |_/ __/ __|   _____ _____ _____ 
// //     |_____|_____|_____|   / /|  __/  _|  _ <  __/  _\__ \__ \  |_____|_____|_____|
// //                          /____\___|_| |_| \_\___|_| |___/___/                     
// //                                                                      
// 	py::class_<zefDB::ZefRefss>(main_module, "ZefRefss", py::buffer_protocol())
// 		.def(py::init<std::vector<ZefRefs>>())
// 		.def("__repr__", [](const ZefRefss& self)->std::string { std::stringstream ss;
// 			ss << "<ZefRefss at " << &self << " of length=" << self.len() << ">"; return ss.str(); })
// 		.def("__len__", [](const ZefRefss& self)->size_t { return self.len(); })
// 		.def("__getitem__", [](const ZefRefss& self, int index) {
// 				if (index < 0 || size_t(index) >= self.len()) throw std::runtime_error("index out of bounds accessing ZefRefss[...]");
// 				return self.v[index];
// 			})
// 		.def("__iter__", [](ZefRefss& self) { return ZefRefss::PyIterator{ self.begin(), self.end() }; },
// 			// 0 referes to returns val, 1 to the first arg. 
// 				// The first arg in the template below is the Nurse (the PyIterator returned), the second is the Patient (the Parent EZefRefs object)
// 				// What this specifies: keep the Patient alive at least during the lifetime of the Nurse
// 			py::keep_alive<0, 1>()
// 			)
// 		.def_readonly("reference_frame_tx", &ZefRefss::reference_frame_tx)
// 		;


// 	py::class_<zefDB::ZefRefss::PyIterator>(main_module, "ZefRefss_PyIterator", py::buffer_protocol())
// 		.def("__repr__", [](const zefDB::ZefRefss::PyIterator& self)->std::string { std::stringstream ss;
// 	ss << "<ZefRefss_PyIterator: ";
// 	ss << ">";
// 	return ss.str(); }, "short output")
// 		.def("__iter__", [](const zefDB::ZefRefss::PyIterator& self) { return self; })
// 		.def("__next__", [](zefDB::ZefRefss::PyIterator& self)->ZefRefs {
// 			if (self.main_it == self.end_it)
// 				throw py::stop_iteration();
// 			return *self.main_it++;
// 		})
// 		;







	main_module.def("index", py::overload_cast<EZefRef>(&index), "get the blob_index of a EZefRef");
	main_module.def("index", py::overload_cast<ZefRef>(&index), "get the blob_index of a ZefRef");





 	main_module.def("merge", py::overload_cast<const json&,Graph,bool>(&merge), py::call_guard<py::gil_scoped_release>(), "graph_delta"_a, "target_graph"_a, "fire_and_forget"_a = false);


	main_module.def("instantiate", py::overload_cast<EntityType, const Graph&, std::optional<BaseUID>>(&instantiate), py::call_guard<py::gil_scoped_release>(), "A function to instantiate an entity", "entity_type"_a, "g"_a, "uid"_a=py::none());
	main_module.def("instantiate", py::overload_cast<RelationType, const Graph&, std::optional<BaseUID>>(&instantiate), py::call_guard<py::gil_scoped_release>(), "Invalid call signature for instantiate (you must pass a source and target along with a relation)", "relation_type"_a, "g"_a, "uid"_a=py::none());
	main_module.def("instantiate", py::overload_cast<AttributeEntityType, const Graph&, std::optional<BaseUID>>(&instantiate), py::call_guard<py::gil_scoped_release>(), "A function to instantiate an atomic entity", "atomic_entity_type"_a, "g"_a, "uid"_a = py::none());
	main_module.def("instantiate", py::overload_cast<ZefRef, RelationType, ZefRef, const Graph&, std::optional<BaseUID>>(&instantiate), py::call_guard<py::gil_scoped_release>(), "A function to instantiate an relation", "src"_a, "relation_type"_a, "dst"_a, "g"_a, "uid"_a = py::none());
	main_module.def("instantiate", py::overload_cast<EZefRef, RelationType, EZefRef, const Graph&, std::optional<BaseUID>>(&instantiate), py::call_guard<py::gil_scoped_release>(), "A function to instantiate an relation", "src"_a, "relation_type"_a, "dst"_a, "g"_a, "uid"_a = py::none());

	main_module.def("instantiate_value_node", &instantiate_value_node<bool>, py::call_guard<py::gil_scoped_release>(), "value"_a, "g"_a);
	main_module.def("instantiate_value_node", &instantiate_value_node<int>, py::call_guard<py::gil_scoped_release>(), "value"_a, "g"_a);
	main_module.def("instantiate_value_node", &instantiate_value_node<double>, py::call_guard<py::gil_scoped_release>(), "value"_a, "g"_a);
	main_module.def("instantiate_value_node", &instantiate_value_node<str>, py::call_guard<py::gil_scoped_release>(), "value"_a, "g"_a);
	main_module.def("instantiate_value_node", &instantiate_value_node<Time>, py::call_guard<py::gil_scoped_release>(), "value"_a, "g"_a);
	main_module.def("instantiate_value_node", &instantiate_value_node<ZefEnumValue>, py::call_guard<py::gil_scoped_release>(), "value"_a, "g"_a);
	main_module.def("instantiate_value_node", &instantiate_value_node<QuantityFloat>, py::call_guard<py::gil_scoped_release>(), "value"_a, "g"_a);
	main_module.def("instantiate_value_node", &instantiate_value_node<QuantityInt>, py::call_guard<py::gil_scoped_release>(), "value"_a, "g"_a);
	main_module.def("instantiate_value_node", &instantiate_value_node<SerializedValue>, py::call_guard<py::gil_scoped_release>(), "value"_a, "g"_a);
	main_module.def("instantiate_value_node", &instantiate_value_node<AttributeEntityType>, py::call_guard<py::gil_scoped_release>(), "value"_a, "g"_a);
	

//                                   _                    __      _                            
//                         ___ _ __ | |_ _ __ _   _      / _| ___| |_ ___                      
//    _____ _____ _____   / _ \ '_ \| __| '__| | | |    | |_ / __| __/ __|   _____ _____ _____ 
//   |_____|_____|_____| |  __/ | | | |_| |  | |_| |    |  _| (__| |_\__ \  |_____|_____|_____|
//                        \___|_| |_|\__|_|   \__, |    |_|  \___|\__|___/                     
//                                            |___/                                            

	main_module.def("blobs", [](GraphData& gd) {return blobs(gd); }, "return all blobs in a graph as a EZefRefs object");
	main_module.def("blobs", [](Graph& g) {return blobs(g); }, "return all blobs in a graph as a EZefRefs object");
	main_module.def("blobs", [](GraphData& gd, blob_index from_index) {return blobs(gd, from_index); }, "return all blobs in a graph as a EZefRefs object");
	main_module.def("blobs", [](Graph& g, blob_index from_index) {return blobs(g, from_index); }, "return all blobs in a graph as a EZefRefs object");
	main_module.def("blobs", [](GraphData& gd, blob_index from_index, blob_index to_index) {return blobs(gd, from_index, to_index); }, "return all blobs in a graph as a EZefRefs object");
    main_module.def("blobs", [](Graph& g, blob_index from_index, blob_index to_index) {return blobs(g, from_index, to_index); }, "return all blobs in a graph as a EZefRefs object");


	main_module.def("make_primary", make_primary, py::call_guard<py::gil_scoped_release>(), "request to take on or give off the primary instance status: make_primary(g, True)", "g"_a, "take_on"_a=true);
	main_module.def("set_keep_alive", set_keep_alive, py::call_guard<py::gil_scoped_release>(), "keep the graph subscribed to, even if all references to it are removed", "g"_a, "keep_alive"_a=true);
	main_module.def("tag", py::overload_cast<const Graph&,const std::string&,bool,bool>(&tag), py::call_guard<py::gil_scoped_release>(), "Add a name tag to a graph to retrieve it by that name in the future: tag(g, 'some_graph_name_tag')", "g"_a, "name_tag"_a, "force"_a=false, "adding"_a=true);
	main_module.def("zef_get", zef_get, py::call_guard<py::gil_scoped_release>(), "get a EZefRef together with the graph for some specified uid (could be on any graph - zefhub searches)", "uid_or_name_tag"_a);
    main_module.def("zearch", zearch, py::call_guard<py::gil_scoped_release>(), "perform a general fuzzy search for stuff on zefhub. Returns a string as an answer.", "zearch_term"_a="");
    main_module.def("lookup_uid", lookup_uid, py::call_guard<py::gil_scoped_release>(), "lookup a UID for a graph by tag. Should return None if not found but will currently error.", "tag"_a);
	main_module.def("sync", zefDB::sync, py::call_guard<py::gil_scoped_release>(), "set the sync state of a graph: should it sed / receive updates to/from zefhub? Zpplies to primary and view instances.", "g"_a, "do_sync"_a=true);

	main_module.def("tag", py::overload_cast<ZefRef,const std::string,bool>(&tag), py::call_guard<py::gil_scoped_release>(), "Add a name tag / key_dict entry to for a specific ZefRef:  tag(my_z, 'my_favorite_zefref')", "z"_a, "name_tag"_a, "force_if_name_tags_other_rel_ent"_a=false);
	main_module.def("tag", py::overload_cast<EZefRef,const std::string,bool>(&tag), py::call_guard<py::gil_scoped_release>(), "Add a name tag / key_dict entry to for a specific ZefRef:  tag(my_z, 'my_favorite_zefref')", "z"_a, "name_tag"_a, "force_if_name_tags_other_rel_ent"_a=false);

	verification_submodule.def("verify_graph_double_linking", verification::verify_graph_double_linking, "Check that node/edge linking is consistent for the indexes.");
	verification_submodule.def("verify_chronological_instantiation_order", verification::verify_chronological_instantiation_order, "Check that RAEs and delegates have a correct chronological instantiation order.");
	verification_submodule.def("break_graph", verification::break_graph, "Internal use checks");
	verification_submodule.def("verify_graph", verification::verify_graph, "Internal use checks", py::call_guard<py::gil_scoped_release>());

	admins_submodule.def("add_user", [](std::string username, std::string key) { zefDB::user_management("add_user", username, "", key);});
	admins_submodule.def("reset_user_key", [](std::string username, std::string key) { zefDB::user_management("reset_user_key", username, "", key);});
	admins_submodule.def("disable_user", [](std::string username) { zefDB::user_management("disable_user", username, "", "");});
	admins_submodule.def("enable_user", [](std::string username) { zefDB::user_management("enable_user", username, "", "");});
	admins_submodule.def("add_group", [](std::string name) { zefDB::user_management("add_group", name, "", "");});
	// admins_submodule.def("remove_group", [](std::string name) { zefDB::user_management("remove_group", name, "", "");});
	admins_submodule.def("add_group_member", [](std::string user, std::string group) { zefDB::user_management("add_group_member", user, group, "");});
	admins_submodule.def("remove_group_member", [](std::string user, std::string group) { zefDB::user_management("remove_group_member", user, group, "");});
	admins_submodule.def("add_right", [](std::string subject, std::string group, std::string right) { zefDB::user_management("add_right", subject, group, right);});
	admins_submodule.def("remove_right", [](std::string subject, std::string group, std::string right) { zefDB::user_management("remove_right", subject, group, right);});

	admins_submodule.def("token_management", py::overload_cast<std::string,EntityType,std::string>(&token_management), py::call_guard<py::gil_scoped_release>());
	admins_submodule.def("token_management", py::overload_cast<std::string,RelationType,std::string>(&token_management), py::call_guard<py::gil_scoped_release>());
	admins_submodule.def("token_management", py::overload_cast<std::string,ZefEnumValue,std::string>(&token_management), py::call_guard<py::gil_scoped_release>());
	admins_submodule.def("token_management", py::overload_cast<std::string,Keyword,std::string>(&token_management), py::call_guard<py::gil_scoped_release>());
	admins_submodule.def("token_management", py::overload_cast<std::string,std::string,std::string,std::string>(&token_management), py::call_guard<py::gil_scoped_release>());





	main_module.def("deep_copy", py::overload_cast<Graph>(&deep_copy), py::call_guard<py::gil_scoped_release>(), "deep copy a graph: overwrite any uid of the old graph with a new uid.");
	main_module.def("revision_graph", py::overload_cast<Graph>(&revision_graph), "revision a graph: uids stay identical, simple revisioning defrags and groups all edge lists.");

    py::class_<zefDB::TimeSlice>(main_module, "TimeSlice", py::buffer_protocol())
        .def(py::init<int>(), "construct TimeSlice from int")
        .def_readwrite("value", &TimeSlice::value)
        .def("__repr__", [](TimeSlice self)->std::string { std::stringstream ss;
                ss << "<TimeSlice: " << self.value << ">";
                return ss.str(); })

        .def("__call__", py::overload_cast<EZefRef>(&TimeSlice::operator(), py::const_))
        .def("__call__", py::overload_cast<ZefRef>(&TimeSlice::operator(), py::const_))

        .def("__ror__", py::overload_cast<EZefRef>(&TimeSlice::operator(), py::const_))
        .def("__ror__", py::overload_cast<ZefRef>(&TimeSlice::operator(), py::const_))
        .def("__eq__", [](TimeSlice self, TimeSlice other) {return self == other; }, py::is_operator())
        .def("__ne__", [](TimeSlice self, TimeSlice other) {return self != other; }, py::is_operator())
        .def("__gt__", [](TimeSlice self, TimeSlice other) {return self > other; }, py::is_operator())
        .def("__lt__", [](TimeSlice self, TimeSlice other) {return self < other; }, py::is_operator())
        .def("__ge__", [](TimeSlice self, TimeSlice other) {return self >= other; }, py::is_operator())
        .def("__le__", [](TimeSlice self, TimeSlice other) {return self <= other; }, py::is_operator())
            
        .def("__int__", [](TimeSlice self) {return self.value; })   // allow casting to int in python:      m = int(my_time_slice)
        ;


    main_module.def("get_config_var", &get_config_var, py::call_guard<py::gil_scoped_release>(), "Get a variable with defaults/override resolution from the config.", py::arg("key"));
    main_module.def("set_config_var", &set_config_var, py::call_guard<py::gil_scoped_release>(), "Set a variable in the config.", py::arg("key"), py::arg("value"));
    main_module.def("list_config", &list_config, py::call_guard<py::gil_scoped_release>(), "List the config including all default/environment set variables.", py::arg("filter")="");
    main_module.def("validate_config_file", &validate_config_file, py::call_guard<py::gil_scoped_release>(), "Ensure the config file and environment overrides have sensible values.");



	fill_internals_module(internals_submodule);
	create_zefops_module(toplevel_module, internals_submodule);
}
