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
#include "butler/auth_server.h"

// use this function to create the bindings and instantiation for a given scalar type
template<typename ScalarType>
void declare_zef_tensor_1(py::module& m, const std::string& typestr) {
	using Class = zefDB::Tensor<ScalarType, 1>;
	std::string pyclass_name = std::string("ZefTensor_") + typestr + "_1";
	py::class_<Class>(m, pyclass_name.c_str(), py::buffer_protocol())
		.def(py::init<int>())   // construct with a fixed size
		.def(py::init<std::vector<ScalarType>>())   // construct from a list		
		.def("__getitem__", [](Class self, int index)->ScalarType {return self[index]; })   // TODO: make this const ref and use the approapriate operator
		.def("__repr__", [typestr](Class self)->std::string {   // TODO: pass as const ref once const_Iterator has been properly implemented
			std::stringstream ss;
			ss << "<ZefTensor<" << typestr << ",1>: [";
			for (const auto& el : self) 
				ss << el << ",";
			ss << "]>";
			return ss.str();
		})
		;
}


void fill_internals_module(py::module_ & internals_submodule) {
    using namespace zefDB;
    using namespace zefDB::zefOps;

    internals_submodule.def("login", []() {
        auto butler = Butler::get_butler();
        // Setup a thread to poll for Ctrl-C from python
        // ....
        // Not so easy - need to be in the main thread to be able to check for ctrl-c. So reverse all of the logic.

        auto f = std::async(&Butler::Butler::user_login, butler.get());
        while(true) {
            auto res = f.wait_for(std::chrono::milliseconds(500));
            if(res == std::future_status::ready)
                break;

            {
                py::gil_scoped_acquire acquire;
                if (PyErr_CheckSignals() != 0) {
                    std::cerr << "Got exception from python, stopping auth server" << std::endl;
                    zefDB::stop_local_auth_server();
                    break;
                }
            }
        }
        f.get();
    }, py::call_guard<py::gil_scoped_release>(), "This is a low-level function. Do not use if you don't know what you are doing.");

    internals_submodule.def("login_manual", [](std::string auth_string) {
        auto butler = Butler::get_butler();
        if(butler->network.is_running())
            throw std::runtime_error("Can't change login details while connected to ZefHub.");

        butler->session_auth_key = auth_string;
    }, py::call_guard<py::gil_scoped_release>(), "This is a low-level function. Do not use if you don't know what you are doing.");

    internals_submodule.def("logout", []() {
        auto butler = Butler::get_butler();
        butler->user_logout();
    }, "This is a low-level function. Do not use if you don't know what you are doing.");

    internals_submodule.def("finished_loading_python_core", []() {
        zefDB::initialised_python_core = true;
    });

    internals_submodule.def("wait_for_auth", [](double timeout_in_sec) {
        auto butler = Butler::get_butler();
        return butler->wait_for_auth(std::chrono::duration<double>(timeout_in_sec));
    }, py::arg("timeout_in_sec") = -1.0, py::call_guard<py::gil_scoped_release>(), "Waits for auth to complete, with optional timeout. Returns false only if timed out before auth finished, otherwise true.");

    internals_submodule.def("start_connection", []() {
        auto butler = Butler::get_butler();
        butler->start_connection();
    }, py::call_guard<py::gil_scoped_release>());

    internals_submodule.def("stop_connection", []() {
        auto butler = Butler::get_butler();
        butler->stop_connection();
    }, py::call_guard<py::gil_scoped_release>());

    internals_submodule.def("who_am_i", []() {
        auto butler = Butler::get_butler();
        return butler->who_am_i();
    }, py::call_guard<py::gil_scoped_release>());
        

	internals_submodule.def("is_any_UID", &is_any_UID);
	internals_submodule.def("is_BaseUID", &is_BaseUID);
	internals_submodule.def("is_EternalUID", &is_EternalUID);
	internals_submodule.def("is_ZefRefUID", &is_ZefRefUID);

    internals_submodule.def("to_uid", [](std::string & s) {
        auto out = to_uid(s);
        using out_t = std::optional<std::variant<BaseUID,EternalUID,ZefRefUID>>;
        return std::visit(overloaded {
                [](const std::monostate & x)->out_t { return out_t{}; },
                [](const auto & x)->out_t { return x; }
            }, out);
    });
    internals_submodule.def("make_random_uid", []() { return make_random_uid(); } );

	internals_submodule.def("size_of_blob", &size_of_blob);

	internals_submodule.def("show_blob_details", [](const EZefRef & uzr) {
        return visit_blob([](const auto & x) { std::stringstream ss; ss << x; return ss.str(); },
                     uzr);
    });

	internals_submodule.def("blob_to_json", &blob_to_json);
	internals_submodule.def("create_from_json", &internals::create_from_json, py::call_guard<py::gil_scoped_release>());

	//declare_zef_tensor_1<bool>(internals_submodule, str("Bool"));   // Careful: uses vector<bool> which is quite flawed
	declare_zef_tensor_1<double>(internals_submodule, str("Float"));
	declare_zef_tensor_1<int>(internals_submodule, str("Int"));
	declare_zef_tensor_1<std::string>(internals_submodule, str("String"));
	//declare_zef_tensor_1<ZefEnumValue>(internals_submodule, str("ZefEnumValue"));
	//declare_zef_tensor_1<Time>(internals_submodule, str("Time"));
	//declare_zef_tensor_1<QuantityFloat>(internals_submodule, str("QuantityFloat"));
	//declare_zef_tensor_1<QuantityInt>(internals_submodule, str("QuantityInt"));

	declare_zef_tensor_1<BlobType>(internals_submodule, str("BlobType"));
	declare_zef_tensor_1<EntityType>(internals_submodule, str("EntityType"));
	declare_zef_tensor_1<RelationType>(internals_submodule, str("RelationType"));


	py::enum_<BlobType>(internals_submodule, "BlobType")
		.value("_unspecified", BlobType::_unspecified)
		.value("ROOT_NODE", BlobType::ROOT_NODE)
		.value("TX_EVENT_NODE", BlobType::TX_EVENT_NODE)
		.value("RAE_INSTANCE_EDGE", BlobType::RAE_INSTANCE_EDGE)
		.value("TO_DELEGATE_EDGE", BlobType::TO_DELEGATE_EDGE)
		.value("NEXT_TX_EDGE", BlobType::NEXT_TX_EDGE)
		.value("ENTITY_NODE", BlobType::ENTITY_NODE)
		.value("ATTRIBUTE_ENTITY_NODE", BlobType::ATTRIBUTE_ENTITY_NODE)
		.value("VALUE_NODE", BlobType::VALUE_NODE)
		.value("RELATION_EDGE", BlobType::RELATION_EDGE)
		.value("DELEGATE_INSTANTIATION_EDGE", BlobType::DELEGATE_INSTANTIATION_EDGE)
		.value("DELEGATE_RETIREMENT_EDGE", BlobType::DELEGATE_RETIREMENT_EDGE)
		.value("INSTANTIATION_EDGE", BlobType::INSTANTIATION_EDGE)
		.value("TERMINATION_EDGE", BlobType::TERMINATION_EDGE)
		.value("ATOMIC_VALUE_ASSIGNMENT_EDGE", BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE)
		.value("DEFERRED_EDGE_LIST_NODE", BlobType::DEFERRED_EDGE_LIST_NODE)
		.value("ASSIGN_TAG_NAME_EDGE", BlobType::ASSIGN_TAG_NAME_EDGE)
		.value("NEXT_TAG_NAME_ASSIGNMENT_EDGE", BlobType::NEXT_TAG_NAME_ASSIGNMENT_EDGE)
		.value("FOREIGN_GRAPH_NODE", BlobType::FOREIGN_GRAPH_NODE)
		.value("ORIGIN_RAE_EDGE", BlobType::ORIGIN_RAE_EDGE)
		.value("ORIGIN_GRAPH_EDGE", BlobType::ORIGIN_GRAPH_EDGE)
		.value("FOREIGN_ENTITY_NODE", BlobType::FOREIGN_ENTITY_NODE)
		.value("FOREIGN_ATTRIBUTE_ENTITY_NODE", BlobType::FOREIGN_ATTRIBUTE_ENTITY_NODE)
		.value("FOREIGN_RELATION_EDGE", BlobType::FOREIGN_RELATION_EDGE)
		.value("VALUE_TYPE_EDGE", BlobType::VALUE_TYPE_EDGE)
		.value("VALUE_EDGE", BlobType::VALUE_EDGE)
		.value("ATTRIBUTE_VALUE_ASSIGNMENT_EDGE", BlobType::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE)
		;

	py::class_<BlobTypeStruct>(internals_submodule, "BlobTypeStruct", py::buffer_protocol())
		.def(py::init<>())
		/*[[[cog
		for n in all_blob_type_names:
		    cog.outl(f'.def_property_readonly("{n}", [](const BlobTypeStruct& self) {{ return BlobType::{n}; }})')		
		]]]*/
		.def_property_readonly("_unspecified", [](const BlobTypeStruct& self) { return BlobType::_unspecified; })
		.def_property_readonly("ROOT_NODE", [](const BlobTypeStruct& self) { return BlobType::ROOT_NODE; })
		.def_property_readonly("TX_EVENT_NODE", [](const BlobTypeStruct& self) { return BlobType::TX_EVENT_NODE; })
		.def_property_readonly("RAE_INSTANCE_EDGE", [](const BlobTypeStruct& self) { return BlobType::RAE_INSTANCE_EDGE; })
		.def_property_readonly("TO_DELEGATE_EDGE", [](const BlobTypeStruct& self) { return BlobType::TO_DELEGATE_EDGE; })
		.def_property_readonly("NEXT_TX_EDGE", [](const BlobTypeStruct& self) { return BlobType::NEXT_TX_EDGE; })
		.def_property_readonly("ENTITY_NODE", [](const BlobTypeStruct& self) { return BlobType::ENTITY_NODE; })
		.def_property_readonly("ATTRIBUTE_ENTITY_NODE", [](const BlobTypeStruct& self) { return BlobType::ATTRIBUTE_ENTITY_NODE; })
		.def_property_readonly("VALUE_NODE", [](const BlobTypeStruct& self) { return BlobType::VALUE_NODE; })
		.def_property_readonly("RELATION_EDGE", [](const BlobTypeStruct& self) { return BlobType::RELATION_EDGE; })
		.def_property_readonly("DELEGATE_INSTANTIATION_EDGE", [](const BlobTypeStruct& self) { return BlobType::DELEGATE_INSTANTIATION_EDGE; })
		.def_property_readonly("DELEGATE_RETIREMENT_EDGE", [](const BlobTypeStruct& self) { return BlobType::DELEGATE_RETIREMENT_EDGE; })
		.def_property_readonly("INSTANTIATION_EDGE", [](const BlobTypeStruct& self) { return BlobType::INSTANTIATION_EDGE; })
		.def_property_readonly("TERMINATION_EDGE", [](const BlobTypeStruct& self) { return BlobType::TERMINATION_EDGE; })
		.def_property_readonly("ATOMIC_VALUE_ASSIGNMENT_EDGE", [](const BlobTypeStruct& self) { return BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE; })
		.def_property_readonly("DEFERRED_EDGE_LIST_NODE", [](const BlobTypeStruct& self) { return BlobType::DEFERRED_EDGE_LIST_NODE; })
		.def_property_readonly("ASSIGN_TAG_NAME_EDGE", [](const BlobTypeStruct& self) { return BlobType::ASSIGN_TAG_NAME_EDGE; })
		.def_property_readonly("NEXT_TAG_NAME_ASSIGNMENT_EDGE", [](const BlobTypeStruct& self) { return BlobType::NEXT_TAG_NAME_ASSIGNMENT_EDGE; })
		.def_property_readonly("FOREIGN_GRAPH_NODE", [](const BlobTypeStruct& self) { return BlobType::FOREIGN_GRAPH_NODE; })
		.def_property_readonly("ORIGIN_RAE_EDGE", [](const BlobTypeStruct& self) { return BlobType::ORIGIN_RAE_EDGE; })
		.def_property_readonly("ORIGIN_GRAPH_EDGE", [](const BlobTypeStruct& self) { return BlobType::ORIGIN_GRAPH_EDGE; })
		.def_property_readonly("FOREIGN_ENTITY_NODE", [](const BlobTypeStruct& self) { return BlobType::FOREIGN_ENTITY_NODE; })
		.def_property_readonly("FOREIGN_ATTRIBUTE_ENTITY_NODE", [](const BlobTypeStruct& self) { return BlobType::FOREIGN_ATTRIBUTE_ENTITY_NODE; })
		.def_property_readonly("FOREIGN_RELATION_EDGE", [](const BlobTypeStruct& self) { return BlobType::FOREIGN_RELATION_EDGE; })
		.def_property_readonly("VALUE_TYPE_EDGE", [](const BlobTypeStruct& self) { return BlobType::VALUE_TYPE_EDGE; })
		.def_property_readonly("VALUE_EDGE", [](const BlobTypeStruct& self) { return BlobType::VALUE_EDGE; })
		.def_property_readonly("ATTRIBUTE_VALUE_ASSIGNMENT_EDGE", [](const BlobTypeStruct& self) { return BlobType::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE; })
		//[[[end]]]

		.def("__call__", [](const BlobTypeStruct& self, ZefRef zr) { return BT(zr); })
		.def("__call__", [](const BlobTypeStruct& self, EZefRef uzr) { return BT(uzr); })

		// .def("__ror__", [](const BlobTypeStruct& self, ZefRef zr) { return BT(zr); })
		// .def("__ror__", [](const BlobTypeStruct& self, EZefRef uzr) { return BT(uzr); })
		;

    internals_submodule.attr("BT") = BT;


	py::class_<ValueRepType>(internals_submodule, "ValueRepType", py::dynamic_attr())
		.def(py::init<enum_indx>())	
		.def_readonly("value", &ValueRepType::value)
		.def("__repr__", [](const ValueRepType& self)->std::string { return to_str(self); })
		.def("__str__", [](const ValueRepType& self)->std::string { return str(self); })
		.def("__eq__", [](const ValueRepType& self, const ValueRepType& other)->bool { return self == other; }, py::is_operator())
		.def("__hash__", [](const ValueRepType& self)->enum_indx {return self.value; })
		.def("__int__", [](const ValueRepType& self)->int {return self.value; })
		.def("__copy__", [](const ValueRepType& self)->ValueRepType {return self; })

		.def_property_readonly("__enum_type", [](const ValueRepType& self) { return internals::get_enum_type_from_vrt(self); })
		.def_property_readonly("__unit", [](const ValueRepType& self) { return internals::get_unit_from_vrt(self); })
		;

	internals_submodule.def("is_vrt_a_enum", [](ValueRepType my_vrt)->bool { return is_zef_subtype(my_vrt, VRT.Enum); });
	internals_submodule.def("is_vrt_a_quantity_float", [](ValueRepType my_vrt)->bool { return is_zef_subtype(my_vrt, VRT.QuantityFloat); });
	internals_submodule.def("is_vrt_a_quantity_int", [](ValueRepType my_vrt)->bool { return is_zef_subtype(my_vrt, VRT.QuantityInt); });

	py::class_<ValueRepTypeStruct>(internals_submodule, "ValueRepTypeStruct", py::buffer_protocol())
		.def_property_readonly("_unspecified", [](const ValueRepTypeStruct& self) { return self._unspecified; })
		.def_property_readonly("Float", [](const ValueRepTypeStruct& self) { return self.Float; })
		.def_property_readonly("Int", [](const ValueRepTypeStruct& self) { return self.Int; })
		.def_property_readonly("Bool", [](const ValueRepTypeStruct& self) { return self.Bool; })
		.def_property_readonly("String", [](const ValueRepTypeStruct& self) { return self.String; })
		.def_property_readonly("Time", [](const ValueRepTypeStruct& self) { return self.Time; })
		.def_property_readonly("Serialized", [](const ValueRepTypeStruct& self) { return self.Serialized; })
		.def_property_readonly("Any", [](const ValueRepTypeStruct& self) { return self.Any; })
		.def_property_readonly("Enum", [](const ValueRepTypeStruct& self) { return self.Enum; })
		.def_property_readonly("QuantityFloat", [](const ValueRepTypeStruct& self) { return self.QuantityFloat; })
		.def_property_readonly("QuantityInt", [](const ValueRepTypeStruct& self) { return self.QuantityInt; })

		.def("__call__", [](const ValueRepTypeStruct& self, ZefRef zr) { return self(zr); })
		.def("__call__", [](const ValueRepTypeStruct& self, EZefRef uzr) { return self(uzr); })

		// .def("__ror__", [](const ValueRepTypeStruct& self, ZefRef zr) { return self(zr); })
		// .def("__ror__", [](const ValueRepTypeStruct& self, EZefRef uzr) { return self(uzr); })
		;
    internals_submodule.attr("VRT") = VRT;

    py::class_<ValueRepTypeStruct::Enum_>(internals_submodule, "ValueRepTypeStruct_Enum", py::buffer_protocol())
		.def("__call__", [](const ValueRepTypeStruct::Enum_& self, std::string key) { return self(key); }, py::call_guard<py::gil_scoped_release>())
		.def("__eq__", [](const ValueRepTypeStruct::Enum_& self, const ValueRepTypeStruct::Enum_& other)->bool { return true; }, py::is_operator())
        .def("__getattr__", [](const ValueRepTypeStruct::Enum_& self, std::string key) {
            if(key.find("__") == 0)
                throw pybind11::attribute_error();
            return self(key); },
            py::call_guard<py::gil_scoped_release>())
        .def("__dir__", [](const ValueRepTypeStruct::Enum_& self) { return global_token_store().ENs.all_enum_types(); }, py::call_guard<py::gil_scoped_release>())
        ;
    py::class_<ValueRepTypeStruct::QuantityFloat_>(internals_submodule, "ValueRepTypeStruct_QuantityFloat", py::buffer_protocol())
        .def("__call__", [](const ValueRepTypeStruct::QuantityFloat_& self, std::string key) { return self(key); }, py::call_guard<py::gil_scoped_release>())
		.def("__eq__", [](const ValueRepTypeStruct::QuantityFloat_& self, const ValueRepTypeStruct::QuantityFloat_& other)->bool { return true; }, py::is_operator())
        .def("__getattr__", [](const ValueRepTypeStruct::QuantityFloat_& self, std::string key) {
            if(key.find("__") == 0)
                throw pybind11::attribute_error();
            return self(key); },
            py::call_guard<py::gil_scoped_release>())
        .def("__dir__", [](const ValueRepTypeStruct::QuantityFloat_& self) { return global_token_store().ENs.all_enum_values("Unit"); }, py::call_guard<py::gil_scoped_release>())
        ;
    py::class_<ValueRepTypeStruct::QuantityInt_>(internals_submodule, "ValueRepTypeStruct_QuantityInt", py::buffer_protocol())
        .def("__call__", [](const ValueRepTypeStruct::QuantityInt_& self, std::string key) { return self(key); }, py::call_guard<py::gil_scoped_release>())
		.def("__eq__", [](const ValueRepTypeStruct::QuantityInt_& self, const ValueRepTypeStruct::QuantityInt_& other)->bool { return true; }, py::is_operator())
        .def("__getattr__", [](const ValueRepTypeStruct::QuantityInt_& self, std::string key) {
            if(key.find("__") == 0)
                throw pybind11::attribute_error();
            return self(key); },
            py::call_guard<py::gil_scoped_release>())
        .def("__dir__", [](const ValueRepTypeStruct::QuantityInt_& self) { return global_token_store().ENs.all_enum_values("Unit"); }, py::call_guard<py::gil_scoped_release>())
        ;

	py::class_<AttributeEntityType>(internals_submodule, "AttributeEntityType", py::dynamic_attr())
		.def(py::init<enum_indx>())	
		.def(py::init<ValueRepType>())	
		.def(py::init<SerializedValue>())	
		.def_readonly("rep_type", &AttributeEntityType::rep_type)
		.def_readonly("complex_value", &AttributeEntityType::complex_value)
		.def("__repr__", [](const AttributeEntityType& self)->std::string { return to_str(self); })
		.def("__str__", [](const AttributeEntityType& self)->std::string { return str(self); })
		.def("__eq__", [](const AttributeEntityType& self, const AttributeEntityType& other)->bool { return self == other; }, py::is_operator())
		.def("__hash__", [](const AttributeEntityType& self) {
            if(self.complex_value)
                return internals::value_hash(*self.complex_value);
            else
                return self.rep_type.value;
        })
		// .def("__int__", [](const AttributeEntityType& self)->int {return self.value; })
		.def("__copy__", [](const AttributeEntityType& self)->AttributeEntityType {return self; })

		// .def_property_readonly("__enum_type", [](const AttributeEntityType& self) { return internals::get_enum_type_from_aet(self); })
		// .def_property_readonly("__unit", [](const AttributeEntityType& self) { return internals::get_unit_from_aet(self); })
		;

	// internals_submodule.def("is_aet_a_enum", [](AttributeEntityType my_aet)->bool { return my_aet <= AET.Enum; });
	// internals_submodule.def("is_aet_a_quantity_float", [](AttributeEntityType my_aet)->bool { return my_aet <= AET.QuantityFloat; });
	// internals_submodule.def("is_aet_a_quantity_int", [](AttributeEntityType my_aet)->bool { return my_aet <= AET.QuantityInt; });



	py::class_<AttributeEntityTypeStruct>(internals_submodule, "AttributeEntityTypeStruct", py::buffer_protocol())
		.def_property_readonly("_unspecified", [](const AttributeEntityTypeStruct& self) { return self._unspecified; })
		.def_property_readonly("Float", [](const AttributeEntityTypeStruct& self) { return self.Float; })
		.def_property_readonly("Int", [](const AttributeEntityTypeStruct& self) { return self.Int; })
		.def_property_readonly("Bool", [](const AttributeEntityTypeStruct& self) { return self.Bool; })
		.def_property_readonly("String", [](const AttributeEntityTypeStruct& self) { return self.String; })
		.def_property_readonly("Time", [](const AttributeEntityTypeStruct& self) { return self.Time; })
		.def_property_readonly("Serialized", [](const AttributeEntityTypeStruct& self) { return self.Serialized; })
		.def_property_readonly("Any", [](const AttributeEntityTypeStruct& self) { return self.Any; })
		.def_property_readonly("Type", [](const AttributeEntityTypeStruct& self) { return self.Type; })
		.def_property_readonly("Enum", [](const AttributeEntityTypeStruct& self) { return self.Enum; })
		.def_property_readonly("QuantityFloat", [](const AttributeEntityTypeStruct& self) { return self.QuantityFloat; })
		.def_property_readonly("QuantityInt", [](const AttributeEntityTypeStruct& self) { return self.QuantityInt; })

		.def("__call__", [](const AttributeEntityTypeStruct& self, ZefRef zr) { return self(zr); })
		.def("__call__", [](const AttributeEntityTypeStruct& self, EZefRef uzr) { return self(uzr); })

		// .def("__ror__", [](const AttributeEntityTypeStruct& self, ZefRef zr) { return self(zr); })
		// .def("__ror__", [](const AttributeEntityTypeStruct& self, EZefRef uzr) { return self(uzr); })
		;
    internals_submodule.attr("AET") = AET;

    py::class_<AttributeEntityTypeStruct::Enum_>(internals_submodule, "AttributeEntityTypeStruct_Enum", py::buffer_protocol())
		.def("__call__", [](const AttributeEntityTypeStruct::Enum_& self, std::string key) { return self(key); }, py::call_guard<py::gil_scoped_release>())
		.def("__eq__", [](const AttributeEntityTypeStruct::Enum_& self, const AttributeEntityTypeStruct::Enum_& other)->bool { return true; }, py::is_operator())
        .def("__getattr__", [](const AttributeEntityTypeStruct::Enum_& self, std::string key) {
            if(key.find("__") == 0)
                throw pybind11::attribute_error();
            return self(key); },
            py::call_guard<py::gil_scoped_release>())
        .def("__dir__", [](const AttributeEntityTypeStruct::Enum_& self) { return global_token_store().ENs.all_enum_types(); }, py::call_guard<py::gil_scoped_release>())
        ;
    py::class_<AttributeEntityTypeStruct::QuantityFloat_>(internals_submodule, "AttributeEntityTypeStruct_QuantityFloat", py::buffer_protocol())
        .def("__call__", [](const AttributeEntityTypeStruct::QuantityFloat_& self, std::string key) { return self(key); }, py::call_guard<py::gil_scoped_release>())
		.def("__eq__", [](const AttributeEntityTypeStruct::QuantityFloat_& self, const AttributeEntityTypeStruct::QuantityFloat_& other)->bool { return true; }, py::is_operator())
        .def("__getattr__", [](const AttributeEntityTypeStruct::QuantityFloat_& self, std::string key) {
            if(key.find("__") == 0)
                throw pybind11::attribute_error();
            return self(key); },
            py::call_guard<py::gil_scoped_release>())
        .def("__dir__", [](const AttributeEntityTypeStruct::QuantityFloat_& self) { return global_token_store().ENs.all_enum_values("Unit"); }, py::call_guard<py::gil_scoped_release>())
        ;
    py::class_<AttributeEntityTypeStruct::QuantityInt_>(internals_submodule, "AttributeEntityTypeStruct_QuantityInt", py::buffer_protocol())
        .def("__call__", [](const AttributeEntityTypeStruct::QuantityInt_& self, std::string key) { return self(key); }, py::call_guard<py::gil_scoped_release>())
		.def("__eq__", [](const AttributeEntityTypeStruct::QuantityInt_& self, const AttributeEntityTypeStruct::QuantityInt_& other)->bool { return true; }, py::is_operator())
        .def("__getattr__", [](const AttributeEntityTypeStruct::QuantityInt_& self, std::string key) {
            if(key.find("__") == 0)
                throw pybind11::attribute_error();
            return self(key); },
            py::call_guard<py::gil_scoped_release>())
        .def("__dir__", [](const AttributeEntityTypeStruct::QuantityInt_& self) { return global_token_store().ENs.all_enum_values("Unit"); }, py::call_guard<py::gil_scoped_release>())
        ;

    py::class_<EntityTypeStruct>(internals_submodule, "EntityTypeStruct", py::buffer_protocol())
		.def("__call__", [](const EntityTypeStruct& self, const std::string & s) { return self(s); }, py::call_guard<py::gil_scoped_release>())
		.def("__getattr__", [](const EntityTypeStruct& self, const std::string & s) {
                if(s.find("__") == 0)
                    throw pybind11::attribute_error();
                return self(s); },
            py::call_guard<py::gil_scoped_release>())
        .def("__call__", [](const EntityTypeStruct& self, ZefRef zr) { return self(zr); }, py::call_guard<py::gil_scoped_release>())
		.def("__call__", [](const EntityTypeStruct& self, EZefRef uzr) { return self(uzr); }, py::call_guard<py::gil_scoped_release>())

		.def("__dir__", [](const EntityTypeStruct& self) { return global_token_store().ETs.all_keys(); }, py::call_guard<py::gil_scoped_release>())
        .def("__str__", [](const EntityTypeStruct& self) { return str(self); }, py::call_guard<py::gil_scoped_release>())
        ;
    internals_submodule.attr("ET") = ET;

    py::class_<RelationTypeStruct>(internals_submodule, "RelationTypeStruct", py::buffer_protocol())
		.def("__call__", [](const RelationTypeStruct& self, const std::string & s) { return RT(s); }, py::call_guard<py::gil_scoped_release>())
		.def("__getattr__", [](const RelationTypeStruct& self, const std::string & s) {
            if(s.find("__") == 0)
                throw pybind11::attribute_error();
            return RT(s); },
            py::call_guard<py::gil_scoped_release>())
        .def("__call__", [](const RelationTypeStruct& self, ZefRef zr) { return RT(zr); }, py::call_guard<py::gil_scoped_release>())
		.def("__call__", [](const RelationTypeStruct& self, EZefRef uzr) { return RT(uzr); }, py::call_guard<py::gil_scoped_release>())

		.def("__dir__", [](const RelationTypeStruct& self) { return global_token_store().RTs.all_keys(); }, py::call_guard<py::gil_scoped_release>())
        ;
    internals_submodule.attr("RT") = RT;

    py::class_<KeywordStruct>(internals_submodule, "KeywordStruct", py::buffer_protocol())
		.def("__call__", [](const KeywordStruct& self, const std::string & s) { return KW(s); }, py::call_guard<py::gil_scoped_release>())
		.def("__getattr__", [](const KeywordStruct& self, const std::string & s) {
            if(s.find("__") == 0)
                throw pybind11::attribute_error();
            return KW(s); },
            py::call_guard<py::gil_scoped_release>())

		.def("__dir__", [](const KeywordStruct& self) { return global_token_store().KWs.all_keys(); }, py::call_guard<py::gil_scoped_release>())
        ;
    internals_submodule.attr("KW") = KW;

    py::class_<ZefEnumStruct>(internals_submodule, "ZefEnumStruct", py::buffer_protocol())
		.def("__call__", [](const ZefEnumStruct& self, const std::string & enum_type, const std::string & enum_val) { return EN(enum_type, enum_val); }, py::call_guard<py::gil_scoped_release>())
		.def("__getattr__", [](const ZefEnumStruct& self, const std::string & enum_type) {
            if(enum_type.find("__") == 0)
                throw pybind11::attribute_error();
            return EN.partial(enum_type); },
            py::call_guard<py::gil_scoped_release>())

		.def("__dir__", [](const ZefEnumStruct& self) { return global_token_store().ENs.all_enum_types(); }, py::call_guard<py::gil_scoped_release>())
        ;
    internals_submodule.attr("EN") = EN;

    py::class_<ZefEnumStruct::Partial>(internals_submodule, "ZefEnumStructPartial", py::buffer_protocol(), py::call_guard<py::gil_scoped_release>())
        .def(py::init<>(), py::call_guard<py::gil_scoped_release>())
		.def("__getattr__", [](const ZefEnumStruct::Partial& self, const std::string & enum_val) {
            if(enum_val.find("__") == 0)
                throw pybind11::attribute_error();
            return EN(self.enum_type, enum_val); },
            py::call_guard<py::gil_scoped_release>())
		.def("__dir__", [](const ZefEnumStruct::Partial& self) { return global_token_store().ENs.all_enum_values(self.enum_type); }, py::call_guard<py::gil_scoped_release>())
        .def_readonly("__enum_type", &ZefEnumStruct::Partial::enum_type, py::call_guard<py::gil_scoped_release>())
        ;

	// get a list output of the newest types know (also post compile time)
	internals_submodule.def("all_entity_types", []() { return global_token_store().RTs.all_entries_as_list(); }, py::call_guard<py::gil_scoped_release>());
	internals_submodule.def("all_relation_types", []() { return global_token_store().RTs.all_entries_as_list(); }, py::call_guard<py::gil_scoped_release>());
	internals_submodule.def("all_enum_types_and_values", [](){ return global_token_store().ENs.all_entries_as_list(); }, py::call_guard<py::gil_scoped_release>());
	// internals_submodule.def("process_zm_tasks", [](){ tasks::apply_immediate_updates_from_zm(); }, py::call_guard<py::gil_scoped_release>());


    py::class_<DelegateTX>(internals_submodule, "DelegateTX", py::buffer_protocol(), py::call_guard<py::gil_scoped_release>())
        .def(py::init<>(), py::call_guard<py::gil_scoped_release>())
        .def("__repr__", [](const DelegateTX & d) { return to_str(d); })
		.def("__eq__", &DelegateTX::operator==, py::is_operator())
        ;
    py::class_<DelegateRoot>(internals_submodule, "DelegateRoot", py::buffer_protocol(), py::call_guard<py::gil_scoped_release>())
        .def(py::init<>(), py::call_guard<py::gil_scoped_release>())
        .def("__repr__", [](const DelegateRoot & d) { return to_str(d); })
		.def("__eq__", &DelegateRoot::operator==, py::is_operator())
        ;
    py::class_<DelegateRelationTriple>(internals_submodule, "DelegateRelationTriple", py::buffer_protocol(), py::call_guard<py::gil_scoped_release>())
        .def(py::init<RelationType,Delegate,Delegate>(), py::call_guard<py::gil_scoped_release>())
		.def_readonly("rt", &DelegateRelationTriple::rt)
		.def_property_readonly("source", [](const DelegateRelationTriple & self) { return *self.source; })
		.def_property_readonly("target", [](const DelegateRelationTriple & self) { return *self.target; })
        .def("__repr__", [](const DelegateRelationTriple & d) { return to_str(d); })
		.def("__eq__", &DelegateRelationTriple::operator==, py::is_operator())
        ;

    py::class_<Delegate>(internals_submodule, "Delegate", py::buffer_protocol(), py::call_guard<py::gil_scoped_release>(), py::dynamic_attr())
        .def(py::init<int,EntityType>(), py::call_guard<py::gil_scoped_release>())
        .def(py::init<int,ValueRepType>(), py::call_guard<py::gil_scoped_release>())
        .def(py::init<int,RelationType>(), py::call_guard<py::gil_scoped_release>())
        .def(py::init<int,DelegateTX>(), py::call_guard<py::gil_scoped_release>())
        .def(py::init<int,DelegateRoot>(), py::call_guard<py::gil_scoped_release>())
        .def(py::init<int,DelegateRelationTriple>(), py::call_guard<py::gil_scoped_release>())
        .def(py::init<EntityType>(), py::call_guard<py::gil_scoped_release>())
        .def(py::init<ValueRepType>(), py::call_guard<py::gil_scoped_release>())
        .def(py::init<RelationType>(), py::call_guard<py::gil_scoped_release>())
        .def(py::init<Delegate,RelationType,Delegate>(), py::call_guard<py::gil_scoped_release>())
        .def(py::init<Delegate>(), py::call_guard<py::gil_scoped_release>())
		.def_readonly("order", &Delegate::order)
		.def_readonly("item", &Delegate::item)
        .def("__repr__", [](const Delegate & d) { return to_str(d); })
		.def("__eq__", &Delegate::operator==, py::is_operator())
		.def("__copy__", [](const Delegate & d)->Delegate { return d; })
        ;


    internals_submodule.def("ezr_to_delegate", py::overload_cast<EZefRef>(&imperative::delegate_rep), py::call_guard<py::gil_scoped_release>());
    internals_submodule.def("ezr_to_delegate", py::overload_cast<ZefRef>(&imperative::delegate_rep), py::call_guard<py::gil_scoped_release>());

    internals_submodule.def("delegate_to_ezr", py::overload_cast<const Delegate&,Graph,bool,int>(&imperative::delegate_to_ezr), py::call_guard<py::gil_scoped_release>());




    internals_submodule.attr("MMAP_STYLE_AUTO") = MMap::MMAP_STYLE_AUTO;
    internals_submodule.attr("MMAP_STYLE_ANONYMOUS") = MMap::MMAP_STYLE_ANONYMOUS;
    internals_submodule.attr("MMAP_STYLE_FILE_BACKED") = MMap::MMAP_STYLE_FILE_BACKED;;

    // Allow python to force destructing a Graph object, mostly for situations in zefhub
    internals_submodule.def("delete_graphdata", [](Graph & g) { g.delete_graphdata(); }, "This is a low-level graph destructor function. Do not use if you don't know what you are doing.");

    py::class_<Messages::UpdatePayload>(internals_submodule, "UpdatePayload")
        // .def(py::init<nlohmann::json, std::vector<std::string>>())
        .def(py::init([](py::dict & j, std::vector<py::bytes> & b) {
            Messages::UpdatePayload out{j};
            // Because pybind_json may turn uint64 into signed integers, we need
            // to manually override this.
            if(out.j.contains("hash_full_graph"))
                out.j["hash_full_graph"] = out.j["hash_full_graph"].get<uint64_t>();
            std::transform(b.begin(), b.end(), std::back_inserter(out.rest),
                               [](const auto & it) { return it; });
            return out;
        }))
        .def_readonly("j", &Messages::UpdatePayload::j)
        // .def_readonly("rest", &Messages::UpdatePayload::rest);
        .def_property_readonly("rest", [](Messages::UpdatePayload & self)->std::vector<py::bytes> {
                std::vector<py::bytes> out;
                std::transform(self.rest.begin(), self.rest.end(), std::back_inserter(out),
                               [](const auto & it) { return it; });
                return out;
            })
        .def("__eq__", [](const Butler::UpdatePayload & self, const Butler::UpdatePayload & other) {
            return self.j == other.j && self.rest == other.rest;
        }, py::is_operator())
        ;

    // Define the special graph constructor seperately
    internals_submodule.def("create_graph_from_bytes", [](Messages::UpdatePayload payload, int mem_style) { return Graph::create_from_bytes(std::move(payload), mem_style); }, py::call_guard<py::gil_scoped_release>(), "This is a low-level graph creation function. Do not use if you don't know what you are doing.");

    internals_submodule.def("Graph_from_ptr", [](const py::object ptr) {
        // Assuming this object is a ctypes.c_void_p type
        auto value = py::cast<long long>(ptr.attr("value"));
        return new Graph((GraphData*)value, false);
    });

    internals_submodule.def("create_partial_graph", &create_partial_graph, py::call_guard<py::gil_scoped_release>(), "This is a low-level graph creation function. Do not use if you don't know what you are doing.");
    internals_submodule.def("partial_hash", &partial_hash, py::call_guard<py::gil_scoped_release>(), "This is a low-level graph creation function. Do not use if you don't know what you are doing.", py::arg("g"), py::arg("index_hi"), py::arg("seed") = 0, py::arg("working_layout")="");

    internals_submodule.def("list_graph_manager_uids", []() { auto butler = Butler::get_butler(); return butler->list_graph_manager_uids(); }, "This is a low-level function. Do not use if you don't know what you are doing.");
    internals_submodule.def("gtd_info_str", [](BaseUID uid)->std::string {
        auto butler = Butler::get_butler();
        auto gtd = butler->find_graph_manager(uid);
        if(!gtd)
            return "\"NOT FOUND\"";
        return gtd->info_str();
    }, "This is a low-level function. Do not use if you don't know what you are doing.");

    py::class_<Butler::UpdateHeads>(internals_submodule, "UpdateHeads", py::buffer_protocol())
        .def("__str__", [](const Butler::UpdateHeads & heads) {
            return to_str(heads);
        })
        .def_property_readonly("blobs", [](Butler::UpdateHeads & self)->py::dict {
            return py::dict("from"_a=self.blobs.from,
                            "to"_a=self.blobs.to);
        })
        .def_readonly("caches", &Butler::UpdateHeads::caches)
        .def("__eq__", [](const Butler::UpdateHeads & self, const Butler::UpdateHeads & other) {
            if(self.blobs.to != other.blobs.to) {
                return false;
            }
            for(auto & my_cache : self.caches) {
                bool saw = false;
                for(auto & other_cache : other.caches) {
                    if(my_cache.name == other_cache.name) {
                        saw = true;
                        if(my_cache.revision != other_cache.revision or my_cache.to != other_cache.to) {
                            return false;
                        }
                    }
                }
                if(!saw) {
                    return false;
                }
            }
            return true;
        }, py::is_operator())
        ;

    py::class_<Butler::UpdateHeads::NamedHeadRange>(internals_submodule, "NamedHeadRange", py::buffer_protocol())
        .def_readonly("name", &Butler::UpdateHeads::NamedHeadRange::name)
        .def_readonly("frm", &Butler::UpdateHeads::NamedHeadRange::from)
        .def_readonly("to", &Butler::UpdateHeads::NamedHeadRange::to)
        .def_readonly("revision", &Butler::UpdateHeads::NamedHeadRange::revision)
        ;

    internals_submodule.def("create_update_payload", &Butler::create_update_payload, "Low-level function to serialize a graph update.", py::call_guard<py::gil_scoped_release>());
    internals_submodule.def("is_up_to_date", &Butler::is_up_to_date, "Low-level function to see if an update needs to be sent out.", py::call_guard<py::gil_scoped_release>());
    internals_submodule.def("heads_apply", &Butler::heads_apply, "Low-level function to see an update can be applied onto a graph.", py::call_guard<py::gil_scoped_release>());
    internals_submodule.def("parse_payload_update_heads", &Butler::parse_payload_update_heads, py::call_guard<py::gil_scoped_release>());

    internals_submodule.def("create_update_heads", [](GraphData & gd, blob_index blob_head, py::dict cache_heads) {
        LockGraphData lock{&gd};

        Butler::UpdateHeads update_heads{ {blob_head, gd.read_head} };

        for(auto & it : cache_heads) {
            std::string name = py::cast<std::string>(it.first);
            if(false) {}
#define GEN_CACHE(x,y) else if(name == x) { \
                auto ptr = gd.y->get(); \
                update_heads.caches.push_back({x, py::cast<size_t>(it.second["head"]), ptr->size(), py::cast<size_t>(it.second["revision"])}); \
            }

            GEN_CACHE("_ETs_used", ETs_used)
            GEN_CACHE("_RTs_used", RTs_used)
            GEN_CACHE("_ENs_used", ENs_used)

            GEN_CACHE("_uid_lookup", uid_lookup)
            GEN_CACHE("_euid_lookup", euid_lookup)
            GEN_CACHE("_tag_lookup", tag_lookup)
            GEN_CACHE("_av_hash_lookup", av_hash_lookup)
            else
                throw std::runtime_error("Don't understand cache: " + name);
        }

        return update_heads;
    },
        // DO NOT RELEASE THE GIL FOR THIS FUNCTION
        "Low-level function to see if an update needs to be sent out.");

    internals_submodule.def("create_update_heads", [](GraphData & gd) {
        return internals::full_graph_heads(gd);
    }, py::call_guard<py::gil_scoped_release>());

	
	py::class_<zefDB::GraphData>(internals_submodule, "GraphData", py::buffer_protocol())
		// .def(py::init<>())
		.def_readonly("number_of_open_tx_sessions", &GraphData::number_of_open_tx_sessions)
		.def_property_readonly("index_of_latest_complete_tx_node", [](GraphData &self) { return self.latest_complete_tx.load(); } )
		.def_property_readonly("latest_complete_tx", [](GraphData &self) { return self.latest_complete_tx.load(); } )
		.def_property_readonly("manager_tx_head", [](GraphData &self) { return self.manager_tx_head.load(); } )
		.def_property_readonly("last_run_subscriptions", [](GraphData &self) { return self.last_run_subscriptions.load(); } )
		.def_readonly("index_of_open_tx_node", &GraphData::index_of_open_tx_node)
		.def_property_readonly("write_head", [](GraphData &self) { return self.write_head.load(); })
		.def_property_readonly("read_head", [](GraphData &self) { return self.read_head.load(); })
		.def_property_readonly("sync_head", [](GraphData &self) { return self.sync_head.load(); })
		// .def_readwrite("zefscription_head_can_send_out_head", &GraphData::zefscription_head_can_send_out_head)
		// .def_readwrite("zefscription_head_was_sent_out_head", &GraphData::zefscription_head_was_sent_out_head)
		.def_readonly("is_primary_instance", &GraphData::is_primary_instance)
		.def_property_readonly("should_sync", [](const GraphData & self) { return self.should_sync.load(); })
		.def_readonly("revision", &GraphData::revision)
		.def_readwrite("tag_list", &GraphData::tag_list)
		.def("__repr__", [](const GraphData& self)->std::string { std::stringstream ss; ss << self; return ss.str(); })
		.def("hash", &GraphData::hash, py::arg("blob_index_lo"), py::arg("blob_index_hi"), py::arg("seed")=0, py::arg("working_layout")="")
		.def("hash", [](GraphData& gd, std::string working_layout) { return gd.hash(constants::ROOT_NODE_blob_index, gd.read_head, 0, working_layout); }, py::arg("working_layout")="")
		;

	py::class_<zefDB::GraphDataWrapper>(internals_submodule, "GraphDataWrapper")
		.def_property_readonly("graph_data", [](GraphDataWrapper gdw)->GraphData& { return *(gdw.gd); }, py::return_value_policy::reference)
        ;

	


	py::class_<BaseUID>(internals_submodule, "BaseUID", py::buffer_protocol())
		.def(py::init([](const std::string & s) { return BaseUID::from_hex(s); } ))
		.def("__repr__", [](const BaseUID& self) { return to_str(self); })
		.def("__str__", [](const BaseUID& self) { return str(self); })
		.def("__eq__", [](const BaseUID& self, const BaseUID& other) { return self == other; }, py::is_operator(), py::is_operator())
		.def("__hash__", [](const BaseUID& self) { return get_hash(self); })
		;
	py::class_<EternalUID>(internals_submodule, "EternalUID", py::buffer_protocol())
		.def(py::init([](const BaseUID & blob_uid, const BaseUID & graph_uid) { return EternalUID{blob_uid, graph_uid}; } ))
		.def("__repr__", [](const EternalUID& self) { return to_str(self); })
		.def("__str__", [](const EternalUID& self) { return str(self); })
		.def("__eq__", [](const EternalUID& self, const EternalUID& other) { return self == other; }, py::is_operator(), py::is_operator())
		.def("__hash__", [](const EternalUID& self) { return get_hash(self); })
		.def_readonly("blob_uid", &EternalUID::blob_uid)
		.def_readonly("graph_uid", &EternalUID::graph_uid)
		;
	py::class_<ZefRefUID>(internals_submodule, "ZefRefUID", py::buffer_protocol())
		.def("__repr__", [](const ZefRefUID& self) { return to_str(self); })
		.def("__str__", [](const ZefRefUID& self) { return str(self); })
		.def("__eq__", [](const ZefRefUID& self, const ZefRefUID& other) { return self == other; }, py::is_operator(), py::is_operator())
		.def("__hash__", [](const ZefRefUID& self) { return get_hash(self); })
		.def_readonly("blob_uid", &ZefRefUID::blob_uid)
		.def_readonly("tx_uid", &ZefRefUID::tx_uid)
		.def_readonly("graph_uid", &ZefRefUID::graph_uid)
		;
	
	py::class_<zefDB::ZefObservables::DictElement>(internals_submodule, "ObservablesDictElement", py::buffer_protocol())
		.def_readonly("callback", &ZefObservables::DictElement::callback)
		.def_readonly("ref_count", &ZefObservables::DictElement::ref_count)
		//.def("__repr__", [](const ZefObservables::DictElement& self) { return to_str(self); })
		;

	
	py::class_<zefDB::Subscription>(internals_submodule, "Subscription", py::buffer_protocol())
		.def_property_readonly("subscription_graph", [](const Subscription& self) {return *(self.zef_observables_ptr.lock()->g_observables); })  // if the Subscription object exists, the 'self.zef_observables_ptr' was definitely created
		// .def_property_readonly("callbacks_and_refcount", [](const Subscription& self) {return self.zef_observables_ptr->callbacks_and_refcount; })
		.def_readonly("uid", &Subscription::uid)
		.def("__repr__", [](const Subscription& self) { return to_str(self); })
		.def("unsubscribe", &Subscription::unsubscribe, py::call_guard<py::gil_scoped_release>())
		;

	internals_submodule.def("StartTransactionReturnTx", [](Graph& g)->ZefRef { return StartTransactionReturnTx(g); }, py::call_guard<py::gil_scoped_release>());
	internals_submodule.def("FinishTransaction", py::overload_cast<Graph&,bool,bool,bool>(&FinishTransaction), py::arg("g"), py::arg("wait"), py::arg("rollback_empty"), py::arg("check_schema"), py::call_guard<py::gil_scoped_release>());
	internals_submodule.def("FinishTransaction", py::overload_cast<Graph&,bool,bool>(&FinishTransaction), py::arg("g"), py::arg("wait"), py::arg("rollback_empty"), py::call_guard<py::gil_scoped_release>());
	internals_submodule.def("FinishTransaction", py::overload_cast<Graph&,bool>(&FinishTransaction), py::arg("g"), py::arg("wait"), py::call_guard<py::gil_scoped_release>());
	internals_submodule.def("FinishTransaction", py::overload_cast<Graph&>(&FinishTransaction), py::arg("g"), py::call_guard<py::gil_scoped_release>());
	internals_submodule.def("AbortTransaction", py::overload_cast<Graph&>(&AbortTransaction), py::call_guard<py::gil_scoped_release>());

	// internals_submodule.def("get_global_entity_type_from_string", &internals::get_global_entity_type_from_string, py::call_guard<py::gil_scoped_release>(),  
	// 	"Function to help interface Python ET singleton and create a EntityType object");	

	// internals_submodule.def("get_relation_type_from_string", &internals::get_relation_type_from_string, py::call_guard<py::gil_scoped_release>(),
	// 	"Function to help interface Python ET singleton and create a RelationType object");	

	// internals_submodule.def("get_aet_from_enum_type_name_string", &internals::get_aet_from_enum_type_name_string, py::call_guard<py::gil_scoped_release>(),
	// 	"Function to help interface Python AET singleton and create a AttributeEntityType object from a new enum type");	
	// internals_submodule.def("get_aet_from_quantity_float_name_string", &internals::get_aet_from_quantity_float_name_string, py::call_guard<py::gil_scoped_release>(),
	// 	"Function to help interface Python AET singleton and create a AttributeEntityType object from a new unit type used in a QuantityFloat");
	// internals_submodule.def("get_aet_from_quantity_int_name_string", &internals::get_aet_from_quantity_int_name_string, py::call_guard<py::gil_scoped_release>(),
	// 	"Function to help interface Python AET singleton and create a AttributeEntityType object from a new unit type used in a QuantityInt");	

	internals_submodule.def("get_enum_value_from_string", &internals::get_enum_value_from_string, py::call_guard<py::gil_scoped_release>(),
		"Function to help interface Python EN singleton and create a ZefEnumValue object");
	


	// internals_submodule.def("fct__rrshift__", [](BlobType self, EZefRef uzr) { return uzr >> self; });
	// internals_submodule.def("fct__rrshift__", [](BlobType self, ZefRef zr) { return zr.blob_uzr >> self; });

	// internals_submodule.def("fct__rlshift__", [](BlobType self, EZefRef uzr) { return uzr << self; });
	// internals_submodule.def("fct__rlshift__", [](BlobType self, ZefRef zr) { return zr.blob_uzr << self; });

	// internals_submodule.def("fct__lt__", [](BlobType self, EZefRef uzr) { return uzr > self; });
	// internals_submodule.def("fct__lt__", [](BlobType self, ZefRef zr) { return zr.blob_uzr > self; });

	// internals_submodule.def("fct__gt__", [](BlobType self, EZefRef uzr) { return uzr < self; });
	// internals_submodule.def("fct__gt__", [](BlobType self, ZefRef zr) { return zr.blob_uzr < self; });

	
	internals_submodule.def("is_root", py::overload_cast<EZefRef>(&zefDB::is_root), "");
	internals_submodule.def("is_root", py::overload_cast<ZefRef>(&zefDB::is_root), "");
	internals_submodule.def("is_delegate", py::overload_cast<EZefRef>(&zefDB::is_delegate), "");
	internals_submodule.def("is_delegate", py::overload_cast<ZefRef>(&zefDB::is_delegate), "");
	internals_submodule.def("is_delegate_relation_group", py::overload_cast<EZefRef>(&zefDB::is_delegate_relation_group), "");
	internals_submodule.def("is_delegate_relation_group", py::overload_cast<ZefRef>(&zefDB::is_delegate_relation_group), "");
	internals_submodule.def("has_delegate", py::overload_cast<EZefRef>(&zefDB::has_delegate), "");
	internals_submodule.def("has_delegate", py::overload_cast<ZefRef>(&zefDB::has_delegate), "");
	

	internals_submodule.def("num_blob_indexes_to_move", &zefDB::num_blob_indexes_to_move, "given an actual blob size in bytes: how many indexes (blobs_ns are spaced / aligned in units of blob_indx_step_in_bytes) do we have to move forward to reach the next blob");






	internals_submodule.def("merge_entity_", &zefDB::internals::merge_entity_,  "A low level function to merge an entity (given type and origin uid) into a graph.", py::call_guard<py::gil_scoped_release>());
	internals_submodule.def("merge_atomic_entity_", &zefDB::internals::merge_atomic_entity_,  "A low level function to merge an atomic entity (given type and origin uid) into a graph.", py::call_guard<py::gil_scoped_release>());
	internals_submodule.def("merge_relation_", &zefDB::internals::merge_relation_,  "A low level function to merge an relation (given type and origin uid) into a graph.", py::call_guard<py::gil_scoped_release>());

	
	internals_submodule.def("get_latest_complete_tx_node", &internals::get_latest_complete_tx_node, "graph"_a, "index_of_latest_complete_tx_node_hint"_a=0,  "give it a hint as an index, otherwise it will start traversing from the root onwards", py::call_guard<py::gil_scoped_release>());

	// internals_submodule.def("start_zefscription_manager", &internals::start_zefscription_manager, "start_python_script_manually");
	internals_submodule.def("initialise_butler", py::overload_cast<>(&Butler::initialise_butler), py::call_guard<py::gil_scoped_release>());
	internals_submodule.def("initialise_butler", py::overload_cast<std::string>(&Butler::initialise_butler), py::call_guard<py::gil_scoped_release>());
	internals_submodule.def("initialise_butler_as_master", &Butler::initialise_butler_as_master, py::call_guard<py::gil_scoped_release>());
    // Note: stopping the butler can cause python functions to be removed from
    // subscriptions, so the GIL needs to be released.
	internals_submodule.def("stop_butler", &Butler::stop_butler, py::call_guard<py::gil_scoped_release>());
	internals_submodule.def("root_node_blob_index", internals::root_node_blob_index, "which blob index does the root node have?");
	internals_submodule.def("validate_message_version", &Messages::validate_message_version);
	internals_submodule.def("early_token_list", &Butler::early_token_list);
	internals_submodule.def("created_token_list", &Butler::created_token_list);

	internals_submodule.def("get_blobs_as_bytes", [](GraphData& gd, blob_index start_index, blob_index end_index)->py::bytes {
        return py::bytes(internals::get_blobs_as_bytes(gd, start_index, end_index)); 
		}, "read the content of the memory pool filled with blobs_ns for a given graph", py::call_guard<py::gil_scoped_release>());
	internals_submodule.def("graph_as_UpdatePayload", &internals::graph_as_UpdatePayload, py::call_guard<py::gil_scoped_release>());
	// internals_submodule.def("full_graph_heads", &internals::full_graph_heads);
	// internals_submodule.def("convert_payload_0_3_0_to_0_2_0", &conversions::convert_payload_0_3_0_to_0_2_0);

    internals_submodule.def("version_layout", &conversions::version_layout, py::call_guard<py::gil_scoped_release>());
    internals_submodule.def("can_represent_graph_as_payload", &conversions::can_represent_graph_as_payload, py::call_guard<py::gil_scoped_release>());
    internals_submodule.def("convert_payload_0_2_0_to_0_3_0", &conversions::convert_payload_0_2_0_to_0_3_0, py::call_guard<py::gil_scoped_release>());
		
	internals_submodule.def("apply_update", &Butler::apply_update_with_caches, py::call_guard<py::gil_scoped_release>());

    // TODO: In the future, these should be compiled into a version of pyzef
    // only available to zefhub/tokoloshes
    internals_submodule.def("add_entity_type", [](const token_value_t indx, const std::string & name) {
        auto & store = global_token_store();
        store.force_add_entity_type(indx, name);
    }, "This should only be called by zefhub.");
    internals_submodule.def("add_relation_type", [](const token_value_t indx, const std::string & name) {
        auto & store = global_token_store();
        store.force_add_relation_type(indx, name);
    }, "This should only be called by zefhub.");
    internals_submodule.def("add_enum_type", [](const token_value_t indx, const std::string & name) {
        auto & store = global_token_store();
        store.force_add_enum_type(indx, name);
    }, "This should only be called by zefhub.");
    internals_submodule.def("add_keyword", [](const token_value_t indx, const std::string & name) {
        auto & store = global_token_store();
        store.force_add_keyword(indx, name);
    }, "This should only be called by zefhub.");

    internals_submodule.def("set_data_layout_version_info", &internals::set_data_layout_version_info, "new_string_value"_a, "g"_a);
	internals_submodule.def("get_data_layout_version_info", &internals::get_data_layout_version_info, "g"_a);
	internals_submodule.def("set_graph_revision_info", &internals::set_graph_revision_info, "new_string_value"_a, "g"_a);
	internals_submodule.def("get_graph_revision_info", &internals::get_graph_revision_info, "g"_a);


	internals_submodule.def("pageout", zefDB::pageout, "Request the graph data is pushed to disk.", "g"_a);
//	internals_submodule.def("memory_details", [](Graph & g) {
//        auto & info = MMap::info_from_blob(&g.my_graph_data());
//        return report_sizes(info);
//    }, "Memory usages of the mmap for the graph", "g"_a);
	// internals_submodule.def("keydict_usage", [](Graph & g) {
    //     auto & gd = g.my_graph_data();
    //     size_t cap = gd.key_dict->capacity();
    //     size_t size = gd.key_dict->size();
    //     size_t item_size = sizeof(GraphData::key_map::slot_type);
    //     return std::make_tuple(cap, size, item_size);
    // }, "Key dict usages of the mmap for the graph", "g"_a);
	internals_submodule.def("current_zefdb_protocol_version", []() { return Butler::get_butler()->zefdb_protocol_version.load(); }, "Get the current negotiated version with zefhub.");
	internals_submodule.def("max_zefdb_protocol_version", []() { return zefDB::Butler::Butler::zefdb_protocol_version_max; }, "Get the maximum support version by the zefDB library.");

    internals_submodule.def("get_local_process_graph", []() {
        return Butler::get_butler()->get_local_process_graph();
    },
        py::call_guard<py::gil_scoped_release>());

    // Message encoding/decoding, exposed for ZefHub
    internals_submodule.def("prepare_ZH_message", [](const py::dict & d, const std::vector<std::string> & list) {
        json j = d;
        std::string s;
        {
            py::gil_scoped_release release;
            s = Communication::prepare_ZH_message(j, list);
        }
        return py::bytes(s);
    });
    internals_submodule.def("prepare_ZH_message", [](const std::string & j_s, const std::vector<std::string> & list) {
        json j = json::parse(j_s);
        if(!j.is_object())
            throw std::runtime_error("When passing json as a string, need it to parse into an object.");
            
        std::string s;
        {
            py::gil_scoped_release release;
            s = Communication::prepare_ZH_message(j, list);
        }
        return py::bytes(s);
    });
    internals_submodule.def("parse_ZH_message", [](const std::string & s) {
        auto tup = Communication::parse_ZH_message(s);
        auto v_in = std::get<1>(tup);
        std::vector<py::bytes> v;
        {
            py::gil_scoped_acquire acquire;
            std::transform(v_in.cbegin(), v_in.cend(), std::back_inserter(v),
                           [](auto & x) { return py::bytes(x); });
            return std::make_tuple(std::get<0>(tup), v);
        }
    },  py::call_guard<py::gil_scoped_release>());

    internals_submodule.def("register_merge_handler", &internals::register_merge_handler);
    internals_submodule.add_object("_cleanup_merge_handler", py::capsule(&internals::remove_merge_handler));

    internals_submodule.def("register_schema_validator", &internals::register_schema_validator);
    internals_submodule.add_object("_cleanup_schema_validator", py::capsule(&internals::remove_schema_validator));

    internals_submodule.def("register_value_type_check", &internals::register_value_type_check);
    internals_submodule.add_object("_cleanup_value_type_check", py::capsule(&internals::remove_value_type_check));

    internals_submodule.def("register_determine_primitive_type", &internals::register_determine_primitive_type);
    internals_submodule.add_object("_cleanup_determine_primitive_type", py::capsule(&internals::remove_determine_primitive_type));

    internals_submodule.def("copy_graph_slice", &copy_graph_slice,  py::call_guard<py::gil_scoped_release>());
}
