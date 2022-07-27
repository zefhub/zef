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

void create_zefops_module(py::module_ & m, py::module_ & internals_submodule) {
    // py::module zefops_submodule = toplevel_module.def_submodule("zefops", "operators to be used in conjunction with (U)ZefRef(s)(s)");  //create submodule
    py::module_ zefops_submodule = m.def_submodule("zefops", "operators to be used in conjunction with (U)ZefRef(s)(s)");  //create submodule

    using namespace zefDB;
    using namespace zefDB::zefOps;



    py::class_<AllowTerminatedRelentPromotion>(zefops_submodule, "AllowTerminatedRelentPromotion", py::buffer_protocol())
        .def(py::init<>())			
        ;
    zefops_submodule.attr("allow_terminated_relent_promotion") = allow_terminated_relent_promotion;  // expose this singleton

    //                         _                    __           __                     
    //                        | |_ ___     _______ / _|_ __ ___ / _|                    
    //    _____ _____ _____   | __/ _ \   |_  / _ \ |_| '__/ _ \ |_   _____ _____ _____ 
    //   |_____|_____|_____|  | || (_) |   / /  __/  _| | |  __/  _| |_____|_____|_____|
    //                         \__\___/___/___\___|_| |_|  \___|_|                      
    //                               |_____|                                                  
    // py::class_<ToZefRef>(zefops_submodule, "ToZefRef", py::buffer_protocol())
    //     .def(py::init<>())
    //     .def("__getitem__", [](const ToZefRef& self, EZefRef reference_tx) { return self[reference_tx]; })
    //     .def("__getitem__", [](const ToZefRef& self, ZefRef reference_tx) { return self[reference_tx]; })
    //     .def("__getitem__", [](const ToZefRef& self, AllowTerminatedRelentPromotion op) { return self[op]; })
    //     .def("__call__", py::overload_cast<EZefRef>(&ToZefRef::operator(), py::const_))
    //     .def("__call__", py::overload_cast<ZefRef>(&ToZefRef::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const EZefRefs&>(&ToZefRef::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const ZefRefs&>(&ToZefRef::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<EZefRef>(&ToZefRef::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<ZefRef>(&ToZefRef::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const EZefRefs&>(&ToZefRef::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const ZefRefs&>(&ToZefRef::operator(), py::const_))
    //     .def("__repr__", [](const ToZefRef& self)->std::string { std::stringstream ss; ss << self; return ss.str(); })
    //     ;
    // zefops_submodule.attr("to_zefref") = to_zefref;  // expose this singleton







    //                         _                         __           __                     
    //                        | |_ ___    _   _ _______ / _|_ __ ___ / _|                    
    //    _____ _____ _____   | __/ _ \  | | | |_  / _ \ |_| '__/ _ \ |_   _____ _____ _____ 
    //   |_____|_____|_____|  | || (_) | | |_| |/ /  __/  _| | |  __/  _| |_____|_____|_____|
    //                         \__\___/___\__,_/___\___|_| |_|  \___|_|                      
    //                               |_____|                                                 
    py::class_<ToEZefRef>(zefops_submodule, "ToEZefRef", py::buffer_protocol())
        .def(py::init<>())
        .def("__call__", py::overload_cast<ZefRef>(&ToEZefRef::operator(), py::const_))
        .def("__call__", py::overload_cast<EZefRef>(&ToEZefRef::operator(), py::const_))
        // .def("__call__", py::overload_cast<const EZefRefs&>(&ToEZefRef::operator(), py::const_))
        // .def("__call__", py::overload_cast<const ZefRefs&>(&ToEZefRef::operator(), py::const_))

        .def("__ror__", py::overload_cast<ZefRef>(&ToEZefRef::operator(), py::const_))
        .def("__ror__", py::overload_cast<EZefRef>(&ToEZefRef::operator(), py::const_))
        // .def("__ror__", py::overload_cast<const EZefRefs&>(&ToEZefRef::operator(), py::const_))
        // .def("__ror__", py::overload_cast<const ZefRefs&>(&ToEZefRef::operator(), py::const_))
        ;
    zefops_submodule.attr("to_ezefref") = to_ezefref;  // expose this singleton





    //                        _                            
    //                       | |___  __                    
    //    _____ _____ _____  | __\ \/ /  _____ _____ _____ 
    //   |_____|_____|_____| | |_ >  <  |_____|_____|_____|
    //                        \__/_/\_\.                    
    //                                                   
    py::class_<Tx>(zefops_submodule, "Tx", py::buffer_protocol())
        .def(py::init<>())
        .def("__getitem__", py::overload_cast<Graph&>(&Tx::operator[], py::const_))
        .def("__getitem__", py::overload_cast<GraphData&>(&Tx::operator[], py::const_))
        .def("__getitem__", py::overload_cast<TimeSlice>(&Tx::operator[], py::const_))

        .def("__call__", py::overload_cast<Graph&>(&Tx::operator(), py::const_))
        .def("__call__", py::overload_cast<ZefRef>(&Tx::operator(), py::const_))
        // .def("__call__", py::overload_cast<const ZefRefs&>(&Tx::operator(), py::const_))

        .def("__ror__", py::overload_cast<Graph&>(&Tx::operator(), py::const_))
        .def("__ror__", py::overload_cast<ZefRef>(&Tx::operator(), py::const_))
        // .def("__ror__", py::overload_cast<const ZefRefs&>(&Tx::operator(), py::const_))
        ;
    zefops_submodule.attr("tx") = tx;  // expose this singleton






    //                         _   _                                     
    //                        | |_(_)_ __ ___   ___                      
    //    _____ _____ _____   | __| | '_ ` _ \ / _ \   _____ _____ _____ 
    //   |_____|_____|_____|  | |_| | | | | | |  __/  |_____|_____|_____|
    //                         \__|_|_| |_| |_|\___|                     
    //                                                            
    py::class_<TimeZefopStruct>(zefops_submodule, "TimeZefopStruct", py::buffer_protocol())
        .def(py::init<>())
        .def("__call__", py::overload_cast<Now>(&TimeZefopStruct::operator(), py::const_))

        .def("__call__", py::overload_cast<EZefRef>(&TimeZefopStruct::operator(), py::const_))
        .def("__call__", py::overload_cast<ZefRef>(&TimeZefopStruct::operator(), py::const_))

        .def("__ror__", py::overload_cast<EZefRef>(&TimeZefopStruct::operator(), py::const_))
        .def("__ror__", py::overload_cast<ZefRef>(&TimeZefopStruct::operator(), py::const_))
        ;
    zefops_submodule.attr("time") = zefDB::time;  // expose this singleton



    //                                                                          
    //                         ___  ___  _   _ _ __ ___ ___                     
    //     _____ _____ _____  / __|/ _ \| | | | '__/ __/ _ \  _____ _____ _____ 
    //    |_____|_____|_____| \__ \ (_) | |_| | | | (_|  __/ |_____|_____|_____|
    //                        |___/\___/ \__,_|_|  \___\___|                    
    //     
    // py::class_<Source>(zefops_submodule, "Source", py::buffer_protocol())
    //     .def(py::init<>())
    //     .def("__call__", py::overload_cast<EZefRef>(&Source::operator(), py::const_))
    //     .def("__call__", py::overload_cast<ZefRef>(&Source::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const EZefRefs&>(&Source::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const ZefRefs&>(&Source::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<EZefRef>(&Source::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<ZefRef>(&Source::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const EZefRefs&>(&Source::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const ZefRefs&>(&Source::operator(), py::const_))
    //     ;

    // //zefops_submodule.def("source", [](EZefRef uzr) {return source(uzr); });  // if we expose a singleton with the same name as a fct, one is shadowed
    // zefops_submodule.attr("source") = source;  // expose this singleton




    //                         _                       _                       
    //                        | |_ __ _ _ __ __ _  ___| |_                     
    //     _____ _____ _____  | __/ _` | '__/ _` |/ _ \ __|  _____ _____ _____ 
    //    |_____|_____|_____| | || (_| | | | (_| |  __/ |_  |_____|_____|_____|
    //                         \__\__,_|_|  \__, |\___|\__|                    
    //                                      |___/                          
    // py::class_<Target>(zefops_submodule, "Target", py::buffer_protocol())
    //     .def(py::init<>())
    //     .def("__call__", py::overload_cast<EZefRef>(&Target::operator(), py::const_))
    //     .def("__call__", py::overload_cast<ZefRef>(&Target::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const EZefRefs&>(&Target::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const ZefRefs&>(&Target::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<EZefRef>(&Target::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<ZefRef>(&Target::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const EZefRefs&>(&Target::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const ZefRefs&>(&Target::operator(), py::const_))
    //     ;

    // zefops_submodule.attr("target") = target;  // expose this singleton




    //                               _                                      
    //                              (_)_ __  ___                            
    //    _____ _____ _____ _____   | | '_ \/ __|   _____ _____ _____ _____ 
    //   |_____|_____|_____|_____|  | | | | \__ \  |_____|_____|_____|_____|
    //                              |_|_| |_|___/                           
    //                                                                     
    py::class_<Ins>(zefops_submodule, "Ins", py::buffer_protocol())
        .def(py::init<>())
        .def("__call__", py::overload_cast<EZefRef>(&Ins::operator(), py::const_))
        .def("__call__", py::overload_cast<ZefRef>(&Ins::operator(), py::const_))
        .def("__ror__", py::overload_cast<EZefRef>(&Ins::operator(), py::const_))
        .def("__ror__", py::overload_cast<ZefRef>(&Ins::operator(), py::const_))
        ;

    zefops_submodule.attr("ins") = ins;  // expose this singleton






    //                                           _                                  
    //                                ___  _   _| |_ ___                            
    //    _____ _____ _____ _____    / _ \| | | | __/ __|   _____ _____ _____ _____ 
    //   |_____|_____|_____|_____|  | (_) | |_| | |_\__ \  |_____|_____|_____|_____|
    //                               \___/ \__,_|\__|___/                           
    //                                                                              
    py::class_<Outs>(zefops_submodule, "Outs", py::buffer_protocol())
        .def(py::init<>())
        .def("__call__", py::overload_cast<EZefRef>(&Outs::operator(), py::const_))
        .def("__call__", py::overload_cast<ZefRef>(&Outs::operator(), py::const_))
        .def("__ror__", py::overload_cast<EZefRef>(&Outs::operator(), py::const_))
        .def("__ror__", py::overload_cast<ZefRef>(&Outs::operator(), py::const_))
        ;

    zefops_submodule.attr("outs") = outs;  // expose this singleton


    //////////////////////////////
    // * Connections



    py::class_<HasIn>(zefops_submodule, "HasIn", py::buffer_protocol())
        .def(py::init<>())
        .def("__getitem__", [](const HasIn& self, RelationType rt) { return self[rt]; }) 
        .def("__getitem__", [](const HasIn& self, BlobType bt) { return self[bt]; }) 
        .def("__call__", py::overload_cast<EZefRef>(&HasIn::operator(), py::const_))
        .def("__call__", py::overload_cast<ZefRef>(&HasIn::operator(), py::const_))
        .def("__ror__", py::overload_cast<EZefRef>(&HasIn::operator(), py::const_))
        .def("__ror__", py::overload_cast<ZefRef>(&HasIn::operator(), py::const_))
        ;

    zefops_submodule.attr("has_in") = has_in;  // expose this singleton

    py::class_<HasOut>(zefops_submodule, "HasOut", py::buffer_protocol())
        .def(py::init<>())
        .def("__getitem__", [](const HasOut& self, RelationType rt) { return self[rt]; }) 
        .def("__getitem__", [](const HasOut& self, BlobType bt) { return self[bt]; }) 
        .def("__call__", py::overload_cast<EZefRef>(&HasOut::operator(), py::const_))
        .def("__call__", py::overload_cast<ZefRef>(&HasOut::operator(), py::const_))
        .def("__ror__", py::overload_cast<EZefRef>(&HasOut::operator(), py::const_))
        .def("__ror__", py::overload_cast<ZefRef>(&HasOut::operator(), py::const_))
        ;

    zefops_submodule.attr("has_out") = has_out;  // expose this singleton

    zefops_submodule.def("has_relation", py::overload_cast<ZefRef,ZefRef>(&has_relation), "return true if there is any connecting relation between the two ZefRefs");
    zefops_submodule.def("has_relation", py::overload_cast<ZefRef,RelationType,ZefRef>(&has_relation), "return true if there is a relation of type rt between the two ZefRefs");
    zefops_submodule.def("has_relation", py::overload_cast<EZefRef,EZefRef>(&has_relation), "return true if there is any connecting relation between the two EZefRefs");
    zefops_submodule.def("has_relation", py::overload_cast<EZefRef,RelationType,EZefRef>(&has_relation), "return true if there is a relation of type rt between the two EZefRefs");

    zefops_submodule.def("relation", py::overload_cast<ZefRef,ZefRef>(&relation), "return the unique connecting relation between the two ZefRefs");
    zefops_submodule.def("relation", py::overload_cast<ZefRef,RelationType,ZefRef>(&relation), "return the unique connecting relation of type rt between the two ZefRefs");
    zefops_submodule.def("relation", py::overload_cast<EZefRef,EZefRef>(&relation), "return the unique connecting relation between the two EZefRefs");
    zefops_submodule.def("relation", py::overload_cast<EZefRef,RelationType,EZefRef>(&relation), "return the unique connecting relation of type rt between the two EZefRefs");

    zefops_submodule.def("relations", py::overload_cast<ZefRef,ZefRef>(&relations), "return all connecting relations between the two ZefRefs");
    zefops_submodule.def("relations", py::overload_cast<ZefRef,RelationType,ZefRef>(&relations), "return all connecting relations of type rt between the two ZefRefs");
    zefops_submodule.def("relations", py::overload_cast<EZefRef,EZefRef>(&relations), "return all connecting relations between the two EZefRefs");
    zefops_submodule.def("relations", py::overload_cast<EZefRef,RelationType,EZefRef>(&relations), "return all connecting relations of type rt between the two EZefRefs");

    //                               _                              _                _                                  
    //                              (_)_ __  ___     __ _ _ __   __| |    ___  _   _| |_ ___                            
    //    _____ _____ _____ _____   | | '_ \/ __|   / _` | '_ \ / _` |   / _ \| | | | __/ __|   _____ _____ _____ _____ 
    //   |_____|_____|_____|_____|  | | | | \__ \  | (_| | | | | (_| |  | (_) | |_| | |_\__ \  |_____|_____|_____|_____|
    //                              |_|_| |_|___/___\__,_|_| |_|\__,_|___\___/ \__,_|\__|___/                           
    //                                         |_____|              |_____|                                          
    py::class_<InsAndOuts>(zefops_submodule, "InsAndOuts", py::buffer_protocol())
        .def(py::init<>())
        .def("__call__", py::overload_cast<EZefRef>(&InsAndOuts::operator(), py::const_))
        .def("__call__", py::overload_cast<ZefRef>(&InsAndOuts::operator(), py::const_))
        .def("__ror__", py::overload_cast<EZefRef>(&InsAndOuts::operator(), py::const_))
        .def("__ror__", py::overload_cast<ZefRef>(&InsAndOuts::operator(), py::const_))
        ;

    zefops_submodule.attr("ins_and_outs") = ins_and_outs;  // expose this singleton


    //                                     _       _                           
    //                         _ __   ___ | |_    (_)_ __                      
    //     _____ _____ _____  | '_ \ / _ \| __|   | | '_ \   _____ _____ _____ 
    //    |_____|_____|_____| | | | | (_) | |_    | | | | | |_____|_____|_____|
    //                        |_| |_|\___/ \__|___|_|_| |_|                    
    //                                       |_____|                           
    py::class_<NotIn>(zefops_submodule, "NotIn", py::buffer_protocol())
        .def(py::init<>())
        ;

    zefops_submodule.attr("not_in") = not_in;  // expose this singleton



    //                                __ _ _ _                                       
    //                               / _(_) | |_ ___ _ __                            
    //    _____ _____ _____ _____   | |_| | | __/ _ \ '__|   _____ _____ _____ _____ 
    //   |_____|_____|_____|_____|  |  _| | | ||  __/ |     |_____|_____|_____|_____|
    //                              |_| |_|_|\__\___|_|                              
    //                                                                             

    // zefops_submodule.def("filter", py::overload_cast<const ZefRefs&, const std::function<bool(ZefRef)>&>(&imperative::filter));
    // zefops_submodule.def("filter", py::overload_cast<const EZefRefs&, const std::function<bool(EZefRef)>&>(&imperative::filter));

    // zefops_submodule.def("filter", py::overload_cast<const ZefRefs&, EntityType>(&imperative::filter));
    // zefops_submodule.def("filter", py::overload_cast<const ZefRefs&, BlobType>(&imperative::filter));
    // zefops_submodule.def("filter", py::overload_cast<const ZefRefs&, RelationType>(&imperative::filter));
    // zefops_submodule.def("filter", py::overload_cast<const ZefRefs&, AttributeEntityType>(&imperative::filter));

    // zefops_submodule.def("filter", py::overload_cast<const EZefRefs&, EntityType>(&imperative::filter));
    // zefops_submodule.def("filter", py::overload_cast<const EZefRefs&, BlobType>(&imperative::filter));
    // zefops_submodule.def("filter", py::overload_cast<const EZefRefs&, RelationType>(&imperative::filter));
    // zefops_submodule.def("filter", py::overload_cast<const EZefRefs&, AttributeEntityType>(&imperative::filter));





    //                                          _                         
    //                           ___  ___  _ __| |_                       
    //    _____ _____ _____     / __|/ _ \| '__| __|    _____ _____ _____ 
    //   |_____|_____|_____|    \__ \ (_) | |  | |_    |_____|_____|_____|
    //                          |___/\___/|_|   \__|                      
    //                                                                    
    // py::class_<Sort>(zefops_submodule, "Sort", py::buffer_protocol())
    //     .def(py::init<>())
    //     .def("__getitem__", [](const Sort& self, std::function<int(ZefRef)> user_defined_ordering_fct) { return sort[user_defined_ordering_fct]; })
    //     //.def("__getitem__", [](const Sort& self, std::function<bool(ZefRef, ZefRef)> user_defined_ordering_fct) { return sort[user_defined_ordering_fct]; })
            
    //     // .def("__call__", py::overload_cast<EZefRefs>(&Sort::operator(), py::const_))
    //     // .def("__call__", py::overload_cast<ZefRefs>(&Sort::operator(), py::const_))
    //     // .def("__ror__", py::overload_cast<EZefRefs>(&Sort::operator(), py::const_))
    //     // .def("__ror__", py::overload_cast<ZefRefs>(&Sort::operator(), py::const_))
    //     ;
    // zefops_submodule.attr("sort") = sort;  // expose this singleton

    // internals_submodule.def("sort_zr_magnitude", [](std::function<int(ZefRef)> user_defined_predicate_fct) { return sort[user_defined_predicate_fct]; });
    // internals_submodule.def("sort_zr", [](std::function<bool(ZefRef, ZefRef)> user_defined_predicate_fct) { return sort[user_defined_predicate_fct]; });
    // internals_submodule.def("sort_uzr_magnitude", [](std::function<int(EZefRef)> user_defined_predicate_fct) { return sort[user_defined_predicate_fct]; });
    // internals_submodule.def("sort_uzr", [](std::function<bool(EZefRef, EZefRef)> user_defined_predicate_fct) { return sort[user_defined_predicate_fct]; });





    //                                           _                                             
    //                               _   _ _ __ (_) __ _ _   _  ___                            
    //    _____ _____ _____ _____   | | | | '_ \| |/ _` | | | |/ _ \   _____ _____ _____ _____ 
    //   |_____|_____|_____|_____|  | |_| | | | | | (_| | |_| |  __/  |_____|_____|_____|_____|
    //                               \__,_|_| |_|_|\__, |\__,_|\___|                           
    //                                                |_|                               
    // py::class_<Unique>(zefops_submodule, "Unique", py::buffer_protocol())
    //     .def(py::init<>())
    //     // .def("__call__", py::overload_cast<EZefRefs>(&Unique::operator(), py::const_))
    //     // .def("__call__", py::overload_cast<ZefRefs>(&Unique::operator(), py::const_))
    //     // .def("__ror__", py::overload_cast<EZefRefs>(&Unique::operator(), py::const_))
    //     // .def("__ror__", py::overload_cast<ZefRefs>(&Unique::operator(), py::const_))
    //     ;

    // zefops_submodule.attr("unique") = unique;  // expose this singleton






    //                           _       _                          _                           
    //                          (_)_ __ | |_ ___ _ __ ___  ___  ___| |_                         
    //    _____ _____ _____     | | '_ \| __/ _ \ '__/ __|/ _ \/ __| __|      _____ _____ _____ 
    //   |_____|_____|_____|    | | | | | ||  __/ |  \__ \  __/ (__| |_      |_____|_____|_____|
    //                          |_|_| |_|\__\___|_|  |___/\___|\___|\__|                        
    //                                                                                    
    // py::class_<Intersect>(zefops_submodule, "Intersect", py::buffer_protocol())
    //     .def(py::init<>())
    //     .def("__call__", py::overload_cast<const ZefRefs&, const ZefRefs&>(&Intersect::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const EZefRefs&, const EZefRefs&>(&Intersect::operator(), py::const_))
    //     ;

    // zefops_submodule.attr("intersect") = intersect;  // expose this singleton

    // py::class_<Concatenate>(zefops_submodule, "Concatenate", py::buffer_protocol())
    //     .def(py::init<>())
    //     .def("__call__", py::overload_cast<const ZefRefs&, const ZefRefs&>(&Concatenate::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const EZefRefs&, const EZefRefs&>(&Concatenate::operator(), py::const_))
    //     ;

    // zefops_submodule.attr("concatenate") = concatenate;  // expose this singleton

    // py::class_<SetUnion>(zefops_submodule, "SetUnion", py::buffer_protocol())
    //     .def(py::init<>())
    //     .def("__call__", py::overload_cast<const ZefRefs&, const ZefRefs&>(&SetUnion::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const EZefRefs&, const EZefRefs&>(&SetUnion::operator(), py::const_))
    //     ;

    // zefops_submodule.attr("set_union") = set_union;  // expose this singleton

    // py::class_<Without>(zefops_submodule, "Without", py::buffer_protocol())
    //     .def(py::init<>())
    //     .def("__call__", py::overload_cast<const ZefRefs&, const ZefRefs&>(&Without::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const EZefRefs&, const EZefRefs&>(&Without::operator(), py::const_))

    //     .def("__getitem__", [](const Without& self, const ZefRefs& zrs) { return without[zrs]; })
    //     .def("__getitem__", [](const Without& self, const EZefRefs& uzrs) { return without[uzrs]; })

    //     .def("__ror__", py::overload_cast<const EZefRefs&>(&Without::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const ZefRefs&>(&Without::operator(), py::const_))
    //     ;

    // zefops_submodule.attr("without") = without;  // expose this singleton









    // //                             _      _                  _                         
    // //                          __| | ___| | ___  __ _  __ _| |_ ___                   
    // //     _____ _____ _____   / _` |/ _ \ |/ _ \/ _` |/ _` | __/ _ \   _____ _____ _____ 
    // //    |_____|_____|_____| | (_| |  __/ |  __/ (_| | (_| | ||  __/  |_____|_____|_____|
    // //                         \__,_|\___|_|\___|\__, |\__,_|\__\___|                  
    // //                                           |___/                            
    // py::class_<Delegate>(zefops_submodule, "Delegate", py::buffer_protocol())
    //     .def(py::init<>())
    //     // Danny's mind broke here...
    //     // .def("__call__", py::overload_cast<EZefRef>(&Delegate::operator()))
    //     // .def("__call__", py::overload_cast<const EZefRefs&>(&Delegate::operator()))
    //     // .def("__call__", py::overload_cast<ZefRef>(&Delegate::operator()))
    //     // .def("__call__", py::overload_cast<const ZefRefs&>(&Delegate::operator()))
    //     .def("__call__", [](const Delegate& self, const EZefRef& uzr) { return self(uzr); })
    //     .def("__call__", [](const Delegate& self, const EZefRefs& uzrs) { return self(uzrs); })
    //     .def("__call__", [](const Delegate& self, const ZefRef& zr) { return self(zr); })
    //     .def("__call__", [](const Delegate& self, const ZefRefs& zrs) { return self(zrs); })
    //     .def("__call__", [](const Delegate& self, const Graph& g) { return self(g); })

    //     // .def("__ror__", py::overload_cast<EZefRef>(&Delegate::operator()))
    //     // .def("__ror__", py::overload_cast<const EZefRefs&>(&Delegate::operator()))
    //     // .def("__ror__", py::overload_cast<ZefRef>(&Delegate::operator()))
    //     // .def("__ror__", py::overload_cast<const ZefRefs&>(&Delegate::operator()))
    //     .def("__ror__", [](const Delegate& self, const EZefRef& uzr) { return self(uzr); })
    //     .def("__ror__", [](const Delegate& self, const EZefRefs& uzrs) { return self(uzrs); })
    //     .def("__ror__", [](const Delegate& self, const ZefRef& zr) { return self(zr); })
    //     .def("__ror__", [](const Delegate& self, const ZefRefs& zrs) { return self(zrs); })
    //     .def("__ror__", [](const Delegate& self, const Graph& g) { return self(g); })

    //     .def("__getitem__", [](const Delegate& self, const EntityType& et) { return delegate[et]; })
    //     .def("__getitem__", [](const Delegate& self, const AttributeEntityType& aet) { return delegate[aet]; })
    //     ;


    // zefops_submodule.attr("delegate") = delegate;  // expose this singleton








    //                                __ _       _   _                                        
    //                               / _| | __ _| |_| |_ ___ _ __                             
    //    _____ _____ _____ _____   | |_| |/ _` | __| __/ _ \ '_ \    _____ _____ _____ _____ 
    //   |_____|_____|_____|_____|  |  _| | (_| | |_| ||  __/ | | |  |_____|_____|_____|_____|
    //                              |_| |_|\__,_|\__|\__\___|_| |_|                           
    //                                                                                       


    // py::class_<Flatten>(zefops_submodule, "Flatten", py::buffer_protocol())
    //     .def(py::init<>())
    //     // .def("__call__", py::overload_cast<const EZefRefss&>(&Flatten::operator(), py::const_))
    //     // .def("__call__", py::overload_cast<const ZefRefss&>(&Flatten::operator(), py::const_))
    //     // .def("__ror__", py::overload_cast<const EZefRefss&>(&Flatten::operator(), py::const_))
    //     // .def("__ror__", py::overload_cast<const ZefRefss&>(&Flatten::operator(), py::const_))
    //     .def("__call__", py::overload_cast<EZefRefss&>(&Flatten::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<EZefRefss&>(&Flatten::operator(), py::const_))
    //     .def("__call__", py::overload_cast<ZefRefss&>(&Flatten::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<ZefRefss&>(&Flatten::operator(), py::const_))
    //     ;

    // zefops_submodule.attr("flatten") = flatten;  // expose this singleton


    //                                                _       _                  _   _                         
    //                         __ _ ___ ___  ___ _ __| |_    | | ___ _ __   __ _| |_| |__                      
    //    _____ _____ _____   / _` / __/ __|/ _ \ '__| __|   | |/ _ \ '_ \ / _` | __| '_ \   _____ _____ _____ 
    //   |_____|_____|_____| | (_| \__ \__ \  __/ |  | |_    | |  __/ | | | (_| | |_| | | | |_____|_____|_____|
    //                        \__,_|___/___/\___|_|   \__|___|_|\___|_| |_|\__, |\__|_| |_|                    
    //                                                  |_____|            |___/                               

    // py::class_<AssertLength>(zefops_submodule, "AssertLength", py::buffer_protocol())
    //     .def(py::init<>())
    //     .def("__getitem__", [](const AssertLength& self, int len) { return assert_length[len]; })  // do not modify self, but return a new First object
    //     .def("__call__", py::overload_cast<const EZefRefs&>(&AssertLength::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const ZefRefs&>(&AssertLength::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const EZefRefs&>(&AssertLength::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const ZefRefs&>(&AssertLength::operator(), py::const_))
    //     ;

    // zefops_submodule.attr("assert_length") = assert_length;  // expose this singleton




    //                        _     _  __ _                       
    //                       | |   (_)/ _| |_                     
    //    _____ _____ _____  | |   | | |_| __|  _____ _____ _____ 
    //   |_____|_____|_____| | |___| |  _| |_  |_____|_____|_____|
    //                       |_____|_|_|  \__|                    
    //                                                       
    // py::class_<LiftedOnly_>(zefops_submodule, "LiftedOnly_", py::buffer_protocol())
    //     .def("__call__", py::overload_cast<const EZefRefs&>(&LiftedOnly_::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const EZefRefss&>(&LiftedOnly_::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const ZefRefs&>(&LiftedOnly_::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const ZefRefss&>(&LiftedOnly_::operator(), py::const_))

    //     .def("__ror__", py::overload_cast<const EZefRefs&>(&LiftedOnly_::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const EZefRefss&>(&LiftedOnly_::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const ZefRefs&>(&LiftedOnly_::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const ZefRefss&>(&LiftedOnly_::operator(), py::const_))
    //     ;

    // py::class_<LiftedFirst_>(zefops_submodule, "LiftedFirst_", py::buffer_protocol())
    //     .def("__call__", py::overload_cast<const EZefRefs&>(&LiftedFirst_::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const EZefRefss&>(&LiftedFirst_::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const ZefRefs&>(&LiftedFirst_::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const ZefRefss&>(&LiftedFirst_::operator(), py::const_))

    //     .def("__ror__", py::overload_cast<const EZefRefs&>(&LiftedFirst_::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const EZefRefss&>(&LiftedFirst_::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const ZefRefs&>(&LiftedFirst_::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const ZefRefss&>(&LiftedFirst_::operator(), py::const_))
    //     ;

    // py::class_<LiftedLast_>(zefops_submodule, "LiftedLast_", py::buffer_protocol())
    //     .def("__call__", py::overload_cast<const EZefRefs&>(&LiftedLast_::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const EZefRefss&>(&LiftedLast_::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const ZefRefs&>(&LiftedLast_::operator(), py::const_))
    //     .def("__call__", py::overload_cast<const ZefRefss&>(&LiftedLast_::operator(), py::const_))

    //     .def("__ror__", py::overload_cast<const EZefRefs&>(&LiftedLast_::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const EZefRefss&>(&LiftedLast_::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const ZefRefs&>(&LiftedLast_::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const ZefRefss&>(&LiftedLast_::operator(), py::const_))
    //     ;



    // py::class_<Lift>(zefops_submodule, "Lift", py::buffer_protocol())
    //     .def(py::init<>())
    //     .def("__getitem__", py::overload_cast<Only>(&Lift::operator[], py::const_))
    //     .def("__getitem__", py::overload_cast<First>(&Lift::operator[], py::const_))
    //     .def("__getitem__", py::overload_cast<Last>(&Lift::operator[], py::const_))			
    //     ;
    // zefops_submodule.attr("lift") = lift;  // expose this singleton










    //                         __ _          _                       
    //                        / _(_)_ __ ___| |_                     
    //    _____ _____ _____  | |_| | '__/ __| __|  _____ _____ _____ 
    //   |_____|_____|_____| |  _| | |  \__ \ |_  |_____|_____|_____|
    //                       |_| |_|_|  |___/\__|                    
    //                                                               
    // py::class_<First>(zefops_submodule, "First", py::buffer_protocol())
    //     .def(py::init<>())
    //     .def("__call__", py::overload_cast<const EZefRefs&>(&First::operator(), py::const_))   //special tag for const methods
    //     .def("__call__", py::overload_cast<const ZefRefs&>(&First::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const EZefRefs&>(&First::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const ZefRefs&>(&First::operator(), py::const_))
    //     ;

    // zefops_submodule.attr("first") = first;  // expose this singleton

    //                        _           _                       
    //                       | | __ _ ___| |_                     
    //    _____ _____ _____  | |/ _` / __| __|  _____ _____ _____ 
    //   |_____|_____|_____| | | (_| \__ \ |_  |_____|_____|_____|
    //                       |_|\__,_|___/\__|                    
    //      
    // py::class_<Last>(zefops_submodule, "Last", py::buffer_protocol())
    //     .def(py::init<>())
    //     .def("__call__", py::overload_cast<const EZefRefs&>(&Last::operator(), py::const_))   //special tag for const methods
    //     .def("__call__", py::overload_cast<const ZefRefs&>(&Last::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const EZefRefs&>(&Last::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const ZefRefs&>(&Last::operator(), py::const_))
    //     ;

    // zefops_submodule.attr("last") = last;  // expose this singleton



    //                                     _                            
    //                          ___  _ __ | |_   _                      
    //    _____ _____ _____    / _ \| '_ \| | | | |   _____ _____ _____ 
    //   |_____|_____|_____|  | (_) | | | | | |_| |  |_____|_____|_____|
    //                         \___/|_| |_|_|\__, |                     
    //                                       |___/                      

    // py::class_<Only>(zefops_submodule, "Only", py::buffer_protocol())
    //     .def(py::init<>())
    //     .def("__call__", py::overload_cast<const EZefRefs&>(&Only::operator(), py::const_))   //special tag for const methods
    //     .def("__call__", py::overload_cast<const ZefRefs&>(&Only::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const EZefRefs&>(&Only::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const ZefRefs&>(&Only::operator(), py::const_))
    //     ;

    // zefops_submodule.attr("only") = only;  // expose this singleton




    //                                      _                                          
    //                           ___  _ __ | |_   _     ___  _ __                      
    //    _____ _____ _____     / _ \| '_ \| | | | |   / _ \| '__|   _____ _____ _____ 
    //   |_____|_____|_____|   | (_) | | | | | |_| |  | (_) | |     |_____|_____|_____|
    //                          \___/|_| |_|_|\__, |___\___/|_|                        
    //                                        |___/_____|                              
    // py::class_<OnlyOr>(zefops_submodule, "OnlyOr", py::buffer_protocol())
    //     .def(py::init<>())
    //     .def("__getitem__", [](const OnlyOr& self, std::function<ZefRef(ZefRefs)> compute_alternative) { return only_or[compute_alternative]; }) 
    //     //.def("__call__", py::overload_cast<const EZefRefs&>(&OnlyOr::operator(), py::const_))   //special tag for const methods
    //     .def("__call__", py::overload_cast<const ZefRefs&>(&OnlyOr::operator(), py::const_))
    //     //.def("__ror__", py::overload_cast<const EZefRefs&>(&OnlyOr::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const ZefRefs&>(&OnlyOr::operator(), py::const_))
    //     ;

    // zefops_submodule.attr("only_or") = only_or;  // expose this singleton



    //                             _                           _                       
    //                         ___| | ___ _ __ ___   ___ _ __ | |_                     
    //    _____ _____ _____   / _ \ |/ _ \ '_ ` _ \ / _ \ '_ \| __|  _____ _____ _____ 
    //   |_____|_____|_____| |  __/ |  __/ | | | | |  __/ | | | |_  |_____|_____|_____|
    //                        \___|_|\___|_| |_| |_|\___|_| |_|\__|                    
    //                                                                               
    // py::class_<Element>(zefops_submodule, "Element", py::buffer_protocol())
    //     .def(py::init<>())
    //     .def("__getitem__", [](const Element& self, int number_of_elements) { return element[number_of_elements]; })  // do not modify self, but return a new First object
    //     .def("__call__", py::overload_cast<const EZefRefs&>(&Element::operator(), py::const_))   //special tag for const methods
    //     .def("__call__", py::overload_cast<const ZefRefs&>(&Element::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const EZefRefs&>(&Element::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const ZefRefs&>(&Element::operator(), py::const_))
        ;

    // zefops_submodule.attr("element") = element;  // expose this singleton


    //                          _        _                             
    //                         | |_ __ _| | _____                      
    //    _____ _____ _____    | __/ _` | |/ / _ \   _____ _____ _____ 
    //   |_____|_____|_____|   | || (_| |   <  __/  |_____|_____|_____|
    //                          \__\__,_|_|\_\___|                     
    //                                                                 
    // py::class_<Take>(zefops_submodule, "Take", py::buffer_protocol())
    //     .def(py::init<>())
    //     .def("__getitem__", [](const Take& self, int number_of_elements) { return take[number_of_elements]; })  // do not modify self, but return a new First object
    //     .def("__call__", py::overload_cast<const EZefRefs&>(&Take::operator(), py::const_))   //special tag for const methods
    //     .def("__call__", py::overload_cast<const ZefRefs&>(&Take::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const EZefRefs&>(&Take::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<const ZefRefs&>(&Take::operator(), py::const_))
    //     ;

    // zefops_submodule.attr("take") = take;  // expose this singleton
    // zefops_submodule.def("take_impl", [](const ZefRefs& zrs, int n) { return zrs | take[n]; });
    // zefops_submodule.def("take_impl", [](const EZefRefs& uzrs, int n) { return uzrs | take[n]; });




    //                                   _     _                _                        
    //                          _____  _(_)___| |_ ___     __ _| |_                      
    //    _____ _____ _____    / _ \ \/ / / __| __/ __|   / _` | __|   _____ _____ _____ 
    //   |_____|_____|_____|  |  __/>  <| \__ \ |_\__ \  | (_| | |_   |_____|_____|_____|
    //                         \___/_/\_\_|___/\__|___/___\__,_|\__|                     
    //                                               |_____|                             
    // py::class_<ExistsAt>(zefops_submodule, "ExistsAt", py::buffer_protocol())
    //     .def("__getitem__", [](const ExistsAt& self, EZefRef tx_node) { return exists_at[tx_node]; })  // do not modify self, but return a new First object
    //     .def("__getitem__", [](const ExistsAt& self, ZefRef tx_node) { return exists_at[tx_node]; })  // do not modify self, but return a new First object
            
    //     .def("__call__", py::overload_cast<EZefRef>(&ExistsAt::operator(), py::const_))
    //     //.def("__call__", py::overload_cast<const EZefRefs&>(&ExistsAt::operator(), py::const_))			
    //     .def("__ror__", py::overload_cast<EZefRef>(&ExistsAt::operator(), py::const_))
    //     //.def("__ror__", py::overload_cast<const EZefRefs&>(&ExistsAt::operator(), py::const_))

    //     .def("__call__", py::overload_cast<ZefRef>(&ExistsAt::operator(), py::const_))
    //     .def("__ror__", py::overload_cast<ZefRef>(&ExistsAt::operator(), py::const_))
    //     ;

    // zefops_submodule.attr("exists_at") = exists_at;  // expose this singleton



    //                  _                  __           __                                    _        _     _                         _              _     _                _                 
    //                 (_)___     _______ / _|_ __ ___ / _|   _ __  _ __ ___  _ __ ___   ___ | |_ __ _| |__ | | ___     __ _ _ __   __| |    _____  _(_)___| |_ ___     __ _| |_               
    //    _____ _____  | / __|   |_  / _ \ |_| '__/ _ \ |_   | '_ \| '__/ _ \| '_ ` _ \ / _ \| __/ _` | '_ \| |/ _ \   / _` | '_ \ / _` |   / _ \ \/ / / __| __/ __|   / _` | __|  _____ _____ 
    //   |_____|_____| | \__ \    / /  __/  _| | |  __/  _|  | |_) | | | (_) | | | | | | (_) | || (_| | |_) | |  __/  | (_| | | | | (_| |  |  __/>  <| \__ \ |_\__ \  | (_| | |_  |_____|_____|
    //                 |_|___/___/___\___|_| |_|  \___|_|____| .__/|_|  \___/|_| |_| |_|\___/ \__\__,_|_.__/|_|\___|___\__,_|_| |_|\__,_|___\___/_/\_\_|___/\__|___/___\__,_|\__|              
    //                      |_____|                    |_____|_|                                                  |_____|              |_____|                    |_____|                        
    py::class_<IsZefRefPromotableAndExistsAt>(zefops_submodule, "IsZefRefPromotableAndExistsAt", py::buffer_protocol())
        .def("__getitem__", [](const IsZefRefPromotableAndExistsAt& self, EZefRef tx_node) { return is_zefref_promotable_and_exists_at[tx_node]; })  // do not modify self, but return a new First object

        .def("__call__", py::overload_cast<EZefRef>(&IsZefRefPromotableAndExistsAt::operator(), py::const_))
        //.def("__call__", py::overload_cast<const EZefRefs&>(&IsZefRefPromotableAndExistsAt::operator(), py::const_))			
        .def("__ror__", py::overload_cast<EZefRef>(&IsZefRefPromotableAndExistsAt::operator(), py::const_))
        //.def("__ror__", py::overload_cast<const EZefRefs&>(&IsZefRefPromotableAndExistsAt::operator(), py::const_))
        ;

    zefops_submodule.attr("is_zefref_promotable_and_exists_at") = is_zefref_promotable_and_exists_at;  // expose this singleton


        


    //                  _                  __           __                                    _        _     _    
    //                 (_)___     _______ / _|_ __ ___ / _|   _ __  _ __ ___  _ __ ___   ___ | |_ __ _| |__ | | ___ 
    //    _____ _____  | / __|   |_  / _ \ |_| '__/ _ \ |_   | '_ \| '__/ _ \| '_ ` _ \ / _ \| __/ _` | '_ \| |/ _ \  _____ _____ _____ 
    //   |_____|_____| | \__ \    / /  __/  _| | |  __/  _|  | |_) | | | (_) | | | | | | (_) | || (_| | |_) | |  __/ |_____|_____|_____|
    //                 |_|___/___/___\___|_| |_|  \___|_|____| .__/|_|  \___/|_| |_| |_|\___/ \__\__,_|_.__/|_|\___|
    //                      |_____|                    |_____|_|                                                  
    py::class_<IsZefRefPromotable>(zefops_submodule, "IsZefRefPromotable", py::buffer_protocol())			
        .def(py::init<>())
        .def("__call__", py::overload_cast<EZefRef>(&IsZefRefPromotable::operator(), py::const_))
        .def("__ror__", py::overload_cast<EZefRef>(&IsZefRefPromotable::operator(), py::const_))
        ;

    zefops_submodule.attr("is_zefref_promotable") = is_zefref_promotable;  // expose this singleton





    //                              _     _                     
    //                        _   _(_) __| |                    
    //    _____ _____ _____  | | | | |/ _` |  _____ _____ _____ 
    //   |_____|_____|_____| | |_| | | (_| | |_____|_____|_____|
    //                        \__,_|_|\__,_|                    
    //                                                        
    py::class_<Uid>(zefops_submodule, "Uid", py::buffer_protocol())
        .def(py::init<>())			
        .def("__call__", py::overload_cast<const Graph&>(&Uid::operator(), py::const_))   //special tag for const methods
        // .def("__call__", py::overload_cast<GraphData&>(&Uid::operator(), py::const_))
        .def("__call__", py::overload_cast<EZefRef>(&Uid::operator(), py::const_))
        .def("__call__", py::overload_cast<ZefRef>(&Uid::operator(), py::const_))
        .def("__ror__", py::overload_cast<const Graph&>(&Uid::operator(), py::const_))   //special tag for const methods
        // .def("__ror__", py::overload_cast<GraphData&>(&Uid::operator(), py::const_))
        .def("__ror__", py::overload_cast<EZefRef>(&Uid::operator(), py::const_))
        .def("__ror__", py::overload_cast<ZefRef>(&Uid::operator(), py::const_))
        ;

    zefops_submodule.attr("uid") = uid;  // expose this singleton










    //                        _           _              _   _       _   _                _                            
    //                       (_)_ __  ___| |_ __ _ _ __ | |_(_) __ _| |_(_) ___  _ __    | |___  __                    
    //    _____ _____ _____  | | '_ \/ __| __/ _` | '_ \| __| |/ _` | __| |/ _ \| '_ \   | __\ \/ /  _____ _____ _____ 
    //   |_____|_____|_____| | | | | \__ \ || (_| | | | | |_| | (_| | |_| | (_) | | | |  | |_ >  <  |_____|_____|_____|
    //                       |_|_| |_|___/\__\__,_|_| |_|\__|_|\__,_|\__|_|\___/|_| |_|___\__/_/\_\.                    
    //                                                                               |_____|                     
    py::class_<InstantiationTx>(zefops_submodule, "InstantiationTx", py::buffer_protocol())
        .def(py::init<>())
        .def("__call__", py::overload_cast<EZefRef>(&InstantiationTx::operator(), py::const_))
        // .def("__call__", py::overload_cast<const EZefRefs&>(&InstantiationTx::operator(), py::const_))
        .def("__call__", py::overload_cast<ZefRef>(&InstantiationTx::operator(), py::const_))
        // .def("__call__", py::overload_cast<const ZefRefs&>(&InstantiationTx::operator(), py::const_))

        .def("__ror__", py::overload_cast<EZefRef>(&InstantiationTx::operator(), py::const_))
        // .def("__ror__", py::overload_cast<const EZefRefs&>(&InstantiationTx::operator(), py::const_))
        .def("__ror__", py::overload_cast<ZefRef>(&InstantiationTx::operator(), py::const_))
        // .def("__ror__", py::overload_cast<const ZefRefs&>(&InstantiationTx::operator(), py::const_))
        ;
    zefops_submodule.attr("instantiation_tx") = instantiation_tx;  // expose this singleton



    //                        _                      _             _   _                _                            
    //                       | |_ ___ _ __ _ __ ___ (_)_ __   __ _| |_(_) ___  _ __    | |___  __                    
    //    _____ _____ _____  | __/ _ \ '__| '_ ` _ \| | '_ \ / _` | __| |/ _ \| '_ \   | __\ \/ /  _____ _____ _____ 
    //   |_____|_____|_____| | ||  __/ |  | | | | | | | | | | (_| | |_| | (_) | | | |  | |_ >  <  |_____|_____|_____|
    //                        \__\___|_|  |_| |_| |_|_|_| |_|\__,_|\__|_|\___/|_| |_|___\__/_/\_\.                 
    //                                                                             |_____|       
    py::class_<TerminationTx>(zefops_submodule, "TerminationTx", py::buffer_protocol())
        .def(py::init<>())
        .def("__call__", py::overload_cast<EZefRef>(&TerminationTx::operator()))
        // .def("__call__", py::overload_cast<const EZefRefs&>(&TerminationTx::operator()))
        .def("__call__", py::overload_cast<ZefRef>(&TerminationTx::operator()))
        // .def("__call__", py::overload_cast<const ZefRefs&>(&TerminationTx::operator()))

        .def("__ror__", py::overload_cast<EZefRef>(&TerminationTx::operator()))
        // .def("__ror__", py::overload_cast<const EZefRefs&>(&TerminationTx::operator()))
        .def("__ror__", py::overload_cast<ZefRef>(&TerminationTx::operator()))
        // .def("__ror__", py::overload_cast<const ZefRefs&>(&TerminationTx::operator()))
        ;
    zefops_submodule.attr("termination_tx") = termination_tx;  // expose this singleton






    //                                   _                             _                                  _      _                                
    //                       __   ____ _| |_   _  ___     __ _ ___ ___(_) __ _ _ __  _ __ ___   ___ _ __ | |_   | |___  _____                     
    //    _____ _____ _____  \ \ / / _` | | | | |/ _ \   / _` / __/ __| |/ _` | '_ \| '_ ` _ \ / _ \ '_ \| __|  | __\ \/ / __|  _____ _____ _____ 
    //   |_____|_____|_____|  \ V / (_| | | |_| |  __/  | (_| \__ \__ \ | (_| | | | | | | | | |  __/ | | | |_   | |_ >  <\__ \ |_____|_____|_____|
    //                         \_/ \__,_|_|\__,_|\___|___\__,_|___/___/_|\__, |_| |_|_| |_| |_|\___|_| |_|\__|___\__/_/\_\___/                    
    //                                              |_____|              |___/                              |_____|                               

    py::class_<ValueAssignmentTxs>(zefops_submodule, "ValueAssignmentTxs", py::buffer_protocol())
        .def(py::init<>())
        .def("__call__", py::overload_cast<EZefRef>(&ValueAssignmentTxs::operator(), py::const_))
        .def("__call__", py::overload_cast<ZefRef>(&ValueAssignmentTxs::operator(), py::const_))

        .def("__ror__", py::overload_cast<EZefRef>(&ValueAssignmentTxs::operator(), py::const_))
        .def("__ror__", py::overload_cast<ZefRef>(&ValueAssignmentTxs::operator(), py::const_))
        ;
    zefops_submodule.attr("value_assignment_txs") = value_assignment_txs;  // expose this singleton








    //                               _           _                                                       
    //                              (_)_ __  ___| |_ __ _ _ __   ___ ___  ___                            
    //    _____ _____ _____ _____   | | '_ \/ __| __/ _` | '_ \ / __/ _ \/ __|   _____ _____ _____ _____ 
    //   |_____|_____|_____|_____|  | | | | \__ \ || (_| | | | | (_|  __/\__ \  |_____|_____|_____|_____|
    //                              |_|_| |_|___/\__\__,_|_| |_|\___\___||___/                           
    //                                                                                               
    py::class_<Instances>(zefops_submodule, "Instances", py::buffer_protocol())
        .def("__call__", py::overload_cast<const Graph&>(&Instances::operator(), py::const_))
        .def("__call__", py::overload_cast<ZefRef>(&Instances::operator(), py::const_))
        .def("__call__", py::overload_cast<EZefRef>(&Instances::operator(), py::const_))
        .def("__ror__", py::overload_cast<const Graph&>(&Instances::operator(), py::const_))
        .def("__ror__", py::overload_cast<ZefRef>(&Instances::operator(), py::const_))
        .def("__ror__", py::overload_cast<EZefRef>(&Instances::operator(), py::const_))
        .def("__getitem__", py::overload_cast<EntityType>(&Instances::operator[], py::const_))
        .def("__getitem__", py::overload_cast<ValueRepType>(&Instances::operator[], py::const_))
        //.def("__getitem__", py::overload_cast<Zuple>(&Instances::operator[], py::const_))
        .def("__getitem__", py::overload_cast<EZefRef>(&Instances::operator[], py::const_))
        .def("__getitem__", py::overload_cast<ZefRef>(&Instances::operator[], py::const_))
        .def("__getitem__", py::overload_cast<Now>(&Instances::operator[], py::const_))
        ;
    zefops_submodule.attr("instances") = instances;  // expose this singleton
    zefops_submodule.def("instances_impl", py::overload_cast<EZefRef>(&Instances::pure));
    zefops_submodule.def("instances_impl", py::overload_cast<EZefRef,EntityType>(&Instances::pure));
    zefops_submodule.def("instances_impl", py::overload_cast<EZefRef,ValueRepType>(&Instances::pure));
    zefops_submodule.def("instances_impl", py::overload_cast<EZefRef,EZefRef>(&Instances::pure));
    zefops_submodule.def("instances_impl", py::overload_cast<ZefRef>(&Instances::pure));
    zefops_submodule.def("instances_impl", py::overload_cast<ZefRef,EntityType>(&Instances::pure));
    zefops_submodule.def("instances_impl", py::overload_cast<ZefRef,ValueRepType>(&Instances::pure));
    zefops_submodule.def("instances_impl", py::overload_cast<ZefRef,EZefRef>(&Instances::pure));

    py::class_<InstancesEternal>(zefops_submodule, "InstancesEternal", py::buffer_protocol())			
        .def("__call__", py::overload_cast<const Graph&>(&InstancesEternal::operator(), py::const_))		
        .def("__call__", py::overload_cast<EZefRef>(&InstancesEternal::operator(), py::const_))		
        .def("__ror__", py::overload_cast<EZefRef>(&InstancesEternal::operator(), py::const_))			
        .def("__ror__", [](InstancesEternal op, const Graph& g) { return g | op; })
        .def("__getitem__", py::overload_cast<EntityType>(&InstancesEternal::operator[], py::const_))
        .def("__getitem__", py::overload_cast<ValueRepType>(&InstancesEternal::operator[], py::const_))
        //.def("__getitem__", py::overload_cast<Zuple>(&InstancesEternal::operator[], py::const_))
        ;
    zefops_submodule.attr("instances_eternal") = instances_eternal;  // expose this singleton





    //                        _   _                        _ _                              
    //                       | |_(_)_ __ ___   ___     ___| (_) ___ ___                     
    //    _____ _____ _____  | __| | '_ ` _ \ / _ \   / __| | |/ __/ _ \  _____ _____ _____ 
    //   |_____|_____|_____| | |_| | | | | | |  __/   \__ \ | | (_|  __/ |_____|_____|_____|
    //                        \__|_|_| |_| |_|\___|___|___/_|_|\___\___|                    
    //                                           |_____|                                    
    zefops_submodule.attr("time_slice") = time_slice;  // expose this singleton



    //                        _   _                   _                       _                     
    //                       | |_(_)_ __ ___   ___   | |_ _ __ __ ___   _____| |                    
    //    _____ _____ _____  | __| | '_ ` _ \ / _ \  | __| '__/ _` \ \ / / _ \ |  _____ _____ _____ 
    //   |_____|_____|_____| | |_| | | | | | |  __/  | |_| | | (_| |\ V /  __/ | |_____|_____|_____|
    //                        \__|_|_| |_| |_|\___|___\__|_|  \__,_| \_/ \___|_|                    
    //                                           |_____|                                  
    py::class_<TimeTravel>(zefops_submodule, "TimeTravel", py::buffer_protocol())
        .def("__getitem__", py::overload_cast<int>(&TimeTravel::operator[], py::const_))
        .def("__getitem__", py::overload_cast<str>(&TimeTravel::operator[], py::const_))
        .def("__getitem__", py::overload_cast<QuantityFloat>(&TimeTravel::operator[], py::const_))
        .def("__getitem__", py::overload_cast<EZefRef>(&TimeTravel::operator[], py::const_))

        .def("__call__", py::overload_cast<ZefRef>(&TimeTravel::operator(), py::const_))
        // .def("__call__", py::overload_cast<const ZefRefs&>(&TimeTravel::operator(), py::const_))
        // .def("__call__", py::overload_cast<const ZefRefss&>(&TimeTravel::operator(), py::const_))

        .def("__ror__", py::overload_cast<ZefRef>(&TimeTravel::operator(), py::const_))
        // .def("__ror__", py::overload_cast<const ZefRefs&>(&TimeTravel::operator(), py::const_))
        // .def("__ror__", py::overload_cast<const ZefRefss&>(&TimeTravel::operator(), py::const_))			
        ;
    zefops_submodule.attr("time_travel") = time_travel;  // expose this singleton







    //                                                                
    //                        _ __   _____      __                    
    //    _____ _____ _____  | '_ \ / _ \ \ /\ / /  _____ _____ _____ 
    //   |_____|_____|_____| | | | | (_) \ V  V /  |_____|_____|_____|
    //                       |_| |_|\___/ \_/\_/                      
    //                                                                
    py::class_<Now>(zefops_submodule, "Now", py::buffer_protocol())
        .def("__call__", py::overload_cast<>(&Now::operator(), py::const_))

        .def("__call__", py::overload_cast<const Graph&>(&Now::operator(), py::const_))
        .def("__call__", py::overload_cast<EZefRef>(&Now::operator(), py::const_))
        .def("__call__", py::overload_cast<ZefRef>(&Now::operator(), py::const_))            
        // .def("__call__", py::overload_cast<const ZefRefs&>(&Now::operator()))
        // .def("__call__", py::overload_cast<const EZefRefs&>(&Now::operator()))
            
        .def("__ror__", py::overload_cast<const Graph&>(&Now::operator(), py::const_))
        .def("__ror__", py::overload_cast<EZefRef>(&Now::operator(), py::const_))
        .def("__ror__", py::overload_cast<ZefRef>(&Now::operator(), py::const_))            
        // .def("__ror__", py::overload_cast<const ZefRefs&>(&Now::operator()))
        // .def("__ror__", py::overload_cast<const EZefRefs&>(&Now::operator()))
        ;
    zefops_submodule.attr("now") = now;  // expose this singleton





    //                                 __  __           _           _                       
    //                           __ _ / _|/ _| ___  ___| |_ ___  __| |                      
    //    _____ _____ _____     / _` | |_| |_ / _ \/ __| __/ _ \/ _` |    _____ _____ _____ 
    //   |_____|_____|_____|   | (_| |  _|  _|  __/ (__| ||  __/ (_| |   |_____|_____|_____|
    //                          \__,_|_| |_|  \___|\___|\__\___|\__,_|                      
    //           
    // we can't return ZefRefs here: it may contain terminated relents
    py::class_<Affected>(zefops_submodule, "Affected", py::buffer_protocol())
        .def("__call__", py::overload_cast<EZefRef>(&Affected::operator(), py::const_))
        .def("__call__", py::overload_cast<ZefRef>(&Affected::operator(), py::const_))
        .def("__ror__", py::overload_cast<EZefRef>(&Affected::operator(), py::const_))
        .def("__ror__", py::overload_cast<ZefRef>(&Affected::operator(), py::const_))
        ;
    zefops_submodule.attr("affected") = affected;  // expose this singleton




    //                          _           _              _   _       _           _                       
    //                         (_)_ __  ___| |_ __ _ _ __ | |_(_) __ _| |_ ___  __| |                      
    //    _____ _____ _____    | | '_ \/ __| __/ _` | '_ \| __| |/ _` | __/ _ \/ _` |    _____ _____ _____ 
    //   |_____|_____|_____|   | | | | \__ \ || (_| | | | | |_| | (_| | ||  __/ (_| |   |_____|_____|_____|
    //                         |_|_| |_|___/\__\__,_|_| |_|\__|_|\__,_|\__\___|\__,_|                      
    //                                                                                                    
    py::class_<Instantiated>(zefops_submodule, "Instantiated", py::buffer_protocol())
        .def("__call__", py::overload_cast<EZefRef>(&Instantiated::operator(), py::const_))
        .def("__call__", py::overload_cast<ZefRef>(&Instantiated::operator(), py::const_))
        .def("__ror__", py::overload_cast<EZefRef>(&Instantiated::operator(), py::const_))
        .def("__ror__", py::overload_cast<ZefRef>(&Instantiated::operator(), py::const_))
        ;
    zefops_submodule.attr("instantiated") = instantiated;  // expose this singleton




    //                          _                      _             _           _                       
    //                         | |_ ___ _ __ _ __ ___ (_)_ __   __ _| |_ ___  __| |                      
    //    _____ _____ _____    | __/ _ \ '__| '_ ` _ \| | '_ \ / _` | __/ _ \/ _` |    _____ _____ _____ 
    //   |_____|_____|_____|   | ||  __/ |  | | | | | | | | | | (_| | ||  __/ (_| |   |_____|_____|_____|
    //                          \__\___|_|  |_| |_| |_|_|_| |_|\__,_|\__\___|\__,_|                      
    //                                                                                          
    py::class_<Terminated>(zefops_submodule, "Terminated", py::buffer_protocol())
        .def("__call__", py::overload_cast<EZefRef>(&Terminated::operator(), py::const_))
        .def("__call__", py::overload_cast<ZefRef>(&Terminated::operator(), py::const_))
        .def("__ror__", py::overload_cast<EZefRef>(&Terminated::operator(), py::const_))
        .def("__ror__", py::overload_cast<ZefRef>(&Terminated::operator(), py::const_))
        ;
    zefops_submodule.attr("terminated") = terminated;  // expose this singleton




    //                                     _                             _                      _                       
    //                         __   ____ _| |_   _  ___     __ _ ___ ___(_) __ _ _ __   ___  __| |                      
    //    _____ _____ _____    \ \ / / _` | | | | |/ _ \   / _` / __/ __| |/ _` | '_ \ / _ \/ _` |    _____ _____ _____ 
    //   |_____|_____|_____|    \ V / (_| | | |_| |  __/  | (_| \__ \__ \ | (_| | | | |  __/ (_| |   |_____|_____|_____|
    //                           \_/ \__,_|_|\__,_|\___|___\__,_|___/___/_|\__, |_| |_|\___|\__,_|                      
    //                                                |_____|              |___/                                      
    py::class_<ValueAssigned>(zefops_submodule, "ValueAssigned", py::buffer_protocol())
        .def("__call__", py::overload_cast<EZefRef>(&ValueAssigned::operator(), py::const_))
        .def("__call__", py::overload_cast<ZefRef>(&ValueAssigned::operator(), py::const_))
        .def("__ror__", py::overload_cast<EZefRef>(&ValueAssigned::operator(), py::const_))
        .def("__ror__", py::overload_cast<ZefRef>(&ValueAssigned::operator(), py::const_))
        ;
    zefops_submodule.attr("value_assigned") = value_assigned;  // expose this singleton









    py::class_<KeepAlive>(zefops_submodule, "KeepAlive", py::buffer_protocol())
        .def("__getitem__", py::overload_cast<bool>(&KeepAlive::operator[], py::const_))
        .def("__repr__", [](const KeepAlive& self) { return to_str(self); })
        ;
    zefops_submodule.attr("keep_alive") = keep_alive;  // expose this singleton		
                


    py::class_<Outgoing>(zefops_submodule, "Outgoing", py::buffer_protocol())
        .def("__repr__", [](const Outgoing& self) { return to_str(self); })
        ;
    zefops_submodule.attr("outgoing") = outgoing;  // expose this singleton		
        


    py::class_<Incoming>(zefops_submodule, "Incoming", py::buffer_protocol())
        .def("__repr__", [](const Incoming& self) { return to_str(self); })
        ;
    zefops_submodule.attr("incoming") = incoming;  // expose this singleton		

        

    py::class_<OnValueAssignment>(zefops_submodule, "OnValueAssignment", py::buffer_protocol())
        .def("__repr__", [](const OnValueAssignment& self) { return to_str(self); })
        ;
    zefops_submodule.attr("on_value_assignment") = on_value_assignment;  // expose this singleton		



    py::class_<OnInstantiation>(zefops_submodule, "OnInstantiation", py::buffer_protocol())
        .def("__getitem__", py::overload_cast<RelationType>(&OnInstantiation::operator[], py::const_))
        .def("__getitem__", py::overload_cast<Outgoing>(&OnInstantiation::operator[], py::const_))
        .def("__getitem__", py::overload_cast<Incoming>(&OnInstantiation::operator[], py::const_))
        .def("__repr__", [](const OnInstantiation& self) { return to_str(self); })
        ;
    zefops_submodule.attr("on_instantiation") = on_instantiation;  // expose this singleton
        
        
    py::class_<OnTermination>(zefops_submodule, "OnTermination", py::buffer_protocol())
        .def("__getitem__", py::overload_cast<RelationType>(&OnTermination::operator[], py::const_))
        .def("__getitem__", py::overload_cast<Outgoing>(&OnTermination::operator[], py::const_))
        .def("__getitem__", py::overload_cast<Incoming>(&OnTermination::operator[], py::const_))
        .def("__repr__", [](const OnTermination& self) { return to_str(self); })
        ;
    zefops_submodule.attr("on_termination") = on_termination;  // expose this singleton
        


    py::class_<Subscribe>(zefops_submodule, "Subscribe", py::buffer_protocol())
        .def("__getitem__", py::overload_cast<OnValueAssignment>(&Subscribe::operator[], py::const_))
        .def("__getitem__", py::overload_cast<OnInstantiation>(&Subscribe::operator[], py::const_))
        .def("__getitem__", py::overload_cast<OnTermination>(&Subscribe::operator[], py::const_))

        .def("__getitem__", py::overload_cast<KeepAlive>(&Subscribe::operator[], py::const_))
        .def("__getitem__", py::overload_cast<std::function<void(ZefRef)>>(&Subscribe::operator[], py::const_))

        .def("__call__", py::overload_cast<ZefRef>(&Subscribe::operator(),  py::const_), py::call_guard<py::gil_scoped_release>())
        .def("__call__", py::overload_cast<EZefRef>(&Subscribe::operator(), py::const_), py::call_guard<py::gil_scoped_release>())
        .def("__call__", py::overload_cast<Graph>(&Subscribe::operator(), py::const_), py::call_guard<py::gil_scoped_release>())
        .def("__ror__", py::overload_cast<ZefRef>(&Subscribe::operator(), py::const_), py::call_guard<py::gil_scoped_release>())
        .def("__ror__", py::overload_cast<EZefRef>(&Subscribe::operator(), py::const_), py::call_guard<py::gil_scoped_release>())
        .def("__ror__", py::overload_cast<Graph>(&Subscribe::operator(), py::const_), py::call_guard<py::gil_scoped_release>())

        .def("__repr__", [](const Subscribe& self) { return to_str(self); })
        ;
    zefops_submodule.attr("subscribe") = subscribe;  // expose this singleton	




















    //                                                  _                                                   
    //                                                 | |                                                  
    //    _____ _____ _____ _____ _____ _____ _____    | |        _____ _____ _____ _____ _____ _____ _____ 
    //   |_____|_____|_____|_____|_____|_____|_____|   | |___    |_____|_____|_____|_____|_____|_____|_____|
    //                                                 |_____|                                              
    //                                                                                                    	
    py::class_<L_Class>(zefops_submodule, "L_Class", py::buffer_protocol())
        .def_readonly("data", &L_Class::data)
        //.def("__call__", [](L_Class self, RelationType rt) {throw std::runtime_error("Fixing Ulf's design flaw: the round brakcets L(my_relation_type) are being deprecated. Replace expression with []");  })
        //.def("__call__", [](L_Class self, BlobType bt) {throw std::runtime_error("Fixing Ulf's design flaw: the round brakcets L(my_blob_type) are being deprecated. Replace expression with []");  })
        .def("__getitem__", py::overload_cast<RelationType>(&L_Class::operator[], py::const_))
        .def("__getitem__", py::overload_cast<BlobType>(&L_Class::operator[], py::const_))
        .def("__getitem__", py::overload_cast<Tensor<RelationType, 1>>(&L_Class::operator[], py::const_))
        .def("__getitem__", py::overload_cast<Tensor<BlobType, 1>>(&L_Class::operator[], py::const_))

        // passing multiple args , e.g. L[rt1, rt2] from Python does not bind to this, but calls L(self, tuple) where tuple groups all the other args
        //.def("__getitem__", [](const L_Class& self, RelationType rt1, RelationType rt2 )->int {return 42; } )
                        
        .def("__getitem__", [](const L_Class& self, const py::tuple& grouped_args)->L_Class {
            if (py::isinstance<RelationType>(grouped_args[0])) {
                std::vector<RelationType> tmp;
                for (auto el : grouped_args)
                    tmp.push_back(py::cast<RelationType>(el));
                return L_Class{ Tensor<RelationType, 1> {tmp} };
            }
            if (py::isinstance<BlobType>(grouped_args[0])) {
                std::vector<BlobType> tmp;
                for (auto el : grouped_args)
                    tmp.push_back(py::cast<BlobType>(el));
                return L_Class{ Tensor<BlobType, 1> {tmp} };
            }
            throw std::runtime_error("L[...] with multiple args called. Args have to be BT or RT");
            return L;  // should not be reached, make compiler happy
        })
        .def("__getitem__", [](const L_Class& self, const Tensor<RelationType, 1>& my_tensor)->L_Class { return L_Class{ my_tensor }; })
        .def("__rrshift__", [](L_Class self, EZefRef uzr) { return uzr >> self; })
        .def("__rlshift__", [](L_Class self, EZefRef uzr) { return uzr << self; })
        .def("__lt__", [](L_Class self, EZefRef uzr) { return uzr > self; })  // similar functionality as non-existent '__rgt__' :)
        .def("__gt__", [](L_Class self, EZefRef uzr) { return uzr < self; })

        .def("__rrshift__", [](L_Class self, ZefRef zr) { return zr >> self; })
        .def("__rlshift__", [](L_Class self, ZefRef zr) { return zr << self; })
        .def("__lt__", [](L_Class self, ZefRef zr) { return zr > self; })  // similar functionality as non-existent '__rgt__' :)
        .def("__gt__", [](L_Class self, ZefRef zr) { return zr < self; })

        // .def("__rrshift__", [](L_Class self, EZefRefs& uzrs) { return uzrs >> self; })
        // .def("__rlshift__", [](L_Class self, EZefRefs& uzrs) { return uzrs << self; })
        // .def("__lt__", [](L_Class self, EZefRefs uzrs) { return uzrs > self; })  // similar functionality as non-existent '__rgt__' :)
        // .def("__gt__", [](L_Class self, EZefRefs uzrs) { return uzrs < self; })

        // .def("__rrshift__", [](L_Class self, ZefRefs& zrs) { return zrs >> self; })
        // .def("__rlshift__", [](L_Class self, ZefRefs& zrs) { return zrs << self; })
        // .def("__lt__", [](L_Class self, ZefRefs zrs) { return zrs > self; })  // similar functionality as non-existent '__rgt__' :)
        // .def("__gt__", [](L_Class self, ZefRefs zrs) { return zrs < self; })
        ;
    zefops_submodule.attr("L") = L;  // expose this singleton
        


    //                               ___                            
    //                              / _ |                           
    //    _____ _____ _____ _____  | | | |  _____ _____ _____ _____ 
    //   |_____|_____|_____|_____| | |_| | |_____|_____|_____|_____|
    //                              \___/                           
    //                                                             
    py::class_<O_Class>(zefops_submodule, "O_Class", py::buffer_protocol())
        .def_readonly("data", &O_Class::data)
        //.def("__call__", [](O_Class self, RelationType rt) {throw std::runtime_error("Fixing Ulf's design flaw: the round brakcets O(my_relation_type) are being deprecated. Replace expression with []");  })
        //.def("__call__", [](O_Class self, BlobType bt) {throw std::runtime_error("Fixing Ulf's design flaw: the round brakcets O(my_blob_type) are being deprecated. Replace expression with []");  })
        .def("__getitem__", py::overload_cast<RelationType>(&O_Class::operator[], py::const_))
        .def("__getitem__", py::overload_cast<BlobType>(&O_Class::operator[], py::const_))
        .def("__getitem__", py::overload_cast<Tensor<RelationType, 1>>(&O_Class::operator[], py::const_))
        .def("__getitem__", py::overload_cast<Tensor<BlobType, 1>>(&O_Class::operator[], py::const_))

        // passing multiple args , e.g. O[rt1, rt2] from Python does not bind to this, but calls O(self, tuple) where tuple groups all the other args
        //.def("__getitem__", [](const O_Class& self, RelationType rt1, RelationType rt2 )->int {return 42; } )
                        
        .def("__getitem__", [](const O_Class& self, const py::tuple& grouped_args)->O_Class {
            if (py::isinstance<RelationType>(grouped_args[0])) {
                std::vector<RelationType> tmp;
                for (auto el : grouped_args)
                    tmp.push_back(py::cast<RelationType>(el));
                return O_Class{ Tensor<RelationType, 1> {tmp} };
            }
            if (py::isinstance<BlobType>(grouped_args[0])) {
                std::vector<BlobType> tmp;
                for (auto el : grouped_args)
                    tmp.push_back(py::cast<BlobType>(el));
                return O_Class{ Tensor<BlobType, 1> {tmp} };
            }
            throw std::runtime_error("O[...] with multiple args called. Args have to be BT or RT");
            return O;  // should not be reached, make compiler happy
        })
        .def("__getitem__", [](const O_Class& self, const Tensor<RelationType, 1>& my_tensor)->O_Class { return O_Class{ my_tensor }; })
        .def("__rrshift__", [](O_Class self, EZefRef uzr) { return uzr >> self; })
        .def("__rlshift__", [](O_Class self, EZefRef uzr) { return uzr << self; })
        .def("__lt__", [](O_Class self, EZefRef uzr) { return uzr > self; })  // similar functionality as non-existent '__rgt__' :)
        .def("__gt__", [](O_Class self, EZefRef uzr) { return uzr < self; })

        .def("__rrshift__", [](O_Class self, ZefRef zr) { return zr >> self; })
        .def("__rlshift__", [](O_Class self, ZefRef zr) { return zr << self; })
        .def("__lt__", [](O_Class self, ZefRef zr) { return zr > self; })  // similar functionality as non-existent '__rgt__' :)
        .def("__gt__", [](O_Class self, ZefRef zr) { return zr < self; })

        // TODO: This is currently not possible.
        // .def("__rrshift__", [](O_Class self, EZefRefs& uzrs) { return uzrs >> self; })
        // .def("__rlshift__", [](O_Class self, EZefRefs& uzrs) { return uzrs << self; })
        // .def("__lt__", [](O_Class self, EZefRefs uzrs) { return uzrs > self; })  // similar functionality as non-existent '__rgt__' :)
        // .def("__gt__", [](O_Class self, EZefRefs uzrs) { return uzrs < self; })

        // .def("__rrshift__", [](O_Class self, ZefRefs& zrs) { return zrs >> self; })
        // .def("__rlshift__", [](O_Class self, ZefRefs& zrs) { return zrs << self; })
        // .def("__lt__", [](O_Class self, ZefRefs zrs) { return zrs > self; })  // similar functionality as non-existent '__rgt__' :)
        // .def("__gt__", [](O_Class self, ZefRefs zrs) { return zrs < self; })
        ;
    zefops_submodule.attr("O") = O;  // expose this singleton
        
        


    //                               ___                            
    //                              / _ |                           
    //    _____ _____ _____ _____  | | | |  _____ _____ _____ _____ 
    //   |_____|_____|_____|_____| | |_| | |_____|_____|_____|_____|
    //                              \__\_|                          
    //                                                             

    py::class_<Q_Class>(zefops_submodule, "Q_Class", py::buffer_protocol())
        .def("__getitem__", py::overload_cast<Batch>(&Q_Class::operator[], py::const_))
        .def("__getitem__", py::overload_cast<double>(&Q_Class::operator[], py::const_))
        .def("__getitem__", [](const Q_Class& self, std::function<void(Graph)> fct) { return self[fct]; })  // overload_cast does not work for std::function
                        
        .def("__ror__", [](const Q_Class& self, Graph g) { return g | self; })		// this adds it to the Q on g and returns g
            
        .def("__call__", [](const Q_Class& self, Graph g) { return g | self; })		
        ;
    zefops_submodule.attr("Q") = Q;  // expose this singleton





    //                               ____        _       _                               
    //                              | __ )  __ _| |_ ___| |__                            
    //     _____ _____ _____ _____  |  _ \ / _` | __/ __| '_ \   _____ _____ _____ _____ 
    //    |_____|_____|_____|_____| | |_) | (_| | || (__| | | | |_____|_____|_____|_____|
    //                              |____/ \__,_|\__\___|_| |_|                          
    //                                                                                   

    // TODO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! the zefdb Qing system does NOT make use of the batch id if passed into the Queue yet. This still has to be implemented !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    py::class_<Batch>(zefops_submodule, "Batch", py::buffer_protocol())
        .def("__getitem__", py::overload_cast<str>(&Batch::operator[], py::const_))			
        ;
    zefops_submodule.attr("batch") = batch;  // expose this singleton


    ////////////////////////////////////////
    // * Imperative zefops


    zefops_submodule.def("exists_at", py::overload_cast<EZefRef, TimeSlice>(&imperative::exists_at), py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("exists_at", py::overload_cast<EZefRef, EZefRef>(&imperative::exists_at), py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("exists_at", py::overload_cast<EZefRef, ZefRef>(&imperative::exists_at), py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("exists_at", py::overload_cast<ZefRef, TimeSlice>(&imperative::exists_at), py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("exists_at", py::overload_cast<ZefRef, EZefRef>(&imperative::exists_at), py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("exists_at", py::overload_cast<ZefRef, ZefRef>(&imperative::exists_at), py::call_guard<py::gil_scoped_release>());

    zefops_submodule.def("to_frame", py::overload_cast<EZefRef, EZefRef, bool>(&imperative::to_frame), "entity"_a, "tx"_a, "allow_terminated"_a = false, py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("to_frame", py::overload_cast<ZefRef, EZefRef, bool>(&imperative::to_frame), "entity"_a, "tx"_a, "allow_terminated"_a = false, py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("to_frame", py::overload_cast<EZefRef, ZefRef, bool>(&imperative::to_frame), "entity"_a, "tx"_a, "allow_terminated"_a = false, py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("to_frame", py::overload_cast<ZefRef, ZefRef, bool>(&imperative::to_frame), "entity"_a, "tx"_a, "allow_terminated"_a = false, py::call_guard<py::gil_scoped_release>());

    // zefops_submodule.def("to_frame", py::overload_cast<EZefRefs, EZefRef, bool>(&imperative::to_frame), "uzrs"_a, "tx"_a, "allow_terminated"_a = false, py::call_guard<py::gil_scoped_release>());
    // zefops_submodule.def("to_frame", py::overload_cast<ZefRefs, EZefRef, bool>(&imperative::to_frame), "zrs"_a, "tx"_a, "allow_terminated"_a = false, py::call_guard<py::gil_scoped_release>());
    // zefops_submodule.def("to_frame", py::overload_cast<EZefRefs, ZefRef, bool>(&imperative::to_frame), "uzrs"_a, "tx"_a, "allow_terminated"_a = false, py::call_guard<py::gil_scoped_release>());
    // zefops_submodule.def("to_frame", py::overload_cast<ZefRefs, ZefRef, bool>(&imperative::to_frame), "zrs"_a, "tx"_a, "allow_terminated"_a = false, py::call_guard<py::gil_scoped_release>());

    zefops_submodule.def("target", py::overload_cast<EZefRef>(&imperative::target), py::call_guard<py::gil_scoped_release>());
    // zefops_submodule.def("target", py::overload_cast<const EZefRefs&>(&imperative::target), py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("target", py::overload_cast<ZefRef>(&imperative::target), py::call_guard<py::gil_scoped_release>());
    // zefops_submodule.def("target", py::overload_cast<const ZefRefs&>(&imperative::target), py::call_guard<py::gil_scoped_release>());

    zefops_submodule.def("source", py::overload_cast<EZefRef>(&imperative::source), py::call_guard<py::gil_scoped_release>());
    // zefops_submodule.def("source", py::overload_cast<const EZefRefs&>(&imperative::source), py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("source", py::overload_cast<ZefRef>(&imperative::source), py::call_guard<py::gil_scoped_release>());
    // zefops_submodule.def("source", py::overload_cast<const ZefRefs&>(&imperative::source), py::call_guard<py::gil_scoped_release>());


    zefops_submodule.def("traverse_out_edge", py::overload_cast<EZefRef,BlobType>(&imperative::traverse_out_edge));
    zefops_submodule.def("traverse_out_node", py::overload_cast<EZefRef,BlobType>(&imperative::traverse_out_node));
    zefops_submodule.def("traverse_in_edge", py::overload_cast<EZefRef,BlobType>(&imperative::traverse_in_edge));
    zefops_submodule.def("traverse_in_node", py::overload_cast<EZefRef,BlobType>(&imperative::traverse_in_node));

    zefops_submodule.def("traverse_out_edge_multi", py::overload_cast<EZefRef,BlobType>(&imperative::traverse_out_edge_multi));
    zefops_submodule.def("traverse_out_node_multi", py::overload_cast<EZefRef,BlobType>(&imperative::traverse_out_node_multi));
    zefops_submodule.def("traverse_in_edge_multi", py::overload_cast<EZefRef,BlobType>(&imperative::traverse_in_edge_multi));
    zefops_submodule.def("traverse_in_node_multi", py::overload_cast<EZefRef,BlobType>(&imperative::traverse_in_node_multi));

    zefops_submodule.def("traverse_out_edge_optional", py::overload_cast<EZefRef,BlobType>(&imperative::traverse_out_edge_optional));
    zefops_submodule.def("traverse_out_node_optional", py::overload_cast<EZefRef,BlobType>(&imperative::traverse_out_node_optional));
    zefops_submodule.def("traverse_in_edge_optional", py::overload_cast<EZefRef,BlobType>(&imperative::traverse_in_edge_optional));
    zefops_submodule.def("traverse_in_node_optional", py::overload_cast<EZefRef,BlobType>(&imperative::traverse_in_node_optional));

    zefops_submodule.def("traverse_out_edge", py::overload_cast<EZefRef,RelationType>(&imperative::traverse_out_edge));
    zefops_submodule.def("traverse_out_node", py::overload_cast<EZefRef,RelationType>(&imperative::traverse_out_node));
    zefops_submodule.def("traverse_in_edge", py::overload_cast<EZefRef,RelationType>(&imperative::traverse_in_edge));
    zefops_submodule.def("traverse_in_node", py::overload_cast<EZefRef,RelationType>(&imperative::traverse_in_node));

    zefops_submodule.def("traverse_out_edge_multi", py::overload_cast<EZefRef,RelationType>(&imperative::traverse_out_edge_multi));
    zefops_submodule.def("traverse_out_node_multi", py::overload_cast<EZefRef,RelationType>(&imperative::traverse_out_node_multi));
    zefops_submodule.def("traverse_in_edge_multi", py::overload_cast<EZefRef,RelationType>(&imperative::traverse_in_edge_multi));
    zefops_submodule.def("traverse_in_node_multi", py::overload_cast<EZefRef,RelationType>(&imperative::traverse_in_node_multi));

    zefops_submodule.def("traverse_out_edge_optional", py::overload_cast<EZefRef,RelationType>(&imperative::traverse_out_edge_optional));
    zefops_submodule.def("traverse_out_node_optional", py::overload_cast<EZefRef,RelationType>(&imperative::traverse_out_node_optional));
    zefops_submodule.def("traverse_in_edge_optional", py::overload_cast<EZefRef,RelationType>(&imperative::traverse_in_edge_optional));
    zefops_submodule.def("traverse_in_node_optional", py::overload_cast<EZefRef,RelationType>(&imperative::traverse_in_node_optional));

    zefops_submodule.def("traverse_out_edge", py::overload_cast<ZefRef,RelationType>(&imperative::traverse_out_edge));
    zefops_submodule.def("traverse_out_node", py::overload_cast<ZefRef,RelationType>(&imperative::traverse_out_node));
    zefops_submodule.def("traverse_in_edge", py::overload_cast<ZefRef,RelationType>(&imperative::traverse_in_edge));
    zefops_submodule.def("traverse_in_node", py::overload_cast<ZefRef,RelationType>(&imperative::traverse_in_node));

    zefops_submodule.def("traverse_out_edge_multi", py::overload_cast<ZefRef,RelationType>(&imperative::traverse_out_edge_multi));
    zefops_submodule.def("traverse_out_node_multi", py::overload_cast<ZefRef,RelationType>(&imperative::traverse_out_node_multi));
    zefops_submodule.def("traverse_in_edge_multi", py::overload_cast<ZefRef,RelationType>(&imperative::traverse_in_edge_multi));
    zefops_submodule.def("traverse_in_node_multi", py::overload_cast<ZefRef,RelationType>(&imperative::traverse_in_node_multi));

    zefops_submodule.def("traverse_out_edge_optional", py::overload_cast<ZefRef,RelationType>(&imperative::traverse_out_edge_optional));
    zefops_submodule.def("traverse_out_node_optional", py::overload_cast<ZefRef,RelationType>(&imperative::traverse_out_node_optional));
    zefops_submodule.def("traverse_in_edge_optional", py::overload_cast<ZefRef,RelationType>(&imperative::traverse_in_edge_optional));
    zefops_submodule.def("traverse_in_node_optional", py::overload_cast<ZefRef,RelationType>(&imperative::traverse_in_node_optional));

    // zefops_submodule.def("traverse_out_edge", py::overload_cast<const EZefRefs&,BlobType>(&imperative::traverse_out_edge));
    // zefops_submodule.def("traverse_out_node", py::overload_cast<const EZefRefs&,BlobType>(&imperative::traverse_out_node));
    // zefops_submodule.def("traverse_in_edge", py::overload_cast<const EZefRefs&,BlobType>(&imperative::traverse_in_edge));
    // zefops_submodule.def("traverse_in_node", py::overload_cast<const EZefRefs&,BlobType>(&imperative::traverse_in_node));

    // zefops_submodule.def("traverse_out_edge_multi", py::overload_cast<const EZefRefs&,BlobType>(&imperative::traverse_out_edge_multi));
    // zefops_submodule.def("traverse_out_node_multi", py::overload_cast<const EZefRefs&,BlobType>(&imperative::traverse_out_node_multi));
    // zefops_submodule.def("traverse_in_edge_multi", py::overload_cast<const EZefRefs&,BlobType>(&imperative::traverse_in_edge_multi));
    // zefops_submodule.def("traverse_in_node_multi", py::overload_cast<const EZefRefs&,BlobType>(&imperative::traverse_in_node_multi));

    // zefops_submodule.def("traverse_out_edge", py::overload_cast<const EZefRefs&,RelationType>(&imperative::traverse_out_edge));
    // zefops_submodule.def("traverse_out_node", py::overload_cast<const EZefRefs&,RelationType>(&imperative::traverse_out_node));
    // zefops_submodule.def("traverse_in_edge", py::overload_cast<const EZefRefs&,RelationType>(&imperative::traverse_in_edge));
    // zefops_submodule.def("traverse_in_node", py::overload_cast<const EZefRefs&,RelationType>(&imperative::traverse_in_node));

    // zefops_submodule.def("traverse_out_edge_multi", py::overload_cast<const EZefRefs&,RelationType>(&imperative::traverse_out_edge_multi));
    // zefops_submodule.def("traverse_out_node_multi", py::overload_cast<const EZefRefs&,RelationType>(&imperative::traverse_out_node_multi));
    // zefops_submodule.def("traverse_in_edge_multi", py::overload_cast<const EZefRefs&,RelationType>(&imperative::traverse_in_edge_multi));
    // zefops_submodule.def("traverse_in_node_multi", py::overload_cast<const EZefRefs&,RelationType>(&imperative::traverse_in_node_multi));

    // zefops_submodule.def("traverse_out_edge", py::overload_cast<const ZefRefs&,RelationType>(&imperative::traverse_out_edge));
    // zefops_submodule.def("traverse_out_node", py::overload_cast<const ZefRefs&,RelationType>(&imperative::traverse_out_node));
    // zefops_submodule.def("traverse_in_edge", py::overload_cast<const ZefRefs&,RelationType>(&imperative::traverse_in_edge));
    // zefops_submodule.def("traverse_in_node", py::overload_cast<const ZefRefs&,RelationType>(&imperative::traverse_in_node));

    // zefops_submodule.def("traverse_out_edge_multi", py::overload_cast<const ZefRefs&,RelationType>(&imperative::traverse_out_edge_multi));
    // zefops_submodule.def("traverse_out_node_multi", py::overload_cast<const ZefRefs&,RelationType>(&imperative::traverse_out_node_multi));
    // zefops_submodule.def("traverse_in_edge_multi", py::overload_cast<const ZefRefs&,RelationType>(&imperative::traverse_in_edge_multi));
    // zefops_submodule.def("traverse_in_node_multi", py::overload_cast<const ZefRefs&,RelationType>(&imperative::traverse_in_node_multi));





    zefops_submodule.def("assign_value", [](EZefRef uzr, py::int_ other) {pyint_assign_value(uzr, other); },
        "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("assign_value", [](ZefRef zr, py::int_ other) {pyint_assign_value(to_ezefref(zr), other); },
        "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());

    zefops_submodule.def("assign_value", py::overload_cast<EZefRef,const double&>(&imperative::assign_value<double>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
	zefops_submodule.def("assign_value", py::overload_cast<ZefRef,const double&>(&imperative::assign_value<double>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
	zefops_submodule.def("assign_value", py::overload_cast<EZefRef,const bool&>(&imperative::assign_value<bool>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
	zefops_submodule.def("assign_value", py::overload_cast<ZefRef,const bool&>(&imperative::assign_value<bool>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
	zefops_submodule.def("assign_value", py::overload_cast<EZefRef,const std::string&>(&imperative::assign_value<std::string>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
	zefops_submodule.def("assign_value", py::overload_cast<ZefRef,const std::string&>(&imperative::assign_value<std::string>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
	zefops_submodule.def("assign_value", py::overload_cast<EZefRef,const Time&>(&imperative::assign_value<Time>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
	zefops_submodule.def("assign_value", py::overload_cast<ZefRef,const Time&>(&imperative::assign_value<Time>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
	zefops_submodule.def("assign_value", py::overload_cast<EZefRef,const SerializedValue&>(&imperative::assign_value<SerializedValue>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
	zefops_submodule.def("assign_value", py::overload_cast<ZefRef,const SerializedValue&>(&imperative::assign_value<SerializedValue>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
	zefops_submodule.def("assign_value", py::overload_cast<EZefRef,const ZefEnumValue&>(&imperative::assign_value<ZefEnumValue>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
	zefops_submodule.def("assign_value", py::overload_cast<ZefRef,const ZefEnumValue&>(&imperative::assign_value<ZefEnumValue>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
	zefops_submodule.def("assign_value", py::overload_cast<EZefRef,const QuantityFloat&>(&imperative::assign_value<QuantityFloat>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
	zefops_submodule.def("assign_value", py::overload_cast<ZefRef,const QuantityFloat&>(&imperative::assign_value<QuantityFloat>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
	zefops_submodule.def("assign_value", py::overload_cast<EZefRef,const QuantityInt&>(&imperative::assign_value<QuantityInt>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("assign_value", py::overload_cast<ZefRef,const QuantityInt&>(&imperative::assign_value<QuantityInt>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
	zefops_submodule.def("assign_value", py::overload_cast<EZefRef,const AttributeEntityType&>(&imperative::assign_value<AttributeEntityType>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
	zefops_submodule.def("assign_value", py::overload_cast<ZefRef,const AttributeEntityType&>(&imperative::assign_value<AttributeEntityType>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
	zefops_submodule.def("assign_value", py::overload_cast<EZefRef,const EZefRef&>(&imperative::assign_value<EZefRef>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("assign_value", py::overload_cast<ZefRef,const EZefRef&>(&imperative::assign_value<EZefRef>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("assign_value", py::overload_cast<ZefRef,const ZefRef&>(&imperative::assign_value<ZefRef>), "Assign a value to an atomic entity.", py::call_guard<py::gil_scoped_release>());
    
    py::class_<SerializedValue>(zefops_submodule, "SerializedValue", py::buffer_protocol())
        .def(py::init<std::string,std::string>())
        .def_readonly("type", &SerializedValue::type)
        .def_readonly("data", &SerializedValue::data)
        ;
	
    zefops_submodule.def("value", py::overload_cast<ZefRef>(&imperative::value), "Read a value from an atomic entity.", py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("value", py::overload_cast<EZefRef,EZefRef>(&imperative::value), "Read a value from an atomic entity.", py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("value", py::overload_cast<ZefRef,EZefRef>(&imperative::value), "Read a value from an atomic entity.", py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("value", py::overload_cast<EZefRef,ZefRef>(&imperative::value), "Read a value from an atomic entity.", py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("value", py::overload_cast<ZefRef,ZefRef>(&imperative::value), "Read a value from an atomic entity.", py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("value", py::overload_cast<EZefRef>(&imperative::value), "Read a value from an atomic entity.", py::call_guard<py::gil_scoped_release>());

    // zefops_submodule.def("value", py::overload_cast<ZefRefs>(&imperative::value), py::call_guard<py::gil_scoped_release>());
    // zefops_submodule.def("value", py::overload_cast<EZefRefs,EZefRef>(&imperative::value), py::call_guard<py::gil_scoped_release>());
    // zefops_submodule.def("value", py::overload_cast<ZefRefs,EZefRef>(&imperative::value), py::call_guard<py::gil_scoped_release>());
    // zefops_submodule.def("value", py::overload_cast<EZefRefs,ZefRef>(&imperative::value), py::call_guard<py::gil_scoped_release>());
    // zefops_submodule.def("value", py::overload_cast<ZefRefs,ZefRef>(&imperative::value), py::call_guard<py::gil_scoped_release>());

	zefops_submodule.def("terminate", py::overload_cast<EZefRef>(&imperative::terminate), py::call_guard<py::gil_scoped_release>(), "A function to terminate an entity / atomic entity / relation");
	zefops_submodule.def("terminate", py::overload_cast<ZefRef>(&imperative::terminate), py::call_guard<py::gil_scoped_release>(), "A function to terminate an entity / atomic entity / relation");	
	// zefops_submodule.def("terminate", py::overload_cast<EZefRefs>(&imperative::terminate), py::call_guard<py::gil_scoped_release>(), "A function to terminate an entity / atomic entity / relation");
    // zefops_submodule.def("terminate", py::overload_cast<ZefRefs>(&imperative::terminate), py::call_guard<py::gil_scoped_release>(), "A function to terminate an entity / atomic entity / relation");	

	zefops_submodule.def("retire", py::overload_cast<EZefRef>(&imperative::retire), py::call_guard<py::gil_scoped_release>(), "A low level function that retires a delegate.");

    zefops_submodule.def("delegate", py::overload_cast<EZefRef>(&imperative::delegate), py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("delegate", py::overload_cast<ZefRef>(&imperative::delegate), py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("delegate", py::overload_cast<const Graph &,EntityType>(&imperative::delegate), py::call_guard<py::gil_scoped_release>());
    zefops_submodule.def("delegate", py::overload_cast<const Graph &,ValueRepType>(&imperative::delegate), py::call_guard<py::gil_scoped_release>());

    // I have no idea why the overload_cast complains here... just working around it for now.
    // zefops_submodule.def("delegate_of", py::overload_cast<EZefRef>(&delegate_of), py::call_guard<py::gil_scoped_release>());
    // zefops_submodule.def("delegate_of", [](const EZefRef& x) { return delegate_of(x); }, py::call_guard<py::gil_scoped_release>());

#define REPEAT(TYPE)                                                    \
    zefops_submodule.def("delegate_of", [](const TYPE & x) { return delegate_of(x); }, py::call_guard<py::gil_scoped_release>());

    REPEAT(EZefRef);
    REPEAT(ZefRef);
    REPEAT(Delegate);
    REPEAT(EntityType);
    REPEAT(ValueRepType);
    REPEAT(RelationType);
#undef REPEAT

    // All the triples to go in here
#define REPEAT(SRC, TRG)                                                \
    zefops_submodule.def("delegate_of", [](const SRC & x, RelationType rt, const TRG & y) { return delegate_of(x, rt, y); }, py::call_guard<py::gil_scoped_release>());
#define OUTER(SRC)                              \
    REPEAT(SRC, EZefRef);                       \
    REPEAT(SRC, ZefRef);                        \
    REPEAT(SRC, Delegate);                      \
    REPEAT(SRC, EntityType);                    \
    REPEAT(SRC, ValueRepType);                  \
    REPEAT(SRC, RelationType);

    OUTER(EZefRef);
    OUTER(ZefRef);
    OUTER(Delegate);
    OUTER(EntityType);
    OUTER(ValueRepType);
    OUTER(RelationType);

#undef OUTER
#undef REPEAT


    zefops_submodule.def("select_by_field_impl", [](std::vector<ZefRef> zrs, RelationType rt, value_variant_t val) -> std::optional<ZefRef> {
            // This is just to avoid creating a ZefRefs without a tx.
            if(zrs.size() == 0)
                return {};
            
            ZefRefs opts{0, zrs[0].tx};
            opts = zrs | filter[( [&rt,&val](ZefRef z) -> bool {
                auto maybe_field = z >> O[rt];
                if(!maybe_field)
                    return false;
                auto val_contained = imperative::value(*maybe_field);
                if(!val_contained)
                    return false;
                    
                return std::visit([&val](auto & left, auto & right) -> bool {
                    using Tl = typename std::decay_t<decltype(left)>;
                    using Tr = typename std::decay_t<decltype(right)>;
                    if constexpr(std::is_same_v<Tl, Tr>)
                                    return left == right;
                    else
                        return false;
                },
                    *val_contained, val);
            })];

            if(length(opts) == 1)
                return only(opts);
            if(length(opts) >= 2)
                throw std::runtime_error("More than one option");
            return {};
        });
    
}
        


