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

// #include <doctest.h>
#include <functional>   // std::reference_wrapper
#include <variant>
#include "zefDB.h"

#include <pybind11/pybind11.h>
#include <pybind11/stl_bind.h>
#include <pybind11/embed.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h> //Required for Python to support cpp lambda functions
#include <nlohmann/json.hpp>
using json = nlohmann::json;
#include <pybind11_json/pybind11_json.hpp>
#include <iostream>
namespace py = pybind11;
using namespace pybind11::literals; //This does not bring in anything else from the pybind11 namespace except for literals.
using py_obj = pybind11::object;

void create_zefops_module(py::module_ & m, py::module_ & internals_submodule);
void fill_internals_module(py::module_ & internals_submodule);


using namespace zefDB;

// Need to handle ints carefully - throwing an error if they are too big
inline void pyint_assign_value(zefDB::EZefRef uzr, py::int_ val) {
    // The below doesn't work, as I think it truncates the python int before comparing 
    // if(val > std::numeric_limits<int>::max() ||
    //    val < std::numeric_limits<int>::lowest())

    // Annoyingly this segfaults... no idea why so I'm taking out this testing which is bad...
    // static py::int_ temp1(std::numeric_limits<int>::max());
    // static py::int_ temp2(std::numeric_limits<int>::lowest());
    // if(val > temp1 || val < temp2)
    //     throw std::runtime_error("Can't assign integer, bigger than a C int32.");


    // Note: error handling requires the GIL to be held. Ironically, a value of
    // -1 will trigger a call to PyErr_Occured (which is to double check that
    // there is an error) and so needs the GIL.
    int to_assign;
    {
        py::gil_scoped_acquire gil;

        // Dodgy version of the test which can fail if there are massive ints going in.
        if(py::cast<long long>(val) > std::numeric_limits<int>::max() ||
           py::cast<long long>(val) < std::numeric_limits<int>::lowest())
            throw std::runtime_error("Can't assign integer, bigger than a C int32.");

        to_assign = py::cast<int>(val);
    }

    imperative::assign_value(uzr, to_assign);
}

namespace pybind11 { namespace detail {
    // template <> struct type_caster<ZefRefs> {
    // public:
    //     PYBIND11_TYPE_CASTER(ZefRefs, const_name("ZefRefs"));

    //     // /**
    //     //  * Conversion part 1 (Python->C++): convert a PyObject into a inty
    //     //  * instance or return false upon failure. The second argument
    //     //  * indicates whether implicit conversions should be applied.
    //     //  */
    //     // bool load(handle src, bool) {
    //     //     /* Extract PyObject from handle */
    //     //     PyObject *source = src.ptr();
    //     //     /* Try converting into a Python integer value */
    //     //     PyObject *tmp = PyNumber_Long(source);
    //     //     if (!tmp)
    //     //         return false;
    //     //     /* Now try to convert into a C++ int */
    //     //     value.long_value = PyLong_AsLong(tmp);
    //     //     Py_DECREF(tmp);
    //     //     /* Ensure return code was OK (to avoid out-of-range errors etc) */
    //     //     return !(value.long_value == -1 && !PyErr_Occurred());
    //     // }

    //     /**
    //      * Conversion part 2 (C++ -> Python): convert an inty instance into
    //      * a Python object. The second and third arguments are used to
    //      * indicate the return value policy and parent object (for
    //      * ``return_value_policy::reference_internal``) and are generally
    //      * ignored by implicit casters.
    //      */
    //     static handle cast(ZefRefs src, return_value_policy, handle) {
    //         std::vector<ZefRef> vec;
    //         vec.reserve(length(src));
    //         for(const auto & it : src) {
    //             vec.push_back(it);
    //         }
    //         return py::cast(vec);
    //     }
    // };

       template <>
       struct type_caster<ZefRefs> : list_caster<ZefRefs, ZefRef> {}; 
       template <>
       struct type_caster<EZefRefs> : list_caster<EZefRefs, EZefRef> {}; 
}}