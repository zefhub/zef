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

//#include "graph.h"
#include <ostream>
#include <iostream>
#include <string>
#include <chrono>         // std::chrono::seconds
#include <vector>
#include <unordered_map>
#include "range/v3/all.hpp"

#include "fwd_declarations.h"
#include "zefDB_utils.h"
#include "butler/threadsafe_map.h"
#include "tokens.h"
//#include "high_level_api.h"

/*[[[cog
import cog
import json
from functional import seq
with open('zeftypes_ET.json') as F:
    et = json.loads(F.read())
with open('zeftypes_RT.json') as F:
    rt = json.loads(F.read())
with open('zeftypes_EN.json') as F:
    en = json.loads(F.read())
]]]*/
//[[[end]]]	

namespace zefDB {
















    struct LIBZEF_DLL_EXPORTED TimeSlice {
        int value = 0;  // first actual tx has time slice=1. Then 0 can be used as sentinel if needed

        constexpr TimeSlice() = default;
        constexpr TimeSlice(int value) : value(value) {}

        operator int() const { return value; }  // allow casting to an int in C++:   int n = my_timeslice;

        const int& operator* () const { return value; }
        TimeSlice operator++(int) { return TimeSlice{ value++ }; }  // post increment. Mutate the actual local value and return a new object with the old value
        TimeSlice operator++() { value++; return *this; }  // pre increment. Mutate the actual local value

        bool operator> (TimeSlice other) const { return value > other.value; }
        bool operator>= (TimeSlice other) const { return value >= other.value; }
        bool operator< (TimeSlice other) const { return value < other.value; }
        bool operator<= (TimeSlice other) const { return value <= other.value; }
        bool operator== (TimeSlice other) const { return value == other.value; }
        bool operator!= (TimeSlice other) const { return value != other.value; }

        TimeSlice operator() (EZefRef uzr) const;   // enable "tx_uzr | time_slice" to get TimeSlice object
        TimeSlice operator() (ZefRef zr) const;
    };

    inline void to_json(json& j, const TimeSlice & time_slice) {
        j = json{
            {"zef_type", "TimeSlice"},
            {"slice", time_slice.value}
        };
    }

    inline void from_json(const json& j, TimeSlice & time_slice) {
        assert(j["zef_type"].get<std::string>() == "TimeSlice");
        j.at("slice").get_to(time_slice.value);
    }








    namespace internals {
        inline ZefEnumValue assert_that_is_unit_val(ZefEnumValue x) {
            if (x.enum_type() != "Unit" || x.enum_value() == "")
                throw std::runtime_error("ZefEnumValue passed is not a Unit value (e.g. 'kilograms')");
            return x;
        }
    }


    struct LIBZEF_DLL_EXPORTED QuantityFloat {
        double value;
        ZefEnumValue unit;
                
        QuantityFloat(double value_=0.0, ZefEnumValue unit_=EN.Unit._undefined) : value(value_), unit(internals::assert_that_is_unit_val(unit_)) {};
        bool operator== (const QuantityFloat& rhs) const { return value == rhs.value && unit == rhs.unit; }
        bool operator!= (const QuantityFloat& rhs) const { return value != rhs.value || unit != rhs.unit; }
        bool operator> (const QuantityFloat& rhs) const {
            if(unit != rhs.unit) throw std::runtime_error("Units do not agree. " + to_str(unit) + " and " + to_str(rhs.unit));
            return value > rhs.value;
        }
        bool operator>= (const QuantityFloat& rhs) const {
            if(unit != rhs.unit) throw std::runtime_error("Units do not agree. " + to_str(unit) + " and " + to_str(rhs.unit));
            return value >= rhs.value;
        }
        bool operator< (const QuantityFloat& rhs) const {
            if(unit != rhs.unit) throw std::runtime_error("Units do not agree. " + to_str(unit) + " and " + to_str(rhs.unit));
            return value < rhs.value;
        }
        bool operator<= (const QuantityFloat& rhs) const {
            if(unit != rhs.unit) throw std::runtime_error("Units do not agree. " + to_str(unit) + " and " + to_str(rhs.unit));
            return value <= rhs.value;
        }

        QuantityFloat operator-() const {
            return QuantityFloat{-value, unit};
        }
    };
    inline std::ostream& operator << (std::ostream& o, QuantityFloat q) { o << q.value << " " << q.unit; return o; }


    struct LIBZEF_DLL_EXPORTED QuantityInt {
        int value = 0;
        ZefEnumValue unit = EN.Unit._undefined;
                
        QuantityInt(int value_=0, ZefEnumValue unit_ = EN.Unit._undefined) : value(value_), unit(internals::assert_that_is_unit_val(unit_)) {};
        bool operator== (const QuantityInt& rhs) const { return value == rhs.value && unit == rhs.unit; }
        bool operator!= (const QuantityInt& rhs) const { return value != rhs.value || unit != rhs.unit; }
        bool operator> (const QuantityInt& rhs) const {
            if(unit != rhs.unit) throw std::runtime_error("Units do not agree. " + to_str(unit) + " and " + to_str(rhs.unit));
            return value > rhs.value;
        }
        bool operator>= (const QuantityInt& rhs) const {
            if(unit != rhs.unit) throw std::runtime_error("Units do not agree. " + to_str(unit) + " and " + to_str(rhs.unit));
            return value >= rhs.value;
        }
        bool operator< (const QuantityInt& rhs) const {
            if(unit != rhs.unit) throw std::runtime_error("Units do not agree. " + to_str(unit) + " and " + to_str(rhs.unit));
            return value < rhs.value;
        }
        bool operator<= (const QuantityInt& rhs) const {
            if(unit != rhs.unit) throw std::runtime_error("Units do not agree. " + to_str(unit) + " and " + to_str(rhs.unit));
            return value <= rhs.value;
        }
        QuantityInt operator-() const {
            return QuantityInt{-value, unit};
        }
    };
    inline std::ostream& operator << (std::ostream& o, QuantityInt q) { o << q.value << " " << q.unit; return o; }








    LIBZEF_DLL_EXPORTED QuantityFloat operator+ (QuantityFloat d1, QuantityFloat d2);
    LIBZEF_DLL_EXPORTED QuantityFloat operator- (QuantityFloat d1, QuantityFloat d2);

    inline QuantityFloat operator* (double x, QuantityFloat q) { return QuantityFloat{ x * q.value, q.unit }; }
    inline QuantityFloat operator* (int x, QuantityFloat q) { return QuantityFloat{ x * q.value, q.unit }; }
    inline QuantityFloat operator* (QuantityFloat q, double x) { return QuantityFloat{ x * q.value, q.unit }; }
    inline QuantityFloat operator* (QuantityFloat q, int x) { return QuantityFloat{ x * q.value, q.unit }; }
    inline QuantityFloat operator/ (QuantityFloat q, double x) { return QuantityFloat{ q.value / x, q.unit }; }
    inline QuantityFloat operator/ (QuantityFloat q, int x) { return QuantityFloat{ q.value / x, q.unit }; }
    

    LIBZEF_DLL_EXPORTED QuantityInt operator+ (QuantityInt d1, QuantityInt d2);
    LIBZEF_DLL_EXPORTED QuantityInt operator- (QuantityInt d1, QuantityInt d2);

    inline QuantityInt operator* (int x, QuantityInt q) { return QuantityInt{ x * q.value, q.unit }; }
    inline QuantityFloat operator* (double x, QuantityInt q) { return QuantityFloat{ x * q.value, q.unit }; }
    inline QuantityInt operator* (QuantityInt q, int x) { return QuantityInt{ x * q.value, q.unit }; }
    inline QuantityFloat operator* (QuantityInt q, double x) { return QuantityFloat{ x * q.value, q.unit }; }
    LIBZEF_DLL_EXPORTED QuantityInt operator/ (QuantityInt q, int m);   // integer division, only allow if no remainder
    inline QuantityFloat operator/ (QuantityInt q, double x) { return QuantityFloat{ q.value / x, q.unit }; }

    LIBZEF_DLL_EXPORTED QuantityFloat operator+ (QuantityFloat q1, QuantityInt q2);
    LIBZEF_DLL_EXPORTED QuantityFloat operator+ (QuantityInt q1, QuantityFloat q2);
    LIBZEF_DLL_EXPORTED QuantityFloat operator- (QuantityFloat q1, QuantityInt q2);
    LIBZEF_DLL_EXPORTED QuantityFloat operator- (QuantityInt q1, QuantityFloat q2);











//                                 __      _   _                                     
//                        _______ / _|    | |_(_)_ __ ___   ___                      
//    _____ _____ _____  |_  / _ \ |_     | __| | '_ ` _ \ / _ \   _____ _____ _____ 
//   |_____|_____|_____|  / /  __/  _|    | |_| | | | | | |  __/  |_____|_____|_____|
//                       /___\___|_|       \__|_|_| |_| |_|\___|                     
//                                                                                   

    struct LIBZEF_DLL_EXPORTED Time {

        // TODO: implement various ctors 

        Time() = default;
        Time(double t_unix_time) : seconds_since_1970(t_unix_time) {}
        //Time(ZefRef);  // Constructor from a ZefRef using the time from its reference frame tx
        //Time(EZefRef);  // Constructor from a tx EZefRef

        double seconds_since_1970 = std::numeric_limits<double>::quiet_NaN();
        Time& operator= (const Time& other);
    };

    inline void to_json(json& j, const Time & time) {
        j = json{
            {"zef_type", "Time"},
            {"seconds_since_1970", time.seconds_since_1970}
        };
    }

    inline void from_json(const json& j, Time & time) {
        assert(j["zef_type"].get<std::string>() == "Time");
        j.at("seconds_since_1970").get_to(time.seconds_since_1970);
    }


    inline bool operator== (Time t1, Time t2) { return t1.seconds_since_1970 == t2.seconds_since_1970; }
    inline bool operator!= (Time t1, Time t2) { return t1.seconds_since_1970 != t2.seconds_since_1970; }
    inline bool operator> (Time t1, Time t2) { return t1.seconds_since_1970 > t2.seconds_since_1970; }
    inline bool operator< (Time t1, Time t2) { return t1.seconds_since_1970 < t2.seconds_since_1970; }
    inline bool operator<= (Time t1, Time t2) { return t1.seconds_since_1970 <= t2.seconds_since_1970; }
    inline bool operator>= (Time t1, Time t2) { return t1.seconds_since_1970 >= t2.seconds_since_1970; }

    inline QuantityFloat operator- (Time t1, Time t2) { return QuantityFloat{ t1.seconds_since_1970 - t2.seconds_since_1970, EN.Unit.seconds }; }

    LIBZEF_DLL_EXPORTED Time operator+ (Time t, QuantityFloat duration);
    LIBZEF_DLL_EXPORTED Time operator+ (Time t, QuantityInt duration);
    inline Time operator+ (QuantityFloat duration, Time t) { return t + duration; }
    inline Time operator+ (QuantityInt duration, Time t) { return t + duration; }
    LIBZEF_DLL_EXPORTED Time operator- (Time t, QuantityFloat duration);
    LIBZEF_DLL_EXPORTED Time operator- (Time t, QuantityInt duration);




    const QuantityFloat seconds{ 1, EN.Unit.seconds };
    const QuantityFloat minutes{ 60, EN.Unit.seconds };
    const QuantityFloat hours{ 60 * 60, EN.Unit.seconds };
    const QuantityFloat days{ 60 * 60 * 24, EN.Unit.seconds };
    const QuantityFloat weeks{ 60 * 60 * 24 * 7, EN.Unit.seconds };
    const QuantityFloat months{ 60 * 60 * 24 * 30, EN.Unit.seconds };
    const QuantityFloat years{ 60 * 60 * 24 * 365, EN.Unit.seconds };


    LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, Time t);











}
