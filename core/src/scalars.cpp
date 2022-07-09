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

#include "scalars.h"
// TODO: Figure out these manual circular dependency breaks
#include "graph.h"
#include "butler/butler.h"

namespace zefDB {
    //////////////////////////////
    // * ZefEnumValue
    ZefEnumValue ZefEnumStruct::operator() (const std::string& enum_type, const std::string& enum_val) const {
        // call with EN("MachineStatus", "IDLE")
        return internals::get_enum_value_from_string(enum_type, enum_val);
    }    

	std::ostream& operator<<(std::ostream& os, ZefEnumValue zef_enum_val) {
		auto sp = internals::get_enum_string_pair(zef_enum_val);
		os << "EN." << sp.first << "." << sp.second;
		return os;
	}

	std::string ZefEnumValue::enum_type() const { return internals::get_enum_string_pair(*this).first; }
	std::string ZefEnumValue::enum_value() const { return  internals::get_enum_string_pair(*this).second; }

    ////////////////////////////////
    // * QuantityFloat


    QuantityFloat operator+ (QuantityFloat q1, QuantityFloat q2) { 
        if (q1.unit != q2.unit) throw std::runtime_error("QuantityFloat + QuantityFloat called, but units don't match.");
        return QuantityFloat{ q1.value + q2.value, q1.unit }; 
    }    

    QuantityFloat operator- (QuantityFloat q1, QuantityFloat q2) { 
        if (q1.unit != q2.unit) throw std::runtime_error("QuantityFloat - QuantityFloat called, but units don't match.");
        return QuantityFloat{ q1.value - q2.value, q1.unit }; 
    }

    


    QuantityInt operator+ (QuantityInt q1, QuantityInt q2) { 
        if (q1.unit != q2.unit) throw std::runtime_error("QuantityInt + QuantityInt called, but units don't match.");
        return QuantityInt{ q1.value + q2.value, q1.unit }; 
    }    

    QuantityInt operator- (QuantityInt q1, QuantityInt q2) { 
        if (q1.unit != q2.unit) throw std::runtime_error("QuantityInt - QuantityInt called, but units don't match.");
        return QuantityInt{ q1.value - q2.value, q1.unit }; 
    }
    
    QuantityInt operator/ (QuantityInt q1, int m) { 
        if (q1.value % m != 0) throw std::runtime_error("QuantityInt / m called, but the QuantityInt is not divisble by m");
        return QuantityInt{ q1.value / m, q1.unit };        
    }




    QuantityFloat operator+ (QuantityFloat q1, QuantityInt q2) {
        if (q1.unit != q2.unit) throw std::runtime_error("QuantityFloat + QuantityInt called, but units don't match.");
        return QuantityFloat{ q1.value + q2.value, q1.unit };
    }    

    QuantityFloat operator+ (QuantityInt q1, QuantityFloat q2) {
        if (q1.unit != q2.unit) throw std::runtime_error("QuantityInt + QuantityFloat called, but units don't match.");
        return QuantityFloat{ q1.value + q2.value, q1.unit };
    }

    QuantityFloat operator- (QuantityFloat q1, QuantityInt q2) {
        if (q1.unit != q2.unit) throw std::runtime_error("QuantityFloat - QuantityInt called, but units don't match.");
        return QuantityFloat{ q1.value - q2.value, q1.unit };
    }    

    QuantityFloat operator- (QuantityInt q1, QuantityFloat q2) {
        if (q1.unit != q2.unit) throw std::runtime_error("QuantityInt - QuantityFloat called, but units don't match.");
        return QuantityFloat{ q1.value - q2.value, q1.unit };
    }




    //////////////////////////////
    // * Time

    Time& Time::operator= (const Time& other) {
        if (this != &other)
            new(this) Time(other); // placement new            
        return *this;
    }


    Time operator+ (Time t, QuantityFloat duration) {
        if (duration.unit != EN.Unit.seconds) throw std::runtime_error("Time + QuantityFloat called, but units of the latter is not EN.Unit.seconds");
        return Time{ t.seconds_since_1970 + duration.value }; 
    }
    Time operator- (Time t, QuantityFloat duration) {
        if (duration.unit != EN.Unit.seconds) throw std::runtime_error("Time - QuantityFloat called, but units of the latter is not EN.Unit.seconds");
        return Time{ t.seconds_since_1970 - duration.value };
    }
    

    Time operator+ (Time t, QuantityInt duration) {
        if (duration.unit != EN.Unit.seconds) throw std::runtime_error("Time + QuantityInt called, but units of the latter is not EN.Unit.seconds");
        return Time{ t.seconds_since_1970 + duration.value }; 
    }
    Time operator- (Time t, QuantityInt duration) {
        if (duration.unit != EN.Unit.seconds) throw std::runtime_error("Time - QuantityInt called, but units of the latter is not EN.Unit.seconds");
        return Time{ t.seconds_since_1970 - duration.value };
    }



    std::ostream& operator<< (std::ostream& os, Time t) { os << "unix time: " << std::fixed << t.seconds_since_1970; return os; }


    TimeSlice::TimeSlice(EZefRef uzr) {
        if ( (uzr | BT) == BT.ROOT_NODE)
            value = 0;
        else if ( (uzr | BT) == BT.TX_EVENT_NODE)
            value = get<blobs_ns::TX_EVENT_NODE>(uzr).time_slice;
        else
            throw std::runtime_error("time_slice(uzr) called for a uzr that is not a BT.TX_EVENT_NODE.");
    }
    TimeSlice operator| (EZefRef uzr, TimeSlice op) { return TimeSlice(uzr); }

    TimeSlice::TimeSlice(ZefRef zr) : TimeSlice(zr.blob_uzr) {}

    TimeSlice TimeSlice::operator() (ZefRef zr) const {
        return TimeSlice(zr.blob_uzr);
    }
    TimeSlice operator| (ZefRef zr, TimeSlice op) { return TimeSlice(zr); }

    TimeSlice TimeSlice::operator() (EZefRef ezr) const {
        return TimeSlice{ezr};
    }

    

}
