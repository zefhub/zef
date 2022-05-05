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

#include <iostream>
#include <variant>
#include <chrono>
#include <ctime>
#include "scalars.h"


namespace zefDB{

    // the zefop can be used to initialize time: time(now)
    // or should 'now()' return a struct of type 'Time'
    struct Now {};




    struct Time {
        const double seconds_since_1970;

        // using the 'now' zefop: we want to enable t1 = time(now)
        Time() (Now n) : seconds_since_1970(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count() * 1E-6){}
        Time& operator= (const Time& other) { 
            if (this != &other)
                seconds_since_1970{ other.seconds_since_1970 };
            return *this;            
        }        
    };    

    struct Duration{
        const double value_in_seconds = 0.0;
    };


    Time operator+ (Time t, Duration d){ return Time { t.seconds_since_1970 + d.value_in_seconds }; }
    Time operator+ (Duration d, Time t){ return Time { t.seconds_since_1970 + d.value_in_seconds }; }
    Time operator- (Time t, Duration d){ return Time { t.seconds_since_1970 - d.value_in_seconds }; }

    Duration operator- (Time t1, Time t2) { return Duration{ t1.seconds_since_1970 - t2.seconds_since_1970 }; }

    Duration operator+ (Duration d1, Duration d2) { return Duration{ d1.value_in_seconds + d2.value_in_seconds }; }
    Duration operator- (Duration d1, Duration d2) { return Duration{ d1.value_in_seconds - d2.value_in_seconds }; }

    Duration operator* (double x, Duration d) { return Duration{ x * d.value_in_seconds }; }
    Duration operator* (Duration d, double x) { return Duration{ x * d.value_in_seconds }; }
    Duration operator* (int x, Duration d) { return Duration{ x * d.value_in_seconds }; }
    Duration operator* (Duration d, int x) { return Duration{ x * d.value_in_seconds }; }
    
    Duration operator/ (Duration d, double x) { return Duration{ d.value_in_seconds / x }; }    
    Duration operator/ (Duration d, int x) { return Duration{ d.value_in_seconds / x }; }
    
    double operator/ (Duration d1, Duration d2) { return d1.value_in_seconds / d2.value_in_seconds; };




    constexpr Duration seconds{ 1 };
    constexpr Duration minutes{ 60 };
    constexpr Duration hours{ 60*60 };
    constexpr Duration days{ 60*60*24 };
    constexpr Duration weeks{ 60*60*24*7 };
    constexpr Duration years{ 60*60*24*365 };

    std::ostream& operator<< (std::ostream& os, Time t){ os << "Time: " << t.seconds_since_1970 << " seconds since 1970"; return os; }
}

int main() {
    using namespace zefDB;
//   time_t rawtime = (const time_t) 1603676887.161292;
//   struct tm * timeinfo;
//   timeinfo = localtime (&rawtime);
//   printf ("Current local time and date: %s", asctime(timeinfo));
  

    auto now = Now{};
    auto t1 =  zefDB::time(now) + 2.0*days - 24*hours;




    std::cout.precision(16);
    std::cout << t1 << "\n\n";
    std::cout << std::ctime(t1) << "\n";

    

	std::cout << "done\n";
	return 0;
}
