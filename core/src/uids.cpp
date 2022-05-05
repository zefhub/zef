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

#include "uids.h"
#include <iomanip>

#include <iostream>
#include <string>
#include <regex>

namespace zefDB {

	std::string to_hex(const unsigned char& c) {
		std::stringstream ss;
		ss << std::hex;
		if (c < 16) ss << "0";   // a small number would just be represented by a single character
		ss << short(c);
		return ss.str();
	}	
	
	

	// used to print out actual memory settings, typically for debugging.
	// Not to be used in performance critical code, the conversion to string 
	// mapping: (pointer to first byte, number of chars to print to string)  -> string of characters (2*specified length)
    BaseUID::operator std::string() const {
		std::stringstream ss;
		for (size_t pos = 0; pos < size_in_bytes; pos++) {
            ss << std::hex << std::setfill('0') << std::setw(2);
			ss << short(*(binary_uid + pos));
		}
		return ss.str();
	}



    namespace internals {
        void _from_hex(const std::string& uid, void * target_ptr) {
            if (uid.size() != 16)
                throw std::invalid_argument("Invalid uid input size to from_hex(const std::string& uid, unsigned char* target)");

            _from_hex(uid.c_str(), target_ptr);
        }

        void _from_hex(const unsigned char * src, void * target_ptr) {
            // target needs to be a preallocated buffer of the size uid.size()/2 bytes. (1 hex character = 2 bytes of information)
            // taken from https://stackoverflow.com/questions/17261798/converting-a-hex-string-to-a-byte-array
            unsigned char * target = (unsigned char*)target_ptr;

            auto char2int = [](unsigned char input)->int
            {
                if (input >= '0' && input <= '9')
                    return input - '0';
                if (input >= 'A' && input <= 'F')
                    return input - 'A' + 10;
                if (input >= 'a' && input <= 'f')
                    return input - 'a' + 10;
                throw std::invalid_argument("Invalid input string");
            };

            // the snippet below assumes src to be a zero terminated sanitized string with
            // an even number of [0-9a-f] characters, and target to be sufficiently large
            while (*src && src[1])
                {
                    *(target++) = char2int(*src) * 16 + char2int(src[1]);
                    src += 2;
                }
        }
    }


	bool operator== (const BaseUID& u1, const BaseUID& u2) {
        return std::string_view((const char*)u1.binary_uid, BaseUID::size_in_bytes) == std::string_view((const char*)u2.binary_uid, BaseUID::size_in_bytes);
    }

	bool operator!= (const BaseUID& u1, const BaseUID& u2) { return !(u1 == u2); }

	std::ostream& operator<< (std::ostream& os, const BaseUID& x) {		
		os << "BaseUID(\"" << str(x) << "\")";
		return os;
	}

	BaseUID make_random_uid(void * in_place) {
		static_assert(BaseUID::size_in_bytes == 8);
        static_assert(sizeof(unsigned long long) == BaseUID::size_in_bytes);
		*(unsigned long long*)in_place = generate_random_number_random_device();
		return BaseUID::from_ptr(in_place);
	}

	BaseUID make_random_uid() {
        BaseUID uid{};
        make_random_uid(uid.binary_uid);
        return uid;
	}

	bool operator== (const EternalUID& u1, const EternalUID& u2) {
        return u1.blob_uid == u2.blob_uid && u1.graph_uid == u2.graph_uid;
    }
	bool operator!= (const EternalUID& u1, const EternalUID& u2) { return !(u1 == u2); }
	std::ostream& operator<< (std::ostream& os, const EternalUID& x) {		
		os << "EternalUID(\"" << str(x) << "\")";
		return os;
	}

	bool operator== (const ZefRefUID& u1, const ZefRefUID& u2) {
        return (u1.blob_uid == u2.blob_uid
                && u1.graph_uid == u2.graph_uid
                && u1.tx_uid == u2.tx_uid);
    }
	bool operator!= (const ZefRefUID& u1, const ZefRefUID& u2) {
        return !(u1 == u2);
    }
	std::ostream& operator<< (std::ostream& os, const ZefRefUID& x) {		
		os << "ZefRefUID(\"" << str(x) << "\")";
		return os;
	}


    std::string ltrim(const std::string &s) {
        return std::regex_replace(s, std::regex("^\\s+"), std::string(""));
    }

    std::string rtrim(const std::string &s) {
        return std::regex_replace(s, std::regex("\\s+$"), std::string(""));
    }

    std::string trim(const std::string &s) {
        return ltrim(rtrim(s));
    }

    std::variant<std::monostate, BaseUID, EternalUID, ZefRefUID> to_uid(const std::string & maybe_uid_in) {
        auto is_hex = [](const char & c) { 
			if (c >= '0' && c <= '9')
                return true;
			if (c >= 'A' && c <= 'F')
				return true;
			if (c >= 'a' && c <= 'f')
				return true;
            return false;
        };

        std::string maybe_uid = trim(maybe_uid_in);

        if (maybe_uid.size() != 16
            && maybe_uid.size() != 32 
            && maybe_uid.size() != 48)
            return std::monostate{};

        // Check validity
        for (const char &c : maybe_uid) {
            if (!is_hex(c))
                return std::monostate{};
        };

        BaseUID a, b, c;
        a = BaseUID::from_hex(maybe_uid.substr(0,16));
        if(maybe_uid.size() > 16)
            b = BaseUID::from_hex(maybe_uid.substr(16,16));
        if(maybe_uid.size() > 32)
            c = BaseUID::from_hex(maybe_uid.substr(32,16));

        if(maybe_uid.size() == 48)
            return ZefRefUID(a, b, c);
        if(maybe_uid.size() == 32)
            return EternalUID(a, b);

        return a;
    }



    bool is_uid(const std::string & maybe_uid) {
        if (maybe_uid.size() != 32)
            return false;
        
		for (const char &c : maybe_uid) {
			if (c >= '0' && c <= '9')
                continue;
			if (c >= 'A' && c <= 'F')
				continue;
			if (c >= 'a' && c <= 'f')
				continue;
            return false;
		};

        return true;
    }

    bool is_BaseUID(const std::string & maybe_uid) {
        return std::holds_alternative<BaseUID>(to_uid(maybe_uid));
    }
    bool is_EternalUID(const std::string & maybe_uid) {
        return std::holds_alternative<EternalUID>(to_uid(maybe_uid));
    }
    bool is_ZefRefUID(const std::string & maybe_uid) {
        return std::holds_alternative<ZefRefUID>(to_uid(maybe_uid));
    }

    bool is_any_UID(const std::string & maybe_uid) {
        return !std::holds_alternative<std::monostate>(to_uid(maybe_uid));
    }


}
