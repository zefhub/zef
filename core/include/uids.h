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

#include "zefDB_utils.h"
#include <nlohmann/json.hpp>
using json = nlohmann::json;

namespace zefDB {

	// used to print out actual memory settings, typically for debugging.
	// Not to be used in performance critical code, the conversion to string 
	// input: byte  -> string of two characters
	LIBZEF_DLL_EXPORTED std::string to_hex(const unsigned char& c) ;

    namespace internals {
        // sets the uid at the target address in binary form from the base64 representation given by the string
        // Put a _ in front to deprecate this function for common usage.
        LIBZEF_DLL_EXPORTED void _from_hex(const std::string& uid, void * target_ptr);
        LIBZEF_DLL_EXPORTED void _from_hex(const unsigned char * src, void * target_ptr);
    }

	struct LIBZEF_DLL_EXPORTED BaseUID {
		static constexpr int size_in_bytes = 8;
		unsigned char binary_uid[size_in_bytes];

		BaseUID() : binary_uid{0,0,0,0,0,0,0,0} {};
        BaseUID(const BaseUID& to_copy) {
            std::memcpy(binary_uid, to_copy.binary_uid, size_in_bytes);
        };
		operator str() const;

        // Constructing a BaseUID from a string is kept separate from the normal
        // constructors to make it "even more explicit" than an explicit keyword
        // constructor.
        static BaseUID from_hex(const unsigned char * uid) {
            BaseUID base_uid{};
            internals::_from_hex(uid, (void*)base_uid.binary_uid);
            return base_uid;
        }
        static BaseUID from_hex(const std::string& uid) {
            static_assert(BaseUID::size_in_bytes == 8);
            assert(uid.size() == 16);
            return from_hex((unsigned char *)uid.c_str());
        }
        static BaseUID from_ptr(const void * ptr) {
            BaseUID base_uid{};
            std::memcpy(base_uid.binary_uid, ptr, size_in_bytes);
            return base_uid;
        };
        static BaseUID random();

        bool operator<(BaseUID const& rhs) const {
            for(int i = 0 ; i < size_in_bytes ; i++) {
                if(this->binary_uid[i] < rhs.binary_uid[i])
                    return true;
                if(this->binary_uid[i] > rhs.binary_uid[i])
                    return false;
            }
            return false;
        }
	};
	LIBZEF_DLL_EXPORTED bool operator== (const BaseUID& u1, const BaseUID& u2);
	LIBZEF_DLL_EXPORTED bool operator!= (const BaseUID& u1, const BaseUID& u2);
	LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const BaseUID& x);

    inline void to_json(json& j, const BaseUID & uid) {
        j = json{
            {"zef_type", "BaseUID"},
            {"uid", str(uid)}
        };
    }

	LIBZEF_DLL_EXPORTED BaseUID make_random_uid(void * in_place);
	LIBZEF_DLL_EXPORTED BaseUID make_random_uid();


    struct LIBZEF_DLL_EXPORTED EternalUID {
        BaseUID blob_uid;
        BaseUID graph_uid;

        EternalUID(BaseUID blob_uid, BaseUID graph_uid)
        : blob_uid(blob_uid), graph_uid(graph_uid) {}

		operator str() const { return str(blob_uid) + str(graph_uid); };

        bool operator<(EternalUID const& rhs) const {
            return std::tie(this->blob_uid, this->graph_uid) <
                std::tie(rhs.blob_uid, rhs.graph_uid);
        }
    };
	LIBZEF_DLL_EXPORTED bool operator== (const EternalUID& u1, const EternalUID& u2); 
	LIBZEF_DLL_EXPORTED bool operator!= (const EternalUID& u1, const EternalUID& u2); 
    LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const EternalUID& x);

    inline void to_json(json& j, const EternalUID & uid) {
        j = json{
            {"zef_type", "EternalUID"},
            {"blob_uid", uid.blob_uid},
            {"graph_uid", uid.graph_uid},
        };
    }

    struct LIBZEF_DLL_EXPORTED ZefRefUID {
        BaseUID blob_uid;
        BaseUID tx_uid;
        BaseUID graph_uid;

        ZefRefUID(BaseUID blob_uid, BaseUID tx_uid, BaseUID graph_uid)
        : blob_uid(blob_uid), tx_uid(tx_uid), graph_uid(graph_uid) {}
        ZefRefUID(EternalUID blob, EternalUID tx)
        : graph_uid(blob.graph_uid), blob_uid(blob.blob_uid), tx_uid(tx.blob_uid) {
            if(blob.graph_uid != tx.graph_uid)
                throw std::runtime_error("Blob and Tx need to belong to the same graph for a ZefRefUID");
        }

		operator str() const { return str(blob_uid) + str(tx_uid) + str(graph_uid); };

        bool operator<(ZefRefUID const& rhs) const {
            return std::tie(this->blob_uid, this->tx_uid, this->graph_uid) <
                std::tie(rhs.blob_uid, rhs.tx_uid, rhs.graph_uid);
        }
    };
	LIBZEF_DLL_EXPORTED bool operator== (const ZefRefUID& u1, const ZefRefUID& u2);
	LIBZEF_DLL_EXPORTED bool operator!= (const ZefRefUID& u1, const ZefRefUID& u2);
	LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const ZefRefUID& x);

    inline void to_json(json& j, const ZefRefUID & uid) {
        j = json{
            {"zef_type", "ZefRefUID"},
            {"blob_uid", uid.blob_uid},
            {"tx_uid", uid.tx_uid},
            {"graph_uid", uid.graph_uid},
        };
    }

    LIBZEF_DLL_EXPORTED std::variant<std::monostate,BaseUID,EternalUID,ZefRefUID> to_uid(const std::string & maybe_uid);
    LIBZEF_DLL_EXPORTED bool is_any_UID(const std::string & maybe_uid) ;
    LIBZEF_DLL_EXPORTED bool is_BaseUID(const std::string & maybe_uid) ;
    LIBZEF_DLL_EXPORTED bool is_EternalUID(const std::string & maybe_uid) ;
    LIBZEF_DLL_EXPORTED bool is_ZefRefUID(const std::string & maybe_uid) ;



    inline std::string generate_random_task_uid() {
        return str(make_random_uid());
    }

}

namespace std {
    template<>
    struct hash<zefDB::BaseUID> {
        std::size_t operator() (const zefDB::BaseUID& u) const { 
            static_assert(sizeof(std::size_t) == zefDB::BaseUID::size_in_bytes);
            return (* (const std::size_t*)(u.binary_uid));
        }
    };

    template<>
    struct hash<zefDB::EternalUID> {
        std::size_t operator() (const zefDB::EternalUID& u) const { 
            size_t s = zefDB::hash_char_array("EternalUID");
            zefDB::hash_combine(s, u.graph_uid);
            zefDB::hash_combine(s, u.blob_uid);
            return s;
        }
    };

    template<>
    struct hash<zefDB::ZefRefUID> {
        std::size_t operator() (const zefDB::ZefRefUID& u) const { 
            size_t s = zefDB::hash_char_array("ZefRefUID");
            zefDB::hash_combine(s, u.graph_uid);
            zefDB::hash_combine(s, u.blob_uid);
            zefDB::hash_combine(s, u.tx_uid);
            return s;
        }
    };
}
