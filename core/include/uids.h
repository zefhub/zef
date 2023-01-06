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
    const wchar_t DB_symbol = L'㏈';
    // const std::string DB_symbol_s(DB_symbol);

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
        std::string to_base64(bool prefixed=true) const {
            std::string s = base64_encode(binary_uid, 8);
            // Remove the final '=' that will always be there.
            if(s.size() != 12)
                throw std::runtime_error("Some odd length problem with the base64 of a BaseUID");
            s = s.substr(0, 11);
            if(prefixed) {
                auto DB_symbol_s = get_DB_symbol_s();
                return DB_symbol_s + s;
            } else {
                return s;
            }
        }

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
            if(uid.size() != 16)
                throw std::runtime_error("UID string is not of length 16");
            return from_hex((unsigned char *)uid.c_str());
        }
        static BaseUID from_ptr(const void * ptr) {
            BaseUID base_uid{};
            std::memcpy(base_uid.binary_uid, ptr, size_in_bytes);
            return base_uid;
        }
        static std::string get_DB_symbol_s() {
            static bool prepared = false;
            static std::string s;
            if(prepared)
                return s;
            std::mbstate_t state {};
            // For some reason MB_CUR_MAX is not a constant on windows.
            char * mb = new char[MB_CUR_MAX];
            std::size_t ret = std::wcrtomb(mb, DB_symbol, &state);
            s = std::string(mb, ret) + "_";
            prepared = true;
            delete[] mb;
            return s;
        }
        static BaseUID random();
        static BaseUID from_base64(std::string uid, bool prefixed=true) {
            if(prefixed) {
                auto DB_symbol_s = get_DB_symbol_s();
                if(uid.size() != 11 + DB_symbol_s.size())
                    throw std::runtime_error("base64 encoding of uid is the wrong length");
                if(uid.find(DB_symbol_s) != 0)
                    throw std::runtime_error("base64 encoding does not have required prefix");
                uid = uid.substr(DB_symbol_s.size());
            } else {
                if(uid.size() != 11)
                    throw std::runtime_error("base64 encoding of uid is the wrong length");
            }
            BaseUID base_uid{};
            std::string decoded = base64_decode(uid);
            std::memcpy(base_uid.binary_uid, decoded.data(), size_in_bytes);
            return base_uid;
        }

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

        std::string to_base64() const {
            return blob_uid.to_base64(true) + graph_uid.to_base64(false);
        }
        static EternalUID from_base64(std::string uid) {
            auto DB_symbol_s = BaseUID::get_DB_symbol_s();
            if(uid.size() != 22 + DB_symbol_s.size())
                throw std::runtime_error("base64 encoding of EternalUID is the wrong length");
            if(uid.find(DB_symbol_s) != 0)
                throw std::runtime_error("base64 encoding does not have required prefix");
            uid = uid.substr(DB_symbol_s.size());
            return EternalUID(
                              BaseUID::from_base64(uid.substr(0,11), false),
                              BaseUID::from_base64(uid.substr(11,11), false)
            );
        }

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

        std::string to_base64() const {
            return blob_uid.to_base64(true) + tx_uid.to_base64(false) + graph_uid.to_base64(false);
        }

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
