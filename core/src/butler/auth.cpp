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
#include <chrono>

#include <curl/curl.h>

#include "nlohmann/json.hpp"
using json = nlohmann::json;

#include <jwt-cpp/jwt.h>
#include "jwt-cpp/traits/nlohmann-json/traits.h"

#include "zefDB_utils.h"
#include "zwitch.h"

#include "butler/auth.h"

namespace zefDB {

    // Curl has all kinds of warnings about thread safety. We try and avoid
    // these issues by setting curl up as early as possible (on load of library)
    //
    // Note that curl easy handles need to be thread local so they don't go into
    // CurlGlobal.
    struct CurlGlobal {
        CurlGlobal() {
            curl_global_init(CURL_GLOBAL_ALL);
        }

        ~CurlGlobal() {
            curl_global_cleanup();
        }
    };

    static CurlGlobal curl_global;

    CURL* initialise_curl() {
        thread_local CURL* easy_handle;
        thread_local bool done = false;
        if(!done) {
            easy_handle = curl_easy_init();
            // Suppressing warnings?
            auto temp = easy_handle;
            on_thread_exit("curl_cleanup", [temp]() {
                curl_easy_cleanup(easy_handle);
            });
            done = true;
        }
        curl_easy_reset(easy_handle);
        return easy_handle;
    }

    size_t _curl_write_data(void * curl_buffer, size_t size, size_t nmemb, void * userp) {
        std::string & buf = *(std::string*)userp;
        assert(size == 1);

        buf += std::string((char*)curl_buffer, nmemb);
        return nmemb;
    };


    json curl_json_req(std::string url, json j) {
        auto handle = initialise_curl();
        curl_easy_setopt(handle, CURLOPT_URL, url.c_str());
        if(check_env_bool("ZEF_DEVELOPER_CURL_DEBUG"))
            curl_easy_setopt(handle, CURLOPT_VERBOSE, 1L);

        // Reading CA details which should be provided by python.
        // TODO: This needs to go in a block that is only run if we were compiled as a bundled library and then only if we are using the same as the python core (openssl?)
        char * env = std::getenv("LIBZEF_CA_BUNDLE");
        if(env != nullptr && env[0] != '\0')
            curl_easy_setopt(handle, CURLOPT_CAINFO, env);
        env = std::getenv("LIBZEF_CA_PATH");
        if(env != nullptr && env[0] != '\0')
            curl_easy_setopt(handle, CURLOPT_CAPATH, env);

        curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, _curl_write_data);
        std::string buf;
        curl_easy_setopt(handle, CURLOPT_WRITEDATA, &buf);
        std::string content = j.dump();
        const char * content_s = content.c_str();
        curl_easy_setopt(handle, CURLOPT_POSTFIELDS, content_s);
        curl_easy_setopt(handle, CURLOPT_POSTFIELDSIZE, content.size());
        struct curl_slist * headers = NULL;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(handle, CURLOPT_HTTPHEADER, headers);

        char errbuf[CURL_ERROR_SIZE];
        errbuf[0] = 0;
        curl_easy_setopt(handle, CURLOPT_ERRORBUFFER, errbuf);
        CURLcode res = curl_easy_perform(handle);

        curl_slist_free_all(headers);

        if(res != CURLE_OK) {
            std::cerr << "Curl error! Code: " << res << std::endl;
            std::cerr << "Simple explanation: " << curl_easy_strerror(res) << std::endl;
            std::cerr << "Detailed error: " << std::string(errbuf) << std::endl;
            // TODO: Probably should retry here.
            throw std::runtime_error("Problem in curl getting token");
        }

        if (!json::accept(buf))
            throw std::runtime_error("Firebase response is not valid json");
        return json::parse(buf);
    }


    using clock = std::chrono::steady_clock;
    using time = std::chrono::time_point<clock>;

    std::string last_refresh_token = "";
    std::string last_email = "";
    std::string last_token = "";
    time token_expire_time = clock::now();

    bool using_private_key() {
        char * env = getenv("ZEFDB_PRIVATE_KEY");
        return (env != NULL && env[0] != '\0');
    }

    std::string get_private_token(std::string auth_id) {
        assert(using_private_key());

        char * env = getenv("ZEFDB_PRIVATE_KEY");
        std::string private_key(env);

        std::string token = jwt::create<jwt::traits::nlohmann_json>()
            .set_issuer("local")
            .set_type("JWT")
            .set_audience("zefhub-io")
            .set_payload_claim("sub", auth_id)
            .set_issued_at(jwt::date::clock::now())
            .set_expires_at(jwt::date::clock::now() + std::chrono::minutes(60))
            .sign(jwt::algorithm::hs256{private_key});

        return token;
    }

    std::string get_firebase_refresh_token_email(std::string key_string) {
        if(using_private_key()) {
            if(zwitch.developer_output())
                std::cerr << "Going to generate JWT from private key ourselves." << std::endl;
            return get_private_token(key_string);
        }
        int colon = key_string.find(':');
        std::string auth_username = key_string.substr(0,colon);
        std::string key = key_string.substr(colon+1);
        return get_firebase_refresh_token_email(auth_username, key);
    }
    std::string get_firebase_refresh_token_email(std::string email, std::string password) {
        time start = clock::now();

        if(last_email == email && last_refresh_token != "") {
            if(zwitch.developer_output())
                std::cerr << "Auth token expire - now is " << (token_expire_time - clock::now()) / std::chrono::seconds(1) << " secs" <<  std::endl;
            if(token_expire_time > clock::now())
                return last_refresh_token;
        }

        debug_time_print("start auth");
        if(zwitch.zefhub_communication_output())
            std::cerr << "Trying to auth with email: '" << email << "'" << std::endl;

        const std::string api_key{"AIzaSyD3kLQjN2yDch3ptct-xcFfGewCzFFE1mM"}; 
        std::string url("https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword");
        url += "?key=";
        url += api_key;

        json j{
            {"email", email},
            {"password", password},
            {"returnSecureToken", true}
        };

        json response = curl_json_req(url, j);
        
        if(response.contains("expiresIn")) {
            int expire_time = std::stoi(response["expiresIn"].get<std::string>());
            token_expire_time = clock::now() + std::chrono::seconds{expire_time};
            if(zwitch.developer_output())
                std::cerr << "Auth token expires in " << (token_expire_time - clock::now()) / std::chrono::seconds{1} << " secs" << std::endl;
        }

        if(!response.contains("idToken"))
            throw std::runtime_error("No token in response to auth request using email/password");
        if(!response.contains("refreshToken"))
            throw std::runtime_error("No token in response to auth request using email/password");

        last_refresh_token = response["refreshToken"].get<std::string>();
        last_token = response["idToken"].get<std::string>();
        last_email = email;

        debug_time_print("finished auth");
        if(zwitch.developer_output())
            std::cerr << "Time to receive auth token: " << double(std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - start).count())/1000 <<  " s" << std::endl;

        return last_refresh_token;
    }

    std::string get_firebase_token_refresh_token(std::string refresh_token) {
        if(using_private_key())
            return refresh_token;

        time start = clock::now();

        if(refresh_token == "")
            return "";

        if(last_refresh_token == refresh_token && last_token != "") {
            if(zwitch.developer_output())
                std::cerr << "Auth token expire - now is " << (token_expire_time - clock::now()) / std::chrono::seconds(1) << " secs" <<  std::endl;
            if(token_expire_time > clock::now())
                return last_token;
        }

        debug_time_print("start auth");
        if(zwitch.zefhub_communication_output())
            std::cerr << "Trying to auth with refresh token" << std::endl;

        const std::string api_key{"AIzaSyD3kLQjN2yDch3ptct-xcFfGewCzFFE1mM"}; 
        std::string url("https://identitytoolkit.googleapis.com/v1/token");
        url += "?key=";
        url += api_key;

        json j{
            {"grant_type", "refresh_token"},
            {"refresh_token", refresh_token},
        };

        json response = curl_json_req(url, j);

        if(response.contains("expires_in")) {
            int expire_time = std::stoi(response["expires_in"].get<std::string>());
            token_expire_time = clock::now() + std::chrono::seconds{expire_time};
            if(zwitch.developer_output())
                std::cerr << "Auth token expires in " << (token_expire_time - clock::now()) / std::chrono::seconds{1} << " secs" << std::endl;
        }

        if(!response.contains("id_token"))
            throw std::runtime_error("No token in response to auth request using request token");

        last_token = response["id_token"].get<std::string>();
        last_refresh_token = refresh_token;

        using traits = jwt::traits::nlohmann_json;
        auto decoded = jwt::decode<traits>(last_token);
        auto email = decoded.get_payload_claim("email").to_json();
        auto firebase_field = decoded.get_payload_claim("firebase").to_json();
        std::string provider = "";
        if(firebase_field.contains("sign_in_provider"))
            provider = " through provider: " + firebase_field["sign_in_provider"].get<std::string>();

        if(zwitch.zefhub_communication_output())
            std::cout << "Authenticating with email: " << email.get<std::string>() << provider << std::endl;

        debug_time_print("finished auth");
        if(zwitch.developer_output())
            std::cerr << "Time to receive auth token: " << double(std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - start).count())/1000 <<  " s" << std::endl;
        return last_token;
    }
}
