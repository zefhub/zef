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

namespace zefDB {
    namespace Butler {

        // This file contains the implementation of the template functions, that
        // I want to keep out of the butler.h file itself.


        template<class T>
        T Butler::wait_on_zefhub_message(json & j, const std::vector<std::string> & rest, double timeout, bool throw_on_failure, bool chunked, std::optional<activity_callback_t> activity_callback) {
            auto response = wait_on_zefhub_message_any(j, rest, timeout, throw_on_failure, chunked, activity_callback);

            return std::visit(overloaded {
                    [](const T & response) { return response; },
                    [](const auto & response) -> T {
                        if constexpr(std::is_constructible_v<T, decltype(response)>) {
                            return T(response);
                        } else {
                            std::string name = typeid(response).name();
                            std::string desired = typeid(T).name();
                            std::cerr << "Response from ZefHub is not of the right type. " + name + " not " + desired + "." << std::endl;
                            throw std::runtime_error("Response from ZefHub is not of the right type. " + name + " not " + desired + ".");
                        }
                    }
                }, response);
        }

        template <class T>
        T Butler::msg_push(Request && content) {
            auto future = msg_push_internal(std::move(content), false);
            auto response = future.get();

            if(!std::holds_alternative<T>(response)) {
                std::string name = std::visit([&](auto & x) { return typeid(x).name(); },
                                              response);
                std::string desired = typeid(T).name();
                throw std::runtime_error("Response is not of the right type. " + name + " not " + desired + ".");
            }

            return std::get<T>(response);
        }

        template <class T>
        T Butler::msg_push_timeout(Request && content, double timeout, bool ignore_closed) {
            std::string msg_type = std::visit([](auto & content) {return msgqueue_to_str(content);}, content);
            auto future = msg_push_internal(std::move(content), ignore_closed);
            std::future_status status;
            // This needs to be changed to timeout on the butler side instead of here.
            // if(timeout.value == 0)
            //     status = std::future_status::ready;
            // else
            //     status = future.wait_for(std::chrono::duration<double>(timeout.value));
            Messages::Response response;
            try {
                response = future.get();
            } catch(const std::exception & e) {
                // std::cerr << "Exception: " << e.what() << std::endl;
                throw std::runtime_error("Got unexpected butler error when processing message of type: " + msg_type + "\nException was: " + e.what());
            }

            return std::visit(overloaded {
                    [](const T & response) { return response; },
                    [](const auto & response) -> T {
                        if constexpr(std::is_constructible_v<T, decltype(response)>) {
                            return T(response);
                        } else {
                            std::string name = typeid(response).name();
                            std::string desired = typeid(T).name();
                            std::cerr << "Response from ZefHub is not of the right type. " + name + " not " + desired + "." << std::endl;
                            throw std::runtime_error("Response from ZefHub is not of the right type. " + name + " not " + desired + ".");
                        }
                    }
                }, response);

        }
    }
}
