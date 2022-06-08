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

#include <string>

namespace zefDB {
    LIBZEF_DLL_EXPORTED std::string get_firebase_refresh_token_email(std::string key_string);
    LIBZEF_DLL_EXPORTED std::string get_firebase_refresh_token_email(std::string email, std::string password);

    LIBZEF_DLL_EXPORTED std::string get_firebase_token_refresh_token(std::string refresh_token);
}
