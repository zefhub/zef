#pragma once

#include <string>

#include "export_statement.h"

namespace zefDB {
    LIBZEF_DLL_EXPORTED std::string get_firebase_refresh_token_email(std::string key_string);
    LIBZEF_DLL_EXPORTED std::string get_firebase_refresh_token_email(std::string email, std::string password);

    LIBZEF_DLL_EXPORTED std::string get_firebase_token_refresh_token(std::string refresh_token);
}
