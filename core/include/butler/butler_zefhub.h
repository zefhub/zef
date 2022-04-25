#pragma once

#include "butler/butler.h"
 
// These are low-level functions necessary for the functionality of zefhub.

namespace zefDB {

    namespace Butler {
        LIBZEF_DLL_EXPORTED void add_entity_type(const token_value_t & indx, const std::string & name);
        LIBZEF_DLL_EXPORTED void add_relation_type(const token_value_t & indx, const std::string & name);
        LIBZEF_DLL_EXPORTED void add_enum_type(const enum_indx & indx, const std::string& name);
        LIBZEF_DLL_EXPORTED void add_keyword(const token_value_t & indx, const std::string & name);
    }
}
