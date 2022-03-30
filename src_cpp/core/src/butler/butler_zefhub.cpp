#include "butler/butler_zefhub.h"

namespace zefDB {
    namespace Butler {
        
        void add_entity_type(const token_value_t & indx, const std::string & name) {
            if (!butler_is_master)
                throw std::runtime_error("Shouldn't be calling this function in normal execution.");

            update_bidirection_name_map(global_token_store().ETs, indx, name);
        }

        void add_relation_type(const token_value_t & indx, const std::string & name) {
            if (!butler_is_master)
                throw std::runtime_error("Shouldn't be calling this function in normal execution.");

            update_bidirection_name_map(global_token_store().RTs, indx, name);
        }

        void add_enum_type(const enum_indx & indx, const std::string& name) {
            if (!butler_is_master)
                throw std::runtime_error("Shouldn't be calling this function in normal execution.");

            update_zef_enum_bidirectional_map(global_token_store().ENs, indx, name);
        }

        void add_keyword(const token_value_t & indx, const std::string & name) {
            if (!butler_is_master)
                throw std::runtime_error("Shouldn't be calling this function in normal execution.");

            update_bidirection_name_map(global_token_store().KWs, indx, name);
        }
    }
}
