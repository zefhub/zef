# Copyright 2022 Synchronous Technologies Pte Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ..core import *
from ..ops import *

sample_json = {
    "auth": {
        "JWKURL": "https://www.googleapis.com/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com",
        "Header": "X-Auth-Token",
        "Audience": "ikura-9437c",
    },
    
    "types": {
        "User": {
            "firebaseID": {"type": "String",
                           "required": True,
                           "unique": True},

            "email": {"type": "String",
                      "required": True,
                      "unique": True},
            
            "givenName": {"type": "String"},
            
            "familyName": {"type": "String"},
            
            "phone": {"type": "String"},
            
            "birthday": {"type": "DateTime"},

            "active": {"type": "Boolean",
                       "search": True},
            
            "transactions": {"type": "Transaction",
                             "list": True,
                             "relation": (ET.Transaction, RT.User, ET.User),
                             "incoming": True},
            
            "categories": {"type": "Category",
                             "list": True,
                             "relation": (ET.Category, RT.User, ET.User),
                             "incoming": True},
        },
        
        "Transaction": {
            # Allow query only if user id matches graph data
            "_AllowQuery": "z >> RT.User >> RT.FirebaseID | value | equals[info.context['auth']['user_id']]",
            # Allow add only if user id matches incoming payload
            # This is currently available - could make this better (i.e. the same style as AllowQuery)
            "_AllowAdd": "params['input']['user']['id'] == info.context['auth']['user_id']",
            # Allow update only if user id matches both the graph data and the incoming payload (or is not overwritten in the incoming payload)
            "_AllowUpdate": """(Z >> RT.User >> RT.FirebaseID | value | equals[info.context['auth']['user_id']] | collect)
            and
            (get_path(params, ['input', 'user', 'id'], None) in [None, info.context['auth']['user_id']])
            """,
            # Note: AllowDelete falls back to AllowUpdate, with an empty payload
            
            "user": {"type": "User"},
            
            "type": {"type": "TransactionType",
                     "search": True},
            
            "amount": {"type": "Float",
                       "relation": (ET.Transaction, RT.Amount, AET.Float),
                       "search": True},
            
            "when": {"type": "DateTime",
                     "search": True},
            
            "category": {"type": "Category", "relation": (ET.Transaction, RT.Category, ET.Category)},
            
            "comment": {"type": "String",
                        "relation": (ET.Transaction, RT.Comment, AET.String)},
        },

        "Category": {
            # # Allow query only if user id matches graph data
            # "_AllowQuery": "z >> RT.User >> RT.ID | value | equals[info.context['auth']['user_id']",
            # # Allow add only if user id matches incoming payload
            # # This is currently available - could make this better (i.e. the same style as AllowQuery)
            # "_AllowAdd": "params['input']['user']['id'] == info.context['auth']['user_id']",
            # # Allow update only if user id matches both the graph data and the incoming payload (or is not overwritten in the incoming payload)
            # "_AllowUpdate": """(Z >> RT.User >> RT.ID | value | equals[info.context['auth']['user_id'] | collect
            # and
            # (get_path(params, ['input', 'user', 'id'], None) in [None, info.context['auth']['user_id']])
            # """,
            # # Note: AllowDelete falls back to AllowUpdate, with an empty payload
            
            "type": {"type": "CategoryType",
                     "search": True},

            "user": {"type": "User"},
            
            "title": {"type": "String",
                      "required": True},

            "titleLangEn": {"type": "String"},
            "titleLangJa": {"type": "String"},
            "description": {"type": "String"},
            "descriptionLangEn": {"type": "String"},
            "descriptionLangJa": {"type": "String"},
            "color": {"type": "String"},
            "icon": {"type": "String"},
            
            "createdAt": {"type": "DateTime",
                          "required": True},
            
            "transactions": {"type": "Transaction",
                             "list": True,
                             "relation": (ET.Transaction, RT.Category, ET.Category),
                             "incoming": True},
        },
    },

    "enums": {
        "TransactionType": [
            "EXPENSE",
            "INCOME"
        ],

        "CategoryType": [
            "DEFAULT",
            "PRIVATE"
        ],
    },
}