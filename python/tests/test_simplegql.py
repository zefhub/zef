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

import unittest  # pytest takes ages to run anything as soon as anything from zef is imported
from zef import *
from zef.ops import *

class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.key = "asdf"
        schema_gql = """
# Zef.SchemaVersion: v1
# Zef.Authentication: {"Algo": "HS256", "VerificationKey": \"""" + self.key + """\", "Audience": "test", "Header": "X-Auth-Token"}
# Zef.Route: {"route": "/customroute", "hook": "customRoute"}
        
type User
  @auth(
    add: "info.context | get_in[('auth', 'admin')][False]"
    query: "z | F.Email | equals[info.context | get_in[('auth', 'email')][None] | collect]"
  )
  @upfetch(field: "email")
  @hook(onCreate: "userCreate")
 {
  email: String! @unique @search
  transactions: [Transaction]
    @incoming
    @relation(source: "Transaction", rt: "User", target: "User")
    @search

  testString: String
  testEnum: TestEnum! @search
  testOut: Transaction
  testTime: DateTime
  testDynamic: String @dynamic(hook: "dynamicHook") @search
}

type Transaction
  @auth(
    query: "auth_field('user', 'query')"
  ) {
  user: User @relation(rt: "User") @search
  amount: Int @search
  date: DateTime @search
  category: Category
}

type Category {
  transactions: [Transaction] @relation(rt: "Category") @incoming
  name: String @unique

  testListScalar: [String]
  testListEntity: [Simple]
}

type Simple {
  name: String
}

enum TestEnum {
    ABC
    DEF
}
"""
        hooks_string = """
from zef import *
from zef.ops import *

@func(g)
def userCreate(z):
    z | set_field[RT.HookDidThis][True] | Graph(z) | run

@func(g)
def dynamicHook(z, auth, z_field, context):
    return "dynamic"

@func(g)
def customRoute(req, context):
    return {**req, "response_status": 400}
"""

        from zef.graphql.simplegql.main import create_schema_graph
        from zef.graphql.simplegql.server2 import start_server
        self.port = 4991
        root = create_schema_graph(schema_gql, hooks_string)
        g_data = Graph()
        self.server_uuid = start_server(root, g_data,
                                        port=self.port,
                                        bind_address="localhost",
                                        logging=False,
                                        debug_level=-1)

    def tearDown(self):
        {
            "type": FX.HTTP.StopServer,
            "server_uuid": self.server_uuid,
        } | run

    def test_simplegql(self):
        import jwt
        jwt_user_no_aud = jwt.encode({"email": "user1"}, self.key, "HS256")
        jwt_user1 = jwt.encode({"email": "user1", "aud": "test"}, self.key, "HS256")
        jwt_user2 = jwt.encode({"email": "user2", "aud": "test"}, self.key, "HS256")
        jwt_admin = jwt.encode({"email": "", "admin": True, "aud": "test"}, self.key, "HS256")

        import time
        old_token = jwt.encode({"email": "", "admin": True, "aud": "test", "exp": time.time()-1}, self.key, "HS256")

        import requests
        url = f"http://localhost:{self.port}/gql"

        boring_query = 'query { queryUser { id } }'
        def do_query(token, query):
            return requests.post(url, headers={"X-Auth-Token": "Bearer " + token},
                                 json={"query": query})
        def assert_error_with(r, msg):
            self.assertEqual(r.status_code, 200)
            self.assertIn("errors", r.json())
            print("NEED TO FIX UP SIMPLEGQL ERROR CHECK")
            # self.assertIn(msg, r.json()["errors"][0]["message"])
        def assert_no_error(r):
            self.assertEqual(r.status_code, 200)
            self.assertNotIn("errors", r.json())

        # Test aud required
        r = do_query(jwt_user_no_aud, boring_query)
        self.assertEqual(r.status_code, 400)
        r = do_query(jwt_user1, boring_query)
        assert_no_error(r)
        self.assertEqual(r.json()["data"]["queryUser"], [])

        # Test expired token
        r = do_query(old_token, boring_query)
        self.assertEqual(r.status_code, 400)

        # Test no token in non-public mode
        r = do_query("", boring_query)
        self.assertEqual(r.status_code, 400)

        # Test no admin rights
        r = do_query(jwt_user1, 'mutation { addUser(input: {email: "user1", testEnum: ABC, testString: "test"}) { user { id } count } }')
        assert_error_with(r, "Add auth check")

        r = do_query(jwt_admin, 'mutation { addUser(input: {email: "user1", testEnum: ABC}) { user { id } count } }')
        assert_no_error(r)
        user1_id = r.json()["data"]["addUser"]["user"][0]["id"]

        # Test required input
        r = do_query(jwt_admin, 'mutation { addUser(input: {email: "user2"}) { user { id } count } }')
        assert_error_with(r, "Required field")

        # Test getting existing user from correct/incorrect account
        r = do_query(jwt_user1, 'query { getUser(id: "' + user1_id + '") { id } }')
        assert_no_error(r)

        r = do_query(jwt_user2, 'query { getUser(id: "' + user1_id + '") { id } }')
        assert_no_error(r)
        self.assertIsNone(r.json()["data"]["getUser"])

        # Test adding transaction for auth_field checking
        r = do_query(jwt_user1, 'mutation { addTransaction(input: {user: {id: "' + user1_id + '"}, amount: 50, date: "' + str(now()) + '"}) { transaction { id } } }')
        assert_no_error(r)
        trans_id = r.json()["data"]["addTransaction"]["transaction"][0]["id"]

        r = do_query(jwt_user2, 'query { getTransaction(id: "' + trans_id + '") { id } }')
        assert_no_error(r)
        self.assertIsNone(r.json()["data"]["getTransaction"])

        # Test not allowing to change username
        r = do_query(jwt_user1, 'mutation { updateUser(input: {filter: {id: "' + user1_id + '"}, set: {email: "user2"}}) { user { id } } }')
        assert_error_with(r, "Post-update auth check")

        r = do_query(jwt_admin, 'mutation { addUser(input: {email: "user2", testEnum: ABC}) { user { id } count } }')
        assert_no_error(r)
        user2_id = r.json()["data"]["addUser"]["user"][0]["id"]

        r = do_query(jwt_user1, 'mutation { updateTransaction(input: {filter: {id: "' + trans_id + '"}, set: {user: {id: "' + user2_id + '"}}}) { transaction { id } } }')
        assert_error_with(r, "Unable to find entity")

        # Test updating and removing things
        r = do_query(jwt_user1, '''mutation {
updateUser(input: {filter: {id: "''' + user1_id + '''"},
set: {
testTime: "12:00:00"
},
remove: {
testString: ""
}
}) { user { id email transactions { id } testString testEnum testOut testTime testDynamic } } }''')

        # Test querying with filters
        r = do_query(jwt_user1, 'query { queryTransaction(filter: {id: "' + trans_id + '"} ) { id } }')
        assert_no_error(r)
        self.assertEqual(len(r.json()["data"]["queryTransaction"]), 1)

        r = do_query(jwt_user1, 'query { queryTransaction(filter: {user: {testEnum: {eq: ABC}, testDynamic: {eq: "dynamic"}}}) { id } }')
        assert_no_error(r)
        self.assertEqual(len(r.json()["data"]["queryTransaction"]), 1)

        r = do_query(jwt_user1, 'query { queryTransaction(filter: {user: {testEnum: {eq: DEF}}}) { id } }')
        assert_no_error(r)
        self.assertEqual(len(r.json()["data"]["queryTransaction"]), 0)

        r = do_query(jwt_user1, 'query { queryTransaction(filter: {amount: {lt: 1}}) { id } }')
        assert_no_error(r)
        self.assertEqual(len(r.json()["data"]["queryTransaction"]), 0)

        r = do_query(jwt_user1, 'query { queryTransaction(filter: {amount: {gt: 1}}) { id } }')
        assert_no_error(r)
        self.assertEqual(len(r.json()["data"]["queryTransaction"]), 1)

        r = do_query(jwt_user1, 'query { queryUser(filter: {transactions: {any: {amount: {gt: 1}}}}) { id transactions(filter: {amount: {lt: 1}}) { id } } }')
        assert_no_error(r)
        self.assertEqual(len(r.json()["data"]["queryUser"]), 1)
        self.assertEqual(len(r.json()["data"]["queryUser"][0]["transactions"]), 0)

        # Adding Category explicitly and updating transaction
        r = do_query(jwt_user1, 'mutation { addCategory(input: {name: "explicit create"}) { category { id } } }')
        assert_no_error(r)
        cat_id = r.json()["data"]["addCategory"]["category"][0]["id"]

        r = do_query(jwt_user1, 'mutation { updateTransaction(input: {filter: {id: "' + trans_id + '"}, set: {category: {id: "' + cat_id + '"} } }) { count } }')
        assert_no_error(r)

        # Adding category during update to transaction
        r = do_query(jwt_user1, 'mutation { updateTransaction(input: {filter: {id: "' + trans_id + '"}, set: {category: {name: "on the fly"} } }) { transaction { category { id name } } } }')
        assert_no_error(r)
        cat_data = r.json()["data"]["updateTransaction"]["transaction"][0]["category"]
        self.assertNotEqual(cat_data["id"], cat_id)
        self.assertEqual(cat_data["name"], "on the fly")

        # Unique checks
        r = do_query(jwt_user1, 'mutation { addCategory(input: {name: "soon to be duplicated"}) { category { id } } }')
        assert_no_error(r)

        r = do_query(jwt_user1, 'mutation { updateCategory(input: {filter: {id: "' + cat_id + '"}, set: {name: "soon to be duplicated"} }) { category { id name } } }')
        assert_error_with(r, "Non-unique values")


        # List creation
        r = do_query(jwt_user1, 'mutation { addCategory(input: {testListScalar: ["a", "b", "c"], testListEntity: [{name: "a"}, {name: "b"}]}) { category { id testListScalar testListEntity { name } } } }')
        assert_no_error(r)
        cat_data = r.json()["data"]["addCategory"]["category"][0]
        cat_list_id = cat_data["id"]
        self.assertSetEqual(set(cat_data["testListScalar"]), {"a", "b", "c"})
        expected = [{"name": "a"}, {"name": "b"}]
        for x in cat_data["testListEntity"]:
            self.assertIn(x, expected)
        for x in expected:
            self.assertIn(x, cat_data["testListEntity"])

        # TODO: List set
        # TODO: List manipulation
        # TODO: List remove


        # Custom routes
        r = requests.get(f"http://localhost:{self.port}/customroute")
        self.assertEqual(r.status_code, 400)



if __name__ == '__main__':
    unittest.main()
