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
        
type User
  @auth(
    add: "info.context | get_in[('auth', 'admin')][False]"
    query: "F.Email | equals[info.context | get_in[('auth', 'email')][None] | collect]"
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
def dynamicHook(z, info):
    return "dynamic"
"""

        from zef.gql.simplegql.main import create_schema_graph
        from zef.gql.simplegql.server import start_server
        self.port = 4991
        root = create_schema_graph(schema_gql, hooks_string)
        g_data = Graph()
        self.server_uuid = start_server(root, g_data, self.port, "localhost", logging=False)

    def tearDown(self):
        Effect({
            "type": FX.HTTP.StopServer,
            "server_uuid": self.server_uuid,
        }) | run

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
            self.assertIn(msg, r.json()["errors"][0]["message"])
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

        if False:
            # This needs to be implemented!
            r = do_query(jwt_user1, 'mutation { updateTransaction(input: {filter: {id: "' + trans_id + '"}, set: {user: {id: "' + user2_id + '"}}}) { transaction { id } } }')
            assert_error_with(r, "TODO")

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
        self.assertEquals(len(r.json()["data"]["queryTransaction"]), 1)

        r = do_query(jwt_user1, 'query { queryTransaction(filter: {user: {testEnum: {eq: ABC}, testDynamic: {eq: "dynamic"}}}) { id } }')
        assert_no_error(r)
        self.assertEquals(len(r.json()["data"]["queryTransaction"]), 1)

        r = do_query(jwt_user1, 'query { queryTransaction(filter: {user: {testEnum: {eq: DEF}}}) { id } }')
        assert_no_error(r)
        self.assertEquals(len(r.json()["data"]["queryTransaction"]), 0)

        r = do_query(jwt_user1, 'query { queryTransaction(filter: {amount: {lt: 1}}) { id } }')
        assert_no_error(r)
        self.assertEquals(len(r.json()["data"]["queryTransaction"]), 0)

        r = do_query(jwt_user1, 'query { queryTransaction(filter: {amount: {gt: 1}}) { id } }')
        assert_no_error(r)
        self.assertEquals(len(r.json()["data"]["queryTransaction"]), 1)

        r = do_query(jwt_user1, 'query { queryUser(filter: {transactions: {any: {amount: {gt: 1}}}}) { id transactions(filter: {amount: {lt: 1}}) { id } } }')
        assert_no_error(r)
        self.assertEquals(len(r.json()["data"]["queryUser"]), 1)
        self.assertEquals(len(r.json()["data"]["queryUser"][0]["transactions"]), 0)


if __name__ == '__main__':
    unittest.main()
