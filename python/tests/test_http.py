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
    def test_http_server(self):
        from zef.core.fx.http import send_response, permit_cors, middleware, middleware_worker, fallback_not_found, route
        port = 4990
        payload = b"successful test"
        eff = {
            'type': FX.HTTP.StartServer,
            # We hope that this is not taken by anything else for this test...
            'port': port,
            'pipe_into': (map[middleware_worker[permit_cors,
                                                route["/fixed-url"][lambda query: {"type": FX.HTTP.SendResponse,
                                                                                   "server_uuid": query["server_uuid"],
                                                                                   "request_id": query["request_id"],
                                                                                   "response": "Fixed"}],
                                                route["/test-url"][lambda query: {**query, "response_body": payload}],
                                                fallback_not_found,
                                                send_response]]
                          | subscribe[run]),
            'bind_address': "localhost",
        }

        eff_resp = eff | run

        import requests
        r = requests.get(f"http://localhost:{port}/test-url")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.content, payload)

        r = requests.get(f"http://localhost:{port}/fixed-url")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.content, b"Fixed")

        r = requests.get(f"http://localhost:{port}")
        self.assertEqual(r.status_code, 404)

        {
            "type": FX.HTTP.StopServer,
            "server_uuid": eff_resp["server_uuid"],
        } | run

if __name__ == '__main__':
    unittest.main()
