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


#                            ____                           _        _    _                   _____                      _                           
#                           / ___|  ___   _ __ ___   _ __  | |  ___ | |_ (_)  ___   _ __     | ____|__   __  ___  _ __  | |_                         
#   _____  _____  _____    | |     / _ \ | '_ ` _ \ | '_ \ | | / _ \| __|| | / _ \ | '_ \    |  _|  \ \ / / / _ \| '_ \ | __|    _____  _____  _____ 
#  |_____||_____||_____|   | |___ | (_) || | | | | || |_) || ||  __/| |_ | || (_) || | | |   | |___  \ V / |  __/| | | || |_    |_____||_____||_____|
#                           \____| \___/ |_| |_| |_|| .__/ |_| \___| \__||_| \___/ |_| |_|   |_____|  \_/   \___||_| |_| \__|                        
#                                                   |_|                                                                                              

class CompletionEvent():
    """
    Separate value ONLY used for indicating the completion of a stream or an awaitable.
    Don't use other zef values for this, such that those can flow as data without
    tearing down the streams.
    This magic behavior is specific to the singleton value of this type only.
    """
    def __repr__(self):
        return "completion_event"

completion_event = CompletionEvent()

