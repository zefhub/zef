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
#include<vector>

struct zstring_sentinel{};
bool operator!=(const char* str, zstring_sentinel)
{
    return *str != '\0';
}


struct zstring_range
{
    const char* str;

    auto begin() const
    {
        // The begin is just the pointer to our string.
        return str;
    }
    auto end() const
    {
        // The end is a different type, the sentinel.
        return zstring_sentinel{};
    }
};

void do_sth(const char* str)
{
    std::vector<char> buffer;
    auto x = zstring_range{ str };
    for (auto c : zstring_range{ str })
        buffer.push_back(c);


}


TEST_CASE("sentinel") {
	using std::cout;
	cout << "hello!\n";
}
