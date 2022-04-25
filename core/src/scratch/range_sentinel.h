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
