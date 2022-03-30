INCLUDES=( -I../include -I../../../libraries/range-v3/include -I../../../libraries/doctest/doctest -I../websocketpp -I../asio-1.18.1/include -I../nlohmann_json/include )
OTHERS=( -L. -lzef -lssl -lcrypto -lzstd -ggdb )
# g++ -std=c++20 -lpthread test.cpp -o test
g++ -std=c++17 -lpthread $INCLUDES $OTHERS test_network.cpp -o network
# g++ -std=c++17 -lpthread $INCLUDES $OTHERS test.cpp -o test
