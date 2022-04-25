#include <iostream>
#include <string>
#include <vector>
#include <algorithm>    // std::for_each
#include <sstream>
#include <fstream>

using str = std::string;
constexpr auto print = [](const auto& x)->void { std::cout << x << std::endl; };
auto to_str = [](auto&& val)->std::string {
	std::stringstream ss;
	ss << val;
	return ss.str();
};




struct ZefLog {   //: public std::ostream {   we don't need to inherit from ostream
	std::string filename_base = "my_log";
	bool dump_to_file_on_each_new_record = true;
	bool print_to_screen = false;
	std::vector<str> all_records_to_date;

	ZefLog() {};
	ZefLog(str fname) : filename_base(fname) {};


	void write_to_file() {
		using namespace std;
		ofstream myfile;
		str fname = filename_base + str("_") + to_str(all_records_to_date.size()) + ".zeflog";
		myfile.open(fname);
		myfile << "[\n";
		for (const auto& ro : all_records_to_date)
			myfile << ro << ",\n";
		myfile << "]";
		myfile.close();
	}

	template<typename T>
	ZefLog& operator<<(T&& value) {
		all_records_to_date.emplace_back(to_str(std::move(value)));
		if (print_to_screen)
			std::for_each(all_records_to_date.begin(), all_records_to_date.end(), [](const str& s) {print(s); });
		if (dump_to_file_on_each_new_record)
			write_to_file();

		return *this;
	}

};






// a sample data structure for which we define a general ostream<< op
struct A {
	int x;
	str s;
};
std::ostream& operator<< (std::ostream& o, A a) {
	o << "<" << a.s << a.x << a.s << ">";
	return o;
}






int main() {
	ZefLog my_zeflogger("bug3_logger");  // filename base


	A a = { 4, "So Zef " };
	std::cout << a;
	my_zeflogger << a;
	my_zeflogger << "hello";
	my_zeflogger << "bye";
	my_zeflogger << "bye3";
	my_zeflogger << "bye";

	print("\n---- done ----\n");
	return 0;
}