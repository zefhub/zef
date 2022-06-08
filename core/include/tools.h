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

#include "export_statement.h"

#include <fstream>
#include <functional>
#include <variant>
#include <optional>

//#include "fwd_declarations.h"
//#include "zefDB_utils.h"
#include "zefDB.h"
#include "zwitch.h"

namespace zefDB {
	


namespace tools {
	
	struct ZefLog {   //: public std::ostream {   we don't need to inherit from ostream

		using event_element = std::variant<str, EZefRef>;

		std::string filename_base = "my_log";
		bool dump_to_file_on_each_new_record = true;
		bool dump_all_blobs_to_file = true;
		bool print_to_screen = false;
		std::vector<event_element> all_records_to_date;
		std::optional<Graph> g_maybe = {}; // if any EZefRef is pumped in, we want to keep track of which graph we are debugging to write out all blobs to the log


		ZefLog() {};
		ZefLog(str fname) : filename_base(fname) {};



		void write_to_file();
		void clear_log() { all_records_to_date.clear(); }


		ZefLog& operator<<(str s) {
			all_records_to_date.push_back(s);
			if (dump_to_file_on_each_new_record)
				write_to_file();
			return *this;
		}
		ZefLog& operator<<(ZefRef z) {
			all_records_to_date.push_back(z.blob_uzr);
			if (dump_to_file_on_each_new_record)
				write_to_file();
			g_maybe = Graph(z);
			return *this;
		}
		ZefLog& operator<<(EZefRef z) {
			all_records_to_date.push_back(z);
			if (dump_to_file_on_each_new_record)
				write_to_file();
			g_maybe = Graph(z);
			return *this;
		}


		template<typename T>
		ZefLog& operator<<(T&& value) {
			all_records_to_date.push_back(event_element(to_str(value)));
			//if (print_to_screen)
			//	std::for_each(all_records_to_date.begin(), all_records_to_date.end(), [](const str& s) {print(s); });
			if (dump_to_file_on_each_new_record)
				write_to_file();

			return *this;
		}
	};

    extern tools::ZefLog zeflog;

} //tools


LIBZEF_DLL_EXPORTED Graph deep_copy(Graph g);
LIBZEF_DLL_EXPORTED Graph revision_graph(Graph g);



} //zefDB
