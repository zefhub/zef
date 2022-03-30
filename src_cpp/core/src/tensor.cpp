#include "tensor.h"

namespace zefDB {

	Tensor<RelationType, 1> operator, (RelationType rt1, RelationType rt2) {		
		return Tensor<RelationType, 1> {std::array{ rt1, rt2 }};
	}


	Tensor<RelationType, 1> operator, (Tensor<RelationType, 1> exisiting_rel_list, RelationType rt2) {		
		Tensor<RelationType, 1> res(exisiting_rel_list.size() + 1);
		for (int c = 0; c < exisiting_rel_list.size(); c++) res[c] = exisiting_rel_list[c];
		res[exisiting_rel_list.size()] = rt2;
		return res;
	}
	
	Tensor<BlobType, 1> operator, (BlobType bt1, BlobType bt2) {
		return Tensor<BlobType, 1> {std::array{ bt1, bt2 }};
	}


	Tensor<BlobType, 1> operator, (Tensor<BlobType, 1> exisiting_bt_list, BlobType rt2) {
		Tensor<BlobType, 1> res(exisiting_bt_list.size() + 1);
		for (int c = 0; c < exisiting_bt_list.size(); c++) res[c] = exisiting_bt_list[c];
		res[exisiting_bt_list.size()] = rt2;
		return res;
	}

}