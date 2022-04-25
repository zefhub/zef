from ctypes import *


def convert_from_v1(blobs):
    out = {}

    for index,item in blobs.items():
        new_item = {**item}
        new_item["_old_index"] = index
        if "uid" in item:
            new_item["_internalUID"] = item["uid"]
        else:
            new_item["_internalUID"] = index

        out[index] = new_item

    return out
