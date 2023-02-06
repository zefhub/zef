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

from ..VT import RT
from .implementation_typing_functions import * 
from .yo import yo_implementation, yo_type_info
from .graphviz import graphviz_imp, graphviz_tp
from . to_flatgraph import to_flatgraph_imp
from .func import unpack_implementation, unpack_type_info
from .. import internals
from ..fx.fx_runtime import fx_runtime
from .data_wrangling import infer_types, deduplicate, identify_entities

_op_to_functions = {
        internals.RT.ApplyFunctions: (apply_functions_imp, None),
        internals.RT.Map:            (map_implementation, map_type_info),
        internals.RT.MapCat:         (map_cat_imp, map_cat_tp),
        internals.RT.Reduce:         (reduce_implementation, reduce_type_info),
        internals.RT.Scan:           (scan_implementation, scan_type_info),
        internals.RT.GroupBy:        (group_by_implementation, group_by_type_info),
        internals.RT.Group:          (group_imp, None),
        internals.RT.Transpose:      (transpose_imp, transpose_tp),
        internals.RT.Frequencies:    (frequencies_imp, frequencies_tp),
        internals.RT.Iterate:        (iterate_implementation, iterate_type_info),
        internals.RT.Identity:       (identity_implementation, identity_type_info),
        internals.RT.Length:         (length_implementation, length_type_info),
        internals.RT.Count:          (count_imp, None),
        internals.RT.Take :          (take_implementation, take_type_info),
        internals.RT.TakeWhile:      (take_while_imp, take_while_tp),
        internals.RT.TakeWhilePair:  (take_while_pair_imp, take_while_pair_tp),
        internals.RT.TakeUntil:      (take_until_imp, take_until_tp),
        internals.RT.SkipWhile:      (skip_while_imp, skip_while_tp),
        internals.RT.SkipUntil:      (skip_until_imp, None),
        internals.RT.Skip :          (skip_imp, skip_tp),
        internals.RT.Nth:            (nth_implementation, nth_type_info),
        internals.RT.Filter:         (filter_implementation, filter_type_info),
        internals.RT.SelectKeys:     (select_keys_imp, None),
        internals.RT.Modulo:         (modulo_imp, None),
        internals.RT.SelectByField:  (select_by_field_imp, select_by_field_tp),
        internals.RT.Without:        (without_imp, without_tp),
        internals.RT.First:          (first_imp, None),
        internals.RT.Second:         (second_imp, second_tp),
        internals.RT.Last:           (last_imp, last_tp),
        internals.RT.Single:         (single_imp, single_tp),
        internals.RT.SingleOr:       (single_or_imp, single_or_tp),
        internals.RT.Zip:            (zip_imp, zip_tp),
        internals.RT.Concat:         (concat_implementation, concat_type_info),
        internals.RT.Prepend:        (prepend_imp, prepend_tp),
        internals.RT.Append:         (append_imp, append_tp),
        internals.RT.Interleave:     (interleave_imp, interleave_tp),
        internals.RT.InterleaveLongest: (interleave_longest_imp, interleave_longest_tp),
        internals.RT.Sort:            (sort_implementation, sort_type_info),
        internals.RT.Now:             (now_implementation, now_type_info),
        internals.RT.Events:          (events_imp, events_tp),
        internals.RT.PrecedingEvents: (preceding_events_imp, None),
        internals.RT.ToDelegate:      (to_delegate_implementation, to_delegate_type_info),
        internals.RT.DelegateOf:      (delegate_of_implementation, delegate_of_type_info),
        internals.RT.IsBlueprintAtom: (is_blueprint_atom_imp, None),
        
        internals.RT.In:              (In_imp, None),
        internals.RT.Ins:             (Ins_imp, None),
        internals.RT.Out:             (Out_imp, None),
        internals.RT.Outs:            (Outs_imp, None),
        internals.RT.InsAndOuts:      (ins_and_outs_imp, None),
        internals.RT.InRel:           (in_rel_imp, None),
        internals.RT.InRels:          (in_rels_imp, None),
        internals.RT.OutRel:          (out_rel_imp, None),
        internals.RT.OutRels:         (out_rels_imp, None),
        
        internals.RT.Source:          (source_implementation, source_type_info),
        internals.RT.Target:          (target_implementation, target_type_info),
        internals.RT.Value:           (value_implementation, None),
        internals.RT.Time:            (time_implementation, time_type_info),
        internals.RT.TimeSlice:       (time_slice_implementation, time_slice_type_info),
        internals.RT.NextTX:          (next_tx_imp, next_tx_tp),
        internals.RT.PreviousTX:      (previous_tx_imp, previous_tx_tp),
        internals.RT.InstantiationTx: (instantiation_tx_implementation, instantiation_tx_type_info),                              # TODO: retire
        internals.RT.TerminationTx:   (termination_tx_implementation, termination_tx_type_info),                            # TODO: retire
        internals.RT.Uid:             (uid_implementation, uid_type_info),
        internals.RT.Frame:           (frame_imp, frame_tp),
        internals.RT.DiscardFrame:    (discard_frame_imp, None),
        internals.RT.BaseUid:         (base_uid_implementation, base_uid_type_info),
        internals.RT.OriginUid:       (origin_uid_imp, origin_uid_tp),
        internals.RT.ExistsAt:        (exists_at_implementation, exists_at_type_info),
        internals.RT.AwareOf:         (aware_of_implementation, None),
        internals.RT.IsZefRefPromotable:(is_zefref_promotable_implementation, is_zefref_promotable_type_info),
        internals.RT.ToFrame:         (to_frame_imp, to_frame_tp),
        internals.RT.ToGraphSlice:    (to_graph_slice_imp, to_graph_slice_tp),
        internals.RT.ToTx:            (to_tx_imp, to_tx_tp),
        internals.RT.TimeTravel:      (time_travel_imp, time_travel_tp),
        internals.RT.ToEZefRef:       (to_ezefref_implementation, to_ezefref_type_info),
        internals.RT.Terminate:       (terminate_implementation, terminate_type_info),
        internals.RT.Assign:          (assign_imp, assign_tp),
        internals.RT.ET:              (ET_implementation, ET_type_info),
        internals.RT.RT:              (RT_implementation, RT_type_info),
        internals.RT.AET:             (AET_implementation, AET_type_info),
        internals.RT.BT:              (BT_implementation, BT_type_info),
        internals.RT.SetField:        (set_field_implementation, set_field_type_info),
        internals.RT.Assert:          (assert_implementation, assert_type_info),
        internals.RT.HasOut:          (hasout_implementation, hasout_type_info),
        internals.RT.HasIn:           (hasin_implementation, hasin_type_info),
        internals.RT.Run:             (fx_runtime, None),
        internals.RT.Tap:             (tap_imp, tap_tp),
        internals.RT.Push:            (push_imp, push_tp),
        internals.RT.IsA:             (is_a_implementation, is_a_type_info),
        internals.RT.IsRepresentedAs: (is_represented_as_implementation, is_represented_as_type_info),
        internals.RT.RepresentationType: (representation_type_imp, None),
        internals.RT.HasRelation:     (has_relation_implementation, has_relation_type_info),
        internals.RT.Relation:        (relation_implementation, relation_type_info),
        internals.RT.Relations:       (relations_implementation, relations_type_info),
        internals.RT.Chunk:           (chunk_imp, chunk_tp),
        internals.RT.Sliding:         (sliding_imp, sliding_tp),
        internals.RT.Stride:          (stride_imp, stride_tp),
        internals.RT.Insert:          (insert_imp, insert_tp),
        internals.RT.InsertInto:      (insert_into_imp, None),
        internals.RT.ReverseArgs:     (reverse_args_imp, None),
        internals.RT.Remove:          (remove_imp, remove_tp),
        internals.RT.Get:             (get_imp, get_tp),
        internals.RT.GetField:        (get_field_imp, get_field_tp),
        internals.RT.Enumerate:       (enumerate_imp, enumerate_tp),
        internals.RT.Items:           (items_imp, items_tp),
        internals.RT.Values:          (values_imp, values_tp),
        internals.RT.Keys:            (keys_imp, keys_tp),
        internals.RT.Reverse:         (reverse_imp, reverse_tp),
        internals.RT.RaeType:         (rae_type_implementation, rae_type_type_info),
        internals.RT.AbstractType:    (abstract_type_implementation, abstract_type_type_info),
        internals.RT.Root:            (root_imp, root_tp),
        internals.RT.Blueprint:       (blueprint_imp, None),
        internals.RT.Docstring:       (docstring_imp, None),
        internals.RT.SourceCode:      (source_code_imp, None),
        
        
        internals.RT.GetIn:           (get_in_imp, get_in_tp),
        internals.RT.InsertIn:        (insert_in_imp, insert_in_tp),
        internals.RT.Update:          (update_imp, None),
        internals.RT.UpdateIn:        (update_in_imp, update_in_tp),
        internals.RT.UpdateAt:        (update_at_imp, update_at_tp),
        internals.RT.InsertAt:        (insert_at_imp, None),
        internals.RT.RemoveIn:        (remove_in_imp, remove_in_tp),
        internals.RT.RemoveAt:        (remove_at_imp, None),
        internals.RT.Merge:           (merge_imp, merge_tp),
        internals.RT.MergeWith:       (merge_with_imp, merge_with_tp),
        internals.RT.IntToAlpha:      (int_to_alpha_imp, None),
        internals.RT.PermuteTo:       (permute_to_imp, None),
                
        internals.RT.Cycle:           (cycle_imp, cycle_tp),
        internals.RT.Repeat:          (repeat_imp, repeat_tp),
        internals.RT.Contains:        (contains_imp, contains_tp),
        internals.RT.ContainedIn:     (contained_in_imp, contained_in_tp),
        # internals.RT.Skip:            (skip_imp, skip_tp),
        internals.RT.All:             (all_imp, None),
        internals.RT.Any:             (any_imp, any_tp),
        internals.RT.Join:            (join_imp, join_tp),
        internals.RT.Trim:            (trim_imp, trim_tp),
        internals.RT.TrimLeft:        (trim_left_imp, trim_left_tp),
        internals.RT.TrimRight:       (trim_right_imp, trim_right_tp),
        internals.RT.Yo:              (yo_implementation, yo_type_info),
        
        internals.RT.Sign:            (sign_imp, sign_tp),
        internals.RT.Attempt:         (attempt_imp, attempt_tp),
        internals.RT.Bypass:          (bypass_imp, bypass_tp),
        internals.RT.Pattern:         (pattern_imp, pattern_tp),
        internals.RT.Replace:         (replace_imp, replace_tp),
        internals.RT.Distinct:        (distinct_imp, distinct_tp),
        internals.RT.DistinctBy:      (distinct_by_imp, distinct_by_tp),
        internals.RT.IsDistinct:      (is_distinct_imp, is_distinct_tp),
        internals.RT.IsDistinctBy:    (is_distinct_by_imp, is_distinct_by_tp),
        internals.RT.Shuffle:         (shuffle_imp, shuffle_tp),
        internals.RT.Slice:           (slice_imp, slice_tp),
        internals.RT.Split:           (split_imp, split_tp),
        internals.RT.SplitLeft:       (split_left_imp, None),
        internals.RT.SplitRight:      (split_right_imp, None),
        internals.RT.Graphviz:        (graphviz_imp, graphviz_tp),
        internals.RT.ToFlatGraph:     (to_flatgraph_imp, None),
        internals.RT.Parse:           (parse_imp, None),
        
        internals.RT.Always:              (always_imp, always_tp),
        
        internals.RT.WithoutAbsorbed:     (without_absorbed_imp, None),
        internals.RT.Absorbed:            (absorbed_imp, None),
        
        internals.RT.CartesianProduct:    (cartesian_product_imp, cartesian_product_tp),
        internals.RT.Permutations:        (permutations_imp, permutations_tp),
        internals.RT.Combinations:        (combinations_imp, combinations_tp),
        internals.RT.Add:                 (add_imp, add_tp),
        internals.RT.Sum:                 (sum_imp, None),
        internals.RT.Product:             (product_imp, None),
        internals.RT.Subtract:            (subtract_imp, subtract_tp),
        internals.RT.Multiply:            (multiply_imp, multiply_tp),
        internals.RT.Divide:              (divide_imp, divide_tp),
        internals.RT.Mean:                (mean_imp, mean_tp),
        internals.RT.Variance:            (variance_imp, variance_tp),
        internals.RT.Power:               (power_imp, power_tp),
        internals.RT.Exponential:         (exponential_imp, exponential_tp),
        internals.RT.Logarithm:           (logarithm_imp, logarithm_tp),
        internals.RT.Max:                 (max_imp, max_tp),
        internals.RT.Min:                 (min_imp, min_tp),
        internals.RT.Clamp:               (clamp_imp, None),
        internals.RT.MaxBy:               (max_by_imp, max_by_tp),
        internals.RT.MinBy:               (min_by_imp, min_by_tp),
        internals.RT.Equals:              (equals_imp, equals_tp),
        internals.RT.GreaterThan:          (greater_than_imp, greater_than_tp),
        internals.RT.LessThan:            (less_than_imp, less_than_tp),
        internals.RT.GreaterThanOrEqual:  (greater_than_or_equal_imp, greater_than_or_equal_to),
        internals.RT.LessThanOrEqual:     (less_than_or_equal_imp, less_than_or_equal_to),
        internals.RT.Not:                 (not_imp, not_tp),
        internals.RT.And:                 (and_imp, and_tp),
        internals.RT.Or:                  (or_imp, or_tp),
        internals.RT.Xor:                 (xor_imp, xor_tp),
        internals.RT.Peel:                (peel_imp, peel_tp),
        internals.RT.Match:               (match_imp, match_tp),
        internals.RT.MatchOn:             (match_on_imp, None),

        internals.RT.Sync:                (sync_imp, sync_tp),
        internals.RT.Tag:                 (tag_imp, tag_tp),
        internals.RT.Untag:               (untag_imp, untag_tp),        
        internals.RT.ToClipboard:         (to_clipboard_imp, to_clipboard_tp),
        internals.RT.FromClipboard:       (from_clipboard_imp, from_clipboard_tp),
        internals.RT.TextArt:             (text_art_imp, text_art_tp),

        internals.RT.ToJSON:             (to_json_imp, to_json_tp), 
        internals.RT.FromJSON:           (from_json_imp, from_json_tp),

        internals.RT.ToYaml:             (to_yaml_imp, to_yaml_tp),
        internals.RT.FromYaml:           (from_yaml_imp, from_yaml_tp),

        internals.RT.ToToml:              (to_toml_imp, to_toml_tp),
        internals.RT.FromToml:            (from_toml_imp, from_toml_tp),

        internals.RT.ToCSV:               (to_csv_imp, from_csv_tp),
        internals.RT.FromCSV:             (from_csv_imp, from_csv_tp),

        internals.RT.ReadFile:            (read_file_imp, read_file_tp),
        internals.RT.LoadFile:            (load_file_imp, load_file_tp),
        internals.RT.WriteFile:           (write_file_imp, write_file_tp),
        internals.RT.SaveFile:            (save_file_imp, save_file_tp),

        internals.RT.PandasToGd:          (pandas_to_gd_imp, pandas_to_gd_tp),
        
        internals.RT.ToPipeline:          (to_pipeline_imp, to_pipeline_tp),
        internals.RT.Inject:              (inject_imp, inject_tp),
        internals.RT.InjectList:          (inject_list_imp, inject_list_tp),

        internals.RT.ZasciiToAsg:         (zascii_to_asg_imp, zascii_to_asg_tp),
        internals.RT.ZasciiToFlatGraph:   (zascii_to_flatgraph_imp, zascii_to_flatgraph_tp,),
        internals.RT.ZasciiToBlueprintFg:  (zascii_to_blueprint_fg_imp, zascii_to_blueprint_fg_tp),

        internals.RT.ReplaceAt:           (replace_at_imp, replace_at_tp),
        internals.RT.RandomPick:          (random_pick_imp, random_pick_tp),
        internals.RT.PadLeft:             (pad_left_imp, None),
        internals.RT.PadRight:            (pad_right_imp, None),
        internals.RT.PadCenter:           (pad_center_imp, None),
        internals.RT.Floor:               (floor_imp, None),
        internals.RT.Ceil:                (ceil_imp, None),
        internals.RT.Round:               (round_imp, None),
        internals.RT.IsAlpha:             (is_alpha_imp, is_alpha_tp),
        internals.RT.IsNumeric:           (is_numeric_imp, None),
        internals.RT.IsAlphaNumeric:      (is_alpha_numeric_imp, None),
        internals.RT.IsAlpha:             (is_alpha_imp, is_alpha_tp),
        internals.RT.ToUpperCase:         (to_upper_case_imp, to_upper_case_tp),
        internals.RT.ToLowerCase:         (to_lower_case_imp, to_lower_case_tp),
        internals.RT.ToPascalCase:        (to_pascal_case_imp, to_pascal_case_tp),
        internals.RT.ToCamelCase:         (to_camel_case_imp, to_camel_case_tp),
        internals.RT.ToKebabCase:         (to_kebab_case_imp, to_kebab_case_tp),
        internals.RT.ToSnakeCase:         (to_snake_case_imp, to_snake_case_tp),
        internals.RT.ToScreamingSnakeCase:(to_screaming_snake_case_imp, to_screaming_snake_case_tp),


        internals.RT.Unpack:              (unpack_implementation, unpack_type_info),
        internals.RT.IndexesOf:           (indexes_of_imp, None),
        internals.RT.GraphSliceIndex:     (graph_slice_index_imp, None),

        internals.RT.MakeRequest:        (make_request_imp, make_request_tp),
        internals.RT.Blake3:             (blake3_imp, blake3_tp),
        internals.RT.ValueHash:          (value_hash_imp, value_hash_tp),
        internals.RT.ToZefList:          (to_zef_list_imp, to_zef_list_tp),
        internals.RT.Transact:           (transact_imp, transact_tp),

        internals.RT.Function:           (function_imp, function_tp),
        internals.RT.On:                 (on_implementation, None),
        internals.RT.Range:              (range_imp, range_tp),
        internals.RT.ZstandardCompress:  (zstandard_compress_imp, None),
        internals.RT.ZstandardDecompress:(zstandard_decompress_imp, None),        
        internals.RT.ToBytes:            (to_bytes_imp, None),
        internals.RT.Utf8bytesToString:  (utf8bytes_to_string_imp, None),
        internals.RT.Base64stringToBytes:(base64string_to_bytes_imp, None),
        internals.RT.BytesToBase64string:(bytes_to_base64string_imp, None),
        internals.RT.IsBetween:          (is_between_imp, None),
        internals.RT.If:                 (If_imp, None),
        internals.RT.Field:              (field_imp, None),
        internals.RT.Fields:             (fields_imp, None),
        internals.RT.Apply:              (apply_imp, None),
        internals.RT.SplitOnNext:        (split_on_next_imp, None),


        internals.RT.Examples:           (examples_imp, None),
        internals.RT.Signature:          (signature_imp, None),
        internals.RT.Tags:               (tags_imp, None),
        internals.RT.RelatedOps:         (related_ops_imp, None),
        internals.RT.OperatesOn:         (operates_on_imp, None), 
        internals.RT.UsedFor:            (used_for_imp, None),

        internals.RT.Gather:             (gather_imp, None),
        internals.RT.Alias:              (alias_imp, None),
        internals.RT.Splice:             (splice_imp, None),
        internals.RT.FlattenDict:        (flatten_dict_imp, None),
        internals.RT.UnflattenDict:      (unflatten_dict_imp, None),
        internals.RT.TokenName:          (token_name_imp, None),

        internals.RT.ZefID:              (zef_id_imp, None),

        internals.RT.ToObject:             (to_object_imp, None),

        internals.RT.InferTypes :          (infer_types, None),
        internals.RT.Deduplicate:          (deduplicate, None),
        internals.RT.IdentifyEntities:     (identify_entities, None),


 
        internals.RT.RecursiveFlatten:     (recursive_flatten_imp, None),
        internals.RT.SplitAt:              (split_at_imp, None),
        internals.RT.SplitLines:           (split_lines_imp, None),
        internals.RT.FilterMap:            (filter_map_imp, None),
        internals.RT.EndsWith:             (ends_with_imp, None),
        internals.RT.StartsWith:           (starts_with_imp, None),
}


