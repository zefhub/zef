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

from .._core import RT
from .implementation_typing_functions import * 
from .yo import yo_implementation, yo_type_info
from .graphviz import graphviz_imp, graphviz_tp
from .func import call_implementation, call_type_info
from .func import unpack_implementation, unpack_type_info

_op_to_functions = {
        RT.ApplyFunctions: (apply_functions_imp, None),
        RT.Map:            (map_implementation, map_type_info),
        RT.FlatMap:        (flat_map_imp, flat_map_tp),
        RT.Reduce:         (reduce_implementation, reduce_type_info),
        RT.Scan:           (scan_implementation, scan_type_info),
        RT.GroupBy:        (group_by_implementation, group_by_type_info),
        RT.Transpose:      (transpose_imp, transpose_tp),
        RT.Frequencies:    (frequencies_imp, frequencies_tp),
        RT.Iterate:        (iterate_implementation, iterate_type_info),
        RT.Identity:       (identity_implementation, identity_type_info),
        RT.Length:         (length_implementation, length_type_info),
        RT.Take :          (take_implementation, take_type_info),
        RT.TakeWhile:      (take_while_imp, take_while_tp),
        RT.TakeWhilePair:  (take_while_pair_imp, take_while_pair_tp),
        RT.TakeUntil:      (take_until_imp, take_until_tp),
        RT.SkipWhile:      (skip_while_imp, skip_while_tp),
        RT.Drop :          (drop_imp, drop_tp),
        RT.Skip :          (skip_imp, skip_tp),
        RT.Nth:            (nth_implementation, nth_type_info),
        RT.Filter:         (filter_implementation, filter_type_info),
        RT.SelectKeys:     (select_keys_imp, None),
        RT.Modulo:         (modulo_imp, None),
        RT.SelectByField:  (select_by_field_imp, select_by_field_tp),
        RT.Without:        (without_imp, without_tp),
        RT.First:          (first_imp, first_tp),
        RT.Second:         (second_imp, second_tp),
        RT.Last:           (last_imp, last_tp),
        RT.Single:         (single_imp, single_tp),
        RT.SingleOr:       (single_or_imp, single_or_tp),
        RT.Zip:            (zip_imp, zip_tp),
        RT.Concat:         (concat_implementation, concat_type_info),
        RT.Prepend:        (prepend_imp, prepend_tp),
        RT.Append:         (append_imp, append_tp),
        RT.Interleave:     (interleave_imp, interleave_tp),
        RT.InterleaveLongest: (interleave_longest_imp, interleave_longest_tp),
        RT.Sort:            (sort_implementation, sort_type_info),
        RT.Now:             (now_implementation, now_type_info),
        RT.Merged:          (merged_imp, merged_tp),
        RT.Affected:        (affected_imp, affected_tp),
        RT.ValueAssigned:   (value_assigned_imp, value_assigned_tp),
        RT.Instantiated:    (instantiated_imp, instantiated_tp),
        RT.Terminated:      (terminated_imp, terminated_tp),
        RT.Events:          (events_imp, events_tp),
        RT.ToDelegate:      (to_delegate_implementation, to_delegate_type_info),
        RT.DelegateOf:      (delegate_of_implementation, delegate_of_type_info),
        
        RT.In:              (In_imp, None),
        RT.Ins:             (Ins_imp, None),
        RT.Out:             (Out_imp, None),
        RT.Outs:            (Outs_imp, None),
        RT.InRel:           (in_rel_imp, None),
        RT.InRels:          (in_rels_imp, None),
        RT.OutRel:          (out_rel_imp, None),
        RT.OutRels:         (out_rels_imp, None),

        RT.InOld:              (InOld_implementation, None),
        RT.InInOld:            (InInOld_implementation, None),
        RT.OutOld:             (OutOld_implementation, None),
        RT.OutOutOld:          (OutOutOld_implementation, None),
        
        RT.InsOld:          (ins_implementation_old, ins_type_info),
        RT.OutsOld:         (outs_implementation_old, outs_type_info),
        RT.InsAndOutsOld:   (ins_and_outs_implementation_old, ins_and_outs_type_info),
        RT.Source:          (source_implementation, source_type_info),
        RT.Target:          (target_implementation, target_type_info),
        RT.Value:           (value_implementation, value_type_info),
        RT.Time:            (time_implementation, time_type_info),
        RT.TimeSlice:       (time_slice_implementation, time_slice_type_info),
        RT.Tx:              (tx_imp, tx_tp),
        RT.NextTX:          (next_tx_imp, next_tx_tp),
        RT.PreviousTX:      (previous_tx_imp, previous_tx_tp),
        RT.Tx:              (tx_imp, tx_tp),
        RT.InstantiationTx: (instantiation_tx_implementation, instantiation_tx_type_info),                              # TODO: retire
        RT.TerminationTx:   (termination_tx_implementation, termination_tx_type_info),                            # TODO: retire
        RT.Instances:       (instances_implementation, instances_type_info),
        RT.Uid:             (uid_implementation, uid_type_info),
        RT.Frame:           (frame_imp, frame_tp),
        RT.BaseUid:         (base_uid_implementation, base_uid_type_info),
        RT.OriginUid:       (origin_uid_imp, origin_uid_tp),
        RT.OriginRAE:       (origin_rae_imp, origin_rae_tp),
        RT.ExistsAt:        (exists_at_implementation, exists_at_type_info),
        RT.IsZefRefPromotable:(is_zefref_promotable_implementation, is_zefref_promotable_type_info),
        RT.InFrame:         (in_frame_imp, in_frame_tp),
        RT.ToGraphSlice:    (to_graph_slice_imp, to_graph_slice_tp),
        RT.ToTx:            (to_tx_imp, to_tx_tp),
        RT.TimeTravel:      (time_travel_imp, time_travel_tp),
        RT.ToEZefRef:       (to_ezefref_implementation, to_ezefref_type_info),
        RT.O:               (o_implementation, o_type_info),
        RT.L:               (l_implementation, l_type_info),
        RT.Terminate:       (terminate_implementation, terminate_type_info),
        RT.AssignValue:     (assign_value_imp, assign_value_tp),
        RT.ET:              (ET_implementation, ET_type_info),
        RT.RT:              (RT_implementation, RT_type_info),
        RT.AET:             (AET_implementation, AET_type_info),
        RT.BT:              (BT_implementation, BT_type_info),
        RT.FillOrAttach:    (fill_or_attach_implementation, fill_or_attach_type_info),
        RT.Assert:          (assert_implementation, assert_type_info),
        RT.HasOut:          (hasout_implementation, hasout_type_info),
        RT.HasIn:           (hasin_implementation, hasin_type_info),
        RT.Run:             (run_effect_implementation, None),
        RT.Tap:             (tap_imp, tap_tp),
        RT.Push:            (push_imp, push_tp),
        RT.IsA:             (is_a_implementation, is_a_type_info),
        RT.IsRepresentedAs: (is_represented_as_implementation, is_represented_as_type_info),
        RT.ZefType:         (zef_type_imp, None),
        RT.HasRelation:     (has_relation_implementation, has_relation_type_info),
        RT.Relation:        (relation_implementation, relation_type_info),
        RT.Relations:       (relations_implementation, relations_type_info),
        RT.Chunk:           (chunk_imp, chunk_tp),
        RT.Sliding:         (sliding_imp, sliding_tp),
        RT.Stride:          (stride_imp, stride_tp),
        RT.Insert:          (insert_imp, insert_tp),
        RT.InsertInto:      (insert_into_imp, None),
        RT.ReverseArgs:     (reverse_args_imp, None),
        RT.Remove:          (remove_imp, remove_tp),
        RT.Get:             (get_imp, get_tp),
        RT.GetField:        (get_field_imp, get_field_tp),
        RT.Enumerate:       (enumerate_imp, enumerate_tp),
        RT.Items:           (items_imp, items_tp),
        RT.Values:          (values_imp, values_tp),
        RT.Keys:            (keys_imp, keys_tp),
        RT.Reverse:         (reverse_imp, reverse_tp),
        RT.RaeType:         (rae_type_implementation, rae_type_type_info),
        RT.AbstractType:    (abstract_type_implementation, abstract_type_type_info),
        RT.Root:            (root_imp, root_tp),
        RT.Blobs:           (blobs_imp, blobs_tp),
        RT.Schema:          (schema_imp, schema_tp),
        RT.Z:               (Z_imp, Z_tp),
        RT.Docstring:       (docstring_imp, None),
        
        
        RT.GetIn:           (get_in_imp, get_in_tp),
        RT.InsertIn:        (insert_in_imp, insert_in_tp),
        RT.Update:          (update_imp, None),
        RT.UpdateIn:        (update_in_imp, update_in_tp),
        RT.UpdateAt:        (update_at_imp, update_at_tp),
        RT.RemoveIn:        (remove_in_imp, remove_in_tp),
        RT.RemoveAt:        (remove_at_imp, None),
        RT.Merge:           (merge_imp, merge_tp),
        RT.MergeWith:       (merge_with_imp, merge_with_tp),
        RT.IntToAlpha:      (int_to_alpha_imp, None),
        RT.PermuteTo:       (permute_to_imp, None),
                
        RT.Cycle:           (cycle_imp, cycle_tp),
        RT.Repeat:          (repeat_imp, repeat_tp),
        RT.Contains:        (contains_imp, contains_tp),
        RT.ContainedIn:     (contained_in_imp, contained_in_tp),
        # RT.Skip:            (skip_imp, skip_tp),
        RT.All:             (all_imp, all_tp),
        RT.Any:             (any_imp, any_tp),
        RT.Join:            (join_imp, join_tp),
        RT.Trim:            (trim_imp, trim_tp),
        RT.TrimLeft:        (trim_left_imp, trim_left_tp),
        RT.TrimRight:       (trim_right_imp, trim_right_tp),
        RT.Yo:              (yo_implementation, yo_type_info),
        
        RT.Sign:            (sign_imp, sign_tp),
        RT.IfThenElse:      (if_then_else_imp, if_then_else_tp),
        RT.IfThenElseApply:      (if_then_else_apply_imp, if_then_else_apply_tp),
        RT.Attempt:         (attempt_imp, attempt_tp),
        RT.Bypass:          (bypass_imp, bypass_tp),
        RT.Pattern:         (pattern_imp, pattern_tp),
        RT.Replace:         (replace_imp, replace_tp),
        RT.Distinct:        (distinct_imp, distinct_tp),
        RT.DistinctBy:      (distinct_by_imp, distinct_by_tp),
        RT.IsDistinct:      (is_distinct_imp, is_distinct_tp),
        RT.IsDistinctBy:    (is_distinct_by_imp, is_distinct_by_tp),
        RT.Shuffle:         (shuffle_imp, shuffle_tp),
        RT.Slice:           (slice_imp, slice_tp),
        RT.Split:           (split_imp, split_tp),
        RT.Graphviz:        (graphviz_imp, graphviz_tp),
        
        RT.Always:              (always_imp, always_tp),
        
        RT.WithoutAbsorbed:     (without_absorbed_imp, None),
        RT.Absorbed:            (absorbed_imp, None),
        
        RT.CartesianProduct:    (cartesian_product_imp, cartesian_product_tp),
        RT.Permutations:        (permutations_imp, permutations_tp),
        RT.Combinations:        (combinations_imp, combinations_tp),
        RT.Add:                 (add_imp, add_tp),
        RT.Subtract:            (subtract_imp, subtract_tp),
        RT.Multiply:            (multiply_imp, multiply_tp),
        RT.Divide:              (divide_imp, divide_tp),
        RT.Mean:                (mean_imp, mean_tp),
        RT.Variance:            (variance_imp, variance_tp),
        RT.Power:               (power_imp, power_tp),
        RT.Exponential:         (exponential_imp, exponential_tp),
        RT.Logarithm:           (logarithm_imp, logarithm_tp),
        RT.Max:                 (max_imp, max_tp),
        RT.Min:                 (min_imp, min_tp),
        RT.MaxBy:               (max_by_imp, max_by_tp),
        RT.MinBy:               (min_by_imp, min_by_tp),
        RT.Equals:              (equals_imp, equals_tp),
        RT.LargerThan:          (greater_than_imp, greater_than_tp),
        RT.LessThan:            (less_than_imp, less_than_tp),
        RT.GreaterThanOrEqual:  (greater_than_or_equal_imp, greater_than_or_equal_to),
        RT.LessThanOrEqual:     (less_than_or_equal_imp, less_than_or_equal_to),
        RT.Not:                 (not_imp, not_tp),
        RT.And:                 (and_imp, and_tp),
        RT.Or:                  (or_imp, or_tp),
        RT.Xor:                 (xor_imp, xor_tp),
        RT.Peel:                (peel_imp, peel_tp),
        RT.Match:               (match_imp, match_tp),
        RT.MatchApply:          (match_apply_imp, match_apply_tp),

        RT.Sync:                (sync_imp, sync_tp),
        RT.Tag:                 (tag_imp, tag_tp),
        RT.Untag:               (untag_imp, untag_tp),        
        RT.ToClipboard:         (to_clipboard_imp, to_clipboard_tp),
        RT.FromClipboard:       (from_clipboard_imp, from_clipboard_tp),
        RT.TextArt:             (text_art_imp, text_art_tp),

        RT.ToJSON:             (to_json_imp, to_json_tp), 
        RT.FromJSON:           (from_json_imp, from_json_tp),

        RT.ToYaml:             (to_yaml_imp, to_yaml_tp),
        RT.FromYaml:           (from_yaml_imp, from_yaml_tp),

        RT.ToToml:              (to_toml_imp, to_toml_tp),
        RT.FromToml:            (from_toml_imp, from_toml_tp),

        RT.ToCSV:               (to_csv_imp, from_csv_tp),
        RT.FromCSV:             (from_csv_imp, from_csv_tp),

        RT.ReadFile:            (read_file_imp, read_file_tp),
        RT.LoadFile:            (load_file_imp, load_file_tp),
        RT.WriteFile:           (write_file_imp, write_file_tp),
        RT.SaveFile:            (save_file_imp, save_file_tp),

        RT.PandasToGd:          (pandas_to_gd_imp, pandas_to_gd_tp),
        
        RT.AsPipeline:          (as_pipeline_imp, as_pipeline_tp),
        RT.Inject:              (inject_imp, inject_tp),
        RT.InjectList:          (inject_list_imp, inject_list_tp),

        RT.ZasciiToAsg:         (zascii_to_asg_imp, zascii_to_asg_tp),
        RT.ZasciiToScehma:      (zascii_to_schema_imp, zascii_to_schema_tp),

        RT.ReplaceAt:           (replace_at_imp, replace_at_tp),
        RT.RandomPick:          (random_pick_imp, random_pick_tp),
        RT.PadToLength:         (pad_to_length_imp, pad_to_length_tp),
        RT.IsAlpha:             (is_alpha_imp, is_alpha_tp),
        RT.ToUpperCase:         (to_upper_case_imp, to_upper_case_tp),
        RT.ToLowerCase:         (to_lower_case_imp, to_lower_case_tp),
        RT.ToPascalCase:        (to_pascal_case_imp, to_pascal_case_tp),
        RT.ToCamelCase:         (to_camel_case_imp, to_camel_case_tp),
        RT.ToKebabCase:         (to_kebab_case_imp, to_kebab_case_tp),
        RT.ToSnakeCase:         (to_snake_case_imp, to_snake_case_tp),
        RT.ToScreamingSnakeCase:(to_screaming_snake_case_imp, to_screaming_snake_case_tp),


        RT.Call:                (call_implementation, call_type_info),
        RT.Unpack:              (unpack_implementation, unpack_type_info),

        RT.MakeRequest:        (make_request_imp, make_request_tp),
        RT.Blake3:             (blake3_imp, blake3_tp),
        RT.ToZefList:          (to_zef_list_imp, to_zef_list_tp),
        RT.Transact:           (transact_imp, transact_tp),

        RT.Function:           (function_imp, function_tp),
        RT.On:                 (on_implementation, None),
        RT.Range:              (range_imp, range_tp),
}
