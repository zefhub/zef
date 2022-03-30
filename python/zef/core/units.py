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

from . import QuantityInt, QuantityFloat, EN

# create a singleton as a namespace and auto-complete 
# functionality for convenience: e.g. write x = 5*unit.seconds

from dataclasses import dataclass
@dataclass(frozen=True)
class _Unit:
    # just used as a container for various units"""
    milliseconds = QuantityFloat(1E-3, EN.Unit.seconds)
    seconds = QuantityInt(1, EN.Unit.seconds)
    minutes = QuantityInt(60, EN.Unit.seconds)
    hours = QuantityInt(3600, EN.Unit.seconds)
    days = QuantityInt(24*3600, EN.Unit.seconds)
    weeks = QuantityInt(7*24*3600, EN.Unit.seconds)
    years = QuantityInt(365*24*3600, EN.Unit.seconds)
    
    grams = QuantityInt(1, EN.Unit.grams)
    kilograms = QuantityInt(1, EN.Unit.kilograms)
    
    meters = QuantityInt(1, EN.Unit.meters)
    centimeters = QuantityInt(1, EN.Unit.centimeters)
    kilometers = QuantityInt(1000, EN.Unit.meters)
    
        
unit = _Unit()      
