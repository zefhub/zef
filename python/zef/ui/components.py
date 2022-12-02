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


from ..core.VT import ValueType_
from functools import partial


def zef_ui_ctor(type_name, self, *args, **kwargs):
    data = None
    if len(args) == 1: 
        data = args[0]
    elif len(args) > 1:
        raise ValueError(f'{type_name} constructor takes at most one positional argument, got {args}')

    if data:
        if "data" in kwargs: raise ValueError(f'{type_name} was passed both a data positional arg and a data key in kwargs which isn\'t allowed')
        else: kwargs = {**kwargs, "data": data}
    
    ctor_dict = {"type_name":type_name, "constructor_func": partial(zef_ui_ctor, type_name), "pass_self": True}
    if len(self._d['absorbed']) == 1:
        return self._replace(absorbed = ({**self._d['absorbed'][0], **kwargs},))

    return self._replace(absorbed = (kwargs,))



Text  = ValueType_(type_name='Text', constructor_func = partial(zef_ui_ctor, 'Text'), pass_self = True)
Code  = ValueType_(type_name='Code', constructor_func = partial(zef_ui_ctor, 'Code'), pass_self = True)
Style = ValueType_(type_name='Style', constructor_func = partial(zef_ui_ctor, 'Style'), pass_self = True)
Table = ValueType_(type_name='Table', constructor_func = partial(zef_ui_ctor, 'Table'), pass_self = True)
Column = ValueType_(type_name='Column', constructor_func = partial(zef_ui_ctor, 'Column'), pass_self = True)
Frame = ValueType_(type_name='Frame', constructor_func = partial(zef_ui_ctor, 'Frame'), pass_self = True)
HStack = ValueType_(type_name='HStack', constructor_func = partial(zef_ui_ctor, 'HStack'), pass_self = True)
VStack = ValueType_(type_name='VStack', constructor_func = partial(zef_ui_ctor, 'VStack'), pass_self = True)
BulletList   = ValueType_(type_name='BulletList', constructor_func = partial(zef_ui_ctor, 'BulletList'), pass_self = True)
NumberedList = ValueType_(type_name='NumberedList', constructor_func = partial(zef_ui_ctor, 'NumberedList'), pass_self = True)
Paragraph = ValueType_(type_name='Paragraph', constructor_func = partial(zef_ui_ctor, 'Paragraph'), pass_self = True)