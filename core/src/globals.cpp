// Copyright 2022 Synchronous Technologies Pte Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file exists to order the initialisation of any globals that need
// ordering.
#include "zwitch.h"
#include "scalars.h"
#include "tokens.h"

namespace zefDB {
    Zwitch zwitch;

    const QuantityFloat seconds{ 1, EN.Unit.seconds };
    const QuantityFloat minutes{ 60, EN.Unit.seconds };
    const QuantityFloat hours{ 60 * 60, EN.Unit.seconds };
    const QuantityFloat days{ 60 * 60 * 24, EN.Unit.seconds };
    const QuantityFloat weeks{ 60 * 60 * 24 * 7, EN.Unit.seconds };
    const QuantityFloat months{ 60 * 60 * 24 * 30, EN.Unit.seconds };
    const QuantityFloat years{ 60 * 60 * 24 * 365, EN.Unit.seconds };
}
