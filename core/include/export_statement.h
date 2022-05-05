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

#if BUILDING_LIBZEF && __GNUC__ >= 4
#define LIBZEF_DLL_EXPORTED __attribute__((__visibility__("default")))
/* #pragma message "Building for exported DLL (gcc)" */
#elif BUILDING_LIBZEF && defined _MSC_VER
#define LIBZEF_DLL_EXPORTED __declspec(dllexport)
/* #pragma message "Building for exported DLL (MSVC)" */
#elif defined _MSC_VER
#define LIBZEF_DLL_EXPORTED __declspec(dllimport)
/* #pragma message "Building to import DLL (MSVC)" */
#else
#define LIBZEF_DLL_EXPORTED
/* #pragma message "Building to import DLL (gcc)" */
#endif
