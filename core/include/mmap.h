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

#include "mmap_common.h"

#ifdef ZEF_WIN32
#include "mmap_windows.h"
#else
#include "mmap_unix.h"
#endif

// The layout of memory:
//
// The below description has changed a bit, in that there is no more BLOBS
// section. However, the rest remains the same.
//
//  Note: the entire range is initially reserved (an mmap with PROT_NONE)
//  When "mmap-ed" is written below, it implies PROT_READ and maybe PROT_WRITE
// ┌──────────────────────────────────────┐
// │                                      │  Alignments
// │               UNUSED                 │  ──────────
// │                                      │
// ├──────────────────────────────────────┤◄─System page
// │          mmap-ed but unused          │
// │                                      │
// ├──────────────────────────────────────┤
// │         Info structure (mmaped)      │
// ├──────────────────────────────────────┤◄─ZEF_UID_SHIFT
// │                   Blob1              │
// │   BLOBS (mmaped)  Blob2              │
// │                   Blob3              │
// │xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx│
// │                                      │
// │           Unused blob range          │
// │                                      │
// ├──────────────────────────────────────┤◄─ZEF_UID_SHIFT
// │                                      │
// │                UNUSED                │
// │                                      │
// │xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx│
//
// Note: the info structure appears directly before the BLOBS range.
// Note: the UNUSED ranges at the beginning and end are variable depending on what has been returned by the initial mmap reservation.
// Note: the size of the blobs/uids ranges are each ZEF_UID_SHIFT.

// Note: there used to be a UIDs range but now that is gone in favour of UIDs
// being on each blob itself.