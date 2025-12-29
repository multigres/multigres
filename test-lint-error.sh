#!/bin/bash
# Copyright 2025 Supabase, Inc.
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

# Intentional lint errors for testing GitHub Actions step summary

# SC2086: Double quote to prevent globbing and word splitting
echo $UNQUOTED_VAR

# SC2034: Unused variable
UNUSED_VAR="never used"

# SC2162: read without -r will mangle backslashes
read input
