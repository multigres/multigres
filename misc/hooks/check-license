#!/bin/bash
# Copyright 2025 The Multigres Authors
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

set -euo pipefail

# Check if files have proper license headers
files_without_license=()

for file in "$@"; do
    # Skip files in excluded directories
    if [[ "$file" == site/* ]] || [[ "$file" == pb/* ]] || [[ "$file" == dist/* ]] || [[ "$file" == bin/* ]]; then
        continue
    fi
    
    # Check if file has license header
    if ! head -20 "$file" | grep -q "Copyright.*The Multigres Authors"; then
        files_without_license+=("$file")
    fi
done

if [ ${#files_without_license[@]} -ne 0 ]; then
    echo "Adding license headers to files missing them:"
    printf '%s\n' "${files_without_license[@]}"
    echo ""
    
    # Add license headers to the specific files
    for file in "${files_without_license[@]}"; do
        echo "Adding license to: $file"
        addlicense -c "The Multigres Authors" -l apache "$file"
        # Stage the modified file so it's included in this commit
        git add "$file"
    done
    
    echo "License headers added and staged successfully"
fi