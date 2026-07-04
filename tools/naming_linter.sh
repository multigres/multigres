#!/bin/bash
# Copyright 2026 Supabase, Inc.
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

# naming_linter - Enforce consistent capitalization of the service names
# Multipooler, Multiorch, and Multigateway.
#
# These are single words: only the leading "M" is capitalized (or the whole
# token is lower/upper case). The camel-cased forms "MultiPooler",
# "MultiOrch", and "MultiGateway" (and their leading-lowercase variants
# "multiPooler", etc.) are forbidden.
#
# Allowed:   Multipooler  multipooler  MULTIPOOLER
# Forbidden: MultiPooler  multiPooler
#
# The check matches "[Mm]ulti(Pooler|Orch|Gateway)", which flags the
# PascalCase and camelCase forms while leaving the correct single-word forms
# and the SCREAMING_CASE constant form (e.g. ID_MULTIPOOLER) untouched.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# The offending internal-capitalization pattern.
PATTERN='[Mm]ulti(Pooler|Orch|Gateway)'

# Scan all tracked files. Exclude:
#   - this script (it necessarily spells out the forbidden forms above)
#   - the external/ vendored tree (not ours to rename)
if matches=$(git grep -nE "$PATTERN" -- \
  ':!tools/naming_linter.sh' \
  ':!external/**'); then
  echo "ERROR: found camelCased service names. The services are single words:" >&2
  echo "       use Multipooler / Multiorch / Multigateway (only the leading M is capital)," >&2
  echo "       not MultiPooler / MultiOrch / MultiGateway." >&2
  echo >&2
  echo "$matches" >&2
  exit 1
fi

echo "naming_linter: OK — no camelCased service names found."
