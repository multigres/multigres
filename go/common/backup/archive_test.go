// Copyright 2026 Supabase, Inc.
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

package backup

import "testing"

func TestArchiveNames(t *testing.T) {
	for _, ok := range []string{"17-1", "17-12", "9-1"} {
		if !ValidArchiveID(ok) {
			t.Errorf("archive id %q should be valid", ok)
		}
	}
	for _, bad := range []string{"", "9.6-1", "17", "17-", "-1", "17-1/x", "../", "17-1\n", " 17-1"} {
		if ValidArchiveID(bad) {
			t.Errorf("archive id %q should be invalid", bad)
		}
	}
	for _, ok := range []string{"000000010000000A", "FFFFFFFFFFFFFFFF"} {
		if !ValidWALDir(ok) {
			t.Errorf("wal dir %q should be valid", ok)
		}
	}
	for _, bad := range []string{"", "000000010000000a", "00000001", "000000010000000A0", "../", "000000010000000A\n"} {
		if ValidWALDir(bad) {
			t.Errorf("wal dir %q should be invalid", bad)
		}
	}
}
