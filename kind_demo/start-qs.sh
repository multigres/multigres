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

set -ex

# 1. Docker image for supafirehose should be built already. Run this in supafirehose directory.
# docker build -t supafirehose:latest .

# 2. Load image into kind
kind load docker-image supafirehose:latest --name multidemo

# 3. Init the database (creates DB + seeds 100k users)
kubectl apply -f k8s-supafirehose-init.yaml

# 4. Deploy supafirehose
kubectl apply -f k8s-supafirehose.yaml

# 5. Port-forward to access the UI
# kubectl port-forward svc/supafirehose 8080:8080
echo "Supafirehose UI:"
echo "  kubectl port-forward svc/supafirehose 8080:8080"
