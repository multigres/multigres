#!/bin/bash
# Copyright 2025 The Multigres Authors.
#
# Test script for PostgreSQL wire protocol implementation

set -e

echo "Building multigateway with PostgreSQL support..."
make build

echo "Creating test configuration..."
cat > test-multigateway.yaml <<EOF
# Test configuration for multigateway with PostgreSQL
http-port: 18080
grpc-port: 18081
pg-port: 15432
log-level: debug
cell: zone1
EOF

echo "Starting test etcd..."
if ! command -v etcd &> /dev/null; then
    echo "etcd not found. Installing..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install etcd
    else
        echo "Please install etcd manually"
        exit 1
    fi
fi

# Start etcd in background
etcd --data-dir /tmp/test-etcd-data \
     --listen-client-urls http://127.0.0.1:12379 \
     --advertise-client-urls http://127.0.0.1:12379 \
     --log-level error &
ETCD_PID=$!

echo "Waiting for etcd to start..."
sleep 2

# Create test cell in etcd
echo "Creating test cell in topology..."
./bin/multigres cluster init --config-path . 2>/dev/null || true

echo "Starting multigateway with PostgreSQL server on port 15432..."
./bin/multigateway \
    --config test-multigateway.yaml \
    --topo-implementation etcd2 \
    --topo-global-server-addresses 127.0.0.1:12379 \
    --topo-global-root /multigres/test \
    --cell zone1 \
    --pg-port 15432 &
GATEWAY_PID=$!

echo "Waiting for multigateway to start..."
sleep 3

echo "Testing PostgreSQL connection..."
echo ""
echo "You can now test the PostgreSQL server with:"
echo "  psql -h localhost -p 15432 -U test -d test"
echo ""
echo "Try these commands:"
echo "  SELECT 1;"
echo "  SELECT version();"
echo "  SHOW server_version;"
echo ""
echo "Press Ctrl+C to stop the test servers"

# Trap to cleanup on exit
trap "echo 'Stopping servers...'; kill $GATEWAY_PID $ETCD_PID 2>/dev/null; rm -rf /tmp/test-etcd-data test-multigateway.yaml" EXIT

# Wait for interrupt
wait $GATEWAY_PID