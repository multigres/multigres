// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0
//go:build !linux

package latencydiag

import "net"

func tcpInfo(net.Conn) (any, string) { return nil, "unsupported_os" }
