// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0
//go:build linux

package latencydiag

import (
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

func tcpInfo(conn net.Conn) (any, string) {
	c, ok := conn.(syscall.Conn)
	if !ok {
		return nil, "unsupported_transport"
	}
	raw, err := c.SyscallConn()
	if err != nil {
		return nil, "unavailable"
	}
	var info *unix.TCPInfo
	var socketErr error
	err = raw.Control(func(fd uintptr) { info, socketErr = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO) })
	if err != nil || socketErr != nil {
		return nil, "unavailable"
	}
	return map[string]any{"rtt_us": info.Rtt, "rtt_var_us": info.Rttvar, "unacked": info.Unacked, "total_retrans": info.Total_retrans, "snd_cwnd": info.Snd_cwnd, "bytes_acked": info.Bytes_acked, "bytes_received": info.Bytes_received, "notsent_bytes": info.Notsent_bytes}, "ok"
}
