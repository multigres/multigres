// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backingconn

import (
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// ToProto builds the BackingConnection a default primary replies with; c must pass ForPooler.
func (c *Conn) ToProto() *multipoolerservicepb.BackingConnection {
	ports := make([]int32, len(c.Ports))
	for i, p := range c.Ports {
		ports[i] = int32(p)
	}
	return &multipoolerservicepb.BackingConnection{
		Name:           c.Name,
		Hosts:          c.Hosts,
		Ports:          ports,
		Database:       c.Database,
		User:           c.User,
		Password:       c.Password,
		SslMode:        c.SSLMode,
		SslRootCert:    c.SSLRootCert,
		SslNegotiation: c.SSLNegotiation,
	}
}
