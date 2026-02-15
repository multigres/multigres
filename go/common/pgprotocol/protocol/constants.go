// Copyright 2025 Supabase, Inc.
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

// Package protocol defines PostgreSQL wire protocol constants and types.
package protocol

// Message type constants for frontend (client) messages
const (
	MsgBind         = 'B' // Bind
	MsgClose        = 'C' // Close
	MsgDescribe     = 'D' // Describe
	MsgExecute      = 'E' // Execute
	MsgFunctionCall = 'F' // Function call
	MsgFlush        = 'H' // Flush
	MsgParse        = 'P' // Parse
	MsgQuery        = 'Q' // Query (simple query)
	MsgSync         = 'S' // Sync
	MsgTerminate    = 'X' // Terminate
	MsgCopyFail     = 'f' // Copy fail
	MsgCopyData     = 'd' // Copy data (bidirectional)
	MsgCopyDone     = 'c' // Copy done (bidirectional)
	MsgPasswordMsg  = 'p' // Password message (also used for SASL/GSS responses)
)

// Message type constants for backend (server) messages
const (
	MsgParseComplete         = '1' // Parse complete
	MsgBindComplete          = '2' // Bind complete
	MsgCloseComplete         = '3' // Close complete
	MsgNotificationResponse  = 'A' // Notification response
	MsgCommandComplete       = 'C' // Command complete
	MsgDataRow               = 'D' // Data row
	MsgErrorResponse         = 'E' // Error response
	MsgCopyInResponse        = 'G' // Copy-in response
	MsgCopyOutResponse       = 'H' // Copy-out response
	MsgEmptyQueryResponse    = 'I' // Empty query response
	MsgBackendKeyData        = 'K' // Backend key data
	MsgNoticeResponse        = 'N' // Notice response
	MsgAuthenticationRequest = 'R' // Authentication request
	MsgParameterStatus       = 'S' // Parameter status
	MsgRowDescription        = 'T' // Row description
	MsgFunctionCallResponse  = 'V' // Function call response
	MsgCopyBothResponse      = 'W' // Copy-both response
	MsgReadyForQuery         = 'Z' // Ready for query
	MsgNoData                = 'n' // No data
	MsgPortalSuspended       = 's' // Portal suspended
	MsgParameterDescription  = 't' // Parameter description
)

// Authentication request codes
const (
	AuthOk                = 0  // Authentication successful
	AuthKerberosV4        = 1  // Kerberos V4 (not supported)
	AuthKerberosV5        = 2  // Kerberos V5 (not supported)
	AuthCleartextPassword = 3  // Cleartext password
	AuthCryptPassword     = 4  // Crypt password (not supported)
	AuthMD5Password       = 5  // MD5 password
	AuthSCMCredential     = 6  // SCM credential (not supported)
	AuthGSS               = 7  // GSSAPI
	AuthGSSContinue       = 8  // GSSAPI continue
	AuthSSPI              = 9  // SSPI
	AuthSASL              = 10 // SASL authentication
	AuthSASLContinue      = 11 // SASL continue
	AuthSASLFinal         = 12 // SASL final
)

// Error and Notice message field codes
const (
	FieldSeverity         = 'S' // Severity (always present)
	FieldSeverityV        = 'V' // Severity (non-localized, always present)
	FieldCode             = 'C' // SQLSTATE code (always present)
	FieldMessage          = 'M' // Primary message (always present)
	FieldDetail           = 'D' // Detail message
	FieldHint             = 'H' // Hint message
	FieldPosition         = 'P' // Position in query string
	FieldInternalPosition = 'p' // Position in internal query
	FieldInternalQuery    = 'q' // Internal query
	FieldWhere            = 'W' // Context (where the error occurred)
	FieldSchema           = 's' // Schema name
	FieldTable            = 't' // Table name
	FieldColumn           = 'c' // Column name
	FieldDataType         = 'd' // Data type name
	FieldConstraint       = 'n' // Constraint name
	FieldFile             = 'F' // Source file name
	FieldLine             = 'L' // Source line number
	FieldRoutine          = 'R' // Source routine name
)

// TransactionStatus represents the transaction state sent in ReadyForQuery messages.
type TransactionStatus byte

// Transaction status indicators for ReadyForQuery
const (
	TxnStatusIdle    TransactionStatus = 'I' // Idle (not in transaction)
	TxnStatusInBlock TransactionStatus = 'T' // In transaction block
	TxnStatusFailed  TransactionStatus = 'E' // In failed transaction block
)

// Format codes for data types
const (
	FormatText   = 0 // Text format
	FormatBinary = 1 // Binary format
)

// Protocol version
const (
	ProtocolVersionMajor  = 3
	ProtocolVersionMinor  = 0
	ProtocolVersionNumber = (ProtocolVersionMajor << 16) | ProtocolVersionMinor
)

// Special protocol version codes
const (
	CancelRequestCode = (1234 << 16) | 5678 // Cancel request code
	SSLRequestCode    = (1234 << 16) | 5679 // SSL negotiation request code
	GSSENCRequestCode = (1234 << 16) | 5680 // GSSAPI encryption request code
)

// Packet length constants
const (
	MaxStartupPacketLength = 10000 // Maximum startup packet length
	PacketHeaderSize       = 4     // Size of packet length field (does not include message type byte)
)

// ALPN protocol identifier for PostgreSQL
const (
	ALPNProtocol = "postgresql"
)
