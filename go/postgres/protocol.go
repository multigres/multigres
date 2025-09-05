// Copyright 2025 The Multigres Authors.
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

// Package postgres implements the PostgreSQL wire protocol for Multigres.
// It allows multigateway to masquerade as a PostgreSQL server and handle
// client connections using the native PostgreSQL protocol.
package postgres

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Protocol version number
const (
	// ProtocolVersion3 is the PostgreSQL protocol version 3.0
	ProtocolVersion3 = 196608 // (3 << 16)
	
	// SSLRequestCode is the magic number for SSL requests
	SSLRequestCode = 80877103
	
	// CancelRequestCode is the magic number for cancel requests
	CancelRequestCode = 80877102
)

// Message type identifiers for frontend (client) messages
const (
	MsgTypeStartup       = 0    // Special case, no type byte
	MsgTypeCancelRequest = 0    // Special case, no type byte
	MsgTypeSSLRequest    = 0    // Special case, no type byte
	MsgTypePassword      = 'p'
	MsgTypeQuery         = 'Q'
	MsgTypeParse         = 'P'
	MsgTypeBind          = 'B'
	MsgTypeDescribe      = 'D'
	MsgTypeExecute       = 'E'
	MsgTypeClose         = 'C'
	MsgTypeFlush         = 'H'
	MsgTypeSync          = 'S'
	MsgTypeTerminate     = 'X'
	MsgTypeCopyData      = 'd'
	MsgTypeCopyDone      = 'c'
	MsgTypeCopyFail      = 'f'
)

// Message type identifiers for backend (server) messages
const (
	MsgTypeAuthentication     = 'R'
	MsgTypeBackendKeyData     = 'K'
	MsgTypeBindComplete       = '2'
	MsgTypeCloseComplete      = '3'
	MsgTypeCommandComplete    = 'C'
	MsgTypeDataRow            = 'D'
	MsgTypeEmptyQueryResponse = 'I'
	MsgTypeErrorResponse      = 'E'
	MsgTypeNoticeResponse     = 'N'
	MsgTypeNotificationResp   = 'A'
	MsgTypeParameterStatus    = 'S'
	MsgTypeParseComplete      = '1'
	MsgTypePortalSuspended    = 's'
	MsgTypeReadyForQuery      = 'Z'
	MsgTypeRowDescription     = 'T'
	MsgTypeNoData             = 'n'
)

// Authentication types
const (
	AuthOK                = 0
	AuthKerberosV5        = 2
	AuthCleartextPassword = 3
	AuthMD5Password       = 5
	AuthSCMCredential     = 6
	AuthGSS               = 7
	AuthGSSContinue       = 8
	AuthSSPI              = 9
	AuthSASL              = 10
	AuthSASLContinue      = 11
	AuthSASLFinal         = 12
)

// Transaction status indicators for ReadyForQuery message
const (
	TxStatusIdle        = 'I' // Not in a transaction block
	TxStatusInTx        = 'T' // In a transaction block
	TxStatusInFailedTx  = 'E' // In a failed transaction block
)

// Message represents a PostgreSQL protocol message
type Message struct {
	Type byte
	Data []byte
}

// MessageReader reads PostgreSQL protocol messages from an io.Reader
type MessageReader struct {
	reader io.Reader
}

// NewMessageReader creates a new MessageReader
func NewMessageReader(r io.Reader) *MessageReader {
	return &MessageReader{reader: r}
}

// ReadMessage reads the next message from the reader
func (r *MessageReader) ReadMessage() (*Message, error) {
	// Read message type (1 byte)
	typeByte := make([]byte, 1)
	if _, err := io.ReadFull(r.reader, typeByte); err != nil {
		return nil, fmt.Errorf("failed to read message type: %w", err)
	}

	// Read message length (4 bytes, includes itself but not type byte)
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(r.reader, lengthBytes); err != nil {
		return nil, fmt.Errorf("failed to read message length: %w", err)
	}
	
	length := int(binary.BigEndian.Uint32(lengthBytes))
	if length < 4 {
		return nil, fmt.Errorf("invalid message length: %d", length)
	}
	
	// Read message data (length - 4 bytes, since length includes itself)
	dataLength := length - 4
	data := make([]byte, dataLength)
	if dataLength > 0 {
		if _, err := io.ReadFull(r.reader, data); err != nil {
			return nil, fmt.Errorf("failed to read message data: %w", err)
		}
	}
	
	return &Message{
		Type: typeByte[0],
		Data: data,
	}, nil
}

// ReadStartupMessage reads the special startup message which doesn't have a type byte
func (r *MessageReader) ReadStartupMessage() (*StartupMessage, error) {
	// Read message length (4 bytes, includes itself)
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(r.reader, lengthBytes); err != nil {
		return nil, fmt.Errorf("failed to read startup message length: %w", err)
	}
	
	length := int(binary.BigEndian.Uint32(lengthBytes))
	if length < 8 { // Minimum: 4 bytes length + 4 bytes protocol version
		return nil, fmt.Errorf("invalid startup message length: %d", length)
	}
	
	// Read the rest of the message (length - 4 bytes)
	data := make([]byte, length-4)
	if _, err := io.ReadFull(r.reader, data); err != nil {
		return nil, fmt.Errorf("failed to read startup message data: %w", err)
	}
	
	// Parse protocol version (first 4 bytes)
	protocolVersion := binary.BigEndian.Uint32(data[:4])
	
	// Check for special requests
	switch protocolVersion {
	case SSLRequestCode:
		return &StartupMessage{
			IsSSLRequest: true,
		}, nil
	case CancelRequestCode:
		if len(data) < 12 { // 4 bytes version + 4 bytes PID + 4 bytes secret
			return nil, fmt.Errorf("invalid cancel request message")
		}
		return &StartupMessage{
			IsCancelRequest: true,
			ProcessID:       int32(binary.BigEndian.Uint32(data[4:8])),
			SecretKey:       int32(binary.BigEndian.Uint32(data[8:12])),
		}, nil
	case ProtocolVersion3:
		// Parse parameters (null-terminated key-value pairs)
		params := make(map[string]string)
		paramData := data[4:] // Skip protocol version
		
		for len(paramData) > 0 {
			// Find null terminator for key
			nullPos := -1
			for i, b := range paramData {
				if b == 0 {
					nullPos = i
					break
				}
			}
			
			if nullPos == -1 {
				return nil, fmt.Errorf("unterminated parameter key")
			}
			
			// Empty key signals end of parameters
			if nullPos == 0 {
				break
			}
			
			key := string(paramData[:nullPos])
			paramData = paramData[nullPos+1:]
			
			// Find null terminator for value
			nullPos = -1
			for i, b := range paramData {
				if b == 0 {
					nullPos = i
					break
				}
			}
			
			if nullPos == -1 {
				return nil, fmt.Errorf("unterminated parameter value for key: %s", key)
			}
			
			value := string(paramData[:nullPos])
			paramData = paramData[nullPos+1:]
			
			params[key] = value
		}
		
		return &StartupMessage{
			ProtocolVersion: protocolVersion,
			Parameters:      params,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported protocol version: %d", protocolVersion)
	}
}

// MessageWriter writes PostgreSQL protocol messages to an io.Writer
type MessageWriter struct {
	writer io.Writer
}

// NewMessageWriter creates a new MessageWriter
func NewMessageWriter(w io.Writer) *MessageWriter {
	return &MessageWriter{writer: w}
}

// WriteMessage writes a message to the writer
func (w *MessageWriter) WriteMessage(msgType byte, data []byte) error {
	// Write message type
	if err := w.writeByte(msgType); err != nil {
		return fmt.Errorf("failed to write message type: %w", err)
	}
	
	// Write message length (data length + 4 for the length itself)
	length := uint32(len(data) + 4)
	if err := w.writeUint32(length); err != nil {
		return fmt.Errorf("failed to write message length: %w", err)
	}
	
	// Write message data
	if len(data) > 0 {
		if _, err := w.writer.Write(data); err != nil {
			return fmt.Errorf("failed to write message data: %w", err)
		}
	}
	
	return nil
}

// WriteAuthenticationOK sends an authentication OK message
func (w *MessageWriter) WriteAuthenticationOK() error {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, AuthOK)
	return w.WriteMessage(MsgTypeAuthentication, data)
}

// WriteAuthenticationCleartextPassword requests cleartext password authentication
func (w *MessageWriter) WriteAuthenticationCleartextPassword() error {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, AuthCleartextPassword)
	return w.WriteMessage(MsgTypeAuthentication, data)
}

// WriteAuthenticationMD5Password requests MD5 password authentication
func (w *MessageWriter) WriteAuthenticationMD5Password(salt []byte) error {
	if len(salt) != 4 {
		return fmt.Errorf("MD5 salt must be 4 bytes")
	}
	data := make([]byte, 8)
	binary.BigEndian.PutUint32(data, AuthMD5Password)
	copy(data[4:], salt)
	return w.WriteMessage(MsgTypeAuthentication, data)
}

// WriteParameterStatus sends a parameter status message
func (w *MessageWriter) WriteParameterStatus(name, value string) error {
	data := make([]byte, 0, len(name)+len(value)+2)
	data = append(data, []byte(name)...)
	data = append(data, 0) // null terminator
	data = append(data, []byte(value)...)
	data = append(data, 0) // null terminator
	return w.WriteMessage(MsgTypeParameterStatus, data)
}

// WriteBackendKeyData sends backend key data for cancellation
func (w *MessageWriter) WriteBackendKeyData(processID, secretKey int32) error {
	data := make([]byte, 8)
	binary.BigEndian.PutUint32(data[:4], uint32(processID))
	binary.BigEndian.PutUint32(data[4:], uint32(secretKey))
	return w.WriteMessage(MsgTypeBackendKeyData, data)
}

// WriteReadyForQuery sends a ready for query message
func (w *MessageWriter) WriteReadyForQuery(txStatus byte) error {
	return w.WriteMessage(MsgTypeReadyForQuery, []byte{txStatus})
}

// WriteErrorResponse sends an error response message
func (w *MessageWriter) WriteErrorResponse(fields map[byte]string) error {
	data := make([]byte, 0, 256)
	for field, value := range fields {
		data = append(data, field)
		data = append(data, []byte(value)...)
		data = append(data, 0) // null terminator
	}
	data = append(data, 0) // final null terminator
	return w.WriteMessage(MsgTypeErrorResponse, data)
}

// WriteCommandComplete sends a command complete message
func (w *MessageWriter) WriteCommandComplete(tag string) error {
	data := append([]byte(tag), 0) // null terminator
	return w.WriteMessage(MsgTypeCommandComplete, data)
}

// WriteEmptyQueryResponse sends an empty query response
func (w *MessageWriter) WriteEmptyQueryResponse() error {
	return w.WriteMessage(MsgTypeEmptyQueryResponse, nil)
}

// WriteSSLResponse sends SSL negotiation response
func (w *MessageWriter) WriteSSLResponse(accept bool) error {
	if accept {
		return w.writeByte('S')
	}
	return w.writeByte('N')
}

// Helper methods
func (w *MessageWriter) writeByte(b byte) error {
	_, err := w.writer.Write([]byte{b})
	return err
}

func (w *MessageWriter) writeUint32(n uint32) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, n)
	_, err := w.writer.Write(buf)
	return err
}

// StartupMessage represents a startup message from the client
type StartupMessage struct {
	ProtocolVersion uint32
	Parameters      map[string]string
	IsSSLRequest    bool
	IsCancelRequest bool
	ProcessID       int32 // For cancel requests
	SecretKey       int32 // For cancel requests
}

// QueryMessage represents a simple query message
type QueryMessage struct {
	Query string
}

// ParseQueryMessage parses a Query message
func ParseQueryMessage(data []byte) (*QueryMessage, error) {
	// Query string is null-terminated
	if len(data) == 0 {
		return nil, fmt.Errorf("empty query message")
	}
	
	// Find null terminator
	nullPos := -1
	for i, b := range data {
		if b == 0 {
			nullPos = i
			break
		}
	}
	
	if nullPos == -1 {
		return nil, fmt.Errorf("unterminated query string")
	}
	
	return &QueryMessage{
		Query: string(data[:nullPos]),
	}, nil
}

// ErrorField constants for error responses
const (
	ErrorFieldSeverity         = 'S' // Severity: ERROR, FATAL, PANIC, WARNING, NOTICE, DEBUG, INFO, LOG
	ErrorFieldSeverityV        = 'V' // Severity (non-localized)
	ErrorFieldCode             = 'C' // SQLSTATE code
	ErrorFieldMessage          = 'M' // Human-readable error message
	ErrorFieldDetail           = 'D' // Detail message
	ErrorFieldHint             = 'H' // Hint message
	ErrorFieldPosition         = 'P' // Error position in query
	ErrorFieldInternalPosition = 'p' // Internal position
	ErrorFieldInternalQuery    = 'q' // Internal query
	ErrorFieldWhere            = 'W' // Context
	ErrorFieldSchema           = 's' // Schema name
	ErrorFieldTable            = 't' // Table name
	ErrorFieldColumn           = 'c' // Column name
	ErrorFieldDataType         = 'd' // Data type name
	ErrorFieldConstraint       = 'n' // Constraint name
	ErrorFieldFile             = 'F' // Source file
	ErrorFieldLine             = 'L' // Source line
	ErrorFieldRoutine          = 'R' // Source routine
)