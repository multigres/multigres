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

package postgres

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Server represents a PostgreSQL protocol server
type Server struct {
	listener net.Listener
	handler  Handler
	logger   *slog.Logger
	
	// Server configuration
	config ServerConfig
	
	// Connection management
	connections sync.Map // map[uint64]*Connection
	nextConnID  atomic.Uint64
	
	// Server state
	running atomic.Bool
	wg      sync.WaitGroup
}

// ServerConfig contains configuration for the PostgreSQL server
type ServerConfig struct {
	// Network address to listen on (e.g., ":5432")
	Address string
	
	// Authentication method
	AuthMethod string // "trust", "password", "md5"
	
	// Server parameters to send to clients
	Parameters map[string]string
	
	// Connection limits
	MaxConnections int
	
	// Timeouts
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	
	// TLS configuration (optional)
	TLSConfig *TLSConfig
}

// TLSConfig contains TLS configuration
type TLSConfig struct {
	CertFile string
	KeyFile  string
	Required bool // If true, reject non-TLS connections
}

// Handler processes PostgreSQL protocol messages
type Handler interface {
	// HandleStartup is called when a client connects
	HandleStartup(ctx context.Context, conn *Connection, msg *StartupMessage) error
	
	// HandleQuery processes a simple query
	HandleQuery(ctx context.Context, conn *Connection, query string) error
	
	// HandleParse processes a Parse message (extended query protocol)
	HandleParse(ctx context.Context, conn *Connection, msg *Message) error
	
	// HandleBind processes a Bind message (extended query protocol)
	HandleBind(ctx context.Context, conn *Connection, msg *Message) error
	
	// HandleExecute processes an Execute message (extended query protocol)
	HandleExecute(ctx context.Context, conn *Connection, msg *Message) error
	
	// HandleDescribe processes a Describe message
	HandleDescribe(ctx context.Context, conn *Connection, msg *Message) error
	
	// HandleClose processes a Close message
	HandleClose(ctx context.Context, conn *Connection, msg *Message) error
	
	// HandleTerminate is called when the client disconnects
	HandleTerminate(ctx context.Context, conn *Connection)
}

// Connection represents a client connection
type Connection struct {
	ID         uint64
	Conn       net.Conn
	Reader     *MessageReader
	Writer     *MessageWriter
	
	// Connection state
	Parameters map[string]string // Client parameters from startup
	User       string
	Database   string
	
	// Backend key for cancellation
	ProcessID int32
	SecretKey int32
	
	// Transaction status
	TxStatus byte
	
	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc
	
	// Server reference
	server *Server
}

// NewServer creates a new PostgreSQL protocol server
func NewServer(config ServerConfig, handler Handler, logger *slog.Logger) *Server {
	// Set default parameters
	if config.Parameters == nil {
		config.Parameters = make(map[string]string)
	}
	
	// Set default server parameters if not provided
	if _, ok := config.Parameters["server_version"]; !ok {
		config.Parameters["server_version"] = "15.0 (Multigres)"
	}
	if _, ok := config.Parameters["server_encoding"]; !ok {
		config.Parameters["server_encoding"] = "UTF8"
	}
	if _, ok := config.Parameters["client_encoding"]; !ok {
		config.Parameters["client_encoding"] = "UTF8"
	}
	if _, ok := config.Parameters["DateStyle"]; !ok {
		config.Parameters["DateStyle"] = "ISO, MDY"
	}
	if _, ok := config.Parameters["TimeZone"]; !ok {
		config.Parameters["TimeZone"] = "UTC"
	}
	if _, ok := config.Parameters["integer_datetimes"]; !ok {
		config.Parameters["integer_datetimes"] = "on"
	}
	
	// Set defaults for other config values
	if config.MaxConnections == 0 {
		config.MaxConnections = 100
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = 60 * time.Second
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = 60 * time.Second
	}
	
	return &Server{
		handler: handler,
		logger:  logger,
		config:  config,
	}
}

// Listen starts listening for connections
func (s *Server) Listen() error {
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.config.Address, err)
	}
	
	s.listener = listener
	s.running.Store(true)
	
	s.logger.Info("PostgreSQL server listening",
		"address", s.config.Address,
		"auth_method", s.config.AuthMethod,
		"max_connections", s.config.MaxConnections,
	)
	
	return nil
}

// Serve accepts and handles connections
func (s *Server) Serve() error {
	if s.listener == nil {
		return fmt.Errorf("server not listening")
	}
	
	for s.running.Load() {
		conn, err := s.listener.Accept()
		if err != nil {
			if !s.running.Load() {
				// Server is shutting down
				return nil
			}
			s.logger.Error("Failed to accept connection", "error", err)
			continue
		}
		
		// Check connection limit
		var connCount int
		s.connections.Range(func(_, _ interface{}) bool {
			connCount++
			return connCount < s.config.MaxConnections
		})
		
		if connCount >= s.config.MaxConnections {
			s.logger.Warn("Connection limit reached, rejecting connection",
				"current", connCount,
				"max", s.config.MaxConnections,
			)
			conn.Close()
			continue
		}
		
		// Handle connection in goroutine
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
	
	return nil
}

// Close gracefully shuts down the server
func (s *Server) Close() error {
	s.running.Store(false)
	
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			s.logger.Error("Failed to close listener", "error", err)
		}
	}
	
	// Close all connections
	s.connections.Range(func(key, value interface{}) bool {
		if conn, ok := value.(*Connection); ok {
			conn.Close()
		}
		return true
	})
	
	// Wait for all connections to finish
	s.wg.Wait()
	
	s.logger.Info("PostgreSQL server stopped")
	return nil
}

// handleConnection handles a single client connection
func (s *Server) handleConnection(netConn net.Conn) {
	defer s.wg.Done()
	
	// Create connection object
	ctx, cancel := context.WithCancel(context.Background())
	conn := &Connection{
		ID:       s.nextConnID.Add(1),
		Conn:     netConn,
		Reader:   NewMessageReader(netConn),
		Writer:   NewMessageWriter(netConn),
		TxStatus: TxStatusIdle,
		ctx:      ctx,
		cancel:   cancel,
		server:   s,
	}
	
	// Store connection
	s.connections.Store(conn.ID, conn)
	defer s.connections.Delete(conn.ID)
	defer conn.Close()
	
	clientAddr := netConn.RemoteAddr().String()
	s.logger.Info("Client connected",
		"conn_id", conn.ID,
		"client_addr", clientAddr,
	)
	
	// Handle the connection
	if err := s.handleConnectionLifecycle(conn); err != nil {
		if err != io.EOF {
			s.logger.Error("Connection error",
				"conn_id", conn.ID,
				"client_addr", clientAddr,
				"error", err,
			)
		}
	}
	
	s.logger.Info("Client disconnected",
		"conn_id", conn.ID,
		"client_addr", clientAddr,
	)
}

// handleConnectionLifecycle manages the connection lifecycle
func (s *Server) handleConnectionLifecycle(conn *Connection) error {
	// Handle SSL negotiation if requested
	if err := s.handleSSLNegotiation(conn); err != nil {
		return err
	}
	
	// Read startup message
	startup, err := conn.Reader.ReadStartupMessage()
	if err != nil {
		return fmt.Errorf("failed to read startup message: %w", err)
	}
	
	// Handle cancel request
	if startup.IsCancelRequest {
		return s.handleCancelRequest(conn, startup)
	}
	
	// Store connection parameters
	conn.Parameters = startup.Parameters
	conn.User = startup.Parameters["user"]
	conn.Database = startup.Parameters["database"]
	
	// Handle authentication
	if err := s.handleAuthentication(conn); err != nil {
		return err
	}
	
	// Send server parameters
	for name, value := range s.config.Parameters {
		if err := conn.Writer.WriteParameterStatus(name, value); err != nil {
			return fmt.Errorf("failed to send parameter status: %w", err)
		}
	}
	
	// Generate backend key data
	conn.ProcessID = int32(conn.ID)
	conn.SecretKey = int32(time.Now().UnixNano())
	if err := conn.Writer.WriteBackendKeyData(conn.ProcessID, conn.SecretKey); err != nil {
		return fmt.Errorf("failed to send backend key data: %w", err)
	}
	
	// Notify handler of startup
	if err := s.handler.HandleStartup(conn.ctx, conn, startup); err != nil {
		return err
	}
	
	// Send ready for query
	if err := conn.Writer.WriteReadyForQuery(conn.TxStatus); err != nil {
		return fmt.Errorf("failed to send ready for query: %w", err)
	}
	
	// Main message loop
	for {
		// Set read timeout
		if s.config.ReadTimeout > 0 {
			conn.Conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
		}
		
		msg, err := conn.Reader.ReadMessage()
		if err != nil {
			if err == io.EOF {
				// Clean disconnect
				return nil
			}
			return fmt.Errorf("failed to read message: %w", err)
		}
		
		// Reset read timeout
		conn.Conn.SetReadDeadline(time.Time{})
		
		// Set write timeout
		if s.config.WriteTimeout > 0 {
			conn.Conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
		}
		
		// Process message
		if err := s.processMessage(conn, msg); err != nil {
			// Send error response
			s.sendError(conn, err)
			
			// Check if it's a fatal error
			if _, ok := err.(*FatalError); ok {
				return err
			}
		}
		
		// Reset write timeout
		conn.Conn.SetWriteDeadline(time.Time{})
		
		// Check for termination
		if msg.Type == MsgTypeTerminate {
			s.handler.HandleTerminate(conn.ctx, conn)
			return nil
		}
	}
}

// processMessage processes a single message
func (s *Server) processMessage(conn *Connection, msg *Message) error {
	switch msg.Type {
	case MsgTypeQuery:
		query, err := ParseQueryMessage(msg.Data)
		if err != nil {
			return err
		}
		return s.handler.HandleQuery(conn.ctx, conn, query.Query)
		
	case MsgTypeParse:
		return s.handler.HandleParse(conn.ctx, conn, msg)
		
	case MsgTypeBind:
		return s.handler.HandleBind(conn.ctx, conn, msg)
		
	case MsgTypeExecute:
		return s.handler.HandleExecute(conn.ctx, conn, msg)
		
	case MsgTypeDescribe:
		return s.handler.HandleDescribe(conn.ctx, conn, msg)
		
	case MsgTypeClose:
		return s.handler.HandleClose(conn.ctx, conn, msg)
		
	case MsgTypeFlush:
		// Flush is a no-op for now
		return nil
		
	case MsgTypeSync:
		// Send ready for query
		return conn.Writer.WriteReadyForQuery(conn.TxStatus)
		
	case MsgTypeTerminate:
		// Will be handled by caller
		return nil
		
	default:
		return fmt.Errorf("unhandled message type: %c", msg.Type)
	}
}

// handleSSLNegotiation handles SSL/TLS negotiation
func (s *Server) handleSSLNegotiation(conn *Connection) error {
	// Peek at the first few bytes to check for SSL request
	buf := make([]byte, 8)
	n, err := conn.Conn.Read(buf)
	if err != nil {
		return err
	}
	
	if n >= 8 {
		// Check if it's an SSL request
		length := int(buf[0])<<24 | int(buf[1])<<16 | int(buf[2])<<8 | int(buf[3])
		if length == 8 {
			version := int(buf[4])<<24 | int(buf[5])<<16 | int(buf[6])<<8 | int(buf[7])
			if version == SSLRequestCode {
				// SSL requested
				if s.config.TLSConfig != nil {
					// Accept SSL
					if err := conn.Writer.WriteSSLResponse(true); err != nil {
						return err
					}
					// TODO: Upgrade connection to TLS
					s.logger.Warn("TLS upgrade not yet implemented")
				} else {
					// Reject SSL
					if err := conn.Writer.WriteSSLResponse(false); err != nil {
						return err
					}
				}
				// Continue to read actual startup message
				return nil
			}
		}
	}
	
	// Not an SSL request, push back the data
	// For simplicity, we'll create a new reader that includes the peeked data
	// In production, use a buffered connection
	return fmt.Errorf("SSL negotiation handling needs improvement")
}

// handleAuthentication handles client authentication
func (s *Server) handleAuthentication(conn *Connection) error {
	switch s.config.AuthMethod {
	case "trust", "":
		// No authentication required
		return conn.Writer.WriteAuthenticationOK()
		
	case "password":
		// Request cleartext password
		if err := conn.Writer.WriteAuthenticationCleartextPassword(); err != nil {
			return err
		}
		
		// Read password message
		msg, err := conn.Reader.ReadMessage()
		if err != nil {
			return err
		}
		
		if msg.Type != MsgTypePassword {
			return fmt.Errorf("expected password message, got %c", msg.Type)
		}
		
		// TODO: Verify password
		// For now, accept any password
		return conn.Writer.WriteAuthenticationOK()
		
	case "md5":
		// Generate salt
		salt := []byte{0x12, 0x34, 0x56, 0x78} // TODO: Generate random salt
		
		// Request MD5 password
		if err := conn.Writer.WriteAuthenticationMD5Password(salt); err != nil {
			return err
		}
		
		// Read password message
		msg, err := conn.Reader.ReadMessage()
		if err != nil {
			return err
		}
		
		if msg.Type != MsgTypePassword {
			return fmt.Errorf("expected password message, got %c", msg.Type)
		}
		
		// TODO: Verify MD5 password
		// For now, accept any password
		return conn.Writer.WriteAuthenticationOK()
		
	default:
		return fmt.Errorf("unsupported authentication method: %s", s.config.AuthMethod)
	}
}

// handleCancelRequest handles a cancel request
func (s *Server) handleCancelRequest(conn *Connection, startup *StartupMessage) error {
	// Find the connection to cancel
	var targetConn *Connection
	s.connections.Range(func(key, value interface{}) bool {
		if c, ok := value.(*Connection); ok {
			if c.ProcessID == startup.ProcessID && c.SecretKey == startup.SecretKey {
				targetConn = c
				return false // Stop iteration
			}
		}
		return true
	})
	
	if targetConn != nil {
		// Cancel the target connection's context
		targetConn.cancel()
		s.logger.Info("Cancelled connection",
			"target_conn_id", targetConn.ID,
			"process_id", startup.ProcessID,
		)
	} else {
		s.logger.Warn("Cancel request for unknown connection",
			"process_id", startup.ProcessID,
		)
	}
	
	// Close the cancel connection
	return io.EOF
}

// sendError sends an error response to the client
func (s *Server) sendError(conn *Connection, err error) {
	fields := make(map[byte]string)
	fields[ErrorFieldSeverity] = "ERROR"
	fields[ErrorFieldMessage] = err.Error()
	
	// Add SQLSTATE code if available
	if sqlErr, ok := err.(*SQLError); ok {
		fields[ErrorFieldCode] = sqlErr.Code
		if sqlErr.Detail != "" {
			fields[ErrorFieldDetail] = sqlErr.Detail
		}
		if sqlErr.Hint != "" {
			fields[ErrorFieldHint] = sqlErr.Hint
		}
	} else {
		// Default error code
		fields[ErrorFieldCode] = "58000" // system_error
	}
	
	if err := conn.Writer.WriteErrorResponse(fields); err != nil {
		s.logger.Error("Failed to send error response", "error", err)
	}
}

// Close closes the connection
func (c *Connection) Close() error {
	c.cancel()
	return c.Conn.Close()
}

// SQLError represents a PostgreSQL error with SQLSTATE code
type SQLError struct {
	Code    string
	Message string
	Detail  string
	Hint    string
}

func (e *SQLError) Error() string {
	return e.Message
}

// FatalError represents a fatal error that should close the connection
type FatalError struct {
	Err error
}

func (e *FatalError) Error() string {
	return fmt.Sprintf("fatal: %v", e.Err)
}