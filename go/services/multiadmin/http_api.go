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

package multiadmin

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// apiPrefix is the base path for all API endpoints
const apiPrefix = "/api/v1"

// registerAPIHandlers registers all HTTP API endpoints
func (ma *MultiAdmin) registerAPIHandlers() {
	// Cell endpoints
	ma.senv.HTTPHandleFunc(apiPrefix+"/cells", ma.handleAPICells)
	ma.senv.HTTPHandleFunc(apiPrefix+"/cells/", ma.handleAPICellByName)

	// Database endpoints
	ma.senv.HTTPHandleFunc(apiPrefix+"/databases", ma.handleAPIDatabases)
	ma.senv.HTTPHandleFunc(apiPrefix+"/databases/", ma.handleAPIDatabaseByName)

	// Service discovery endpoints
	ma.senv.HTTPHandleFunc(apiPrefix+"/gateways", ma.handleAPIGateways)
	ma.senv.HTTPHandleFunc(apiPrefix+"/poolers", ma.handleAPIPoolers)
	ma.senv.HTTPHandleFunc(apiPrefix+"/orchs", ma.handleAPIOrchestrators)

	// Backup endpoints
	ma.senv.HTTPHandleFunc(apiPrefix+"/backups", ma.handleAPIBackups)
	ma.senv.HTTPHandleFunc(apiPrefix+"/restores", ma.handleAPIRestores)
	ma.senv.HTTPHandleFunc(apiPrefix+"/jobs/", ma.handleAPIJobStatus)
}

// ErrorResponse represents an API error response
type ErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// writeProtoJSON writes a protobuf message as JSON response
func writeProtoJSON(w http.ResponseWriter, msg proto.Message) {
	w.Header().Set("Content-Type", "application/json")
	marshaler := protojson.MarshalOptions{
		EmitUnpopulated: true,
		UseProtoNames:   true,
	}
	data, err := marshaler.Marshal(msg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, codes.Internal, "failed to marshal response")
		return
	}
	_, _ = w.Write(data)
}

// writeError writes an error response using a gRPC code
func writeError(w http.ResponseWriter, httpCode int, code codes.Code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpCode)
	_ = json.NewEncoder(w).Encode(ErrorResponse{
		Error: message,
		Code:  code.String(),
	})
}

// grpcCodeToHTTP converts gRPC status codes to HTTP status codes
func grpcCodeToHTTP(code codes.Code) int {
	switch code {
	case codes.OK:
		return http.StatusOK
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists:
		return http.StatusConflict
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// writeGRPCError writes an error from a gRPC call
func writeGRPCError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		writeError(w, http.StatusInternalServerError, codes.Internal, err.Error())
		return
	}
	writeError(w, grpcCodeToHTTP(st.Code()), st.Code(), st.Message())
}

// extractPathParam extracts the parameter after the prefix from the URL path
// e.g., extractPathParam("/api/v1/cells/zone1", "/api/v1/cells/") returns "zone1"
func extractPathParam(path, prefix string) string {
	return strings.TrimPrefix(path, prefix)
}

// parseCellsParam parses a comma-separated cells query parameter
func parseCellsParam(r *http.Request) []string {
	cellsStr := r.URL.Query().Get("cells")
	if cellsStr == "" {
		return nil
	}
	return strings.Split(cellsStr, ",")
}

// --- Cell Endpoints ---

// handleAPICells handles GET /api/v1/cells
func (ma *MultiAdmin) handleAPICells(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, codes.InvalidArgument, "method not allowed")
		return
	}

	resp, err := ma.adminServer.GetCellNames(r.Context(), &multiadminpb.GetCellNamesRequest{})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	writeProtoJSON(w, resp)
}

// handleAPICellByName handles GET /api/v1/cells/{name}
func (ma *MultiAdmin) handleAPICellByName(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, codes.InvalidArgument, "method not allowed")
		return
	}

	name := extractPathParam(r.URL.Path, apiPrefix+"/cells/")
	if name == "" {
		writeError(w, http.StatusBadRequest, codes.InvalidArgument, "cell name required")
		return
	}

	resp, err := ma.adminServer.GetCell(r.Context(), &multiadminpb.GetCellRequest{Name: name})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	writeProtoJSON(w, resp)
}

// --- Database Endpoints ---

// handleAPIDatabases handles GET /api/v1/databases
func (ma *MultiAdmin) handleAPIDatabases(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, codes.InvalidArgument, "method not allowed")
		return
	}

	resp, err := ma.adminServer.GetDatabaseNames(r.Context(), &multiadminpb.GetDatabaseNamesRequest{})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	writeProtoJSON(w, resp)
}

// handleAPIDatabaseByName handles GET /api/v1/databases/{name}
func (ma *MultiAdmin) handleAPIDatabaseByName(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, codes.InvalidArgument, "method not allowed")
		return
	}

	name := extractPathParam(r.URL.Path, apiPrefix+"/databases/")
	if name == "" {
		writeError(w, http.StatusBadRequest, codes.InvalidArgument, "database name required")
		return
	}

	resp, err := ma.adminServer.GetDatabase(r.Context(), &multiadminpb.GetDatabaseRequest{Name: name})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	writeProtoJSON(w, resp)
}

// --- Service Discovery Endpoints ---

// handleAPIGateways handles GET /api/v1/gateways
func (ma *MultiAdmin) handleAPIGateways(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, codes.InvalidArgument, "method not allowed")
		return
	}

	req := &multiadminpb.GetGatewaysRequest{
		Cells: parseCellsParam(r),
	}

	resp, err := ma.adminServer.GetGateways(r.Context(), req)
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	writeProtoJSON(w, resp)
}

// handleAPIPoolers handles GET /api/v1/poolers
func (ma *MultiAdmin) handleAPIPoolers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, codes.InvalidArgument, "method not allowed")
		return
	}

	req := &multiadminpb.GetPoolersRequest{
		Cells:    parseCellsParam(r),
		Database: r.URL.Query().Get("database"),
		Shard:    r.URL.Query().Get("shard"),
	}

	resp, err := ma.adminServer.GetPoolers(r.Context(), req)
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	writeProtoJSON(w, resp)
}

// handleAPIOrchestrators handles GET /api/v1/orchs
func (ma *MultiAdmin) handleAPIOrchestrators(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, codes.InvalidArgument, "method not allowed")
		return
	}

	req := &multiadminpb.GetOrchsRequest{
		Cells: parseCellsParam(r),
	}

	resp, err := ma.adminServer.GetOrchs(r.Context(), req)
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	writeProtoJSON(w, resp)
}

// --- Backup Endpoints ---

// handleAPIBackups handles GET /api/v1/backups (list) and POST /api/v1/backups (create)
func (ma *MultiAdmin) handleAPIBackups(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ma.handleAPIGetBackups(w, r)
	case http.MethodPost:
		ma.handleAPICreateBackup(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, codes.InvalidArgument, "method not allowed")
	}
}

// handleAPIGetBackups handles GET /api/v1/backups
func (ma *MultiAdmin) handleAPIGetBackups(w http.ResponseWriter, r *http.Request) {
	req := &multiadminpb.GetBackupsRequest{
		Database:   r.URL.Query().Get("database"),
		TableGroup: r.URL.Query().Get("table_group"),
		Shard:      r.URL.Query().Get("shard"),
	}

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		limit, err := strconv.ParseUint(limitStr, 10, 32)
		if err != nil {
			writeError(w, http.StatusBadRequest, codes.InvalidArgument, "invalid limit parameter")
			return
		}
		req.Limit = uint32(limit)
	}

	resp, err := ma.adminServer.GetBackups(r.Context(), req)
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	writeProtoJSON(w, resp)
}

// handleAPICreateBackup handles POST /api/v1/backups
func (ma *MultiAdmin) handleAPICreateBackup(w http.ResponseWriter, r *http.Request) {
	var req multiadminpb.BackupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, codes.InvalidArgument, "invalid request body: "+err.Error())
		return
	}

	resp, err := ma.adminServer.Backup(r.Context(), &req)
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	writeProtoJSON(w, resp)
}

// handleAPIRestores handles POST /api/v1/restores
func (ma *MultiAdmin) handleAPIRestores(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, codes.InvalidArgument, "method not allowed")
		return
	}

	var req multiadminpb.RestoreFromBackupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, codes.InvalidArgument, "invalid request body: "+err.Error())
		return
	}

	resp, err := ma.adminServer.RestoreFromBackup(r.Context(), &req)
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	writeProtoJSON(w, resp)
}

// handleAPIJobStatus handles GET /api/v1/jobs/{job_id}
func (ma *MultiAdmin) handleAPIJobStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, codes.InvalidArgument, "method not allowed")
		return
	}

	jobID := extractPathParam(r.URL.Path, apiPrefix+"/jobs/")
	if jobID == "" {
		writeError(w, http.StatusBadRequest, codes.InvalidArgument, "job_id required")
		return
	}

	req := &multiadminpb.GetBackupJobStatusRequest{
		JobId:      jobID,
		Database:   r.URL.Query().Get("database"),
		TableGroup: r.URL.Query().Get("table_group"),
		Shard:      r.URL.Query().Get("shard"),
	}

	resp, err := ma.adminServer.GetBackupJobStatus(r.Context(), req)
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	writeProtoJSON(w, resp)
}
