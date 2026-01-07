// API client for MultiAdmin service
// Connects to the multiadmin HTTP/gRPC-gateway endpoints

import type {
  GetCellNamesResponse,
  GetCellResponse,
  GetDatabaseNamesResponse,
  GetDatabaseResponse,
  GetGatewaysResponse,
  GetPoolersResponse,
  GetOrchsResponse,
  BackupRequest,
  BackupResponse,
  RestoreFromBackupRequest,
  RestoreFromBackupResponse,
  GetBackupJobStatusRequest,
  GetBackupJobStatusResponse,
  GetBackupsRequest,
  GetBackupsResponse,
  GetPoolerStatusResponse,
  ID,
} from "./types";

export interface ApiClientConfig {
  baseUrl: string;
}

export class MultiAdminClient {
  private baseUrl: string;

  constructor(config: ApiClientConfig) {
    // Remove trailing slash if present
    this.baseUrl = config.baseUrl.replace(/\/$/, "");
  }

  private async fetch<T>(path: string, options?: RequestInit): Promise<T> {
    const url = `${this.baseUrl}${path}`;

    try {
      const response = await fetch(url, {
        ...options,
        headers: {
          "Content-Type": "application/json",
          ...options?.headers,
        },
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new ApiError(response.status, errorText, url);
      }

      return response.json();
    } catch (error) {
      // If already an ApiError, rethrow it
      if (error instanceof ApiError) {
        throw error;
      }

      // Network error or other fetch failure
      throw new ApiError(
        0,
        error instanceof Error ? error.message : "Network request failed",
        url,
      );
    }
  }

  // Cell operations

  async getCellNames(): Promise<GetCellNamesResponse> {
    return this.fetch<GetCellNamesResponse>("/api/v1/cells");
  }

  async getCell(name: string): Promise<GetCellResponse> {
    return this.fetch<GetCellResponse>(
      `/api/v1/cells/${encodeURIComponent(name)}`,
    );
  }

  // Database operations

  async getDatabaseNames(): Promise<GetDatabaseNamesResponse> {
    return this.fetch<GetDatabaseNamesResponse>("/api/v1/databases");
  }

  async getDatabase(name: string): Promise<GetDatabaseResponse> {
    return this.fetch<GetDatabaseResponse>(
      `/api/v1/databases/${encodeURIComponent(name)}`,
    );
  }

  // Gateway operations

  async getGateways(cells?: string[]): Promise<GetGatewaysResponse> {
    const params = new URLSearchParams();
    if (cells && cells.length > 0) {
      cells.forEach((cell) => params.append("cells", cell));
    }
    const query = params.toString();
    return this.fetch<GetGatewaysResponse>(
      `/api/v1/gateways${query ? `?${query}` : ""}`,
    );
  }

  // Pooler operations

  async getPoolers(options?: {
    cells?: string[];
    database?: string;
    shard?: string;
  }): Promise<GetPoolersResponse> {
    const params = new URLSearchParams();
    if (options?.cells && options.cells.length > 0) {
      options.cells.forEach((cell) => params.append("cells", cell));
    }
    if (options?.database) {
      params.append("database", options.database);
    }
    if (options?.shard) {
      params.append("shard", options.shard);
    }
    const query = params.toString();
    return this.fetch<GetPoolersResponse>(
      `/api/v1/poolers${query ? `?${query}` : ""}`,
    );
  }

  // Orchestrator operations

  async getOrchs(cells?: string[]): Promise<GetOrchsResponse> {
    const params = new URLSearchParams();
    if (cells && cells.length > 0) {
      cells.forEach((cell) => params.append("cells", cell));
    }
    const query = params.toString();
    return this.fetch<GetOrchsResponse>(
      `/api/v1/orchs${query ? `?${query}` : ""}`,
    );
  }

  // Backup operations

  async backup(request: BackupRequest): Promise<BackupResponse> {
    return this.fetch<BackupResponse>("/api/v1/backups", {
      method: "POST",
      body: JSON.stringify({
        database: request.database,
        table_group: request.tableGroup,
        shard: request.shard,
        type: request.type,
        force_primary: request.forcePrimary,
      }),
    });
  }

  async restoreFromBackup(
    request: RestoreFromBackupRequest,
  ): Promise<RestoreFromBackupResponse> {
    return this.fetch<RestoreFromBackupResponse>("/api/v1/restores", {
      method: "POST",
      body: JSON.stringify({
        database: request.database,
        table_group: request.tableGroup,
        shard: request.shard,
        backup_id: request.backupId,
        pooler_id: request.poolerId,
      }),
    });
  }

  async getBackupJobStatus(
    request: GetBackupJobStatusRequest,
  ): Promise<GetBackupJobStatusResponse> {
    const params = new URLSearchParams();
    if (request.database) {
      params.append("database", request.database);
    }
    if (request.tableGroup) {
      params.append("table_group", request.tableGroup);
    }
    if (request.shard) {
      params.append("shard", request.shard);
    }
    const query = params.toString();
    return this.fetch<GetBackupJobStatusResponse>(
      `/api/v1/jobs/${encodeURIComponent(request.jobId)}${query ? `?${query}` : ""}`,
    );
  }

  async getBackups(request?: GetBackupsRequest): Promise<GetBackupsResponse> {
    const params = new URLSearchParams();
    if (request?.database) {
      params.append("database", request.database);
    }
    if (request?.tableGroup) {
      params.append("table_group", request.tableGroup);
    }
    if (request?.shard) {
      params.append("shard", request.shard);
    }
    if (request?.limit) {
      params.append("limit", request.limit.toString());
    }
    const query = params.toString();
    return this.fetch<GetBackupsResponse>(
      `/api/v1/backups${query ? `?${query}` : ""}`,
    );
  }

  // Pooler Status operations

  async getPoolerStatus(poolerId: ID): Promise<GetPoolerStatusResponse> {
    return this.fetch<GetPoolerStatusResponse>(
      `/api/v1/poolers/${encodeURIComponent(poolerId.cell)}/${encodeURIComponent(poolerId.name)}/status`,
    );
  }
}

export class ApiError extends Error {
  constructor(
    public status: number,
    public body: string,
    public url: string,
  ) {
    super(`API error ${status}: ${body}`);
    this.name = "ApiError";
  }
}
