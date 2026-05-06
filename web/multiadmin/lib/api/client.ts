import { createPromiseClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { MultiAdminService } from "./generated/multiadminservice_connect.js";
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
  GetGatewayQueriesResponse,
  GetGatewayConsolidatorResponse,
} from "./types";

export interface ApiClientConfig {
  baseUrl: string;
}

export class MultiAdminClient {
  private client: ReturnType<
    typeof createPromiseClient<typeof MultiAdminService>
  >;

  constructor(config: ApiClientConfig) {
    const baseUrl = config.baseUrl.replace(/\/$/, "");
    const transport = createConnectTransport({ baseUrl });
    this.client = createPromiseClient(MultiAdminService, transport);
  }

  // Cell operations

  async getCellNames(): Promise<GetCellNamesResponse> {
    return this.client.getCellNames({});
  }

  async getCell(name: string): Promise<GetCellResponse> {
    return this.client.getCell({ name });
  }

  // Database operations

  async getDatabaseNames(): Promise<GetDatabaseNamesResponse> {
    return this.client.getDatabaseNames({});
  }

  async getDatabase(name: string): Promise<GetDatabaseResponse> {
    return this.client.getDatabase({ name });
  }

  // Gateway operations

  async getGateways(cells?: string[]): Promise<GetGatewaysResponse> {
    return this.client.getGateways({ cells: cells ?? [] });
  }

  // Pooler operations

  async getPoolers(options?: {
    cells?: string[];
    database?: string;
    shard?: string;
  }): Promise<GetPoolersResponse> {
    return this.client.getPoolers({
      cells: options?.cells ?? [],
      database: options?.database ?? "",
      shard: options?.shard ?? "",
    });
  }

  // Orchestrator operations

  async getOrchs(cells?: string[]): Promise<GetOrchsResponse> {
    return this.client.getOrchs({ cells: cells ?? [] });
  }

  // Backup operations

  async backup(request: BackupRequest): Promise<BackupResponse> {
    return this.client.backup(request);
  }

  async restoreFromBackup(
    request: RestoreFromBackupRequest,
  ): Promise<RestoreFromBackupResponse> {
    return this.client.restoreFromBackup(request);
  }

  async getBackupJobStatus(
    request: GetBackupJobStatusRequest,
  ): Promise<GetBackupJobStatusResponse> {
    return this.client.getBackupJobStatus(request);
  }

  async getBackups(request?: GetBackupsRequest): Promise<GetBackupsResponse> {
    return this.client.getBackups(request ?? {});
  }

  // Pooler Status operations

  async getPoolerStatus(poolerId: {
    cell: string;
    name: string;
  }): Promise<GetPoolerStatusResponse> {
    return this.client.getPoolerStatus({ poolerId });
  }

  // Gateway diagnostics

  async getGatewayQueries(
    gatewayId: { cell: string; name: string },
    options?: { limit?: number; minCalls?: number },
  ): Promise<GetGatewayQueriesResponse> {
    return this.client.getGatewayQueries({
      gatewayId,
      limit: options?.limit ?? 0,
      minCalls: options?.minCalls ? BigInt(options.minCalls) : undefined,
    });
  }

  async getGatewayConsolidator(gatewayId: {
    cell: string;
    name: string;
  }): Promise<GetGatewayConsolidatorResponse> {
    return this.client.getGatewayConsolidator({ gatewayId });
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
