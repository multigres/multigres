"use client";

import { createContext, useContext, useMemo, type ReactNode } from "react";
import { MultiAdminClient } from "./client";

interface ApiContextValue {
  client: MultiAdminClient;
}

const ApiContext = createContext<ApiContextValue | null>(null);

interface ApiProviderProps {
  children: ReactNode;
  baseUrl?: string;
}

// Default to multiadmin API port for local development
const DEFAULT_BASE_URL =
  process.env.NEXT_PUBLIC_MULTIADMIN_API_URL || "http://localhost:15000";

export function ApiProvider({ children, baseUrl }: ApiProviderProps) {
  const client = useMemo(
    () => new MultiAdminClient({ baseUrl: baseUrl || DEFAULT_BASE_URL }),
    [baseUrl],
  );

  return (
    <ApiContext.Provider value={{ client }}>{children}</ApiContext.Provider>
  );
}

export function useApi(): MultiAdminClient {
  const context = useContext(ApiContext);
  if (!context) {
    throw new Error("useApi must be used within an ApiProvider");
  }
  return context.client;
}
