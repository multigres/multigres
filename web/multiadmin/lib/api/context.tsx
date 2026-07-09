"use client";

import { createContext, useContext, useMemo, type ReactNode } from "react";
import { MultiadminClient } from "./client";

interface ApiContextValue {
  client: MultiadminClient;
}

const ApiContext = createContext<ApiContextValue | null>(null);

interface ApiProviderProps {
  children: ReactNode;
  baseUrl?: string;
}

// Use same-origin relative paths — Next.js rewrites proxy to multiadmin.
const DEFAULT_BASE_URL = "";

export function ApiProvider({ children, baseUrl }: ApiProviderProps) {
  const client = useMemo(
    () => new MultiadminClient({ baseUrl: baseUrl ?? DEFAULT_BASE_URL }),
    [baseUrl],
  );

  return (
    <ApiContext.Provider value={{ client }}>{children}</ApiContext.Provider>
  );
}

export function useApi(): MultiadminClient {
  const context = useContext(ApiContext);
  if (!context) {
    throw new Error("useApi must be used within an ApiProvider");
  }
  return context.client;
}
