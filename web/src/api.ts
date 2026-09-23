import type {
  AsyncResult,
  BurstResult,
  Mode,
  OrderDoc,
  StepLog,
  SyncResult,
  SystemState,
} from "./types";

export const API_URL =
  import.meta.env.VITE_API_URL ?? "http://localhost:8080";
export const WS_URL = API_URL.replace(/^http/, "ws") + "/ws";

export interface OrderQuery {
  mode?: string;
  status?: string;
  productId?: string;
  burst?: string;
  page?: number;
}

export interface OrderPage {
  orders: OrderDoc[];
  total: number;
  page: number;
  pages: number;
}

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(API_URL + path, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(
      (body as { message?: string }).message ?? `HTTP ${res.status}`,
    );
  }
  return res.json() as Promise<T>;
}

export const api = {
  orderSync: (productId: string, qty: number) =>
    req<SyncResult>("/api/orders/sync", {
      method: "POST",
      body: JSON.stringify({ productId, qty }),
    }),
  orderAsync: (productId: string, qty: number) =>
    req<AsyncResult>("/api/orders/async", {
      method: "POST",
      body: JSON.stringify({ productId, qty }),
    }),
  burst: (mode: Mode, count: number, productId: string) =>
    req<BurstResult>("/api/burst", {
      method: "POST",
      body: JSON.stringify({ mode, count, productId }),
    }),
  state: () => req<SystemState>("/api/state"),
  logs: () => req<StepLog[]>("/api/logs"),
  orders: (q: OrderQuery = {}) => {
    const p = new URLSearchParams();
    if (q.mode) p.set("mode", q.mode);
    if (q.status) p.set("status", q.status);
    if (q.productId) p.set("productId", q.productId);
    if (q.burst) p.set("burst", q.burst);
    p.set("page", String(q.page ?? 1));
    p.set("limit", "10");
    return req<OrderPage>(`/api/orders?${p.toString()}`);
  },
  reset: () => req<{ ok: boolean }>("/api/reset", { method: "POST" }),
  serviceToggle: (name: string, down: boolean) =>
    req<{ ok: boolean }>(`/api/services/${name}/${down ? "down" : "up"}`, {
      method: "POST",
    }),
};
