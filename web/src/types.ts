// Mirror của server/internal/events và internal/mongo (chỉ thêm kiểu cho UI).
export type Mode = "sync" | "kafka";
export type LogStatus = "start" | "done" | "error";

export interface StepLog {
  id: string;
  mode: Mode;
  service: string;
  instance: string;
  orderId: string;
  productId: string;
  qty: number;
  status: LogStatus;
  message: string;
  partition: number;
  offset: number;
  durationMs: number;
  burst: boolean;
  at: string;
}

export interface OrderStep {
  service: string;
  status: LogStatus;
  message: string;
  durationMs: number;
  at: string;
}

export type OrderStatus = "received" | "processing" | "done" | "error";

export interface OrderDoc {
  id: string;
  productId: string;
  qty: number;
  mode: Mode;
  burst: boolean;
  status: OrderStatus;
  steps: OrderStep[];
  totalMs: number;
  createdAt: string;
  updatedAt: string;
}

export interface SyncStep {
  service: string;
  ok: boolean;
  durationMs: number;
  message: string;
}

export interface SyncResult {
  orderId: string;
  totalMs: number;
  steps: SyncStep[];
  ok: boolean;
}

export interface AsyncResult {
  orderId: string;
  totalMs: number;
}

export interface BurstResult {
  mode: Mode;
  count: number;
  ok: number;
  failed: number;
  p50: number;
  p95: number;
  max: number;
  elapsedMs: number;
}

export interface ServiceState {
  name: string;
  instance: string;
  down: boolean;
  processed: number;
}

export interface SystemState {
  stock: Record<string, number>;
  services: ServiceState[];
}

export const FLOW_SERVICES = ["inventory", "email", "shipping"] as const;

export const SERVICE_LABEL: Record<string, string> = {
  gateway: "Gateway",
  inventory: "Kho",
  email: "Mail",
  shipping: "Ship",
};
