import { motion } from "framer-motion";
import FlowDiagram, {
  type DiagramEdge,
  type DiagramNode,
} from "./FlowDiagram";
import LatencyBar from "./LatencyBar";
import LogList from "./LogList";
import type { Mode, StepLog } from "../types";
import { SERVICE_LABEL } from "../types";

export interface TimelineItem {
  service: string;
  state: "idle" | "running" | "done" | "error";
  durationMs: number;
  message: string;
}

interface Props {
  mode: Mode;
  title: string;
  subtitle: string;
  accent: "red" | "emerald";
  latencyMs: number | null;
  ratio: number;
  steps: TimelineItem[];
  logs: StepLog[];
  active: Record<string, boolean>;
  down: Record<string, boolean>;
  pulses: Record<string, number>;
  burstDone: number;
  orderId: string | null;
}

const SYNC_NODES: DiagramNode[] = [
  { id: "gateway", label: "Gateway", x: 52, y: 115 },
  { id: "inventory", label: "Kho", x: 152, y: 115 },
  { id: "email", label: "Mail", x: 252, y: 115 },
  { id: "shipping", label: "Ship", x: 352, y: 115 },
];
const SYNC_EDGES: DiagramEdge[] = [
  { id: "gateway-inventory", from: "gateway", to: "inventory" },
  { id: "inventory-email", from: "inventory", to: "email" },
  { id: "email-shipping", from: "email", to: "shipping" },
];

const KAFKA_NODES: DiagramNode[] = [
  { id: "gateway", label: "Gateway", x: 52, y: 115 },
  { id: "topic", label: "Topic orders", sub: "3 partitions", x: 172, y: 115, wide: true },
  { id: "inventory", label: "Kho", x: 308, y: 40 },
  { id: "email", label: "Mail", x: 308, y: 115 },
  { id: "shipping", label: "Ship", x: 308, y: 190 },
];
const KAFKA_EDGES: DiagramEdge[] = [
  { id: "gateway-topic", from: "gateway", to: "topic" },
  { id: "topic-inventory", from: "topic", to: "inventory" },
  { id: "topic-email", from: "topic", to: "email" },
  { id: "topic-shipping", from: "topic", to: "shipping" },
];

const icon: Record<TimelineItem["state"], string> = {
  idle: "○",
  running: "⏳",
  done: "✓",
  error: "✗",
};
const iconColor: Record<TimelineItem["state"], string> = {
  idle: "text-slate-600",
  running: "text-sky-400",
  done: "text-emerald-400",
  error: "text-red-400",
};

export default function FlowColumn(p: Props) {
  const nodes = p.mode === "sync" ? SYNC_NODES : KAFKA_NODES;
  const edges = p.mode === "sync" ? SYNC_EDGES : KAFKA_EDGES;
  const border =
    p.accent === "red" ? "border-red-500/40" : "border-emerald-500/40";
  const title =
    p.accent === "red" ? "text-red-400" : "text-emerald-400";

  return (
    <section
      className={`flex flex-col gap-3 rounded-2xl border ${border} bg-slate-900/70 p-4 shadow-xl`}
    >
      <div>
        <h2 className={`text-xl font-extrabold ${title}`}>{p.title}</h2>
        <p className="text-sm text-slate-400">{p.subtitle}</p>
      </div>

      <FlowDiagram
        nodes={nodes}
        edges={edges}
        active={p.active}
        down={p.down}
        pulses={p.pulses}
        accent={p.accent === "red" ? "#f87171" : "#34d399"}
      />

      <LatencyBar
        label="Người dùng phải chờ"
        ms={p.latencyMs}
        ratio={p.ratio}
        accent={p.accent}
      />

      <div className="flex flex-col gap-1.5 rounded-xl border border-slate-700/60 bg-slate-950/60 p-3">
        <div className="flex items-center justify-between text-sm">
          <span className="font-semibold text-slate-300">Các bước</span>
          {p.orderId && (
            <span className="font-mono text-xs text-slate-500">
              {p.orderId}
            </span>
          )}
        </div>
        {p.steps.map((s) => (
          <motion.div
            key={s.service}
            layout
            className="flex items-center gap-2 text-sm"
          >
            <span className={`w-5 text-center font-bold ${iconColor[s.state]}`}>
              {s.state === "running" ? (
                <motion.span
                  className="inline-block"
                  animate={{ rotate: 360 }}
                  transition={{ duration: 1.2, repeat: Infinity, ease: "linear" }}
                >
                  {icon[s.state]}
                </motion.span>
              ) : (
                icon[s.state]
              )}
            </span>
            <span className="w-16 font-semibold text-slate-200">
              {SERVICE_LABEL[s.service] ?? s.service}
            </span>
            <span className="flex-1 truncate text-slate-400">{s.message}</span>
            {s.durationMs > 0 && (
              <span className="font-mono text-xs text-slate-500">
                {s.durationMs}ms
              </span>
            )}
          </motion.div>
        ))}
        {p.burstDone > 0 && (
          <div className="pt-1 font-mono text-xs text-slate-500">
            burst: {p.burstDone} đơn đã xử lý xong
          </div>
        )}
      </div>

      <LogList mode={p.mode} logs={p.logs} />
    </section>
  );
}
