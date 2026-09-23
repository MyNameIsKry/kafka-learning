import { AnimatePresence, motion } from "framer-motion";
import type { Mode, StepLog } from "../types";
import { SERVICE_LABEL } from "../types";

interface Props {
  mode: Mode;
  logs: StepLog[];
}

function fmtTime(iso: string) {
  const d = new Date(iso);
  return d.toLocaleTimeString("vi-VN", { hour12: false });
}

const dot: Record<string, string> = {
  start: "bg-sky-400",
  done: "bg-emerald-400",
  error: "bg-red-400",
};

// Realtime log lines. Kafka rows also show instance, partition, offset.
// Cửa sổ cố định có scroll: log mới nhất trên cùng, cuộn xem cũ.
export default function LogList({ mode, logs }: Props) {
  const shown = logs.slice(0, 200);
  return (
    <div className="flex max-h-80 flex-col gap-1 overflow-y-auto rounded-xl border border-slate-700/60 bg-slate-950/60 p-3 font-mono text-xs">
      <div className="mb-1 font-sans text-sm font-semibold text-slate-300">
        Log realtime
      </div>
      {shown.length === 0 && (
        <div className="text-slate-600">Chưa có log — hãy đặt một đơn.</div>
      )}
      <AnimatePresence initial={false}>
        {shown.map((l) => (
          <motion.div
            key={l.id}
            initial={{ opacity: 0, y: -8 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.25 }}
            className="flex items-start gap-2 leading-relaxed"
          >
            <span
              className={`mt-1.5 h-2 w-2 shrink-0 rounded-full ${dot[l.status] ?? "bg-slate-500"}`}
            />
            <span className="shrink-0 text-slate-500">{fmtTime(l.at)}</span>
            <span className="shrink-0 font-bold text-slate-200">
              [{SERVICE_LABEL[l.service] ?? l.service}]
            </span>
            {l.burst && (
              <span className="shrink-0 rounded bg-slate-700/60 px-1 text-[10px] text-slate-400">
                burst
              </span>
            )}
            <span className="break-words text-slate-300">{l.message}</span>
            {mode === "kafka" && l.partition >= 0 && (
              <span className="ml-auto shrink-0 text-slate-500">
                {l.instance} · p{l.partition} · @{l.offset}
              </span>
            )}
            {l.durationMs > 0 && (
              <span className="shrink-0 text-slate-500">
                {l.durationMs}ms
              </span>
            )}
          </motion.div>
        ))}
      </AnimatePresence>
    </div>
  );
}
