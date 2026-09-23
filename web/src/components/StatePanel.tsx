import { motion } from "framer-motion";
import type { SystemState } from "../types";
import { SERVICE_LABEL } from "../types";

// Tồn kho từng sản phẩm + số đơn mỗi service đã xử lý + service đang tắt.
export default function StatePanel({ state }: { state: SystemState | null }) {
  if (!state) {
    return (
      <div className="rounded-2xl border border-slate-700/60 bg-slate-900/70 p-4 text-slate-500">
        Đang tải trạng thái…
      </div>
    );
  }
  return (
    <div className="rounded-2xl border border-slate-700/60 bg-slate-900/70 p-4 shadow-xl">
      <h3 className="mb-3 text-lg font-extrabold">Trạng thái hệ thống</h3>

      <div className="mb-4 flex flex-col gap-2">
        {Object.entries(state.stock).map(([id, qty]) => (
          <div key={id} className="flex items-center gap-3">
            <span className="w-14 font-mono text-sm text-slate-300">{id}</span>
            <div className="h-2.5 flex-1 overflow-hidden rounded-full bg-slate-800">
              <motion.div
                className={`h-full rounded-full ${qty === 0 ? "bg-red-500" : qty < 20 ? "bg-amber-400" : "bg-sky-500"}`}
                initial={false}
                animate={{ width: `${Math.min(100, qty)}%` }}
                transition={{ type: "spring", stiffness: 120, damping: 20 }}
              />
            </div>
            <span className="w-10 text-right font-mono text-sm tabular-nums">
              {qty}
            </span>
          </div>
        ))}
      </div>

      <div className="flex flex-wrap gap-2">
        {state.services.map((s) => (
          <div
            key={s.name + s.instance}
            className={`flex items-center gap-2 rounded-lg border px-3 py-1.5 text-sm ${
              s.down
                ? "border-red-500/50 bg-red-950/60 text-red-300"
                : "border-slate-700 bg-slate-950/60 text-slate-300"
            }`}
          >
            <span className="font-semibold">
              {SERVICE_LABEL[s.name] ?? s.name}
            </span>
            <span className="font-mono text-xs text-slate-500">
              {s.instance}
            </span>
            <span className="font-mono text-xs tabular-nums">
              {s.processed} đơn
            </span>
            {s.down && (
              <span className="rounded bg-red-500 px-1.5 py-0.5 text-xs font-bold text-white">
                TẮT
              </span>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
