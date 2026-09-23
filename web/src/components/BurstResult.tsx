import { motion } from "framer-motion";
import type { BurstResult } from "../types";

interface Props {
  sync: BurstResult | null;
  kafka: BurstResult | null;
}

function cell(v: number | undefined, suffix = "") {
  if (v === undefined) return <span className="text-slate-600">—</span>;
  return (
    <span className="font-mono tabular-nums">
      {v.toLocaleString("vi-VN")}
      {suffix}
    </span>
  );
}

// Bảng p50/p95/max/số lỗi của hai chế độ cạnh nhau.
export default function BurstResultView({ sync, kafka }: Props) {
  if (!sync && !kafka) {
    return (
      <div className="rounded-2xl border border-slate-700/60 bg-slate-900/70 p-4 text-slate-500">
        Chưa bắn burst — bấm “Bắn N đơn” ở thanh điều khiển để so sánh khả năng
        chịu tải.
      </div>
    );
  }
  const rows: { label: string; get: (b: BurstResult) => number; suffix?: string }[] = [
    { label: "Nhận OK", get: (b) => b.ok },
    { label: "Lỗi", get: (b) => b.failed },
    { label: "p50", get: (b) => b.p50, suffix: "ms" },
    { label: "p95", get: (b) => b.p95, suffix: "ms" },
    { label: "max", get: (b) => b.max, suffix: "ms" },
    { label: "Tổng thời gian", get: (b) => b.elapsedMs, suffix: "ms" },
  ];
  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      className="rounded-2xl border border-slate-700/60 bg-slate-900/70 p-4 shadow-xl"
    >
      <h3 className="mb-3 text-lg font-extrabold">Kết quả bắn burst</h3>
      <table className="w-full text-sm">
        <thead>
          <tr className="text-left text-slate-400">
            <th className="pb-2 font-medium">Chỉ số</th>
            <th className="pb-2 font-semibold text-red-400">
              Không Kafka{sync ? ` (n=${sync.count})` : ""}
            </th>
            <th className="pb-2 font-semibold text-emerald-400">
              Có Kafka{kafka ? ` (n=${kafka.count})` : ""}
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.label} className="border-t border-slate-800">
              <td className="py-1.5 text-slate-300">{r.label}</td>
              <td className="py-1.5">{cell(sync ? r.get(sync) : undefined, r.suffix)}</td>
              <td className="py-1.5">{cell(kafka ? r.get(kafka) : undefined, r.suffix)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </motion.div>
  );
}
