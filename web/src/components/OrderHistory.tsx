import { motion } from "framer-motion";
import type { OrderDoc, OrderStatus } from "../types";

const statusStyle: Record<OrderStatus, string> = {
  received: "bg-slate-500/15 text-slate-300",
  processing: "bg-sky-500/15 text-sky-300",
  done: "bg-emerald-500/15 text-emerald-300",
  error: "bg-red-500/15 text-red-300",
};

const statusLabel: Record<OrderStatus, string> = {
  received: "Đã nhận",
  processing: "Đang xử lý",
  done: "Xong",
  error: "Lỗi",
};

function fmtTime(iso: string) {
  return new Date(iso).toLocaleTimeString("vi-VN", { hour12: false });
}

// Lịch sử đơn từ MongoDB — bảng full-width tầng 4.
export default function OrderHistory({ orders }: { orders: OrderDoc[] }) {
  return (
    <div className="rounded-2xl border border-slate-700/60 bg-slate-900/70 p-4 shadow-xl">
      <h3 className="mb-3 text-lg font-extrabold">
        Lịch sử đơn{" "}
        <span className="text-sm font-medium text-slate-500">
          (MongoDB · mới nhất trước)
        </span>
      </h3>
      {orders.length === 0 ? (
        <div className="text-sm text-slate-500">
          Chưa có đơn nào — đặt hàng để thấy lịch sử hiện ở đây và trong
          Compass.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="text-slate-400">
                <th className="pb-2 pr-3 font-medium">Mã đơn</th>
                <th className="pb-2 pr-3 font-medium">Chế độ</th>
                <th className="pb-2 pr-3 font-medium">Trạng thái</th>
                <th className="pb-2 pr-3 font-medium">SP</th>
                <th className="pb-2 pr-3 font-medium">Chờ (ms)</th>
                <th className="pb-2 pr-3 font-medium">Các bước</th>
                <th className="pb-2 font-medium">Lúc</th>
              </tr>
            </thead>
            <tbody>
              {orders.map((o) => (
                <motion.tr
                  key={o.id}
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  className="border-t border-slate-800"
                >
                  <td className="py-1.5 pr-3 font-mono text-xs">{o.id}</td>
                  <td className="py-1.5 pr-3">
                    <span
                      className={`rounded px-1.5 py-0.5 text-xs font-semibold ${
                        o.mode === "sync"
                          ? "bg-red-500/15 text-red-300"
                          : "bg-emerald-500/15 text-emerald-300"
                      }`}
                    >
                      {o.mode === "sync" ? "Không Kafka" : "Có Kafka"}
                    </span>
                    {o.burst && (
                      <span className="ml-1 text-xs text-slate-500">burst</span>
                    )}
                  </td>
                  <td className="py-1.5 pr-3">
                    <span
                      className={`rounded px-1.5 py-0.5 text-xs font-semibold ${statusStyle[o.status]}`}
                    >
                      {statusLabel[o.status]}
                    </span>
                  </td>
                  <td className="py-1.5 pr-3 font-mono text-xs">
                    {o.productId}×{o.qty}
                  </td>
                  <td className="py-1.5 pr-3 font-mono tabular-nums">
                    {o.totalMs}
                  </td>
                  <td className="py-1.5 pr-3 font-mono text-xs">
                    {o.steps.length === 0 ? (
                      <span className="text-slate-600">—</span>
                    ) : (
                      o.steps.map((s) => (
                        <span
                          key={s.service}
                          title={`${s.service}: ${s.message} (${s.durationMs}ms)`}
                          className={`mr-1 ${s.status === "done" ? "text-emerald-400" : s.status === "error" ? "text-red-400" : "text-sky-400"}`}
                        >
                          {s.status === "done" ? "✓" : s.status === "error" ? "✗" : "…"}
                          {s.service.slice(0, 3)}
                        </span>
                      ))
                    )}
                  </td>
                  <td className="py-1.5 font-mono text-xs text-slate-500">
                    {fmtTime(o.createdAt)}
                  </td>
                </motion.tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
