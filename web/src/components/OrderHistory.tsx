import { useCallback, useEffect, useState } from "react";
import { motion } from "framer-motion";
import { api } from "../api";
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

const selectCls =
  "rounded-lg border border-slate-700 bg-slate-800 px-2 py-1.5 text-sm";

// Lịch sử đơn từ MongoDB — bảng full-width tầng 4, filter + phân trang.
export default function OrderHistory({ refreshKey }: { refreshKey: number }) {
  const [mode, setMode] = useState("");
  const [status, setStatus] = useState("");
  const [productId, setProductId] = useState("");
  const [burst, setBurst] = useState("");
  const [page, setPage] = useState(1);
  const [orders, setOrders] = useState<OrderDoc[]>([]);
  const [total, setTotal] = useState(0);
  const [pages, setPages] = useState(0);

  const load = useCallback(async () => {
    try {
      const res = await api.orders({ mode, status, productId, burst, page });
      // Thủ thế shape lạ (vd gateway cũ trả mảng thuần): không bao giờ crash.
      setOrders(Array.isArray(res.orders) ? res.orders : []);
      setTotal(typeof res.total === "number" ? res.total : 0);
      setPages(typeof res.pages === "number" ? res.pages : 0);
    } catch {
      // Bỏ qua, lần poll sau thử lại.
    }
  }, [mode, status, productId, burst, page]);

  useEffect(() => {
    load();
    const t = setInterval(load, 5000);
    return () => clearInterval(t);
  }, [load, refreshKey]);

  const pick = (setter: (v: string) => void) => (v: string) => {
    setter(v);
    setPage(1);
  };

  return (
    <div className="rounded-2xl border border-slate-700/60 bg-slate-900/70 p-4 shadow-xl">
      <div className="mb-3 flex flex-wrap items-center gap-3">
        <h3 className="text-lg font-extrabold">
          Lịch sử đơn{" "}
          <span className="text-sm font-medium text-slate-500">
            (MongoDB · mới nhất trước · {total} đơn)
          </span>
        </h3>
        <span className="mx-1 hidden h-6 w-px bg-slate-700/70 sm:block" />
        <label className="flex items-center gap-1.5 text-sm text-slate-300">
          Chế độ
          <select
            value={mode}
            onChange={(e) => pick(setMode)(e.target.value)}
            className={selectCls}
          >
            <option value="">Tất cả</option>
            <option value="sync">Không Kafka</option>
            <option value="kafka">Có Kafka</option>
          </select>
        </label>
        <label className="flex items-center gap-1.5 text-sm text-slate-300">
          Trạng thái
          <select
            value={status}
            onChange={(e) => pick(setStatus)(e.target.value)}
            className={selectCls}
          >
            <option value="">Tất cả</option>
            <option value="received">Đã nhận</option>
            <option value="processing">Đang xử lý</option>
            <option value="done">Xong</option>
            <option value="error">Lỗi</option>
          </select>
        </label>
        <label className="flex items-center gap-1.5 text-sm text-slate-300">
          Sản phẩm
          <select
            value={productId}
            onChange={(e) => pick(setProductId)(e.target.value)}
            className={selectCls}
          >
            <option value="">Tất cả</option>
            <option>SP-01</option>
            <option>SP-02</option>
            <option>SP-03</option>
          </select>
        </label>
        <label className="flex items-center gap-1.5 text-sm text-slate-300">
          Burst
          <select
            value={burst}
            onChange={(e) => pick(setBurst)(e.target.value)}
            className={selectCls}
          >
            <option value="">Tất cả</option>
            <option value="true">Chỉ burst</option>
            <option value="false">Ẩn burst</option>
          </select>
        </label>
        <span className="ml-auto flex items-center gap-2 text-sm text-slate-300">
          <button
            disabled={page <= 1}
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            className="rounded-lg border border-slate-600 px-3 py-1.5 hover:bg-slate-800 disabled:opacity-40"
          >
            « Trước
          </button>
          <span className="font-mono tabular-nums">
            {pages === 0 ? "0 / 0" : `${page} / ${pages}`}
          </span>
          <button
            disabled={pages === 0 || page >= pages}
            onClick={() => setPage((p) => p + 1)}
            className="rounded-lg border border-slate-600 px-3 py-1.5 hover:bg-slate-800 disabled:opacity-40"
          >
            Sau »
          </button>
        </span>
      </div>

      {orders.length === 0 ? (
        <div className="text-sm text-slate-500">
          Không có đơn nào khớp filter — đặt hàng để thấy lịch sử hiện ở đây
          và trong Compass.
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
