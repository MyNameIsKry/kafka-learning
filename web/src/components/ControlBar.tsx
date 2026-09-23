interface Props {
  productId: string;
  qty: number;
  burstCount: number;
  busy: boolean;
  emailDown: boolean;
  connected: boolean;
  onProduct: (v: string) => void;
  onQty: (v: number) => void;
  onBurstCount: (v: number) => void;
  onOrder: (mode: "sync" | "kafka") => void;
  onBurst: (mode: "sync" | "kafka") => void;
  onToggleEmail: () => void;
  onReset: () => void;
}

const btn =
  "rounded-lg px-4 py-2.5 font-semibold text-white transition active:scale-95 disabled:cursor-not-allowed disabled:opacity-40";

export default function ControlBar(p: Props) {
  return (
    <header className="rounded-2xl border border-slate-700/60 bg-slate-900/70 p-4 shadow-xl">
      <div className="mb-1 flex items-center gap-3">
        <h1 className="text-2xl font-extrabold tracking-tight">
          Kafka <span className="text-slate-400">vs</span> Không-Kafka
        </h1>
        <span
          className={`flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-medium ${
            p.connected
              ? "bg-emerald-500/15 text-emerald-300"
              : "bg-red-500/15 text-red-300"
          }`}
        >
          <span
            className={`h-2 w-2 rounded-full ${
              p.connected ? "bg-emerald-400" : "bg-red-400"
            }`}
          />
          {p.connected ? "Đã nối realtime" : "Mất kết nối"}
        </span>
      </div>
      <p className="mb-4 text-sm text-slate-400">
        Cùng một nghiệp vụ đặt đơn — so sánh gọi HTTP đồng bộ với event-driven
        qua Kafka
      </p>

      <div className="flex flex-wrap items-end gap-3">
        <label className="flex flex-col gap-1 text-sm text-slate-300">
          Sản phẩm
          <select
            value={p.productId}
            onChange={(e) => p.onProduct(e.target.value)}
            className="rounded-lg border border-slate-700 bg-slate-800 px-3 py-2 font-mono"
          >
            <option>SP-01</option>
            <option>SP-02</option>
            <option>SP-03</option>
          </select>
        </label>

        <label className="flex flex-col gap-1 text-sm text-slate-300">
          Số lượng
          <input
            type="number"
            min={1}
            max={10}
            value={p.qty}
            onChange={(e) => p.onQty(Math.max(1, Number(e.target.value) || 1))}
            className="w-20 rounded-lg border border-slate-700 bg-slate-800 px-3 py-2 font-mono"
          />
        </label>

        <button
          disabled={p.busy}
          onClick={() => p.onOrder("sync")}
          className={`${btn} bg-red-600 hover:bg-red-500`}
        >
          Đặt hàng (Không Kafka)
        </button>
        <button
          disabled={p.busy}
          onClick={() => p.onOrder("kafka")}
          className={`${btn} bg-emerald-600 hover:bg-emerald-500`}
        >
          Đặt hàng (Có Kafka)
        </button>

        <span className="mx-1 hidden h-10 w-px bg-slate-700/70 sm:block" />

        <span className="mx-1 hidden h-10 w-px bg-slate-700/70 sm:block" />

        <label className="flex flex-col gap-1 text-sm text-slate-300">
          Số đơn bắn
          <input
            type="number"
            min={1}
            max={200}
            value={p.burstCount}
            onChange={(e) =>
              p.onBurstCount(
                Math.min(200, Math.max(1, Number(e.target.value) || 1)),
              )
            }
            className="w-24 rounded-lg border border-slate-700 bg-slate-800 px-3 py-2 font-mono"
          />
        </label>
        <button
          disabled={p.busy}
          onClick={() => p.onBurst("sync")}
          className={`${btn} border border-red-500/60 bg-red-950 hover:bg-red-900`}
        >
          Bắn N đơn (Không Kafka)
        </button>
        <button
          disabled={p.busy}
          onClick={() => p.onBurst("kafka")}
          className={`${btn} border border-emerald-500/60 bg-emerald-950 hover:bg-emerald-900`}
        >
          Bắn N đơn (Có Kafka)
        </button>

        <span className="mx-1 hidden h-10 w-px bg-slate-700/70 sm:block" />

        <button
          disabled={p.busy}
          onClick={p.onToggleEmail}
          className={`${btn} ${
            p.emailDown
              ? "bg-amber-500 text-black hover:bg-amber-400"
              : "border border-amber-500/60 bg-transparent text-amber-300 hover:bg-amber-950"
          }`}
        >
          {p.emailDown ? "Bật lại Email" : "Tắt service Email"}
        </button>
        <button
          disabled={p.busy}
          onClick={p.onReset}
          className={`${btn} border border-slate-600 bg-transparent text-slate-200 hover:bg-slate-800`}
        >
          Reset
        </button>
      </div>
    </header>
  );
}
