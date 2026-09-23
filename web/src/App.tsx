import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "./api";
import { useSocket } from "./useSocket";
import type {
  BurstResult,
  Mode,
  OrderDoc,
  StepLog,
  SystemState,
} from "./types";
import { FLOW_SERVICES } from "./types";
import ControlBar from "./components/ControlBar";
import FlowColumn, { type TimelineItem } from "./components/FlowColumn";
import StatePanel from "./components/StatePanel";
import BurstResultView from "./components/BurstResult";
import OrderHistory from "./components/OrderHistory";

interface ColState {
  latencyMs: number | null;
  orderId: string | null;
  steps: TimelineItem[];
  logs: StepLog[];
  active: Record<string, boolean>;
  pulses: Record<string, number>;
  burstDone: number;
}

const idleSteps = (): TimelineItem[] =>
  FLOW_SERVICES.map((s) => ({
    service: s,
    state: "idle" as const,
    durationMs: 0,
    message: "chờ đơn",
  }));

const blankCol = (): ColState => ({
  latencyMs: null,
  orderId: null,
  steps: idleSteps(),
  logs: [],
  active: {},
  pulses: {},
  burstDone: 0,
});

// Edge phát sáng khi service bắt đầu xử lý (luồng Kafka).
function edgeFor(service: string): string | null {
  if (service === "gateway") return "gateway-topic";
  return `topic-${service}`;
}

function syncEdgeFor(service: string): string | null {
  if (service === "inventory") return "gateway-inventory";
  if (service === "email") return "inventory-email";
  if (service === "shipping") return "email-shipping";
  return null;
}

export default function App() {
  const [productId, setProductId] = useState("SP-01");
  const [qty, setQty] = useState(1);
  const [burstCount, setBurstCount] = useState(50);
  const [busy, setBusy] = useState(false);
  const [sync, setSync] = useState<ColState>(blankCol);
  const [kafka, setKafka] = useState<ColState>(blankCol);
  const [system, setSystem] = useState<SystemState | null>(null);
  const [orders, setOrders] = useState<OrderDoc[]>([]);
  const [burstSync, setBurstSync] = useState<BurstResult | null>(null);
  const [burstKafka, setBurstKafka] = useState<BurstResult | null>(null);
  const timers = useRef<ReturnType<typeof setTimeout>[]>([]);
  // Theo dõi burst theo đơn: đủ 3 bước done mới tính 1 đơn xong.
  const burstTrack = useRef<Map<string, Set<string>>>(new Map());

  const updateCol = useCallback(
    (mode: Mode, fn: (c: ColState) => ColState) => {
      if (mode === "sync") setSync((c) => fn(c));
      else setKafka((c) => fn(c));
    },
    [],
  );

  const clearActiveLater = useCallback(
    (mode: Mode, service: string) => {
      const t = setTimeout(
        () =>
          updateCol(mode, (c) => ({
            ...c,
            active: { ...c.active, [service]: false },
          })),
        8000,
      );
      timers.current.push(t);
    },
    [updateCol],
  );

  const pushLog = useCallback(
    (l: StepLog) => {
      const mode = l.mode === "kafka" ? "kafka" : "sync";
      // Log burst hiện đủ như đơn lẻ (cửa sổ scroll + cap dòng lo tràn màn
      // hình); đồng thời đếm số đơn burst đã đủ 3 bước done.
      if (l.burst) {
        if (
          l.status === "done" &&
          (FLOW_SERVICES as readonly string[]).includes(l.service)
        ) {
          const set =
            burstTrack.current.get(l.orderId) ?? new Set<string>();
          set.add(l.service);
          if (set.size >= FLOW_SERVICES.length) {
            burstTrack.current.delete(l.orderId);
            updateCol(mode, (c) => ({ ...c, burstDone: c.burstDone + 1 }));
          } else {
            burstTrack.current.set(l.orderId, set);
          }
        }
      }
      updateCol(mode, (c) => {
        const logs = [l, ...c.logs].slice(0, 300);
        const active = { ...c.active };
        const pulses = { ...c.pulses };
        if (l.status === "start") {
          active[l.service] = true;
          const edge =
            mode === "sync" ? syncEdgeFor(l.service) : edgeFor(l.service);
          if (edge) pulses[edge] = (pulses[edge] ?? 0) + 1;
        } else {
          active[l.service] = false;
        }
        let steps = c.steps;
        if (c.orderId && l.orderId === c.orderId) {
          steps = c.steps.map((s) =>
            s.service === l.service
              ? {
                  ...s,
                  state:
                    l.status === "done"
                      ? "done"
                      : l.status === "error"
                        ? "error"
                        : "running",
                  durationMs: l.durationMs,
                  message: l.message,
                }
              : s,
          );
        }
        return { ...c, logs, active, pulses, steps };
      });
      if (l.status === "start") clearActiveLater(mode, l.service);
    },
    [updateCol, clearActiveLater],
  );

  const connected = useSocket(pushLog);

  const refreshState = useCallback(async () => {
    try {
      setSystem(await api.state());
    } catch {
      // Giữ state cũ khi gateway/service chưa lên.
    }
  }, []);

  const refreshOrders = useCallback(async () => {
    try {
      setOrders(await api.orders(20));
    } catch {
      // Bỏ qua, lần poll sau thử lại.
    }
  }, []);

  // Nạp lần đầu + poll.
  useEffect(() => {
    api
      .logs()
      .then((all) => {
        for (const l of all.filter((x) => !x.burst).slice(-100)) pushLog(l);
      })
      .catch(() => {});
    refreshState();
    refreshOrders();
    const s = setInterval(refreshState, 2000);
    const o = setInterval(refreshOrders, 5000);
    return () => {
      clearInterval(s);
      clearInterval(o);
      timers.current.forEach(clearTimeout);
    };
  }, [pushLog, refreshState, refreshOrders]);

  async function run<T>(fn: () => Promise<T>): Promise<T | null> {
    setBusy(true);
    try {
      return await fn();
    } catch (e) {
      alert(e instanceof Error ? e.message : "Lỗi không rõ");
      return null;
    } finally {
      setBusy(false);
    }
  }

  // Lõi đặt 1 đơn, dùng chung cho nút đặt hàng.
  const placeOrder = async (mode: Mode) => {
    updateCol(mode, (c) => ({
      ...c,
      latencyMs: null,
      orderId: null,
      steps: FLOW_SERVICES.map((s) => ({
        service: s,
        state: "running" as const,
        durationMs: 0,
        message: "đang gọi…",
      })),
    }));
    if (mode === "sync") {
      const res = await api.orderSync(productId, qty);
      updateCol(mode, (c) => ({
        ...c,
        latencyMs: res.totalMs,
        orderId: res.orderId,
        steps: res.steps.map((s) => ({
          service: s.service,
          state: s.ok ? ("done" as const) : ("error" as const),
          durationMs: s.durationMs,
          message: s.message,
        })),
      }));
    } else {
      const res = await api.orderAsync(productId, qty);
      updateCol(mode, (c) => ({
        ...c,
        latencyMs: res.totalMs,
        orderId: res.orderId,
        steps: FLOW_SERVICES.map((s) => ({
          service: s,
          state: "idle" as const,
          durationMs: 0,
          message: "chờ consumer…",
        })),
      }));
    }
    refreshState();
    refreshOrders();
  };

  const onOrder = (mode: Mode) => run(() => placeOrder(mode));

  const onBurst = (mode: Mode) =>
    run(async () => {
      const res = await api.burst(mode, burstCount, productId);
      if (mode === "sync") setBurstSync(res);
      else setBurstKafka(res);
      refreshState();
      refreshOrders();
    });

  const onToggleEmail = () =>
    run(async () => {
      const down = !emailDown;
      await api.serviceToggle("email", down);
      refreshState();
    });

  const onReset = () =>
    run(async () => {
      await api.reset();
      setSync(blankCol());
      setKafka(blankCol());
      setBurstSync(null);
      setBurstKafka(null);
      burstTrack.current.clear();
      refreshState();
      refreshOrders();
    });

  const emailDown =
    system?.services.find((s) => s.name === "email")?.down ?? false;
  const downMap: Record<string, boolean> = {};
  for (const s of system?.services ?? []) {
    if (s.down) downMap[s.name] = true;
  }
  const maxMs = Math.max(sync.latencyMs ?? 0, kafka.latencyMs ?? 0, 1);

  return (
    <div className="mx-auto flex min-h-screen max-w-7xl flex-col gap-4 p-4">
      <ControlBar
        productId={productId}
        qty={qty}
        burstCount={burstCount}
        busy={busy}
        emailDown={emailDown}
        connected={connected}
        onProduct={setProductId}
        onQty={setQty}
        onBurstCount={setBurstCount}
        onOrder={onOrder}
        onBurst={onBurst}
        onToggleEmail={onToggleEmail}
        onReset={onReset}
      />

      <div className="grid gap-4 lg:grid-cols-2">
        <FlowColumn
          mode="sync"
          title="Không Kafka (đồng bộ)"
          subtitle="Gateway gọi Kho → Mail → Ship nối tiếp, user chờ tới khi xong"
          accent="red"
          latencyMs={sync.latencyMs}
          ratio={(sync.latencyMs ?? 0) / maxMs}
          steps={sync.steps}
          logs={sync.logs}
          active={sync.active}
          down={downMap}
          pulses={sync.pulses}
          burstDone={sync.burstDone}
          orderId={sync.orderId}
        />
        <FlowColumn
          mode="kafka"
          title="Có Kafka (event-driven)"
          subtitle="Gateway publish 1 lần vào topic, 3 service xử lý song song"
          accent="emerald"
          latencyMs={kafka.latencyMs}
          ratio={(kafka.latencyMs ?? 0) / maxMs}
          steps={kafka.steps}
          logs={kafka.logs}
          active={kafka.active}
          down={downMap}
          pulses={kafka.pulses}
          burstDone={kafka.burstDone}
          orderId={kafka.orderId}
        />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <StatePanel state={system} />
        <BurstResultView sync={burstSync} kafka={burstKafka} />
      </div>

      <OrderHistory orders={orders} />

      <footer className="pb-4 text-center text-xs text-slate-600">
        Demo tìm hiểu Kafka · React + Go + Kafka + MongoDB
      </footer>
    </div>
  );
}
