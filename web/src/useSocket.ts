import { useEffect, useRef, useState } from "react";
import { WS_URL } from "./api";
import type { StepLog } from "./types";

// Single shared WebSocket with auto-reconnect. Every FlowColumn reads from
// the same stream and filters by mode.
export function useSocket(onLog: (log: StepLog) => void) {
  const [connected, setConnected] = useState(false);
  const handler = useRef(onLog);
  handler.current = onLog;

  useEffect(() => {
    let ws: WebSocket | null = null;
    let timer: ReturnType<typeof setTimeout>;
    let delay = 1000;
    let alive = true;

    function connect() {
      if (!alive) return;
      ws = new WebSocket(WS_URL);
      ws.onopen = () => {
        setConnected(true);
        delay = 1000;
      };
      ws.onmessage = (ev) => {
        try {
          handler.current(JSON.parse(ev.data) as StepLog);
        } catch {
          // Bỏ qua frame lạ, giữ socket sống.
        }
      };
      const retry = () => {
        setConnected(false);
        delay = Math.min(delay * 2, 10000);
        timer = setTimeout(connect, delay);
      };
      ws.onclose = retry;
      ws.onerror = () => ws?.close();
    }
    connect();
    return () => {
      alive = false;
      clearTimeout(timer);
      ws?.close();
    };
  }, []);

  return connected;
}
