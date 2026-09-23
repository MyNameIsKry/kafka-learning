import { useEffect, useState } from "react";
import { animate, motion } from "framer-motion";

interface Props {
  label: string;
  ms: number | null;
  ratio: number;
  accent: "red" | "emerald";
}

function CountUp({ value }: { value: number }) {
  const [display, setDisplay] = useState(0);
  useEffect(() => {
    const controls = animate(0, value, {
      duration: 0.7,
      ease: "easeOut",
      onUpdate: (v) => setDisplay(Math.round(v)),
    });
    return () => controls.stop();
  }, [value]);
  return <>{display.toLocaleString("vi-VN")}</>;
}

// The big "user waiting time" number plus a bar scaled against the sibling.
export default function LatencyBar({ label, ms, ratio, accent }: Props) {
  const bar = accent === "red" ? "bg-red-500" : "bg-emerald-500";
  const text = accent === "red" ? "text-red-400" : "text-emerald-400";
  return (
    <div className="rounded-xl border border-slate-700/60 bg-slate-950/60 p-4">
      <div className="text-sm text-slate-400">{label}</div>
      <div className={`font-mono text-5xl font-extrabold tabular-nums ${text}`}>
        {ms === null ? (
          <span className="text-slate-600">—</span>
        ) : (
          <>
            <CountUp value={ms} />
            <span className="ml-1 text-xl font-semibold">ms</span>
          </>
        )}
      </div>
      <div className="mt-3 h-2.5 overflow-hidden rounded-full bg-slate-800">
        <motion.div
          className={`h-full rounded-full ${bar}`}
          initial={false}
          animate={{ width: `${Math.min(100, Math.max(2, ratio * 100))}%` }}
          transition={{ type: "spring", stiffness: 120, damping: 20 }}
        />
      </div>
    </div>
  );
}
