import { useEffect, useRef, useState } from "react";
import { motion } from "framer-motion";

export interface DiagramNode {
  id: string;
  label: string;
  sub?: string;
  x: number;
  y: number;
  wide?: boolean;
}

export interface DiagramEdge {
  id: string;
  from: string;
  to: string;
}

interface Props {
  nodes: DiagramNode[];
  edges: DiagramEdge[];
  active: Record<string, boolean>;
  down: Record<string, boolean>;
  pulses: Record<string, number>;
  accent: string;
}

interface Dot {
  key: number;
  x1: number;
  y1: number;
  x2: number;
  y2: number;
}

const W = 64;
const H = 40;

// Animated SVG: a light dot travels along an edge whenever its pulse
// counter increases; the target node glows while it is processing.
export default function FlowDiagram({
  nodes,
  edges,
  active,
  down,
  pulses,
  accent,
}: Props) {
  const [dots, setDots] = useState<Dot[]>([]);
  const keyRef = useRef(0);
  const prev = useRef<Record<string, number>>({});
  const byId = Object.fromEntries(nodes.map((n) => [n.id, n]));

  useEffect(() => {
    for (const e of edges) {
      if ((pulses[e.id] ?? 0) > (prev.current[e.id] ?? 0)) {
        const a = byId[e.from];
        const b = byId[e.to];
        if (!a || !b) continue;
        const key = ++keyRef.current;
        const dot = { key, x1: a.x, y1: a.y, x2: b.x, y2: b.y };
        setDots((d) => [...d.slice(-11), dot]);
        setTimeout(
          () => setDots((d) => d.filter((x) => x.key !== key)),
          1200,
        );
      }
    }
    prev.current = pulses;
  }, [pulses, edges]);

  return (
    <svg viewBox="0 0 400 230" className="h-52 w-full">
      <defs>
        <marker
          id="arrow"
          viewBox="0 0 10 10"
          refX="9"
          refY="5"
          markerWidth="7"
          markerHeight="7"
          orient="auto-start-reverse"
        >
          <path d="M 0 1 L 9 5 L 0 9 z" fill="#475569" />
        </marker>
      </defs>

      {edges.map((e) => {
        const a = byId[e.from];
        const b = byId[e.to];
        if (!a || !b) return null;
        return (
          <line
            key={e.id}
            x1={a.x}
            y1={a.y}
            x2={b.x}
            y2={b.y}
            stroke="#475569"
            strokeWidth={2}
            markerEnd="url(#arrow)"
          />
        );
      })}

      {dots.map((d) => (
        <motion.circle
          key={d.key}
          r={5}
          fill={accent}
          style={{ filter: `drop-shadow(0 0 6px ${accent})` }}
          initial={{ cx: d.x1, cy: d.y1, opacity: 1 }}
          animate={{ cx: d.x2, cy: d.y2, opacity: [1, 1, 0.5] }}
          transition={{ duration: 1.1, ease: "linear" }}
        />
      ))}

      {nodes.map((n) => {
        const isDown = !!down[n.id];
        const isActive = !!active[n.id] && !isDown;
        const w = n.wide ? W + 28 : W;
        return (
          <g key={n.id} opacity={isDown ? 0.45 : 1}>
            {isActive && (
              <motion.circle
                cx={n.x}
                cy={n.y}
                fill="none"
                stroke={accent}
                strokeWidth={2}
                initial={{ r: 24, opacity: 0.7 }}
                animate={{ r: 36, opacity: 0 }}
                transition={{ duration: 1, repeat: Infinity, ease: "easeOut" }}
              />
            )}
            <rect
              x={n.x - w / 2}
              y={n.y - H / 2}
              width={w}
              height={H}
              rx={10}
              fill={isDown ? "#1e293b" : isActive ? "#0f172a" : "#151a26"}
              stroke={isDown ? "#475569" : isActive ? accent : "#334155"}
              strokeWidth={isActive ? 2.5 : 1.5}
              strokeDasharray={isDown ? "6 4" : undefined}
            />
            <text
              x={n.x}
              y={n.y - 1}
              textAnchor="middle"
              fill={isDown ? "#64748b" : "#f1f5f9"}
              fontSize={13}
              fontWeight={700}
              textDecoration={isDown ? "line-through" : undefined}
            >
              {n.label}
            </text>
            {n.sub && (
              <text
                x={n.x}
                y={n.y + 13}
                textAnchor="middle"
                fill="#64748b"
                fontSize={9}
                fontFamily="monospace"
              >
                {n.sub}
              </text>
            )}
          </g>
        );
      })}
    </svg>
  );
}
