"use client";

import { LineChart, Line, ResponsiveContainer, YAxis } from "recharts";

interface SparklineProps {
  // Oldest-to-newest sample values.
  values: number[];
  // Pixel height of the chart. Defaults to a single-row size.
  height?: number;
  // Tailwind text color class to apply to the line stroke (e.g. "text-primary").
  className?: string;
}

// Sparkline renders a tiny inline trend line for embedding inside a table cell.
// No axes, no grid, no tooltip — just the trend.
export function Sparkline({
  values,
  height = 28,
  className = "text-primary",
}: SparklineProps) {
  if (!values || values.length === 0) {
    return <div style={{ height }} aria-hidden />;
  }

  // recharts wants objects per point. The actual key doesn't matter; we use
  // index-as-x.
  const data = values.map((v, i) => ({ i, v }));

  // Pad domain so a flat series still shows a horizontal line, not a void.
  let min = Math.min(...values);
  let max = Math.max(...values);
  if (min === max) {
    const epsilon = Math.abs(min) > 0 ? Math.abs(min) * 0.05 : 1;
    min -= epsilon;
    max += epsilon;
  }

  return (
    <div className={className} style={{ height, width: "100%" }}>
      <ResponsiveContainer>
        <LineChart data={data} margin={{ top: 2, right: 2, bottom: 2, left: 2 }}>
          <YAxis hide domain={[min, max]} />
          <Line
            type="monotone"
            dataKey="v"
            stroke="currentColor"
            strokeWidth={1.5}
            dot={false}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
