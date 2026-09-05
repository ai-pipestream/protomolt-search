// A canvas sparkline with no dependencies: a ring of samples drawn as a
// line, the last value as a dot, optional baseline at zero.
export class Sparkline {
  constructor(canvas, { capacity = 120, color, fill = true } = {}) {
    this.canvas = canvas;
    this.capacity = capacity;
    this.samples = [];
    this.color = color;
    this.fill = fill;
    this.reduced = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches ?? false;
  }

  push(value) {
    if (!Number.isFinite(value)) return;
    this.samples.push(value);
    if (this.samples.length > this.capacity) this.samples.shift();
    this.draw();
  }

  reset() {
    this.samples = [];
    this.draw();
  }

  draw() {
    const c = this.canvas;
    const dpr = window.devicePixelRatio || 1;
    const w = c.clientWidth || 240;
    const h = c.clientHeight || 42;
    if (c.width !== Math.round(w * dpr) || c.height !== Math.round(h * dpr)) {
      c.width = Math.round(w * dpr);
      c.height = Math.round(h * dpr);
    }
    const ctx = c.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);
    const s = this.samples;
    if (s.length < 2) return;
    const max = Math.max(...s, 1e-9);
    const min = Math.min(...s, 0);
    const span = max - min || 1;
    const stroke = this.color || getComputedStyle(document.documentElement).getPropertyValue('--accent').trim() || '#245a8d';
    const x = (i) => (i / (this.capacity - 1)) * (w - 4) + 2 + (this.capacity - s.length) * ((w - 4) / (this.capacity - 1));
    const y = (v) => h - 3 - ((v - min) / span) * (h - 8);
    ctx.beginPath();
    s.forEach((v, i) => (i === 0 ? ctx.moveTo(x(i), y(v)) : ctx.lineTo(x(i), y(v))));
    ctx.strokeStyle = stroke;
    ctx.lineWidth = 1.25;
    ctx.stroke();
    if (this.fill) {
      ctx.lineTo(x(s.length - 1), h - 3);
      ctx.lineTo(x(0), h - 3);
      ctx.closePath();
      ctx.globalAlpha = 0.12;
      ctx.fillStyle = stroke;
      ctx.fill();
      ctx.globalAlpha = 1;
    }
    ctx.beginPath();
    ctx.arc(x(s.length - 1), y(s[s.length - 1]), 2, 0, Math.PI * 2);
    ctx.fillStyle = stroke;
    ctx.fill();
  }
}

// Percentile from a cumulative histogram: [{le, cumulativeCount}], count.
// Linear interpolation inside the bucket, which is what Prometheus's
// histogram_quantile does.
export function histogramQuantile(buckets, q) {
  if (!buckets?.length) return null;
  const sorted = [...buckets].map((b) => ({ le: Number(b.le), count: Number(b.cumulativeCount) })).sort((a, b) => a.le - b.le);
  const total = sorted[sorted.length - 1].count;
  if (!total) return null;
  const rank = q * total;
  let prevLe = 0;
  let prevCount = 0;
  for (const b of sorted) {
    if (b.count >= rank) {
      if (!Number.isFinite(b.le)) return prevLe;
      const inBucket = b.count - prevCount;
      const frac = inBucket > 0 ? (rank - prevCount) / inBucket : 1;
      return prevLe + (b.le - prevLe) * frac;
    }
    prevLe = b.le;
    prevCount = b.count;
  }
  return prevLe;
}

// The difference between two snapshots of the same histogram, so a
// window's percentiles come from that window and not the process's life.
export function histogramDelta(now, before) {
  if (!before) return now;
  const prev = new Map((before.buckets || []).map((b) => [String(b.le), Number(b.cumulativeCount)]));
  return {
    ...now,
    count: Number(now.count) - Number(before.count),
    sum: Number(now.sum) - Number(before.sum),
    buckets: (now.buckets || []).map((b) => ({ le: b.le, cumulativeCount: Number(b.cumulativeCount) - (prev.get(String(b.le)) || 0) })),
  };
}
