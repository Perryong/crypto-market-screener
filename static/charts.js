const GREEN = "#53d7ae",
  RED = "#ef7b83",
  MUTED = "#788494";
const num = (n) =>
  Number(n).toLocaleString("en-US", {
    maximumFractionDigits:
      Math.abs(n) < 1
        ? Math.min(
            12,
            Math.max(2, 2 - Math.floor(Math.log10(Math.abs(n) || 1))),
          )
        : 2,
    notation: Math.abs(n) > 1e6 ? "compact" : "standard",
  });
export function createChart(canvas, { kind, onHover = () => {} }) {
  const ctx = canvas.getContext("2d");
  let data = [],
    count = kind === "footprint" ? 8 : 80,
    offset = 0,
    hover = null,
    drag = null,
    frame = 0;
  function schedule() {
    if (!frame)
      frame = requestAnimationFrame(() => {
        frame = 0;
        draw();
      });
  }
  function draw() {
    const rect = canvas.getBoundingClientRect(),
      w = rect.width,
      h = rect.height;
    if (w < 1 || h < 1) return;
    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.round(w * dpr);
    canvas.height = Math.round(h * dpr);
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, w, h);
    ctx.font = '9px "IBM Plex Mono",monospace';
    ctx.fillStyle = MUTED;
    const right = 70,
      top = 15,
      bottom = 25,
      pw = w - right - 10,
      ph = h - top - bottom;
    const end = Math.max(1, data.length - offset),
      rows = data.slice(Math.max(0, end - count), end);
    if (!rows.length) {
      ctx.textAlign = "center";
      ctx.fillText(
        kind === "candles"
          ? "Waiting for Binance candles…"
          : "Collecting local history…",
        w / 2,
        h / 2,
      );
      return;
    }
    let values = [];
    if (kind === "candles") values = rows.flatMap((r) => [r.low, r.high]);
    if (kind === "cvd") values = rows.map((r) => r.value);
    if (kind === "footprint")
      values = rows.flatMap((r) => Object.keys(r.footprint || {}).map(Number));
    if (kind === "heatmap")
      values = rows.flatMap((r) => {
        const mid = (r.bids?.[0]?.price + r.asks?.[0]?.price) / 2;
        return Number.isFinite(mid) ? [mid * 0.995, mid * 1.005] : [];
      });
    values = values.filter(Number.isFinite);
    if (!values.length) return;
    let lo = Math.min(...values),
      hi = Math.max(...values);
    const pad = (hi - lo) * 0.07 || Math.max(Math.abs(hi) * 0.001, 0.000001);
    lo -= pad;
    hi += pad;
    const priceHeight = kind === "candles" ? ph * 0.79 : ph;
    const y = (p) => top + ((hi - p) / (hi - lo)) * priceHeight;
    const step =
      pw /
      Math.max(
        rows.length,
        kind === "candles" ? Math.min(count, 20) : rows.length,
      );
    const x = (i) => 10 + (i + 0.5) * step;
    ctx.textAlign = "left";
    ctx.strokeStyle = "#242a334d";
    ctx.lineWidth = 1;
    for (let i = 0; i < 5; i++) {
      const py = top + (i * priceHeight) / 4;
      ctx.beginPath();
      ctx.moveTo(10, py);
      ctx.lineTo(w - right, py);
      ctx.stroke();
      ctx.fillStyle = MUTED;
      ctx.fillText(num(hi - (i * (hi - lo)) / 4), w - right + 8, py + 3);
    }
    for (
      let i = 0;
      i < rows.length;
      i += Math.max(1, Math.floor(rows.length / 5))
    ) {
      ctx.fillStyle = MUTED;
      ctx.fillText(
        new Date(rows[i].timestamp).toLocaleTimeString([], {
          hour: "2-digit",
          minute: "2-digit",
        }),
        Math.min(x(i), w - right - 40),
        h - 7,
      );
    }
    ctx.save();
    ctx.beginPath();
    ctx.rect(10, 0, pw, h - bottom);
    ctx.clip();
    if (kind === "candles") {
      const maxVol = Math.max(1, ...rows.map((r) => r.volume));
      rows.forEach((r, i) => {
        const color = r.close >= r.open ? GREEN : RED;
        ctx.strokeStyle = color;
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.moveTo(x(i), y(r.high));
        ctx.lineTo(x(i), y(r.low));
        ctx.stroke();
        ctx.fillRect(
          x(i) - step * 0.32,
          Math.min(y(r.open), y(r.close)),
          Math.max(1, step * 0.64),
          Math.max(1, Math.abs(y(r.open) - y(r.close))),
        );
        ctx.globalAlpha = 0.25;
        const vh = (r.volume / maxVol) * ph * 0.15;
        ctx.fillRect(
          x(i) - step * 0.32,
          h - bottom - vh,
          Math.max(1, step * 0.64),
          vh,
        );
        ctx.globalAlpha = 1;
      });
      const last = rows.at(-1);
      ctx.setLineDash([3, 4]);
      ctx.strokeStyle = last.close >= last.open ? GREEN : RED;
      ctx.beginPath();
      ctx.moveTo(10, y(last.close));
      ctx.lineTo(w - right, y(last.close));
      ctx.stroke();
      ctx.setLineDash([]);
    } else if (kind === "cvd") {
      ctx.strokeStyle = rows.at(-1).value >= 0 ? GREEN : RED;
      ctx.lineWidth = 1.7;
      ctx.beginPath();
      rows.forEach((r, i) =>
        i ? ctx.lineTo(x(i), y(r.value)) : ctx.moveTo(x(i), y(r.value)),
      );
      ctx.stroke();
      if (rows.length === 1) {
        ctx.fillStyle = GREEN;
        ctx.fillRect(x(0) - 2, y(rows[0].value) - 2, 4, 4);
      }
    } else if (kind === "footprint") {
      for (const [i, r] of rows.entries()) {
        const levels = Object.entries(r.footprint || {})
          .map(([p, v]) => ({ price: +p, ...v }))
          .sort((a, b) => b.price - a.price);
        const selected =
          levels.length > 18
            ? levels.filter((_, i) => i % Math.ceil(levels.length / 18) === 0)
            : levels;
        const max = Math.max(1, ...selected.map((v) => v.totalVolume));
        let lastLabel = -Infinity;
        for (const v of selected) {
          const py = y(v.price);
          ctx.fillStyle =
            v.askVolume >= v.bidVolume
              ? `rgba(83,215,174,${0.06 + (v.totalVolume / max) * 0.2})`
              : `rgba(239,123,131,${0.06 + (v.totalVolume / max) * 0.2})`;
          ctx.fillRect(x(i) - step * 0.46, py - 5, step * 0.92, 10);
          ctx.textAlign = "center";
          ctx.fillStyle = "#cbd4dd";
          if (step > 55 && py - lastLabel > 12) {
            ctx.fillText(
              `${num(v.bidVolume)} × ${num(v.askVolume)}`,
              x(i),
              py + 3,
            );
            lastLabel = py;
          }
        }
      }
    } else {
      const max = Math.max(
        1,
        ...rows.flatMap((r) =>
          [...(r.bids || []), ...(r.asks || [])]
            .slice(0, 100)
            .map((v) => v.quantity),
        ),
      );
      rows.forEach((r, i) => {
        for (const side of ["bids", "asks"])
          for (const level of (r[side] || []).slice(0, 60)) {
            const intensity = Math.min(1, Math.sqrt(level.quantity / max));
            ctx.fillStyle =
              side === "bids"
                ? `rgba(83,215,174,${0.12 + intensity * 0.8})`
                : `rgba(239,166,112,${0.12 + intensity * 0.8})`;
            ctx.fillRect(
              x(i) - step / 2,
              y(level.price) - 2,
              Math.max(1, step),
              4,
            );
          }
      });
    }
    ctx.restore();
    if (hover && kind === "candles") {
      const i = Math.min(
          rows.length - 1,
          Math.max(0, Math.floor((hover.x - 10) / step)),
        ),
        r = rows[i];
      ctx.strokeStyle = "#83909d";
      ctx.setLineDash([3, 3]);
      ctx.beginPath();
      ctx.moveTo(x(i), 0);
      ctx.lineTo(x(i), h - bottom);
      ctx.moveTo(0, hover.y);
      ctx.lineTo(w - right, hover.y);
      ctx.stroke();
      ctx.setLineDash([]);
      onHover(r);
    }
  }
  function move(e) {
    const r = canvas.getBoundingClientRect();
    hover = { x: e.clientX - r.left, y: e.clientY - r.top };
    if (drag) {
      offset = Math.max(
        0,
        Math.min(
          Math.max(0, data.length - 10),
          drag.offset +
            Math.round((e.clientX - drag.x) / Math.max(1, r.width / count)),
        ),
      );
    }
    schedule();
  }
  function down(e) {
    drag = { x: e.clientX, offset };
    canvas.setPointerCapture(e.pointerId);
  }
  function up() {
    drag = null;
  }
  function leave() {
    hover = null;
    schedule();
  }
  function wheel(e) {
    e.preventDefault();
    count = Math.max(
      kind === "footprint" ? 3 : 10,
      Math.min(500, Math.round(count * (e.deltaY > 0 ? 1.15 : 0.85))),
    );
    schedule();
  }
  canvas.addEventListener("pointermove", move);
  canvas.addEventListener("pointerdown", down);
  canvas.addEventListener("pointerup", up);
  canvas.addEventListener("pointerleave", leave);
  canvas.addEventListener("wheel", wheel, { passive: false });
  const observer = new ResizeObserver(schedule);
  observer.observe(canvas);
  return {
    setData(rows) {
      data = rows;
      schedule();
    },
    reset() {
      offset = 0;
      count = kind === "footprint" ? 8 : 80;
      schedule();
    },
    destroy() {
      observer.disconnect();
      cancelAnimationFrame(frame);
      canvas.removeEventListener("pointermove", move);
      canvas.removeEventListener("pointerdown", down);
      canvas.removeEventListener("pointerup", up);
      canvas.removeEventListener("pointerleave", leave);
      canvas.removeEventListener("wheel", wheel);
    },
  };
}
