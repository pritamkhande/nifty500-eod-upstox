const GH_USER = "pritamkhande";
const GH_REPO = "nifty500-eod-upstox";
const GH_BRANCH = "main";

const INDEX_SYMBOLS = [
  "Nifty",
  "Nifty Bank",
  "Nifty IT",
  "Nifty Midcap 100",
  "Nifty Smallcap 100",
];

const CHART_POINTS = 120;

function csvUrl(symbol) {
  const first = symbol[0].toUpperCase();
  const folder = /[A-Z]/.test(first) ? first : "0-9";
  const fileName = `${symbol.replace(/\//g, "-")}_EOD.csv`;
  return `https://raw.githubusercontent.com/${GH_USER}/${GH_REPO}/${GH_BRANCH}/data/eod/${encodeURIComponent(folder)}/${encodeURIComponent(fileName)}`;
}

async function loadSymbol(symbol) {
  const card = document.querySelector(`.card[data-symbol="${symbol}"]`);
  if (!card) return;

  const url = csvUrl(symbol);

  try {
    const resp = await fetch(url, { cache: "no-store" });
    if (!resp.ok) {
      console.log("No data for", symbol, resp.status);
      return;
    }

    const text = await resp.text();
    const lines = text.trim().split(/\r?\n/);
    if (lines.length <= 1) return;

    const headers = lines[0].split(",");
    const dateIdx = headers.indexOf("Date");
    const closeIdx = headers.indexOf("Close");

    const rows = lines.slice(1)
      .map(line => line.split(","))
      .map(cols => ({
        date: cols[dateIdx],
        close: parseFloat(cols[closeIdx]),
      }))
      .filter(r => !isNaN(r.close));

    if (!rows.length) return;

    const recent = rows.slice(-CHART_POINTS);
    const last = recent[recent.length - 1];
    const prev = recent.length > 1 ? recent[recent.length - 2] : last;

    // Fill stats
    const lastEl = card.querySelector('[data-field="last-close"]');
    const changeEl = card.querySelector('[data-field="change"]');
    if (lastEl) lastEl.textContent = last.close.toFixed(2);

    if (changeEl && prev && prev.close) {
      const diff = last.close - prev.close;
      const pct = (diff / prev.close) * 100;
      const sign = diff >= 0 ? "+" : "";
      changeEl.textContent = `${sign}${diff.toFixed(2)} (${sign}${pct.toFixed(2)}%)`;
    }

    // Chart
    const ctx = document.getElementById(`chart-${symbol}`);
    if (!ctx) return;

    new Chart(ctx, {
      type: "line",
      data: {
        labels: recent.map(r => r.date),
        datasets: [{
          label: symbol,
          data: recent.map(r => r.close),
          borderColor: "#4db2ff",
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.2,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
        },
        scales: {
          x: {
            ticks: { color: "#bbb", maxTicksLimit: 6 },
          },
          y: {
            ticks: { color: "#bbb" },
          },
        },
      },
    });
  } catch (err) {
    console.error("Error loading", symbol, err);
  }
}

document.addEventListener("DOMContentLoaded", () => {
  INDEX_SYMBOLS.forEach(loadSymbol);
  // live signals table is wired later when we add JSON backend; UI is ready.
});
