// === configuration ===

// your GitHub username / repo / branch
const GH_USER = "pritamkhande";
const GH_REPO = "nifty500-eod-upstox";
const GH_BRANCH = "main";

// indices we want to show and their CSV filenames
const INDEX_SYMBOLS = [
  "Nifty",
  "Nifty Bank",
  "Nifty IT",
  "Nifty Midcap 100",
  "Nifty Smallcap 100",
];

// how many last rows to show in chart
const CHART_POINTS = 120;

// Build raw CSV URL for a given symbol
function csvUrlFor(symbol) {
  const firstChar = symbol[0].toUpperCase();
  const folder = /[A-Z]/.test(firstChar) ? firstChar : "0-9";
  const fileName = `${symbol.replace(/\//g, "-")}_EOD.csv`;
  return `https://raw.githubusercontent.com/${GH_USER}/${GH_REPO}/${GH_BRANCH}/data/eod/${encodeURIComponent(folder)}/${encodeURIComponent(fileName)}`;
}

// Parse CSV text into objects [{Date, Close, ...}, ...]
function parseCsv(text) {
  const lines = text.trim().split(/\r?\n/);
  if (!lines.length) return [];

  const headers = lines[0].split(",");
  const dateIdx = headers.indexOf("Date");
  const closeIdx = headers.indexOf("Close");

  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(",");
    if (cols.length < Math.max(dateIdx, closeIdx) + 1) continue;
    rows.push({
      date: cols[dateIdx],
      close: parseFloat(cols[closeIdx]),
    });
  }
  return rows;
}

// Fetch and render one index card
async function loadIndexCard(symbol) {
  const card = document.querySelector(`.card[data-symbol="${symbol}"]`);
  if (!card) return;

  const url = csvUrlFor(symbol);

  try {
    const resp = await fetch(url, { cache: "no-store" });
    if (!resp.ok) {
      console.warn("Failed to fetch", symbol, resp.status);
      return;
    }
    const text = await resp.text();
    const rows = parseCsv(text);
    if (!rows.length) return;

    const recent = rows.slice(-CHART_POINTS);
    const last = recent[recent.length - 1];
    const prev = recent.length > 1 ? recent[recent.length - 2] : null;

    // Fill stats
    const lastCloseEl = card.querySelector('[data-field="last-close"]');
    const changeEl = card.querySelector('[data-field="change"]');

    if (lastCloseEl) {
      lastCloseEl.textContent = last.close.toFixed(2);
    }

    if (changeEl && prev) {
      const diff = last.close - prev.close;
      const pct = (diff / prev.close) * 100;
      const sign = diff >= 0 ? "+" : "";
      changeEl.textContent = `${sign}${diff.toFixed(2)} (${sign}${pct.toFixed(2)}%)`;
      changeEl.classList.remove("positive", "negative");
      changeEl.classList.add(diff >= 0 ? "positive" : "negative");
    }

    // Build chart
    const ctx = document.getElementById(`chart-${symbol}`);
    if (!ctx) return;

    const labels = recent.map(r => r.date);
    const data = recent.map(r => r.close);

    new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: symbol,
            data,
            tension: 0.2,
            borderWidth: 2,
            pointRadius: 0,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            mode: "index",
            intersect: false,
          },
        },
        scales: {
          x: {
            ticks: {
              maxTicksLimit: 6,
              color: "#9ca3af",
            },
          },
          y: {
            ticks: {
              color: "#9ca3af",
            },
          },
        },
      },
    });
  } catch (err) {
    console.error("Error loading", symbol, err);
  }
}

// init
document.addEventListener("DOMContentLoaded", () => {
  INDEX_SYMBOLS.forEach(loadIndexCard);
});
