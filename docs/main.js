const GH_USER = "pritamkhande";
const GH_REPO = "nifty500-eod-upstox";
const GH_BRANCH = "main";

const INDEX_SYMBOLS = ["Nifty", "Nifty Bank", "Nifty IT"];
const CHART_POINTS = 120;

function csvUrl(symbol) {
  const first = symbol[0].toUpperCase();
  const folder = /[A-Z]/.test(first) ? first : "0-9";
  return `https://raw.githubusercontent.com/${GH_USER}/${GH_REPO}/${GH_BRANCH}/data/eod/${folder}/${symbol}_EOD.csv`;
}

async function loadSymbol(symbol) {
  const card = document.querySelector(`.card[data-symbol="${symbol}"]`);
  const url = csvUrl(symbol);

  const resp = await fetch(url);
  if (!resp.ok) {
    console.log("No data for", symbol);
    return;
  }

  const text = await resp.text();
  const rows = text.trim().split("\n").slice(1)
    .map(r => r.split(","))
    .map(c => ({ date: c[1], close: parseFloat(c[5]) }))
    .filter(x => !isNaN(x.close));

  const recent = rows.slice(-CHART_POINTS);
  const last = recent[recent.length - 1];
  const prev = recent[recent.length - 2];

  card.querySelector('[data-field="last-close"]').textContent = last.close.toFixed(2);
  const diff = last.close - prev.close;
  const pct = diff / prev.close * 100;
  const changeEl = card.querySelector('[data-field="change"]');
  changeEl.textContent = `${diff.toFixed(2)} (${pct.toFixed(2)}%)`;

  new Chart(document.getElementById(`chart-${symbol}`), {
    type: "line",
    data: {
      labels: recent.map(r => r.date),
      datasets: [{
        label: symbol,
        data: recent.map(r => r.close),
        borderColor: "#4db2ff",
        tension: 0.2,
        borderWidth: 2,
        pointRadius: 0
      }]
    },
    options: {
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: "#bbb" }},
        y: { ticks: { color: "#bbb" }}
      }
    }
  });
}

document.addEventListener("DOMContentLoaded", () => {
  INDEX_SYMBOLS.forEach(loadSymbol);
});
