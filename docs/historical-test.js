document.addEventListener("DOMContentLoaded", async () => {
  try {
    const res = await fetch("historical-test-data.json", { cache: "no-store" });
    if (!res.ok) {
      console.error("Failed to load historical-test-data.json:", res.status);
      return;
    }

    const data = await res.json();

    const tbody = document.querySelector("#historical-table tbody");
    tbody.innerHTML = "";

    data.sort((a, b) => b.winrate - a.winrate);

    for (const row of data) {
      const tr = document.createElement("tr");

      tr.innerHTML = `
        <td>${row.symbol}</td>
        <td>${row.total_trades}</td>
        <td>${row.winrate}%</td>
        <td>${row.avg_r}</td>
        <td>${row.profit_factor}</td>
        <td>${row.max_dd}%</td>
        <td>${row.last_3_year_winrate}%</td>
        <td>${row.last_1_year_winrate}%</td>
        <td><a href="${row.link}" class="nav-link">View</a></td>
      `;

      tbody.appendChild(tr);
    }

    const searchInput = document.getElementById("search");
    searchInput.addEventListener("input", (e) => {
      const q = e.target.value.toLowerCase();
      [...tbody.rows].forEach(row => {
        row.style.display = row.innerText.toLowerCase().includes(q) ? "" : "none";
      });
    });
  } catch (err) {
    console.error("Error loading historical data:", err);
  }
});
