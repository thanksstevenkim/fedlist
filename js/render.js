(async function renderInstances() {
  const tableBody = document.getElementById("instances-body");
  const sortableHeaders = document.querySelectorAll("th[data-sort-key]");
  let baseRows = [];
  let displayRows = [];
  const sortState = { key: null, direction: "desc" };

  try {
    const instances = await loadInstances();
    const stats = await loadStats();
    const statsMap = createStatsMap(stats);

    baseRows = instances.map((instance, index) => ({
      order: index,
      instance,
      stats: statsMap.get(normalizeUrl(instance.url)) ?? null,
    }));

    if (baseRows.length === 0) {
      tableBody.innerHTML = `<tr><td colspan=\"7\">표시할 인스턴스가 없습니다.</td></tr>`;
      return;
    }

    displayRows = [...baseRows];
    renderRows(displayRows);
    bindSorting();
  } catch (error) {
    console.error(error);
    tableBody.innerHTML = `<tr><td colspan=\"7\">데이터를 불러오는 중 오류가 발생했습니다. 로컬에서 테스트하는 경우 <code>python -m http.server</code>로 간단한 서버를 실행하세요.</td></tr>`;
  }

  function bindSorting() {
    sortableHeaders.forEach((header) => {
      header.addEventListener("click", () => {
        const key = header.dataset.sortKey;
        if (!key) return;

        if (sortState.key === key) {
          sortState.direction = sortState.direction === "asc" ? "desc" : "asc";
        } else {
          sortState.key = key;
          sortState.direction = "desc";
        }

        updateSortIndicators();
        applySorting();
        renderRows(displayRows);
      });
    });
  }

  function applySorting() {
    if (!sortState.key) {
      displayRows = [...baseRows];
      return;
    }

    const directionMultiplier = sortState.direction === "asc" ? 1 : -1;
    const key = sortState.key;

    displayRows = [...baseRows].sort((a, b) => {
      const aValue = getNumericValue(a.stats?.[key]);
      const bValue = getNumericValue(b.stats?.[key]);

      if (aValue === null && bValue === null) {
        return a.order - b.order;
      }
      if (aValue === null) {
        return 1;
      }
      if (bValue === null) {
        return -1;
      }

      if (aValue === bValue) {
        return a.order - b.order;
      }

      const diff = aValue - bValue;
      return diff * directionMultiplier;
    });
  }

  function updateSortIndicators() {
    sortableHeaders.forEach((header) => {
      const key = header.dataset.sortKey;
      if (!key) return;
      if (sortState.key === key) {
        header.dataset.direction = sortState.direction;
      } else {
        header.removeAttribute("data-direction");
      }
    });
  }

  function renderRows(rows) {
    const fragment = document.createDocumentFragment();

    rows.forEach(({ instance, stats }) => {
      const row = document.createElement("tr");

      const nameCell = document.createElement("th");
      nameCell.scope = "row";
      nameCell.textContent = instance.name ?? "-";

      const urlCell = document.createElement("td");
      if (instance.url) {
        const link = document.createElement("a");
        link.href = instance.url;
        link.textContent = instance.url.replace(/^https?:\/\//, "");
        link.rel = "noopener";
        link.target = "_blank";
        urlCell.appendChild(link);
      } else {
        urlCell.textContent = "-";
      }

      const platformCell = document.createElement("td");
      platformCell.textContent = instance.platform ?? "-";

      const usersTotalCell = document.createElement("td");
      usersTotalCell.textContent = formatNumber(stats?.users_total);

      const usersActiveCell = document.createElement("td");
      usersActiveCell.textContent = formatNumber(stats?.users_active_month);

      const statusesCell = document.createElement("td");
      statusesCell.textContent = formatNumber(stats?.statuses);

      const descriptionCell = document.createElement("td");
      descriptionCell.textContent = instance.description ?? "-";

      row.append(
        nameCell,
        urlCell,
        platformCell,
        usersTotalCell,
        usersActiveCell,
        statusesCell,
        descriptionCell
      );
      fragment.appendChild(row);
    });

    tableBody.innerHTML = "";
    tableBody.appendChild(fragment);
  }

  async function loadInstances() {
    const response = await fetch("data/instances.json");
    if (!response.ok) {
      throw new Error(`인스턴스 데이터를 불러올 수 없습니다: ${response.status}`);
    }
    const data = await response.json();
    if (!Array.isArray(data)) {
      throw new Error("인스턴스 데이터 형식이 올바르지 않습니다.");
    }
    return data;
  }

  async function loadStats() {
    try {
      const response = await fetch("data/stats.json", { cache: "no-store" });
      if (!response.ok) {
        throw new Error(`통계 데이터를 불러올 수 없습니다: ${response.status}`);
      }
      const data = await response.json();
      if (!Array.isArray(data)) {
        throw new Error("통계 데이터 형식이 올바르지 않습니다.");
      }
      return data;
    } catch (error) {
      console.info("통계 데이터를 불러오지 못했습니다. 기본 값으로 표시합니다.");
      return [];
    }
  }

  function createStatsMap(stats) {
    const map = new Map();
    stats.forEach((entry) => {
      if (!entry?.url) return;
      const key = normalizeUrl(entry.url);
      if (!key) return;
      map.set(key, {
        users_total: getNumericValue(entry.users_total),
        users_active_month: getNumericValue(entry.users_active_month),
        statuses: getNumericValue(entry.statuses),
        fetched_at: entry.fetched_at ?? null,
      });
    });
    return map;
  }

  function normalizeUrl(url) {
    if (!url || typeof url !== "string") return "";
    try {
      const parsed = new URL(url);
      parsed.hash = "";
      parsed.search = "";
      return parsed.toString().replace(/\/+$/, "").toLowerCase();
    } catch (error) {
      return url.replace(/\/+$/, "").toLowerCase();
    }
  }

  function getNumericValue(value) {
    if (value === null || value === undefined) return null;
    const numberValue = Number(value);
    return Number.isFinite(numberValue) ? numberValue : null;
  }

  function formatNumber(value) {
    if (value === null || value === undefined) {
      return "-";
    }
    return Number(value).toLocaleString("ko-KR");
  }
})();
