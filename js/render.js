(async function renderDirectory() {
  const assetBaseUrl = resolveAssetBaseUrl();
  const locale = document.documentElement.lang || "ko";
  const numberLocale = locale || "ko-KR";

  const KNOWN_SOFTWARE_LABELS = {
    akkoma: "아콤마",
    bookwyrm: "BookWyrm",
    calckey: "Calckey",
    firefish: "파이어피쉬",
    friendica: "Friendica",
    funkwhale: "Funkwhale",
    ghost: "고스트",
    gotosocial: "고투소셜",
    hubzilla: "Hubzilla",
    kbin: "Kbin",
    lemmy: "렘미",
    mastodon: "마스토돈",
    misskey: "미스키",
    peertube: "피어튜브",
    pixelfed: "픽셀페드",
    pleroma: "플레로마",
    sharkey: "Sharkey",
    streams: "Streams",
    takahe: "Takahē",
    wordpress: "워드프레스",
    writefreely: "WriteFreely",
  };

  const FALLBACK_STRINGS = {
    ko: {
      title: "연합우주를 여행하는 히치하이커를 위한 안내서",
      intro: "한국어로 운영되는 페디버스 인스턴스를 수동으로 정리한 목록입니다.",
      search_label: "검색어",
      search_placeholder: "이름 또는 설명 검색",
      software_filter_heading: "소프트웨어 분류",
      software_all: "전체 소프트웨어",
      software_unknown: "기타",
      language_filter_label: "언어",
      language_all: "전체 언어",
      table_heading: "인스턴스 목록",
      table_caption: "한국어 Fediverse 인스턴스 목록",
      table_aria: "한국어 Fediverse 인스턴스 목록",
      name: "이름",
      url: "주소",
      platform: "플랫폼",
      registration: "가입",
      languages: "언어",
      users_total: "총 사용자",
      users_active: "활성 사용자(월)",
      statuses: "게시물 수",
      description: "설명",
      badge_verified_ok: "검증됨",
      badge_verified_fail: "검증 실패",
      registration_open: "가입 열림",
      registration_closed: "가입 닫힘",
      registration_unknown: "가입 상태 불명",
      loading: "데이터를 불러오는 중입니다…",
      no_data: "데이터 없음",
      no_instances: "표시할 인스턴스가 없습니다.",
      no_results: "조건에 맞는 인스턴스가 없습니다.",
      error_fetch:
        "데이터를 불러오는 중 오류가 발생했습니다. 로컬에서 테스트하는 경우 <code>python -m http.server</code>로 간단한 서버를 실행하세요.",
      sort_users_total: "총 사용자 수로 정렬",
      sort_users_active: "월간 활성 사용자 수로 정렬",
      footer_note: "데이터는 data/instances.json과 data/stats.ok.json 파일을 수정해 갱신할 수 있습니다.",
      ...Object.fromEntries(
        Object.entries(KNOWN_SOFTWARE_LABELS).map(([key, label]) => [
          `software_label_${key}`,
          label,
        ])
      ),
    },
  };

  const elements = {
    table: document.getElementById("instances-table"),
    tableBody: document.getElementById("instances-body"),
    tableCaption: document.getElementById("tableCaption"),
    pageTitle: document.getElementById("page-title"),
    pageIntro: document.getElementById("page-intro"),
    directoryTitle: document.getElementById("directory-title"),
    footerNote: document.getElementById("footer-note"),
    searchInput: document.getElementById("q"),
    searchLabel: document.getElementById("searchLabel"),
    languageSelect: document.getElementById("languageFilter"),
    languageLabel: document.getElementById("languageLabel"),
    softwareList: document.getElementById("softwareFilter"),
    softwareHeading: document.getElementById("softwareFilterTitle"),
    filterForm: document.getElementById("filterForm"),
    sortableHeaders: Array.from(document.querySelectorAll("th[data-sort-key]")),
  };

  if (!elements.table || !elements.tableBody) {
    console.error("필수 테이블 요소를 찾을 수 없습니다.");
    return;
  }

  const columnCount = elements.table.querySelectorAll("thead th").length || 8;

  const stringsData = await loadStrings();
  const strings = resolveStrings(stringsData, locale);

  const filters = { query: "", software: "all", language: "all" };
  const sortState = { key: null, direction: "desc" };
  let baseRows = [];

  applyStaticStrings(strings);
  setStatusMessage(strings.loading, { busy: true });

  bindFilters();
  bindSorting();

  try {
    const [manualInstances, stats] = await Promise.all([
      loadInstances().catch((error) => {
        console.info("인스턴스 보조 데이터를 불러오지 못했습니다.", error);
        return [];
      }),
      loadStats(),
    ]);

    const statsMap = createStatsMap(stats);
    const manualMap = createManualInstanceMap(manualInstances);

    const statsHosts = Array.from(statsMap.keys());
    const manualHosts = Array.from(manualMap.keys());
    const manualOnlyHosts = manualHosts.filter((host) => !statsMap.has(host));
    const hosts = statsHosts.length ? [...statsHosts, ...manualOnlyHosts] : manualHosts;

    baseRows = hosts.reduce((acc, host) => {
      if (!host) {
        return acc;
      }

      const statsEntry = statsMap.get(host) ?? null;
      const manualEntry = manualMap.get(host) ?? null;

      const manualLanguages = manualEntry
        ? normalizeLanguageList(manualEntry.languages)
        : [];
      const statsLanguages = Array.isArray(statsEntry?.languages_detected)
        ? statsEntry.languages_detected
        : [];
      const languages = mergeLanguageLists(manualLanguages, statsLanguages);

      const rawSoftwareName =
        stringOrNull(statsEntry?.software?.name) ?? stringOrNull(manualEntry?.platform);
      const softwareKey = normalizeSoftwareKey(rawSoftwareName) || "unknown";
      const softwareLabel = resolveSoftwareLabel(
        rawSoftwareName,
        manualEntry?.platform,
        strings
      );

      const instance = {
        name: stringOrNull(manualEntry?.name) ?? host,
        url: stringOrNull(manualEntry?.url) ?? (host ? `https://${host}` : null),
        platform: softwareLabel,
        description: stringOrNull(manualEntry?.description),
        languages: manualLanguages,
      };

      acc.push({
        order: acc.length,
        instance,
        host,
        stats: statsEntry,
        nodeinfoDescription: null,
        nodeinfoLanguages: [],
        languages,
        softwareKey,
        softwareRaw: rawSoftwareName,
        softwareLabel,
      });
      return acc;
    }, []);

    updateSoftwareSidebar(baseRows, strings);
    updateLanguageOptions(baseRows, strings);

    if (baseRows.length === 0) {
      setStatusMessage(strings.no_instances);
      return;
    }

    updateDisplay();

    preloadNodeInfoDetails(baseRows).catch((error) => {
      console.info("노드 정보 세부 정보를 불러오는 중 문제가 발생했습니다.", error);
    });
  } catch (error) {
    console.error(error);
    setStatusMessage(strings.error_fetch, { allowHTML: true });
  }

  function bindFilters() {
    if (elements.filterForm) {
      elements.filterForm.addEventListener("submit", (event) => event.preventDefault());
    }

    if (elements.searchInput) {
      elements.searchInput.addEventListener("input", () => {
        filters.query = elements.searchInput.value.trim().toLowerCase();
        updateDisplay();
      });
    }

    if (elements.languageSelect) {
      elements.languageSelect.addEventListener("change", () => {
        const value = elements.languageSelect.value || "all";
        filters.language = value;
        updateDisplay();
      });
    }

    if (elements.softwareList) {
      elements.softwareList.addEventListener("click", (event) => {
        const button = event.target.closest("button[data-software]");
        if (!button) {
          return;
        }
        const value = button.dataset.software || "all";
        if (filters.software === value) {
          return;
        }
        filters.software = value;
        updateDisplay();
      });
    }
  }

  function bindSorting() {
    elements.sortableHeaders.forEach((header) => {
      const button = header.querySelector("button");
      if (!button) return;

      button.addEventListener("click", () => {
        const key = header.dataset.sortKey;
        if (!key) return;

        if (sortState.key === key) {
          sortState.direction = sortState.direction === "asc" ? "desc" : "asc";
        } else {
          sortState.key = key;
          sortState.direction = "desc";
        }

        updateDisplay();
      });
    });
  }

  function updateDisplay() {
    if (!baseRows.length) {
      updateSoftwareActiveState();
      setStatusMessage(strings.no_instances);
      return;
    }

    const filteredRows = filterRows(baseRows);
    updateSoftwareActiveState();

    const sortedRows = sortRows(filteredRows);

    renderRows(sortedRows);
    updateSortIndicators();
  }

  function filterRows(rows) {
    return rows.filter((row) => {
      const { instance, nodeinfoDescription, languages = [], softwareKey } = row;

      const matchesSoftware =
        filters.software === "all" || softwareKey === filters.software;

      if (!matchesSoftware) {
        return false;
      }

      const matchesLanguage =
        filters.language === "all" ||
        languages.some((code) => code === filters.language);

      if (!matchesLanguage) {
        return false;
      }

      if (!filters.query) {
        return true;
      }

      const haystack = `${instance.name ?? ""} ${instance.description ?? ""} ${
        nodeinfoDescription ?? ""
      } ${row.host ?? ""}`
        .toString()
        .toLowerCase();
      return haystack.includes(filters.query);
    });
  }

  function sortRows(rows) {
    if (!sortState.key) {
      return [...rows];
    }

    const directionMultiplier = sortState.direction === "asc" ? 1 : -1;
    const key = sortState.key;

    return [...rows].sort((a, b) => {
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
    elements.sortableHeaders.forEach((header) => {
      const key = header.dataset.sortKey;
      const button = header.querySelector("button");
      if (!key || !button) return;

      if (sortState.key === key) {
        header.dataset.direction = sortState.direction;
        header.setAttribute(
          "aria-sort",
          sortState.direction === "asc" ? "ascending" : "descending"
        );
        button.setAttribute("aria-pressed", "true");
      } else {
        header.removeAttribute("data-direction");
        header.removeAttribute("aria-sort");
        button.setAttribute("aria-pressed", "false");
      }
    });
  }

  function renderRows(rows) {
    if (!rows.length) {
      const hasActiveFilters =
        filters.query.length > 0 ||
        filters.software !== "all" ||
        filters.language !== "all";
      setStatusMessage(hasActiveFilters ? strings.no_results : strings.no_instances);
      return;
    }

    const fragment = document.createDocumentFragment();

    rows.forEach((entry) => {
      const { instance, stats, host, nodeinfoDescription, softwareLabel } = entry;
      const tableRow = document.createElement("tr");
      if (host) {
        tableRow.dataset.host = host;
      }

      const nameCell = document.createElement("th");
      nameCell.scope = "row";
      nameCell.className = "cell-name";

      const nameHeading = document.createElement("div");
      nameHeading.className = "cell-name__title";
      nameHeading.textContent = textOrFallback(instance.name);

      const badge = createVerificationBadge(stats, strings);
      if (badge) {
        nameHeading.appendChild(badge);
      }

      nameCell.appendChild(nameHeading);

      const descriptionText =
        stringOrNull(nodeinfoDescription) ?? stringOrNull(instance.description);
      if (descriptionText) {
        const description = document.createElement("p");
        description.className = "cell-name__description";
        description.textContent = descriptionText;
        nameCell.appendChild(description);
      }

      const urlCell = document.createElement("td");
      if (instance.url) {
        const link = document.createElement("a");
        link.href = instance.url;
        link.textContent = instance.url.replace(/^https?:\/\//, "");
        link.rel = "noopener";
        link.target = "_blank";
        urlCell.appendChild(link);
      } else {
        urlCell.textContent = strings.no_data;
      }

      const platformCell = document.createElement("td");
      platformCell.textContent = textOrFallback(softwareLabel ?? instance.platform);

      const registrationCell = document.createElement("td");
      const registrationSummary = formatRegistration(stats, strings);
      registrationCell.textContent = registrationSummary;
      if (registrationSummary && registrationSummary !== strings.no_data) {
        registrationCell.title = registrationSummary;
      }

      const languagesCell = document.createElement("td");
      languagesCell.textContent = formatLanguages(row, strings);

      const usersTotalCell = document.createElement("td");
      usersTotalCell.textContent = formatNumber(stats?.users_total);

      const usersActiveCell = document.createElement("td");
      usersActiveCell.textContent = formatNumber(stats?.users_active_month);

      const statusesCell = document.createElement("td");
      statusesCell.textContent = formatNumber(stats?.statuses);

      tableRow.append(
        nameCell,
        urlCell,
        platformCell,
        registrationCell,
        languagesCell,
        usersTotalCell,
        usersActiveCell,
        statusesCell
      );
      fragment.appendChild(tableRow);
    });

    elements.tableBody.innerHTML = "";
    elements.tableBody.appendChild(fragment);
    elements.table.setAttribute("aria-busy", "false");
  }

  async function loadInstances() {
    const response = await fetch(resolveAssetUrl("data/instances.json"));
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
      const response = await fetch(resolveAssetUrl("data/stats.ok.json"), {
        cache: "no-store",
      });
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
      if (!entry) return;
      const key = normalizeHostValue(entry.host ?? entry.url);
      if (!key) return;
      map.set(key, {
        users_total: getNumericValue(entry.users_total),
        users_active_month: getNumericValue(entry.users_active_month),
        statuses: getNumericValue(entry.statuses),
        verified_activitypub:
          entry.verified_activitypub === true
            ? true
            : entry.verified_activitypub === false
            ? false
            : null,
        open_registrations: parseBoolean(entry.open_registrations),
        software: normalizeSoftware(entry.software),
        languages_detected: normalizeLanguageList(entry.languages_detected),
        fetched_at: entry.fetched_at ?? null,
      });
    });
    return map;
  }

  function createManualInstanceMap(instances) {
    const map = new Map();
    if (!Array.isArray(instances)) {
      return map;
    }

    instances.forEach((instance) => {
      if (!instance || typeof instance !== "object") return;
      const host = extractHost(instance);
      if (!host) return;
      map.set(host, instance);
    });

    return map;
  }

  function extractHost(instance) {
    if (!instance || typeof instance !== "object") return "";
    if (instance.host && typeof instance.host === "string") {
      const normalized = normalizeHostValue(instance.host);
      if (normalized) return normalized;
    }
    if (instance.url && typeof instance.url === "string") {
      try {
        const parsed = new URL(instance.url);
        if (parsed.hostname) {
          return parsed.hostname.toLowerCase();
        }
      } catch (error) {
        // ignore parse errors and fall through
      }
      return normalizeHostValue(instance.url.replace(/^https?:\/\//, ""));
    }
    return "";
  }

  function normalizeHostValue(value) {
    if (!value || typeof value !== "string") return "";
    const trimmed = value.trim();
    if (!trimmed) {
      return "";
    }
    try {
      const parsed = new URL(trimmed);
      if (parsed.hostname) {
        return parsed.hostname.toLowerCase();
      }
    } catch (error) {
      // fall back to manual normalization
    }
    return trimmed
      .replace(/^https?:\/\//i, "")
      .split("/")[0]
      .replace(/\s+/g, "")
      .toLowerCase();
  }

  function getNumericValue(value) {
    if (value === null || value === undefined) return null;
    const numberValue = Number(value);
    return Number.isFinite(numberValue) ? numberValue : null;
  }

  function parseBoolean(value) {
    if (value === true) return true;
    if (value === false) return false;
    return null;
  }

  function normalizeSoftware(software) {
    if (!software || typeof software !== "object") return null;
    const name = stringOrNull(software.name);
    const version = stringOrNull(software.version);
    if (!name && !version) {
      return null;
    }
    return {
      name: name ?? null,
      version: version ?? null,
    };
  }

  function normalizeLanguageList(values) {
    if (!values) return [];
    if (!Array.isArray(values)) {
      values = [values];
    }
    const list = [];
    const seen = new Set();
    values.forEach((value) => {
      const code = normalizeLanguageCode(value);
      if (!code || seen.has(code)) return;
      seen.add(code);
      list.push(code);
    });
    return list;
  }

  function mergeLanguageLists(...collections) {
    const merged = [];
    const seen = new Set();

    collections.forEach((collection) => {
      if (!collection) return;
      const values = Array.isArray(collection) ? collection : [collection];
      values.forEach((value) => {
        const code = normalizeLanguageCode(value);
        if (!code || seen.has(code)) return;
        seen.add(code);
        merged.push(code);
      });
    });

    return merged;
  }

  function normalizeLanguageCode(value) {
    if (value === null || value === undefined) return "";
    const text = String(value).trim();
    if (!text) return "";
    return text.toLowerCase();
  }

  function formatNumber(value) {
    if (value === null || value === undefined) {
      return strings.no_data;
    }
    const numberValue = Number(value);
    if (!Number.isFinite(numberValue)) {
      return strings.no_data;
    }
    return numberValue.toLocaleString(numberLocale);
  }

  function stringOrNull(value) {
    if (value === null || value === undefined) {
      return null;
    }
    const text = String(value).trim();
    return text.length ? text : null;
  }

  function createVerificationBadge(stats, dict) {
    if (!stats || typeof stats !== "object") return null;
    if (stats.verified_activitypub === true) {
      const badge = document.createElement("span");
      badge.className = "badge badge--ok";
      badge.textContent = dict.badge_verified_ok;
      badge.title = dict.badge_verified_ok;
      return badge;
    }
    if (stats.verified_activitypub === false) {
      const badge = document.createElement("span");
      badge.className = "badge badge--warn";
      badge.textContent = dict.badge_verified_fail;
      badge.title = dict.badge_verified_fail;
      return badge;
    }
    return null;
  }

  function formatRegistration(stats, dict) {
    if (!stats || typeof stats !== "object") {
      return dict.no_data;
    }

    const joinState = stats.open_registrations;
    if (joinState === true) {
      return dict.registration_open;
    }
    if (joinState === false) {
      return dict.registration_closed;
    }
    return dict.registration_unknown;
  }

  function formatLanguages(row, dict) {
    const languages = Array.isArray(row?.languages) ? row.languages : [];
    if (!languages.length) {
      return dict.no_data;
    }

    const display = languages.map((code) => formatLanguageDisplay(code));
    return display.length ? display.join(", ") : dict.no_data;
  }

  function formatLanguageDisplay(code) {
    return code
      .split("-")
      .map((part, index) => (index === 0 ? part.toLowerCase() : part.toUpperCase()))
      .join("-");
  }

  function textOrFallback(value) {
    if (value === null || value === undefined) {
      return strings.no_data;
    }
    const text = String(value).trim();
    return text.length ? text : strings.no_data;
  }

  async function loadStrings() {
    try {
      const response = await fetch(resolveAssetUrl("i18n/strings.json"), {
        cache: "no-store",
      });
      if (!response.ok) {
        throw new Error(`문자열 파일을 불러올 수 없습니다: ${response.status}`);
      }
      const data = await response.json();
      if (typeof data !== "object" || data === null) {
        throw new Error("문자열 데이터 형식이 올바르지 않습니다.");
      }
      return data;
    } catch (error) {
      console.info("문자열 데이터를 불러오지 못했습니다. 내장 한국어 문자열을 사용합니다.");
      return FALLBACK_STRINGS;
    }
  }

  function resolveStrings(data, requestedLocale) {
    const fallback = FALLBACK_STRINGS.ko ?? {};
    if (!data || typeof data !== "object") {
      return { ...fallback };
    }
    const candidate = data[requestedLocale] || data.ko;
    if (candidate && typeof candidate === "object") {
      return { ...fallback, ...candidate };
    }
    return { ...fallback };
  }

  function applyStaticStrings(dict) {
    if (dict.title) {
      document.title = dict.title;
    }
    if (elements.pageTitle) {
      elements.pageTitle.textContent = dict.title;
    }
    if (elements.pageIntro) {
      elements.pageIntro.textContent = dict.intro;
    }
    if (elements.directoryTitle) {
      elements.directoryTitle.textContent = dict.table_heading;
    }
    if (elements.tableCaption) {
      elements.tableCaption.textContent = dict.table_caption;
    }
    if (elements.table) {
      elements.table.setAttribute("aria-label", dict.table_aria);
      if (elements.tableCaption?.id) {
        elements.table.setAttribute("aria-describedby", elements.tableCaption.id);
      }
    }
    if (elements.footerNote) {
      elements.footerNote.textContent = dict.footer_note;
    }
    if (elements.searchLabel) {
      elements.searchLabel.textContent = dict.search_label;
    }
    if (elements.searchInput) {
      elements.searchInput.placeholder = dict.search_placeholder;
      elements.searchInput.setAttribute("aria-label", dict.search_label);
    }
    if (elements.languageLabel) {
      elements.languageLabel.textContent = dict.language_filter_label;
    }
    if (elements.languageSelect) {
      elements.languageSelect.setAttribute("aria-label", dict.language_filter_label);
      elements.languageSelect.innerHTML = "";
      const opt = document.createElement("option");
      opt.value = "all";
      opt.textContent = dict.language_all;
      elements.languageSelect.appendChild(opt);
      elements.languageSelect.value = "all";
      filters.language = "all";
    }
    if (elements.softwareHeading) {
      elements.softwareHeading.textContent = dict.software_filter_heading;
    }
    filters.software = "all";

    setColumnText("name", dict.name);
    setColumnText("url", dict.url);
    setColumnText("platform", dict.platform);
    setColumnText("registration", dict.registration);
    setColumnText("languages", dict.languages);
    setColumnText("users_total", dict.users_total, dict.sort_users_total);
    setColumnText("users_active_month", dict.users_active, dict.sort_users_active);
    setColumnText("statuses", dict.statuses);
  }

  function setColumnText(columnKey, text, sortLabel) {
    const header = document.querySelector(`th[data-column="${columnKey}"]`);
    if (!header) return;
    const button = header.querySelector("button");
    if (button) {
      button.textContent = text;
      if (sortLabel) {
        button.setAttribute("aria-label", sortLabel);
        button.setAttribute("title", sortLabel);
      }
      button.setAttribute("aria-pressed", "false");
    } else {
      header.textContent = text;
    }
  }

  function setStatusMessage(message, options = {}) {
    const { allowHTML = false, busy = false } = options;
    elements.tableBody.innerHTML = "";
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = columnCount;
    if (allowHTML) {
      cell.innerHTML = message;
    } else {
      cell.textContent = message;
    }
    row.appendChild(cell);
    elements.tableBody.appendChild(row);
    elements.table.setAttribute("aria-busy", busy ? "true" : "false");
  }

  function normalizeSoftwareKey(value) {
    const text = stringOrNull(value);
    if (!text) {
      return "";
    }
    return text
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "");
  }

  function updateLanguageOptions(rows, dict) {
    if (!elements.languageSelect) return;

    const currentValue = elements.languageSelect.value || filters.language || "all";
    const seen = new Map();

    rows.forEach(({ languages = [] }) => {
      languages.forEach((value) => {
        const code = normalizeLanguageCode(value);
        if (!code || seen.has(code)) return;
        seen.set(code, formatLanguageDisplay(code));
      });
    });

    const sorted = Array.from(seen.entries()).sort((a, b) =>
      a[1].localeCompare(b[1], locale, { sensitivity: "base" })
    );

    elements.languageSelect.innerHTML = "";

    const allOption = document.createElement("option");
    allOption.value = "all";
    allOption.textContent = dict.language_all;
    elements.languageSelect.appendChild(allOption);

    sorted.forEach(([value, label]) => {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = label;
      elements.languageSelect.appendChild(option);
    });

    const validValues = new Set(["all", ...sorted.map(([value]) => value)]);
    const nextValue = validValues.has(currentValue) ? currentValue : "all";
    elements.languageSelect.value = nextValue;
    filters.language = nextValue;
  }

  function updateSoftwareSidebar(rows, dict) {
    if (!elements.softwareList) return;

    const counts = new Map();

    rows.forEach((row) => {
      const key = row.softwareKey || "unknown";
      const label = resolveSoftwareLabel(row.softwareRaw, row.instance?.platform, dict);
      row.softwareLabel = label;
      if (!counts.has(key)) {
        counts.set(key, { label, count: 0 });
      }
      counts.get(key).label = label;
      counts.get(key).count += 1;
    });

    const sorted = Array.from(counts.entries()).sort((a, b) =>
      a[1].label.localeCompare(b[1].label, locale, { sensitivity: "base" })
    );

    const list = elements.softwareList;
    list.innerHTML = "";

    const totalCount = rows.length;
    const allItem = document.createElement("li");
    const allButton = document.createElement("button");
    allButton.type = "button";
    allButton.className = "sidebar__button";
    allButton.dataset.software = "all";

    const allLabel = document.createElement("span");
    allLabel.textContent = dict.software_all;
    const allBadge = document.createElement("span");
    allBadge.className = "sidebar__badge";
    allBadge.textContent = totalCount.toLocaleString(locale);
    allButton.append(allLabel, allBadge);
    allItem.appendChild(allButton);
    list.appendChild(allItem);

    sorted.forEach(([value, info]) => {
      const item = document.createElement("li");
      const button = document.createElement("button");
      button.type = "button";
      button.className = "sidebar__button";
      button.dataset.software = value || "unknown";

      const labelSpan = document.createElement("span");
      labelSpan.textContent = info.label;
      const countBadge = document.createElement("span");
      countBadge.className = "sidebar__badge";
      countBadge.textContent = info.count.toLocaleString(locale);

      button.append(labelSpan, countBadge);
      item.appendChild(button);
      list.appendChild(item);
    });

    const validValues = new Set(["all", ...sorted.map(([value]) => value || "unknown")]);
    if (!validValues.has(filters.software)) {
      filters.software = "all";
    }

    updateSoftwareActiveState();
  }

  function updateSoftwareActiveState() {
    if (!elements.softwareList) return;
    const buttons = elements.softwareList.querySelectorAll("button[data-software]");
    buttons.forEach((button) => {
      const value = button.dataset.software || "all";
      if (value === filters.software) {
        button.setAttribute("aria-current", "true");
      } else {
        button.removeAttribute("aria-current");
      }
    });
  }

  function resolveSoftwareLabel(rawName, fallbackLabel, dict) {
    const primary = stringOrNull(rawName);
    const fallback = stringOrNull(fallbackLabel);
    const normalized = normalizeSoftwareKey(primary ?? fallback);
    if (normalized) {
      const translationKey = `software_label_${normalized}`;
      if (translationKey in dict && stringOrNull(dict[translationKey])) {
        return dict[translationKey];
      }
      if (normalized in KNOWN_SOFTWARE_LABELS) {
        return KNOWN_SOFTWARE_LABELS[normalized];
      }
    }

    if (fallback) {
      return fallback;
    }
    if (primary) {
      const formatted = formatSoftwareDisplayName(primary);
      if (formatted) {
        return formatted;
      }
    }
    return dict.software_unknown;
  }

  function formatSoftwareDisplayName(value) {
    const text = stringOrNull(value);
    if (!text) {
      return "";
    }

    return text
      .replace(/[-_]+/g, " ")
      .split(" ")
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(" ");
  }

  function resolveAssetBaseUrl() {
    try {
      return new URL("./", document.baseURI);
    } catch (error) {
      console.warn("문서 기준 경로를 계산할 수 없습니다.", error);
      return new URL("./", window.location.href);
    }
  }

  function resolveAssetUrl(path) {
    return new URL(path, assetBaseUrl).toString();
  }

  async function preloadNodeInfoDetails(rows) {
    const uniqueHosts = Array.from(
      new Set(rows.map(({ host }) => normalizeHostValue(host)).filter(Boolean))
    );

    if (!uniqueHosts.length) {
      return;
    }

    const results = await Promise.all(
      uniqueHosts.map(async (host) => ({
        host,
        details: await fetchNodeInfoDetails(host),
      }))
    );

    let updated = false;
    results.forEach(({ host, details }) => {
      if (!details) {
        return;
      }
      const description = stringOrNull(details.description);
      const languages = Array.isArray(details.languages)
        ? mergeLanguageLists(details.languages)
        : [];

      rows.forEach((row) => {
        if (row.host !== host) {
          return;
        }
        if (description && row.nodeinfoDescription !== description) {
          row.nodeinfoDescription = description;
          updated = true;
        }

        if (languages.length) {
          const mergedLanguages = mergeLanguageLists(row.languages, languages);
          if (mergedLanguages.length !== row.languages.length) {
            row.languages = mergedLanguages;
            updated = true;
          }

          const mergedNodeinfo = mergeLanguageLists(row.nodeinfoLanguages, languages);
          if (mergedNodeinfo.length !== row.nodeinfoLanguages.length) {
            row.nodeinfoLanguages = mergedNodeinfo;
          }
        }
      });
    });

    if (updated) {
      updateLanguageOptions(rows, strings);
      updateDisplay();
    }
  }

  async function fetchNodeInfoDetails(host) {
    if (!host) return null;

    const origin = `https://${host}`;

    try {
      const wellKnownResponse = await fetch(`${origin}/.well-known/nodeinfo`, {
        cache: "no-store",
        mode: "cors",
      });
      if (!wellKnownResponse.ok) {
        return null;
      }

      const payload = await wellKnownResponse.json();
      if (!payload || !Array.isArray(payload.links)) {
        return null;
      }

      const prioritizedLinks = prioritizeNodeInfoLinks(payload.links);
      for (const link of prioritizedLinks) {
        try {
          const targetUrl = new URL(link.href, `${origin}/`);
          const nodeInfoResponse = await fetch(targetUrl, {
            cache: "no-store",
            mode: "cors",
          });
          if (!nodeInfoResponse.ok) {
            continue;
          }

          const nodeInfo = await nodeInfoResponse.json();
          const description = extractDescriptionFromNodeInfo(nodeInfo);
          const languages = extractLanguagesFromNodeInfo(nodeInfo);
          if (description || languages.length) {
            return { description: description ?? null, languages };
          }
        } catch (error) {
          // Ignore individual nodeinfo fetch errors and try next link.
        }
      }
    } catch (error) {
      // Ignore network errors silently.
    }

    return null;
  }

  function prioritizeNodeInfoLinks(links) {
    const priorities = [
      "https://nodeinfo.diaspora.software/ns/schema/2.1",
      "https://nodeinfo.diaspora.software/ns/schema/2.0",
      "https://nodeinfo.diaspora.software/ns/schema/1.1",
      "https://nodeinfo.diaspora.software/ns/schema/1.0",
      "http://nodeinfo.diaspora.software/ns/schema/2.1",
      "http://nodeinfo.diaspora.software/ns/schema/2.0",
      "http://nodeinfo.diaspora.software/ns/schema/1.1",
      "http://nodeinfo.diaspora.software/ns/schema/1.0",
    ];

    const recognized = links
      .filter((link) => link && typeof link.rel === "string" && typeof link.href === "string")
      .map((link) => ({ ...link, priority: priorities.indexOf(link.rel) }))
      .filter((entry) => entry.priority >= 0)
      .sort((a, b) => a.priority - b.priority)
      .map(({ priority, ...link }) => link);

    if (recognized.length) {
      return recognized;
    }

    return links.filter((link) => link && typeof link.href === "string");
  }

  function extractDescriptionFromNodeInfo(nodeInfo) {
    if (!nodeInfo || typeof nodeInfo !== "object") {
      return null;
    }

    const metadata = nodeInfo.metadata;
    if (!metadata || typeof metadata !== "object") {
      return null;
    }

    const candidates = [
      metadata.nodeDescription,
      metadata.description,
      metadata.shortDescription,
      metadata.summary,
      metadata.defaultDescription,
      metadata.node?.description,
    ];

    for (const candidate of candidates) {
      const text = stringOrNull(candidate);
      if (text) {
        return text;
      }
    }

    return null;
  }

  function extractLanguagesFromNodeInfo(nodeInfo) {
    if (!nodeInfo || typeof nodeInfo !== "object") {
      return [];
    }

    const metadata = typeof nodeInfo.metadata === "object" ? nodeInfo.metadata : null;
    const usage = typeof nodeInfo.usage === "object" ? nodeInfo.usage : null;

    const collections = [
      metadata?.languages,
      metadata?.language,
      metadata?.languages_detected,
      metadata?.languagesDetected,
      metadata?.node?.languages,
      usage?.languages,
      nodeInfo.language,
    ];

    return mergeLanguageLists(...collections);
  }
})();
