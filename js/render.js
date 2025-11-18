(async function renderDirectory() {
  const assetBaseUrl = resolveAssetBaseUrl();
  const locale = document.documentElement.lang || "ko";
  const numberLocale = locale || "ko-KR";

  const KNOWN_SOFTWARE_LABELS = {
    akkoma: "Akkoma",
    bookwyrm: "BookWyrm",
    calckey: "Calckey",
    firefish: "Firefish",
    friendica: "Friendica",
    funkwhale: "Funkwhale",
    ghost: "Ghost",
    gotosocial: "GoToSocial",
    hubzilla: "Hubzilla",
    kbin: "Kbin",
    lemmy: "Lemmy",
    mastodon: "마스토돈",
    misskey: "Misskey",
    peertube: "PeerTube",
    pixelfed: "PixelFed",
    pleroma: "Pleroma",
    sharkey: "Sharkey",
    streams: "Streams",
    takahe: "Takahē",
    wordpress: "WordPress",
    writefreely: "WriteFreely",
  };

  const FALLBACK_STRINGS = {
    ko: {
      title: "연합우주를 여행하는 히치하이커를 위한 안내서",
      intro:
        "한국어로 운영되는 페디버스 인스턴스를 수동으로 정리한 목록입니다.",
      search_label: "검색어",
      search_placeholder: "이름 또는 설명 검색",
      software_filter_heading: "소프트웨어 분류",
      software_all: "전체 소프트웨어",
      software_unknown: "기타",
      language_filter_label: "언어",
      language_all: "전체 언어",
      table_heading: "인스턴스 목록",
      show_closed_label: "가입 닫힌 서버 표시",
      badge_open: "가입 열림",
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
      footer_note:
        "데이터는 data/instances.json과 data/stats.ok.json 파일을 수정해 갱신할 수 있습니다.",
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
    showClosedToggle: document.getElementById("showClosedToggle"),
    showClosedLabel: document.getElementById("showClosedLabel"),
    softwareList: document.getElementById("softwareFilter"),
    softwareHeading: document.getElementById("softwareFilterTitle"),
    filterForm: document.getElementById("filterForm"),
    sortableHeaders: Array.from(document.querySelectorAll("th[data-sort-key]")),
  };

  if (!elements.table || !elements.tableBody) {
    console.error("필수 테이블 요소를 찾을 수 없습니다.");
    return;
  }

  const columnCount = elements.table.querySelectorAll("thead th").length || 7;

  const stringsData = await loadStrings();
  const strings = resolveStrings(stringsData, locale);

  const filters = {
    query: "",
    software: "all",
    language: "all",
    showClosed: false,
  };
  const sortState = { key: null, direction: "desc" };
  let baseRows = [];

  const pinnedSoftware = loadPinnedSoftware();
  let openParent = null; // 하나만 펼쳐둔다

  const SOFTWARE_ORDER = [
    "mastodon",
    "misskey",
    "pleroma",
    "gotosocial",
    "friendica",
    "pixelfed",
    "blog",
    "social",
    "video",
    "forum",
    "bridge",
    "relay",
    "bot",
    "unknown",
  ];

  const SOFTWARE_LINEAGE = {
    mastodon: ["ecko", "fedibird", "hometown", "kmyblue", "wildebeest"],
    misskey: [
      "calckey",
      "catodon",
      "cherrypick",
      "dolphin",
      "firefish",
      "foundkey",
      "goblin",
      "hickey",
      "iceshrimp",
      "magnetar",
      "meisskey",
      "nexkey",
      "pawkey",
      "quollkey",
      "sharkey",
      "yoiyami",
      "yojo-art",
    ],
    pleroma: [
      "akkoma",
      "cagando",
      "gharab-tzereq",
      "incestoma",
      "instance-softwarename",
    ],
    forum: ["lemmy", "kbin", "mbin", "mobilizon"],
    blog: [
      "blog-server",
      "wordpress",
      "contentnation",
      "dch-blog",
      "forte",
      "goblog",
      "ghost",
      "hatsu",
      "hubzilla",
      "oolong",
      "owl-blogs",
      "ma-at",
      "monoplace-ca",
      "microblogpub",
      "nettepuoti-activitypub-limited-support",
      "writefreely",
    ],
    social: [
      "98gravity-electro",
      "amiverse",
      "bonfire",
      "bookwyrm",
      "bovine",
      "bonfou",
      "brighteon",
      "cattle-grid",
      "comal",
      "cosmoslide",
      "ditto",
      "dope-network",
      "dumpster-federation",
      "elgg",
      "emissary",
      "encryptr-net",
      "frequency",
      "funkwhale",
      "gancio",
      "gnusocial",
      "hackerspub",
      "handle",
      "harmony",
      "hollo",
      "honk",
      "hourlyphoto",
      "iceshrimp-net",
      "incise",
      "kiiteitte",
      "ktistec",
      "letter",
      "mammuthus",
      "plume",
      "snac",
      "soapbox",
      "socialhome",
      "smithereen",
      "takahe",
      "wafrn",
      "wellesley",
    ],
    relay: [
      "activity-relay",
      "activityrelay",
      "aoderelay",
      "awakari",
      "buzzrelay",
      "selective-relay",
    ],
    bridge: [
      "bird-meetup",
      "birdsitelive",
      "bridgy-fed",
      "ccworld-ap-bridge",
      "encyclia",
    ],
    bot: ["fedichatbot", "tiofomento-fedverse-bot"],
    video: ["loops", "peertube", "open-streaming-platform", "owncast"],
    unknown: [
      "activitypods",
      "activity-xsrv-win",
      "activitypub-rails",
      "appy",
      "betula",
      "bugle",
      "capubara",
      "careercupid",
      "castling-club",
      "chess-infinito-nexus",
      "claremontwx",
      "codename-merp",
      "d250g2",
      "dailyrucks",
      "divedb",
      "drupal",
      "dxnet",
      "eliza-and-the-moneymakers",
      "fediblock-instance",
      "fedipage",
      "fedirouter",
      "fedsy",
      "flohmarkt",
      "finalboss",
      "forgejo",
      "gathio",
      "g105b",
      "gitea",
      "gush",
      "hanbitfediverse",
      "hiddenphox",
      "hono",
      "ibis",
      "itinerariummentis",
      "lipupini",
      "lotide",
      "lubargw2",
      "manyfold",
      "meshdags-site-a-dags-dk",
      "mitra",
      "mobilizon",
      "nextcloud-social",
      "citizen4",
      "luon-cloud",
      "simcloud",
      "the-lukes-cloud",
      "neodb",
      "nodebb",
      "octofedi",
      "openlink-virtuoso",
      "postmarks",
      "single-file-activitypub-server-in-php",
      "undefined",
      "piefed",
      "private",
      "pub",
      "squidcity",
      "sutty-distributed-press",
      "tootik",
      "wxwclub",
    ],
  };

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
    const hosts = statsHosts.length ? statsHosts : manualHosts;

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
        stringOrNull(statsEntry?.software?.name) ??
        stringOrNull(manualEntry?.platform);
      const softwareKey = normalizeSoftwareKey(rawSoftwareName) || "unknown";
      const softwareLabel = resolveSoftwareLabel(
        rawSoftwareName,
        manualEntry?.platform,
        strings
      );

      const instance = {
        name: stringOrNull(manualEntry?.name) ?? host,
        url:
          stringOrNull(manualEntry?.url) ?? (host ? `https://${host}` : null),
        platform: softwareLabel,
        description: stringOrNull(manualEntry?.description),
        languages: manualLanguages,
      };

      // ✅ stats.ok.json에서 nodeinfo_description을 직접 사용
      const nodeinfoDescription = stringOrNull(
        statsEntry?.nodeinfo_description
      );

      acc.push({
        order: acc.length,
        instance,
        host,
        stats: statsEntry,
        nodeinfoDescription: nodeinfoDescription, // ✅ 여기에 할당
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
      console.info(
        "노드 정보 세부 정보를 불러오는 중 문제가 발생했습니다.",
        error
      );
    });
  } catch (error) {
    console.error(error);
    setStatusMessage(strings.error_fetch, { allowHTML: true });
  }

  function findParentSoftware(key) {
    for (const parent in SOFTWARE_LINEAGE) {
      if (SOFTWARE_LINEAGE[parent].includes(key)) {
        return parent;
      }
    }
    return null;
  }

  function bindFilters() {
    if (elements.filterForm) {
      elements.filterForm.addEventListener("submit", (event) =>
        event.preventDefault()
      );
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

    if (elements.showClosedToggle) {
      const updateToggleUI = () => {
        elements.showClosedToggle.classList.toggle(
          "toggle-button--active",
          filters.showClosed
        );
        elements.showClosedToggle.setAttribute(
          "aria-pressed",
          filters.showClosed ? "true" : "false"
        );
      };

      // 초기 상태 UI 반영
      updateToggleUI();

      elements.showClosedToggle.addEventListener("click", () => {
        filters.showClosed = !filters.showClosed;
        updateToggleUI();
        updateDisplay();
      });
    }

    if (elements.softwareList) {
      elements.softwareList.addEventListener("click", (event) => {
        const toggle = event.target.closest(".sidebar__toggle");

        // 1) 접힘 토글 클릭
        // 상위 카테고리 토글 (아코디언 방식)
        if (toggle) {
          const parentBtn = toggle.closest("button[data-software]");
          const key = parentBtn?.dataset.software;

          if (!key) return;

          // 이미 열려 있던 parent를 눌렀으면 닫기
          if (openParent === key) {
            openParent = null;
          } else {
            openParent = key;
          }

          applyCollapseState();
          return;
        }

        // 3) 카테고리 필터 변경
        const button = event.target.closest("button[data-software]");
        if (!button) return;

        const value = button.dataset.software || "all";
        if (filters.software === value) return;

        filters.software = value;

        // ALL을 누르면 접힘 상태 초기화
        if (value === "all") {
          openParent = null;
          applyCollapseState();
        }

        updateDisplay();
      });
    }
  }

  function togglePin(key) {
    if (pinnedSoftware.has(key)) {
      pinnedSoftware.delete(key);
    } else {
      pinnedSoftware.add(key);
    }
    savePinnedSoftware();
    // 사이드바 구조가 바뀌었으니 다시 렌더
    updateSoftwareSidebar(baseRows, strings);
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
      const {
        instance,
        nodeinfoDescription,
        languages = [],
        softwareKey,
        stats,
      } = row;
      const parent = findParentSoftware(softwareKey);
      const effectiveKey = parent || softwareKey;

      const matchesSoftware =
        filters.software === "all" ||
        effectiveKey === filters.software ||
        softwareKey === filters.software;

      if (!matchesSoftware) {
        return false;
      }

      const matchesLanguage =
        filters.language === "all" ||
        languages.some((code) => code === filters.language);

      if (!matchesLanguage) {
        return false;
      }

      // 가입 상태 필터: showClosed가 false일 때는 가입 열린 서버만 표시
      if (!filters.showClosed) {
        const isOpen = stats && stats.open_registrations === true;
        if (!isOpen) {
          return false;
        }
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
      setStatusMessage(
        hasActiveFilters ? strings.no_results : strings.no_instances
      );
      return;
    }

    const fragment = document.createDocumentFragment();

    rows.forEach((entry) => {
      const { instance, stats, host, nodeinfoDescription, softwareLabel } =
        entry;
      const tableRow = document.createElement("tr");
      if (host) {
        tableRow.dataset.host = host;
      }

      const nameCell = document.createElement("th");
      nameCell.scope = "row";
      nameCell.className = "cell-name";

      const nameHeading = document.createElement("div");
      nameHeading.className = "cell-name__title";

      // 링크 생성
      const nameLink = document.createElement("a");
      const linkHref = instance.url || (host ? `https://${host}` : null);
      const nameText = textOrFallback(instance.name || host);

      if (linkHref) {
        nameLink.href = linkHref;
        nameLink.target = "_blank";
        nameLink.rel = "noopener";
      }
      nameLink.textContent = nameText;

      nameHeading.appendChild(nameLink);

      // 가입 여부 배지
      const openBadge = createOpenRegistrationBadge(stats, strings);
      if (openBadge) {
        nameHeading.appendChild(openBadge);
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

      const platformCell = document.createElement("td");
      platformCell.textContent = textOrFallback(
        softwareLabel ?? instance.platform
      );

      const languagesCell = document.createElement("td");
      languagesCell.textContent = formatLanguages(entry, strings);

      const usersTotalCell = document.createElement("td");
      usersTotalCell.textContent = formatNumber(stats?.users_total);

      const usersActiveCell = document.createElement("td");
      usersActiveCell.textContent = formatNumber(stats?.users_active_month);

      const statusesCell = document.createElement("td");
      statusesCell.textContent = formatNumber(stats?.statuses);

      tableRow.append(
        nameCell,
        platformCell,
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
      throw new Error(
        `인스턴스 데이터를 불러올 수 없습니다: ${response.status}`
      );
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
      console.info(
        "통계 데이터를 불러오지 못했습니다. 기본 값으로 표시합니다."
      );
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
        // ✅ nodeinfo_description 필드 추가!
        nodeinfo_description: stringOrNull(entry.nodeinfo_description),
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
      const expanded = expandLanguageValue(value);
      expanded.forEach((item) => {
        const code = normalizeLanguageCode(item);
        if (!code || seen.has(code)) return;
        seen.add(code);
        list.push(code);
      });
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
    const text = String(value).replace(/[;|]/g, ",").replace(/_/g, "-").trim();
    if (!text) return "";
    const cleaned = text
      .replace(/[^a-z0-9-]/gi, "-")
      .replace(/-{2,}/g, "-")
      .replace(/^-+|-+$/g, "");
    return cleaned.toLowerCase();
  }

  function expandLanguageValue(value) {
    if (Array.isArray(value)) {
      return value.flatMap((item) => expandLanguageValue(item));
    }
    if (value === null || value === undefined) {
      return [];
    }
    const text = String(value).replace(/[;|]/g, ",").trim();
    if (!text) {
      return [];
    }
    return text
      .split(/[,\s]+/)
      .map((part) => part.trim())
      .filter(Boolean);
  }

  function detectLanguagesFromText(value) {
    const text = stringOrNull(value);
    if (!text) {
      return [];
    }

    const detectors = [
      { regex: /[\u1100-\u11ff\u3130-\u318f\uac00-\ud7af]/, code: "ko" },
      { regex: /[\u3040-\u30ff]/, code: "ja" },
      { regex: /[\u4e00-\u9fff\u3400-\u4dbf]/, code: "zh" },
      { regex: /[\u0400-\u04ff]/, code: "ru" },
      { regex: /[\u0e00-\u0e7f]/, code: "th" },
      { regex: /[\u0600-\u06ff]/, code: "ar" },
      { regex: /[\u0900-\u097f]/, code: "hi" },
      { regex: /[\u0590-\u05ff]/, code: "he" },
    ];

    const detected = new Set();
    detectors.forEach(({ regex, code }) => {
      if (regex.test(text)) {
        detected.add(code);
      }
    });

    return Array.from(detected);
  }

  function formatNumber(value) {
    if (value === null || value === undefined) {
      return strings.no_data;
    }
    const numberValue = Number(value);
    if (!Number.isFinite(numberValue)) {
      return strings.no_data;
    }

    try {
      // 지정된 로케일로 먼저 시도
      return numberValue.toLocaleString(numberLocale);
    } catch (error) {
      // 로케일이 지원되지 않거나 이상하면 브라우저 기본 로케일로 fallback
      try {
        return numberValue.toLocaleString();
      } catch {
        // 이것마저도 안 되면 그냥 숫자 그대로
        return String(numberValue);
      }
    }
  }

  function stringOrNull(value) {
    if (value === null || value === undefined) {
      return null;
    }
    const text = String(value).trim();
    return text.length ? text : null;
  }

  function createOpenRegistrationBadge(stats, dict) {
    if (!stats || typeof stats !== "object") return null;
    if (stats.open_registrations === true) {
      const badge = document.createElement("span");
      // 검증됨 배지와 같은 초록색 스타일 사용
      badge.className = "badge badge--ok";
      badge.textContent = dict.badge_open;
      badge.title = dict.badge_open;
      return badge;
    }
    return null;
  }

  function formatLanguages(row, dict) {
    const languages = Array.isArray(row?.languages) ? row.languages : [];
    if (!languages.length) {
      return dict.no_data;
    }

    const display = languages.map((code) => formatLanguageDisplay(code, dict));
    return display.length ? display.join(", ") : dict.no_data;
  }

  function formatLanguageDisplay(code, dict) {
    if (!code) return "";

    // 기존 코드 표기(normalized code) 유지
    const normalized = code
      .split("-")
      .map((part, index) =>
        index === 0 ? part.toLowerCase() : part.toUpperCase()
      )
      .join("-");

    // 첫 부분만 따서 en, ja, ko 등으로 사용
    const base = normalized.split("-")[0];
    const key = `language_name_${base}`;
    const label = dict && dict[key];

    // strings.json에 언어 이름이 있으면 그걸 쓰고, 없으면 코드 그대로 사용
    return label || normalized;
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
      console.info(
        "문자열 데이터를 불러오지 못했습니다. 내장 한국어 문자열을 사용합니다."
      );
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
        elements.table.setAttribute(
          "aria-describedby",
          elements.tableCaption.id
        );
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
      elements.languageSelect.setAttribute(
        "aria-label",
        dict.language_filter_label
      );
      elements.languageSelect.innerHTML = "";
      const opt = document.createElement("option");
      opt.value = "all";
      opt.textContent = dict.language_all;
      elements.languageSelect.appendChild(opt);
      elements.languageSelect.value = "all";
      filters.language = "all";
    }
    if (elements.showClosedLabel) {
      elements.showClosedLabel.textContent = dict.show_closed_label;
    }
    if (elements.softwareHeading) {
      elements.softwareHeading.textContent = dict.software_filter_heading;
    }
    filters.software = "all";

    setColumnText("name", dict.name);
    setColumnText("platform", dict.platform);
    setColumnText("languages", dict.languages);
    setColumnText("users_total", dict.users_total, dict.sort_users_total);
    setColumnText(
      "users_active_month",
      dict.users_active,
      dict.sort_users_active
    );
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

    const currentValue =
      elements.languageSelect.value || filters.language || "all";
    const seen = new Map();

    rows.forEach(({ languages = [] }) => {
      languages.forEach((value) => {
        const code = normalizeLanguageCode(value);
        if (!code || seen.has(code)) return;
        seen.set(code, formatLanguageDisplay(code, dict));
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

  function buildSoftwareTree(rows) {
    const counts = new Map();

    // 개별 키 카운트
    rows.forEach((row) => {
      const key = row.softwareKey || "unknown";
      counts.set(key, (counts.get(key) || 0) + 1);
    });

    const tree = {};
    const usedKeys = new Set();

    // 1) 미리 정의한 상위 카테고리들부터 채우기
    SOFTWARE_ORDER.forEach((parent) => {
      const children = SOFTWARE_LINEAGE[parent] || [];
      const childList = [];
      let total = 0;

      children.forEach((childKey) => {
        const c = counts.get(childKey) || 0;
        if (c > 0) {
          childList.push([childKey, c]);
          total += c;
          usedKeys.add(childKey);
        }
      });

      const parentSelf = counts.get(parent) || 0;
      if (parentSelf > 0) {
        usedKeys.add(parent);
      }
      total += parentSelf;

      if (total === 0) return;

      tree[parent] = {
        total,
        parentSelf,
        children: childList,
      };
    });

    // 2) 어떤 카테고리에도 안 들어간 소프트웨어를 개별 상위 카테고리로 추가
    counts.forEach((count, key) => {
      if (!key) return;
      if (usedKeys.has(key)) return;

      tree[key] = {
        total: count,
        parentSelf: count,
        children: [], // 하위 없음
      };
    });

    return tree;
  }

  function updateSoftwareSidebar(rows, dict) {
    if (!elements.softwareList) return;
    const list = elements.softwareList;
    list.innerHTML = "";

    // 1) ALL
    const totalCount = rows.length;
    const allItem = document.createElement("li");
    const allBtn = document.createElement("button");
    allBtn.className = "sidebar__button";
    allBtn.dataset.software = "all";
    allBtn.textContent = `${dict.software_all} (${totalCount})`;
    allItem.appendChild(allBtn);
    list.appendChild(allItem);

    // 2) SOFTWARE_LINEAGE 기반 트리 구성
    const tree = buildSoftwareTree(rows);

    // 헬퍼: 부모 카테고리 하나 렌더링하는 함수
    function renderParent(parentKey) {
      const node = tree[parentKey];
      if (!node) return;

      const item = document.createElement("li");
      item.className = "sidebar__item";

      const parentBtn = document.createElement("button");
      parentBtn.className = "sidebar__button";
      parentBtn.dataset.software = parentKey;

      const labelText =
        parentKey === "unknown"
          ? dict.software_unknown
          : dict[`software_label_${parentKey}`] || parentKey;

      const hasChildren = node.children && node.children.length > 0;

      if (hasChildren) {
        const toggle = document.createElement("span");
        toggle.className = "sidebar__toggle";

        const isOpen = openParent === parentKey;
        toggle.textContent = isOpen ? "▾" : "▸";
        parentBtn.appendChild(toggle);
      }

      parentBtn.appendChild(
        document.createTextNode(` ${labelText} (${node.total})`)
      );

      item.appendChild(parentBtn);

      if (hasChildren) {
        const sub = document.createElement("ul");
        sub.className = "sidebar__sublist";
        sub.hidden = openParent !== parentKey;

        node.children.forEach(([childKey, count]) => {
          const childItem = document.createElement("li");
          const childBtn = document.createElement("button");
          childBtn.className = "sidebar__button";
          childBtn.dataset.software = childKey;

          const childLabel = dict[`software_label_${childKey}`] || childKey;

          childBtn.textContent = `${childLabel} (${count})`;
          childItem.appendChild(childBtn);
          sub.appendChild(childItem);
        });

        item.appendChild(sub);
      }

      list.appendChild(item);
    }

    // 3) 미리 정한 순서대로 상위 카테고리 렌더
    SOFTWARE_ORDER.forEach((parentKey) => {
      if (tree[parentKey]) {
        renderParent(parentKey);
      }
    });

    // 4) 카테고리에 안 들어간 소프트웨어들을 맨 아래에 알파벳 순으로 추가
    const extraKeys = Object.keys(tree).filter(
      (key) => !SOFTWARE_ORDER.includes(key)
    );

    if (extraKeys.length > 0) {
      // 알파벳(또는 locale) 기준 정렬
      extraKeys.sort((a, b) => {
        const labelA =
          a === "unknown"
            ? dict.software_unknown
            : dict[`software_label_${a}`] || a;
        const labelB =
          b === "unknown"
            ? dict.software_unknown
            : dict[`software_label_${b}`] || b;
        return labelA.localeCompare(labelB, locale, { sensitivity: "base" });
      });

      extraKeys.forEach((key) => {
        renderParent(key);
      });
    }

    updateSoftwareActiveState();
    applyCollapseState();
  }

  function applyCollapseState() {
    if (!elements.softwareList) return;

    const items = elements.softwareList.querySelectorAll(".sidebar__item");
    items.forEach((item) => {
      const btn = item.querySelector("button[data-software]");
      if (!btn) return;

      const key = btn.dataset.software;
      const toggle = btn.querySelector(".sidebar__toggle");
      const sublist = item.querySelector(".sidebar__sublist");

      // 하위가 없는 경우 무시
      if (!sublist) return;

      if (key === openParent) {
        // 펼치기
        sublist.hidden = false;
        item.classList.remove("sidebar__item--collapsed");
        if (toggle) toggle.textContent = "▾";
      } else {
        // 접기
        sublist.hidden = true;
        item.classList.add("sidebar__item--collapsed");
        if (toggle) toggle.textContent = "▸";
      }
    });
  }

  function updateSoftwareActiveState() {
    if (!elements.softwareList) return;
    const buttons = elements.softwareList.querySelectorAll(
      "button[data-software]"
    );
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
    const script = document.currentScript;
    if (script && script.src) {
      try {
        return new URL("../", script.src);
      } catch (error) {
        console.warn("스크립트 기준 경로를 계산할 수 없습니다.", error);
      }
    }

    const baseCandidates = [document.baseURI, window.location.href];
    for (const candidate of baseCandidates) {
      if (!candidate) continue;
      try {
        const url = new URL(candidate);
        if (!url.pathname.endsWith("/")) {
          url.pathname = `${url.pathname}/`;
        }
        return url;
      } catch (error) {
        console.warn("기준 경로를 계산할 수 없습니다.", error);
      }
    }

    return new URL("./", window.location.href);
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

    const concurrency = Math.min(6, Math.max(1, uniqueHosts.length));
    let cursor = 0;
    const results = [];

    async function worker() {
      while (true) {
        const index = cursor;
        if (index >= uniqueHosts.length) {
          break;
        }
        cursor += 1;
        const host = uniqueHosts[index];
        try {
          const details = await fetchInstanceDetails(host);
          results.push({ host, details });
        } catch (error) {
          console.info(
            `호스트 ${host}의 세부 정보를 불러오지 못했습니다.`,
            error
          );
        }
      }
    }

    await Promise.all(Array.from({ length: concurrency }, () => worker()));

    let updated = false;
    results.forEach(({ host, details }) => {
      if (!details) {
        return;
      }
      const description = stringOrNull(details.description);

      rows.forEach((row) => {
        if (row.host !== host) {
          return;
        }

        // ✅ 이미 설명이 있으면 덮어쓰지 않음 (중요!)
        if (!row.nodeinfoDescription && description) {
          row.nodeinfoDescription = description;
          updated = true;
        }
      });
    });

    if (updated) {
      // 🔴 언어 옵션 업데이트는 필요 없음 (설명만 업데이트했으므로)
      // updateLanguageOptions(rows, strings);
      updateDisplay(); // ✅ 설명 업데이트는 화면에 반영
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

  async function fetchInstanceDetails(host) {
    const nodeInfo = await fetchNodeInfoDetails(host);
    let description = stringOrNull(nodeInfo?.description) ?? null;

    if (!description) {
      const siteMetadata = await fetchSiteMetadata(host, {
        includeDescription: true,
        includeLanguages: false,
      });

      if (siteMetadata && siteMetadata.description) {
        description = siteMetadata.description;
      }
    }

    if (!description) {
      return null;
    }

    return {
      description: description ?? null,
      languages: [],
    };
  }

  async function fetchSiteMetadata(host, options = {}) {
    const { includeDescription = true, includeLanguages = false } = options;
    if (!includeDescription && !includeLanguages) {
      return null;
    }

    if (typeof DOMParser !== "function") {
      return null;
    }

    const protocols = ["https", "http"];

    for (const protocol of protocols) {
      const origin = `${protocol}://${host}`;

      try {
        const response = await fetch(`${origin}/`, {
          cache: "no-store",
          mode: "cors",
        });
        if (!response.ok) {
          continue;
        }

        const contentType = response.headers.get("content-type") || "";
        if (contentType && !contentType.includes("text/html")) {
          continue;
        }

        const html = await response.text();
        if (!html) {
          continue;
        }

        const parser = new DOMParser();
        const doc = parser.parseFromString(html, "text/html");
        if (!doc) {
          continue;
        }

        let description = null;
        if (includeDescription) {
          const descriptionSelectors = [
            'meta[name="description"]',
            'meta[property="og:description"]',
            'meta[name="og:description"]',
            'meta[name="twitter:description"]',
            'meta[property="twitter:description"]',
            'meta[name="summary"]',
          ];

          for (const selector of descriptionSelectors) {
            const element = doc.querySelector(selector);
            const content = element?.getAttribute("content");
            const value = stringOrNull(content);
            if (value) {
              description = value;
              break;
            }
          }

          if (!description) {
            const textContent = doc.querySelector("body");
            if (textContent) {
              const candidate = stringOrNull(textContent.textContent);
              if (candidate) {
                description = candidate.slice(0, 400);
              }
            }
          }
        }

        let languages = [];

        if (!description && !languages.length) {
          continue;
        }

        return {
          description: description ?? null,
          languages,
        };
      } catch (error) {
        continue;
      }
    }

    return null;
  }

  function collectLanguagesFromDocument(doc) {
    if (!doc) {
      return [];
    }

    const collected = new Set();
    const root = doc.documentElement;
    if (root) {
      extractLocalesFromMeta(root.getAttribute("lang")).forEach((code) =>
        collected.add(code)
      );
      extractLocalesFromMeta(root.getAttribute("xml:lang")).forEach((code) =>
        collected.add(code)
      );
    }

    const selectors = [
      'meta[name="language"]',
      'meta[name="lang"]',
      'meta[name="content-language"]',
      'meta[http-equiv="content-language"]',
      'meta[name="dc.language"]',
      'meta[property^="og:locale"]',
    ];

    selectors.forEach((selector) => {
      doc.querySelectorAll(selector).forEach((element) => {
        const content = element.getAttribute("content");
        extractLocalesFromMeta(content).forEach((code) => collected.add(code));
      });
    });

    const htmlAttributes = ["data-lang", "data-locale"];
    htmlAttributes.forEach((attribute) => {
      if (root?.hasAttribute(attribute)) {
        extractLocalesFromMeta(root.getAttribute(attribute)).forEach((code) =>
          collected.add(code)
        );
      }
    });

    return normalizeLanguageList(Array.from(collected));
  }

  function extractLocalesFromMeta(value) {
    if (value === null || value === undefined) {
      return [];
    }
    const text = String(value).replace(/[;|]/g, ",").trim();
    if (!text) {
      return [];
    }
    return text
      .split(/[,\s]+/)
      .map((part) => normalizeLocaleTag(part))
      .filter(Boolean);
  }

  function normalizeLocaleTag(value) {
    const text = stringOrNull(value);
    if (!text) {
      return "";
    }
    return text.replace(/_/g, "-").toLowerCase();
  }

  function loadPinnedSoftware() {
    try {
      const raw = window.localStorage.getItem("fedlist_pinned_software");
      if (!raw) return new Set();
      const arr = JSON.parse(raw);
      if (!Array.isArray(arr)) return new Set();
      return new Set(arr);
    } catch {
      return new Set();
    }
  }

  function savePinnedSoftware() {
    try {
      const arr = Array.from(pinnedSoftware);
      window.localStorage.setItem(
        "fedlist_pinned_software",
        JSON.stringify(arr)
      );
    } catch {
      // localStorage 막혀 있어도 무시
    }
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
      .filter(
        (link) =>
          link && typeof link.rel === "string" && typeof link.href === "string"
      )
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

    const metadata =
      typeof nodeInfo.metadata === "object" ? nodeInfo.metadata : null;
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
