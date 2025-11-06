(async function renderInstances() {
  const tableBody = document.getElementById("instances-body");

  try {
    const response = await fetch("data/instances.json");
    if (!response.ok) {
      throw new Error(`데이터를 불러오지 못했습니다: ${response.status}`);
    }
    const instances = await response.json();

    if (!Array.isArray(instances) || instances.length === 0) {
      tableBody.innerHTML = `<tr><td colspan=\"4\">표시할 인스턴스가 없습니다.</td></tr>`;
      return;
    }

    const fragment = document.createDocumentFragment();

    instances.forEach((instance) => {
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

      const descriptionCell = document.createElement("td");
      descriptionCell.textContent = instance.description ?? "-";

      row.append(nameCell, urlCell, platformCell, descriptionCell);
      fragment.appendChild(row);
    });

    tableBody.innerHTML = "";
    tableBody.appendChild(fragment);
  } catch (error) {
    console.error(error);
    tableBody.innerHTML = `<tr><td colspan=\"4\">데이터를 불러오는 중 오류가 발생했습니다. 로컬에서 테스트하는 경우 <code>python -m http.server</code>로 간단한 서버를 실행하세요.</td></tr>`;
  }
})();
