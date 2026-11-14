(() => {
  "use strict";

  const $ = (selector) => document.querySelector(selector);

  function el(tag, text, className) {
    const node = document.createElement(tag);
    if (text !== undefined && text !== null) node.textContent = String(text);
    if (className) node.className = className;
    return node;
  }

  function formatBytes(value) {
    const size = Number(value || 0);
    if (size < 1024) return `${size} B`;
    if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KiB`;
    return `${(size / (1024 * 1024)).toFixed(2)} MiB`;
  }

  async function getJson(url) {
    const response = await fetch(url, { headers: { Accept: "application/json" } });
    if (!response.ok) throw new Error(`${url} returned ${response.status}`);
    return response.json();
  }

  function renderCards(summary) {
    const container = $("#summary-cards");
    container.replaceChildren();
    const cards = [
      ["文件", summary.file_count],
      ["总字节", formatBytes(summary.total_bytes)],
      ["小文件", summary.small_file_count],
      ["孤儿", summary.orphan_count],
      ["缺失", summary.missing_count],
      ["过期快照", summary.expired_snapshot_count],
      ["诊断", summary.diagnostic_count],
    ];
    cards.forEach(([label, value]) => {
      const card = el("div", null, "card");
      card.append(el("span", value, "value"), el("span", label, "label"));
      container.append(card);
    });
  }

  function renderDiagnostics(diagnostics) {
    const container = $("#diagnostics");
    $("#diagnostic-count").textContent = String(diagnostics.length);
    container.replaceChildren();
    if (!diagnostics.length) {
      container.append(el("p", "未发现诊断项。", "muted"));
      return;
    }
    diagnostics.slice(0, 30).forEach((item) => {
      const node = el("div", null, `item ${item.severity}`);
      node.append(el("strong", item.category));
      node.append(document.createTextNode(` · ${item.table}/${item.partition || "root"}`));
      node.append(el("span", item.reason, "meta"));
      if (item.count > 1) node.append(el("span", `${item.count} files`, "meta"));
      container.append(node);
    });
  }

  function renderTables(tables) {
    const body = $("#tables-body");
    body.replaceChildren();
    if (!tables.length) {
      const row = el("tr");
      const cell = el("td", "无表", "muted");
      cell.colSpan = 6;
      row.append(cell);
      body.append(row);
      return;
    }
    tables.forEach((table) => {
      const row = el("tr");
      row.append(el("td", table.name));
      row.append(el("td", table.partition_count, "num"));
      row.append(el("td", table.file_count, "num"));
      row.append(el("td", table.small_file_count, "num"));
      row.append(el("td", formatBytes(table.bytes), "num"));
      row.append(el("td", table.snapshot_count, "num"));
      body.append(row);
    });
  }

  function renderPlan(plan) {
    const summary = plan.summary;
    const container = $("#plan-summary");
    container.replaceChildren();
    container.append(
      el(
        "p",
        `计划 ${plan.plan_hash.slice(0, 16)}… · ${summary.action_count} 个动作 · ` +
          `compact ${summary.by_kind.compact} / repair ${summary.by_kind.repair} / ` +
          `vacuum ${summary.by_kind.vacuum} / adopt ${summary.by_kind.adopt || 0}`
      )
    );
    container.append(el("p", `预计释放 ${formatBytes(summary.expected_released_bytes)}`, "muted"));
    const list = $("#plan-actions");
    list.replaceChildren();
    if (!plan.actions.length) {
      list.append(el("p", "当前没有待执行的维护动作。", "muted"));
      return;
    }
    plan.actions.slice(0, 14).forEach((action) => {
      const item = el("div", null, "item info");
      item.append(el("strong", action.kind));
      item.append(document.createTextNode(` · ${action.table}/${action.partition || "root"}`));
      item.append(el("span", action.reason, "meta"));
      list.append(item);
    });
  }

  function renderTasks(tasks) {
    const container = $("#tasks");
    container.replaceChildren();
    if (!tasks.length) {
      container.append(el("p", "暂无任务。运行本地 apply 后会出现台账。", "muted"));
      return;
    }
    tasks.slice(0, 12).forEach((task) => {
      const item = el("div", null, "item");
      const tag = el("span", task.status, `status-tag ${task.status}`);
      item.append(tag);
      item.append(document.createTextNode(` ${task.kind} · ${task.table_name}/${task.partition_name || "root"}`));
      item.append(
        el(
          "span",
          `${Number(task.duration_ms || 0).toFixed(1)} ms · ${task.task_id.slice(0, 18)}…`,
          "meta"
        )
      );
      container.append(item);
    });
  }

  function renderReports(reports) {
    const container = $("#reports");
    container.replaceChildren();
    if (!reports.length) {
      container.append(el("p", "暂无报告。apply 完成后会写入 Markdown 与 JSON。", "muted"));
      return;
    }
    reports.slice(0, 12).forEach((report) => {
      const item = el("div", null, "item info");
      const link = el("a", report.report_id);
      link.href = `/api/reports/${encodeURIComponent(report.report_id)}`;
      link.target = "_blank";
      link.rel = "noopener";
      const download = el("a", "下载 JSON");
      download.href = `/api/reports/${encodeURIComponent(report.report_id)}/download`;
      item.append(link, document.createTextNode(" · "), download);
      item.append(el("span", report.created_at, "meta"));
      container.append(item);
    });
  }

  function showFailure(where, error) {
    if (where === "#tables-body") {
      const body = $(where);
      body.replaceChildren();
      const row = el("tr");
      const cell = el("td", `读取失败：${error.message}`, "error-text");
      cell.colSpan = 6;
      row.append(cell);
      body.append(row);
      return;
    }
    const container = $(where);
    container.replaceChildren(el("p", `读取失败：${error.message}`, "error-text"));
  }

  async function loadAll() {
    const button = $("#refresh");
    button.disabled = true;
    const [tables, plan, tasks, reports] = await Promise.allSettled([
      getJson("/api/tables"),
      getJson("/api/plan"),
      getJson("/api/tasks"),
      getJson("/api/reports"),
    ]);
    if (tables.status === "fulfilled") {
      renderCards(tables.value.summary);
      renderDiagnostics(tables.value.diagnostics);
      renderTables(tables.value.tables);
    } else {
      showFailure("#diagnostics", tables.reason);
      showFailure("#tables-body", tables.reason);
    }
    if (plan.status === "fulfilled") renderPlan(plan.value);
    else showFailure("#plan-summary", plan.reason);
    if (tasks.status === "fulfilled") renderTasks(tasks.value.tasks);
    else showFailure("#tasks", tasks.reason);
    if (reports.status === "fulfilled") renderReports(reports.value.reports);
    else showFailure("#reports", reports.reason);
    button.disabled = false;
  }

  $("#refresh").addEventListener("click", loadAll);
  loadAll();
})();
