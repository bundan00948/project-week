(() => {
  const FEEDS = {
    alerts: "https://rss.smg.gov.mo/c_WSignal_rss.xml",
    forecast: "https://rss.smg.gov.mo/c_WForecast7days_rss.xml"
  };

  const elements = {
    alertsMeta: document.querySelector("[data-alerts-meta]"),
    alertsList: document.querySelector("[data-alerts-list]"),
    forecastMeta: document.querySelector("[data-forecast-meta]"),
    forecastList: document.querySelector("[data-forecast-list]"),
    sourceLink: document.querySelector("[data-source-link]")
  };

  if (!elements.alertsList || !elements.forecastList) {
    return;
  }

  const parser = new DOMParser();

  const parseXml = (xmlText) =>
    parser.parseFromString(xmlText, "text/xml");

  const parseFeedError = (xmlDocument) =>
    xmlDocument.querySelector("parsererror");

  const fetchFeed = async (url) => {
    try {
      const direct = await fetch(url);
      if (direct.ok) {
        return direct.text();
      }
    } catch (error) {
      // Expected on many static hosts because SMG RSS may not send CORS headers.
    }

    const proxied = await fetch(
      `https://api.allorigins.win/raw?url=${encodeURIComponent(url)}`
    );
    if (!proxied.ok) {
      throw new Error(`Unable to fetch ${url}`);
    }
    return proxied.text();
  };

  const toText = (htmlString) => {
    const container = document.createElement("div");
    container.innerHTML = htmlString.replace(/<br\s*\/?>/gi, "\n");
    return (container.textContent || "").trim();
  };

  const renderAlerts = (xmlDocument) => {
    const item = xmlDocument.querySelector("channel > item");
    const pubDate = xmlDocument.querySelector("channel > pubDate")?.textContent;
    const title = item?.querySelector("title")?.textContent;
    const descriptionHtml = item?.querySelector("description")?.textContent || "";
    const tableHolder = document.createElement("div");
    tableHolder.innerHTML = descriptionHtml;

    const rows = Array.from(tableHolder.querySelectorAll("tr"))
      .slice(1)
      .map((row) => {
        const cells = Array.from(row.querySelectorAll("td")).map((cell) =>
          cell.textContent.trim()
        );
        return {
          type: cells[0] || "N/A",
          updatedAt: cells[1] || "N/A",
          detail: cells[2] || "N/A"
        };
      });

    elements.alertsList.innerHTML = "";

    if (!rows.length) {
      const empty = document.createElement("li");
      empty.textContent = "No weather warning rows were available in the latest RSS item.";
      elements.alertsList.appendChild(empty);
    } else {
      rows.forEach((entry) => {
        const row = document.createElement("li");
        const topRow = document.createElement("div");
        topRow.className = "split";

        const type = document.createElement("strong");
        type.textContent = entry.type;
        const updated = document.createElement("span");
        updated.className = "entry-time";
        updated.textContent = entry.updatedAt;

        const detail = document.createElement("div");
        detail.textContent = entry.detail;

        topRow.append(type, updated);
        row.append(topRow, detail);
        elements.alertsList.appendChild(row);
      });
    }

    elements.alertsMeta.textContent = `${title || "Latest warning bulletin"} ${
      pubDate ? `· Published ${pubDate}` : ""
    }`;
  };

  const renderForecast = (xmlDocument) => {
    const item = xmlDocument.querySelector("channel > item");
    const pubDate = xmlDocument.querySelector("channel > pubDate")?.textContent;
    const descriptionHtml = item?.querySelector("description")?.textContent || "";
    const normalized = toText(descriptionHtml);

    const blocks = normalized
      .split(/\n\s*\n/g)
      .map((line) => line.trim())
      .filter(Boolean);

    elements.forecastList.innerHTML = "";

    if (!blocks.length) {
      const empty = document.createElement("li");
      empty.textContent = "No forecast rows were available in the latest RSS item.";
      elements.forecastList.appendChild(empty);
    } else {
      blocks.slice(0, 7).forEach((block) => {
        const lines = block
          .split("\n")
          .map((line) => line.trim())
          .filter(Boolean);
        const title = lines.shift() || "Forecast";
        const details = lines.join(" ");

        const row = document.createElement("li");
        const heading = document.createElement("strong");
        const content = document.createElement("div");
        heading.textContent = title;
        content.textContent = details || "No details available.";
        row.append(heading, content);
        elements.forecastList.appendChild(row);
      });
    }

    elements.forecastMeta.textContent = pubDate
      ? `Latest forecast update: ${pubDate}`
      : "Latest forecast update is unavailable.";
  };

  const showError = (message) => {
    const alertError = document.createElement("li");
    alertError.className = "error";
    alertError.textContent = message;
    elements.alertsList.innerHTML = "";
    elements.alertsList.appendChild(alertError);

    const forecastError = document.createElement("li");
    forecastError.className = "error";
    forecastError.textContent = message;
    elements.forecastList.innerHTML = "";
    elements.forecastList.appendChild(forecastError);
  };

  const load = async () => {
    try {
      const [alertsXml, forecastXml] = await Promise.all([
        fetchFeed(FEEDS.alerts),
        fetchFeed(FEEDS.forecast)
      ]);

      const alertsDoc = parseXml(alertsXml);
      const forecastDoc = parseXml(forecastXml);

      if (parseFeedError(alertsDoc) || parseFeedError(forecastDoc)) {
        throw new Error("SMG feed could not be parsed.");
      }

      renderAlerts(alertsDoc);
      renderForecast(forecastDoc);
      if (elements.sourceLink) {
        elements.sourceLink.textContent = "SMG RSS source directory";
      }
    } catch (error) {
      showError(
        "Unable to load SMG RSS data right now. Please try again later."
      );
      if (elements.alertsMeta) {
        elements.alertsMeta.textContent = "";
      }
      if (elements.forecastMeta) {
        elements.forecastMeta.textContent = "";
      }
      if (elements.sourceLink) {
        elements.sourceLink.textContent = "SMG RSS source";
      }
    }
  };

  load();
})();
