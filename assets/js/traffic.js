(() => {
  const routes = [
    { name: "Friendship Bridge", baseline: 14 },
    { name: "Sai Van Bridge", baseline: 12 },
    { name: "Portas do Cerco", baseline: 22 },
    { name: "Avenida de Horta e Costa", baseline: 18 },
    { name: "Cotai Strip", baseline: 10 },
    { name: "Praza Ferreira do Amaral", baseline: 16 }
  ];

  const list = document.querySelector("[data-route-list]");
  const stamp = document.querySelector("[data-updated-at]");
  if (!list || !stamp) {
    return;
  }

  const levelForDelay = (delay) => {
    if (delay >= 30) {
      return { label: "Heavy", className: "danger" };
    }
    if (delay >= 18) {
      return { label: "Moderate", className: "warn" };
    }
    return { label: "Light", className: "good" };
  };

  const render = () => {
    const rows = routes.map((route) => {
      const variation = Math.floor(Math.random() * 10) - 4;
      const delay = Math.max(6, route.baseline + variation);
      const level = levelForDelay(delay);
      return `
        <tr>
          <td>${route.name}</td>
          <td>${delay} min</td>
          <td><span class="pill ${level.className}">${level.label}</span></td>
        </tr>
      `;
    });

    list.innerHTML = rows.join("");
    stamp.textContent = new Date().toLocaleTimeString();
  };

  render();
  window.setInterval(render, 30000);
})();
