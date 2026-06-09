export function nextNumericKey(items, getter, max, startAt = 1) {
  let highest = startAt - 1;
  (items || []).forEach((item) => {
    const n = Number.parseInt(getter(item), 10);
    if (Number.isFinite(n) && n >= 0 && n <= max && n > highest) highest = n;
  });
  const next = highest + 1;
  return next > max ? null : next;
}

export function showDevToast(message, type = 'success') {
  let host = document.getElementById('devToastHost');
  if (!host) {
    host = document.createElement('div');
    host.id = 'devToastHost';
    host.className = 'dev-toast-host';
    document.body.appendChild(host);
  }
  const el = document.createElement('div');
  el.className = `dev-toast dev-toast-${type}`;
  el.textContent = message;
  host.appendChild(el);
  requestAnimationFrame(() => el.classList.add('visible'));
  setTimeout(() => {
    el.classList.remove('visible');
    setTimeout(() => el.remove(), 280);
  }, 3200);
}

export function bindFormToggle(root) {
  const toggle = root.querySelector('[data-dev-form-toggle]');
  const panel = root.querySelector('[data-dev-form-panel]');
  const closeBtn = root.querySelector('[data-dev-form-close]');
  if (!toggle || !panel) return;
  const open = () => {
    panel.hidden = false;
    panel.classList.add('open');
    toggle.setAttribute('aria-expanded', 'true');
  };
  const close = () => {
    panel.classList.remove('open');
    toggle.setAttribute('aria-expanded', 'false');
    setTimeout(() => { panel.hidden = true; }, 220);
  };
  toggle.addEventListener('click', () => {
    if (panel.hidden) open();
    else close();
  });
  closeBtn?.addEventListener('click', close);
  return { open, close };
}
