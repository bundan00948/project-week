/**
 * Simple session gate for unlisted /dev/* catalog pages.
 */
export const DEV_ACCESS_CODES = {
  games: 'GU-DEV-GAMES-X7K9',
  movies: 'GU-DEV-MOVIES-R4M2'
};

function storageKey(pageId) {
  return `guDevAccess:${pageId}`;
}

function normalizeCode(value) {
  return String(value || '').trim().toUpperCase();
}

function isValidCode(pageId, candidate) {
  const expected = DEV_ACCESS_CODES[pageId];
  if (!expected) return false;
  return normalizeCode(candidate) === normalizeCode(expected);
}

export function hasDevAccess(pageId) {
  try {
    return sessionStorage.getItem(storageKey(pageId)) === '1';
  } catch (_) {
    return false;
  }
}

export function grantDevAccess(pageId) {
  try {
    sessionStorage.setItem(storageKey(pageId), '1');
  } catch (_) {}
}

export function tryGrantFromUrl(pageId) {
  const fromUrl = new URLSearchParams(window.location.search).get('code');
  if (!fromUrl || !isValidCode(pageId, fromUrl)) return false;
  grantDevAccess(pageId);
  return true;
}

export function mountDevAccessGate(pageId, options = {}) {
  const label = options.label || 'Dev access';
  const hint = options.hint || 'Enter the access code for this catalogue.';

  if (hasDevAccess(pageId) || tryGrantFromUrl(pageId)) {
    options.onUnlock?.();
    return;
  }

  const gate = document.createElement('div');
  gate.className = 'dev-gate';
  gate.innerHTML = `
    <div class="dev-gate-card" role="dialog" aria-modal="true" aria-labelledby="dev-gate-title">
      <p class="dev-gate-kicker">${label}</p>
      <h1 id="dev-gate-title">Access code required</h1>
      <p class="dev-gate-desc">${hint}</p>
      <form class="dev-gate-form" id="devGateForm">
        <label class="dev-gate-label" for="devGateCode">Access code</label>
        <input id="devGateCode" class="dev-gate-input" type="password" autocomplete="off" spellcheck="false" placeholder="Enter code…" required>
        <p class="dev-gate-error" id="devGateError" hidden>Incorrect access code.</p>
        <button type="submit" class="dev-gate-submit">Unlock catalogue</button>
      </form>
    </div>
  `;
  document.body.appendChild(gate);
  document.body.classList.add('dev-gate-open');

  const form = gate.querySelector('#devGateForm');
  const input = gate.querySelector('#devGateCode');
  const errorEl = gate.querySelector('#devGateError');

  form.addEventListener('submit', (e) => {
    e.preventDefault();
    if (!isValidCode(pageId, input.value)) {
      errorEl.hidden = false;
      input.focus();
      return;
    }
    grantDevAccess(pageId);
    gate.remove();
    document.body.classList.remove('dev-gate-open');
    options.onUnlock?.();
  });

  input.focus();
}

export function bindFormToggle(root, options = {}) {
  const toggle = root.querySelector('[data-dev-form-toggle]');
  const panel = root.querySelector('[data-dev-form-panel]');
  const closeBtn = root.querySelector('[data-dev-form-close]');
  const backdrop = root.querySelector(options.backdropSelector || '[data-dev-form-backdrop]');
  if (!toggle || !panel) return {};

  const open = () => {
    panel.hidden = false;
    panel.classList.add('open');
    backdrop?.classList.add('open');
    toggle.setAttribute('aria-expanded', 'true');
    document.body.style.overflow = 'hidden';
  };

  const close = () => {
    panel.classList.remove('open');
    backdrop?.classList.remove('open');
    toggle.setAttribute('aria-expanded', 'false');
    document.body.style.overflow = '';
    setTimeout(() => {
      if (!panel.classList.contains('open')) panel.hidden = true;
    }, 280);
  };

  toggle.addEventListener('click', () => {
    if (panel.hidden || !panel.classList.contains('open')) open();
    else close();
  });
  closeBtn?.addEventListener('click', close);
  backdrop?.addEventListener('click', close);

  return { open, close };
}
