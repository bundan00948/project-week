/**
 * Session gate for unlisted /dev/* catalog pages.
 * Codes are managed in Firestore via the Staff Panel (devAccessCodes collection).
 */
import {
  devAccessErrorMessage,
  grantDevAccessForCatalog,
  hasDevAccess,
  LEGACY_DEV_ACCESS_CODES,
  redeemDevAccessCode
} from './access-code-service.js';

export { LEGACY_DEV_ACCESS_CODES as DEV_ACCESS_CODES };

async function tryUnlock(pageId, code) {
  const result = await redeemDevAccessCode(pageId, code);
  if (!result.ok) return result;
  grantDevAccessForCatalog(result.catalog || pageId);
  return result;
}

export function grantDevAccess(pageId) {
  grantDevAccessForCatalog(pageId);
}

export async function tryGrantFromUrl(pageId) {
  const fromUrl = new URLSearchParams(window.location.search).get('code');
  if (!fromUrl) return false;
  const result = await tryUnlock(pageId, fromUrl);
  return !!result.ok;
}

export async function mountDevAccessGate(pageId, options = {}) {
  const label = options.label || 'Dev access';
  const hint = options.hint || 'Enter the access code for this catalogue.';

  if (hasDevAccess(pageId)) {
    options.onUnlock?.();
    return;
  }

  if (await tryGrantFromUrl(pageId)) {
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
        <button type="submit" class="dev-gate-submit" id="devGateSubmit">Unlock catalogue</button>
      </form>
    </div>
  `;
  document.body.appendChild(gate);
  document.body.classList.add('dev-gate-open');

  const form = gate.querySelector('#devGateForm');
  const input = gate.querySelector('#devGateCode');
  const errorEl = gate.querySelector('#devGateError');
  const submitBtn = gate.querySelector('#devGateSubmit');

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    errorEl.hidden = true;
    submitBtn.disabled = true;
    const previousLabel = submitBtn.textContent;
    submitBtn.textContent = 'Checking…';
    try {
      const result = await tryUnlock(pageId, input.value);
      if (!result.ok) {
        errorEl.textContent = devAccessErrorMessage(result.error);
        errorEl.hidden = false;
        input.focus();
        return;
      }
      gate.remove();
      document.body.classList.remove('dev-gate-open');
      options.onUnlock?.();
    } finally {
      submitBtn.disabled = false;
      submitBtn.textContent = previousLabel;
    }
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
