import { getFirestoreDb } from './firebase-config.js';

export const LEGACY_DEV_ACCESS_CODES = {
  games: 'GU-DEV-GAMES-X7K9',
  movies: 'GU-DEV-MOVIES-R4M2'
};

export function normalizeCode(value) {
  return String(value || '').trim().toUpperCase();
}

export function storageKey(pageId) {
  return `guDevAccess:${pageId}`;
}

export function catalogMatches(codeCatalog, pageId) {
  const catalog = String(codeCatalog || 'games').toLowerCase();
  const target = String(pageId || '').toLowerCase();
  return catalog === 'both' || catalog === target;
}

export function catalogsToGrant(codeCatalog) {
  const catalog = String(codeCatalog || 'games').toLowerCase();
  if (catalog === 'both') return ['games', 'movies'];
  return [catalog];
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

export function grantDevAccessForCatalog(codeCatalog) {
  catalogsToGrant(codeCatalog).forEach((pageId) => grantDevAccess(pageId));
}

export function devAccessErrorMessage(error) {
  switch (error) {
    case 'expired':
      return 'This access code has expired.';
    case 'exhausted':
      return 'This access code has reached its use limit.';
    case 'inactive':
      return 'This access code is no longer active.';
    case 'wrong_catalog':
      return 'This code is not valid for this catalogue.';
    case 'network':
      return 'Could not verify access code. Try again.';
    default:
      return 'Incorrect access code.';
  }
}

function legacyMatch(pageId, code) {
  const legacy = LEGACY_DEV_ACCESS_CODES[pageId];
  return legacy && code === normalizeCode(legacy);
}

export async function redeemDevAccessCode(pageId, candidate) {
  const code = normalizeCode(candidate);
  if (!code) return { ok: false, error: 'invalid' };

  if (legacyMatch(pageId, code)) {
    return { ok: true, catalog: pageId, legacy: true };
  }

  try {
    const { db, fs } = await getFirestoreDb();
    const ref = fs.doc(db, 'devAccessCodes', code);
    return await fs.runTransaction(db, async (tx) => {
      const snap = await tx.get(ref);
      if (!snap.exists()) return { ok: false, error: 'invalid' };
      const data = snap.data() || {};
      if (data.active === false) return { ok: false, error: 'inactive' };

      const catalog = String(data.catalog || 'games').toLowerCase();
      if (!catalogMatches(catalog, pageId)) return { ok: false, error: 'wrong_catalog' };

      const expiresAt = data.expiresAt;
      if (expiresAt && typeof expiresAt.toMillis === 'function' && expiresAt.toMillis() <= Date.now()) {
        return { ok: false, error: 'expired' };
      }

      const maxUses = data.maxUses;
      const useCount = Number(data.useCount) || 0;
      if (maxUses != null && Number.isFinite(Number(maxUses)) && useCount >= Number(maxUses)) {
        return { ok: false, error: 'exhausted' };
      }

      tx.update(ref, {
        useCount: useCount + 1,
        lastUsedAt: fs.serverTimestamp()
      });

      return { ok: true, catalog, label: String(data.label || '') };
    });
  } catch (err) {
    console.warn('redeemDevAccessCode:', err);
    return { ok: false, error: 'network' };
  }
}

export function generateDevAccessCode(catalog = 'games') {
  const prefix = catalog === 'movies' ? 'MOV' : catalog === 'both' ? 'ALL' : 'GAM';
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let suffix = '';
  for (let i = 0; i < 6; i += 1) {
    suffix += chars[Math.floor(Math.random() * chars.length)];
  }
  return `DEV-${prefix}-${suffix}`;
}
