import { initializeApp, getApps } from "https://www.gstatic.com/firebasejs/9.22.1/firebase-app.js";
import {
  getAuth,
  onAuthStateChanged,
  signInWithEmailAndPassword,
  signOut
} from "https://www.gstatic.com/firebasejs/9.22.1/firebase-auth.js";
import {
  getFirestore,
  doc,
  getDoc,
  setDoc,
  collection,
  query,
  orderBy,
  limit,
  getDocs,
  writeBatch,
  serverTimestamp,
  runTransaction,
  increment
} from "https://www.gstatic.com/firebasejs/9.22.1/firebase-firestore.js";

const ADMIN_EMAIL = 'chonhouliu@gmail.com';
const CAPSULE_COLLECTION = 'capsuleCodes';
const CAPSULE_USERS_COLLECTION = 'capsuleUsers';
const CAPSULE_GIFTS_COLLECTION = 'capsuleGifts';
const DEFAULT_AVATAR = 'https://t3.ftcdn.net/jpg/00/64/67/80/360_F_64678017_zUpiZFjj04cnLri7oADnyMH0XBYyQghG.jpg';

const firebaseConfig = {
  apiKey: "AIzaSyC49VFcW1pjHq0sCkdcps_DwUAoo4z5oaw",
  authDomain: "blacket-65c5b.firebaseapp.com",
  databaseURL: "https://blacket-65c5b-default-rtdb.firebaseio.com",
  projectId: "blacket-65c5b",
  storageBucket: "blacket-65c5b.firebasestorage.app",
  messagingSenderId: "497023905730",
  appId: "1:497023905730:web:a59093052a3f93a476305b",
  measurementId: "G-XZZR5DH79B"
};

const app = getApps().length ? getApps()[0] : initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);

const TIERS = {
  common: { label: 'Common', capsule: 'Green Capsule', color: '#38d66b', defaultTokens: 25, defaultStars: 5 },
  uncommon: { label: 'Uncommon', capsule: 'Blue Capsule', color: '#3f8cff', defaultTokens: 75, defaultStars: 15 },
  rare: { label: 'Rare', capsule: 'Red Capsule', color: '#ff4f5f', defaultTokens: 150, defaultStars: 25 },
  epic: { label: 'Epic', capsule: 'Purple Capsule', color: '#a66cff', defaultTokens: 300, defaultStars: 75 },
  legendary: { label: 'Legendary', capsule: 'Yellow Capsule', color: '#ffd84a', defaultTokens: 600, defaultStars: 150 },
  bronze: { label: 'Bronze', capsule: 'Bronze Capsule', color: '#cd7f32', defaultTokens: 150, defaultStars: 25 },
  silver: { label: 'Silver', capsule: 'Silver Capsule', color: '#c7d4e8', defaultTokens: 350, defaultStars: 75 },
  gold: { label: 'Gold', capsule: 'Gold Capsule', color: '#ffbf2f', defaultTokens: 800, defaultStars: 150 }
};

let currentUser = null;
let currentUserData = null;
let lastGeneratedCodes = [];

const $ = (id) => document.getElementById(id);

const elements = {
  loginPanel: $('capsule-login-panel'),
  signedInPanel: $('capsule-signed-in-panel'),
  loginForm: $('capsule-login-form'),
  loginEmail: $('capsule-login-email'),
  loginPassword: $('capsule-login-password'),
  logoutBtn: $('capsule-logout-btn'),
  authStatus: $('capsule-auth-status'),
  userEmail: $('capsule-user-email'),
  accountName: $('capsule-account-name'),
  accountAvatar: $('capsule-account-avatar'),
  accountShell: $('capsule-account'),
  redeemForm: $('capsule-redeem-form'),
  redeemCode: $('capsule-redeem-code'),
  redeemStatus: $('capsule-redeem-status'),
  adminPanel: $('capsule-admin-panel'),
  adminStatus: $('capsule-admin-status'),
  generateForm: $('capsule-generate-form'),
  generatedTable: $('capsule-generated-table'),
  recentTable: $('capsule-recent-table'),
  downloadPdfBtn: $('capsule-download-pdf'),
  rewardType: $('capsule-reward-type'),
  tier: $('capsule-tier'),
  count: $('capsule-count'),
  tokenAmount: $('capsule-token-amount'),
  giftName: $('capsule-gift-name'),
  giftImage: $('capsule-gift-image'),
  giftStars: $('capsule-gift-stars'),
  batchLabel: $('capsule-batch-label'),
  qrWork: $('capsule-qr-work')
};

function normalizeEmail(email) {
  return String(email || '').trim().toLowerCase();
}

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, (ch) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#039;'
  }[ch]));
}

function numberOrZero(value) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? Math.max(0, parsed) : 0;
}

function capsuleUrlForCode(code) {
  const url = new URL('/capsule/', window.location.origin);
  url.searchParams.set('code', code);
  return url.toString();
}

function setStatus(el, message, type = '') {
  if (!el) return;
  el.textContent = message;
  el.classList.remove('success', 'error');
  if (type) el.classList.add(type);
}

function randomBlock(length = 4) {
  const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  const cryptoApi = window.crypto || window.msCrypto;
  if (cryptoApi?.getRandomValues) {
    const bytes = new Uint8Array(length);
    cryptoApi.getRandomValues(bytes);
    return Array.from(bytes, (b) => alphabet[b % alphabet.length]).join('');
  }
  return Array.from({ length }, () => alphabet[Math.floor(Math.random() * alphabet.length)]).join('');
}

function makeCode(tierKey) {
  const prefix = (TIERS[tierKey]?.label || 'Capsule').slice(0, 3).toUpperCase();
  return `CAP-${prefix}-${randomBlock()}-${randomBlock()}`;
}

function fillTierOptions() {
  if (!elements.tier) return;
  elements.tier.innerHTML = Object.entries(TIERS).map(([key, tier]) => (
    `<option value="${key}">${tier.label} (${tier.capsule})</option>`
  )).join('');
}

function fillTierLegend() {
  const legend = $('capsule-tier-legend');
  if (!legend) return;
  legend.innerHTML = Object.values(TIERS).map((tier) => `
    <div class="capsule-tier-chip">
      <span class="capsule-tier-dot" style="--tier-color:${tier.color};"></span>
      <span>${escapeHtml(tier.label)}</span>
    </div>
  `).join('');
}

function syncDefaultsForTier() {
  const tier = TIERS[elements.tier?.value] || TIERS.common;
  if (elements.tokenAmount && !elements.tokenAmount.dataset.touched) {
    elements.tokenAmount.value = String(tier.defaultTokens);
  }
  if (elements.giftStars && !elements.giftStars.dataset.touched) {
    elements.giftStars.value = String(tier.defaultStars);
  }
}

function setSignedInUi(user, userData) {
  currentUser = user;
  currentUserData = userData || null;
  const signedIn = Boolean(user);
  elements.loginPanel?.classList.toggle('active', !signedIn);
  elements.signedInPanel?.classList.toggle('active', signedIn);
  if (elements.userEmail) elements.userEmail.textContent = user?.email || '';
  if (elements.accountShell) elements.accountShell.style.display = signedIn ? 'flex' : 'none';
  if (elements.accountName) elements.accountName.textContent = userData?.displayName || user?.email || '';
  if (elements.accountAvatar) elements.accountAvatar.src = userData?.avatar || DEFAULT_AVATAR;
}

function isCapsuleAdmin(user, userData) {
  return normalizeEmail(user?.email) === ADMIN_EMAIL ||
    userData?.isAdmin === true ||
    String(userData?.title || '').toLowerCase() === 'owner';
}

async function ensureUserProfile(user) {
  const userRef = doc(db, CAPSULE_USERS_COLLECTION, user.uid);
  const snap = await getDoc(userRef);
  const owner = normalizeEmail(user.email) === ADMIN_EMAIL;
  if (!snap.exists()) {
    const profile = {
      email: user.email || '',
      emailLower: normalizeEmail(user.email),
      displayName: (user.email || 'player').split('@')[0],
      avatar: DEFAULT_AVATAR,
      coins: owner ? 999999 : 0,
      stars: 0,
      title: owner ? 'Owner' : 'User',
      isAdmin: owner,
      badges: [],
      ownedBannerIds: [],
      favoriteMovieIds: [],
      createdAt: serverTimestamp()
    };
    await setDoc(userRef, profile, { merge: true });
    return profile;
  }

  const data = snap.data() || {};
  if (owner && (data.isAdmin !== true || data.title !== 'Owner' || data.coins !== 999999)) {
    await setDoc(userRef, {
      email: user.email || data.email || '',
      emailLower: ADMIN_EMAIL,
      coins: 999999,
      title: 'Owner',
      isAdmin: true
    }, { merge: true });
    return { ...data, email: user.email || data.email || '', emailLower: ADMIN_EMAIL, coins: 999999, title: 'Owner', isAdmin: true };
  }
  return data;
}

async function handleLogin(event) {
  event.preventDefault();
  const email = elements.loginEmail?.value.trim();
  const password = elements.loginPassword?.value;
  if (!email || !password) {
    setStatus(elements.authStatus, 'Enter your Capsule email and password.', 'error');
    return;
  }
  setStatus(elements.authStatus, 'Signing in...');
  try {
    await signInWithEmailAndPassword(auth, email, password);
    if (elements.loginPassword) elements.loginPassword.value = '';
    setStatus(elements.authStatus, 'Signed in.', 'success');
  } catch (err) {
    setStatus(elements.authStatus, `Login failed: ${err.message || 'check your email and password'}`, 'error');
  }
}

async function handleRedeem(event) {
  event.preventDefault();
  if (!currentUser) {
    setStatus(elements.redeemStatus, 'Sign in before redeeming a capsule QR code.', 'error');
    return;
  }
  const code = String(elements.redeemCode?.value || '').trim().toUpperCase();
  if (!code) {
    setStatus(elements.redeemStatus, 'Enter or scan a capsule code first.', 'error');
    return;
  }

  setStatus(elements.redeemStatus, 'Checking capsule code...');
  try {
    const result = await redeemCapsuleCode(code, currentUser);
    const tier = TIERS[result.tier] || { label: result.tier || 'Capsule' };
    if (result.rewardType === 'tokens') {
      setStatus(elements.redeemStatus, `${tier.label} capsule redeemed: ${result.tokenAmount} tokens added to your Capsule balance.`, 'success');
    } else {
      setStatus(elements.redeemStatus, `${tier.label} capsule redeemed: ${result.giftName} added to your Capsule collection.`, 'success');
    }
    if (elements.redeemCode) elements.redeemCode.value = '';
  } catch (err) {
    setStatus(elements.redeemStatus, err.message || 'Could not redeem this capsule code.', 'error');
  }
}

async function redeemCapsuleCode(code, user) {
  const userEmail = user.email || '';
  return runTransaction(db, async (tx) => {
    const codeRef = doc(db, CAPSULE_COLLECTION, code);
    const userRef = doc(db, CAPSULE_USERS_COLLECTION, user.uid);
    const codeSnap = await tx.get(codeRef);
    if (!codeSnap.exists()) throw new Error('That capsule code was not found.');
    const codeData = codeSnap.data() || {};
    if (codeData.redeemed === true) throw new Error('That capsule code has already been redeemed.');

    const userSnap = await tx.get(userRef);
    const rewardType = codeData.rewardType === 'gift' ? 'gift' : 'tokens';
    const tier = String(codeData.tier || 'common').toLowerCase();
    const tierMeta = TIERS[tier] || TIERS.common;
    const tokenAmount = Math.max(0, numberOrZero(codeData.tokenAmount || tierMeta.defaultTokens));
    const giftName = String(codeData.giftName || `${tierMeta.label} Capsule Gift`).trim();
    const giftStars = Math.max(0, numberOrZero(codeData.giftStars ?? tierMeta.defaultStars));

    const baseProfile = {
      email: userEmail,
      emailLower: normalizeEmail(userEmail),
      displayName: (userEmail || 'player').split('@')[0],
      avatar: DEFAULT_AVATAR,
      title: normalizeEmail(userEmail) === ADMIN_EMAIL ? 'Owner' : 'User',
      isAdmin: normalizeEmail(userEmail) === ADMIN_EMAIL,
      badges: [],
      ownedBannerIds: [],
      favoriteMovieIds: []
    };

    if (rewardType === 'tokens') {
      if (userSnap.exists()) {
        tx.update(userRef, { coins: increment(tokenAmount) });
      } else {
        tx.set(userRef, { ...baseProfile, coins: tokenAmount, stars: 0, createdAt: serverTimestamp() }, { merge: true });
      }
    } else {
      if (userSnap.exists()) {
        if (giftStars > 0) tx.update(userRef, { stars: increment(giftStars) });
      } else {
        tx.set(userRef, { ...baseProfile, coins: 0, stars: giftStars, createdAt: serverTimestamp() }, { merge: true });
      }
      const giftRef = doc(collection(db, CAPSULE_GIFTS_COLLECTION));
      tx.set(giftRef, {
        userId: user.uid,
        capsuleMachineId: 'capsule-machine',
        sourceName: 'Capsule Machine',
        giftName,
        rarity: tier,
        imageUrl: String(codeData.giftImageUrl || '').trim() || null,
        starsGained: giftStars,
        source: 'capsule-code',
        capsuleCode: code,
        timestamp: serverTimestamp()
      });
    }

    tx.update(codeRef, {
      redeemed: true,
      redeemedBy: user.uid,
      redeemedByEmail: userEmail,
      redeemedAt: serverTimestamp()
    });

    return {
      rewardType,
      tier,
      tokenAmount,
      giftName,
      giftStars
    };
  });
}

async function handleGenerate(event) {
  event.preventDefault();
  if (!currentUser || !isCapsuleAdmin(currentUser, currentUserData)) {
    setStatus(elements.adminStatus, 'Admin access is required to generate capsule QR codes.', 'error');
    return;
  }

  const count = Math.min(120, Math.max(1, numberOrZero(elements.count?.value || 1)));
  const tierKey = String(elements.tier?.value || 'common');
  const tier = TIERS[tierKey] || TIERS.common;
  const rewardType = elements.rewardType?.value === 'gift' ? 'gift' : 'tokens';
  const tokenAmount = numberOrZero(elements.tokenAmount?.value || tier.defaultTokens);
  const giftStars = numberOrZero(elements.giftStars?.value || tier.defaultStars);
  const giftName = String(elements.giftName?.value || `${tier.label} Capsule Gift`).trim();
  const giftImageUrl = String(elements.giftImage?.value || '').trim();
  const batchLabel = String(elements.batchLabel?.value || '').trim();
  const batchId = `batch-${Date.now()}-${randomBlock(5)}`.toLowerCase();

  setStatus(elements.adminStatus, `Generating ${count} code${count === 1 ? '' : 's'}...`);
  try {
    const batch = writeBatch(db);
    const generated = [];
    const used = new Set();

    for (let i = 0; i < count; i += 1) {
      let code = makeCode(tierKey);
      while (used.has(code) || (await getDoc(doc(db, CAPSULE_COLLECTION, code))).exists()) {
        code = makeCode(tierKey);
      }
      used.add(code);

      const payload = {
        code,
        batchId,
        batchLabel,
        rewardType,
        tier: tierKey,
        tierLabel: tier.label,
        capsule: tier.capsule,
        tokenAmount: rewardType === 'tokens' ? tokenAmount : 0,
        giftName: rewardType === 'gift' ? giftName : '',
        giftImageUrl: rewardType === 'gift' ? giftImageUrl : '',
        giftStars: rewardType === 'gift' ? giftStars : 0,
        qrUrl: capsuleUrlForCode(code),
        redeemed: false,
        createdBy: currentUser.uid,
        createdByEmail: currentUser.email || '',
        createdAt: serverTimestamp()
      };

      batch.set(doc(db, CAPSULE_COLLECTION, code), payload);
      generated.push(payload);
    }

    await batch.commit();
    lastGeneratedCodes = generated;
    renderGeneratedCodes(generated);
    await renderRecentCodes();
    if (elements.downloadPdfBtn) elements.downloadPdfBtn.disabled = generated.length === 0;
    setStatus(elements.adminStatus, `Generated ${generated.length} QR code${generated.length === 1 ? '' : 's'}. Download the PDF before clearing this page.`, 'success');
  } catch (err) {
    setStatus(elements.adminStatus, `Generation failed: ${err.message || 'unknown error'}`, 'error');
  }
}

function rewardLabel(code) {
  if (code.rewardType === 'gift') {
    return `${code.giftName || 'Gift'} (${code.giftStars || 0} stars)`;
  }
  return `${code.tokenAmount || 0} tokens`;
}

function renderGeneratedCodes(codes) {
  if (!elements.generatedTable) return;
  if (!codes.length) {
    elements.generatedTable.innerHTML = '<p style="color:var(--text-secondary);">No generated codes yet.</p>';
    return;
  }
  elements.generatedTable.innerHTML = `
    <div class="capsule-table-wrap">
      <table class="capsule-table">
        <thead><tr><th>Code</th><th>Capsule</th><th>Reward</th><th>QR Link</th></tr></thead>
        <tbody>${codes.map((code) => `
          <tr>
            <td class="capsule-code">${escapeHtml(code.code)}</td>
            <td>${escapeHtml(code.tierLabel || code.tier)}</td>
            <td>${escapeHtml(rewardLabel(code))}</td>
            <td><a href="${escapeHtml(code.qrUrl)}" target="_blank" rel="noopener">Open</a></td>
          </tr>
        `).join('')}</tbody>
      </table>
    </div>
  `;
}

async function renderRecentCodes() {
  if (!elements.recentTable) return;
  if (!currentUser || !isCapsuleAdmin(currentUser, currentUserData)) {
    elements.recentTable.innerHTML = '';
    return;
  }
  try {
    const snap = await getDocs(query(collection(db, CAPSULE_COLLECTION), orderBy('createdAt', 'desc'), limit(30)));
    const rows = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
    if (!rows.length) {
      elements.recentTable.innerHTML = '<p style="color:var(--text-secondary);">No capsule codes have been generated yet.</p>';
      return;
    }
    elements.recentTable.innerHTML = `
      <div class="capsule-table-wrap">
        <table class="capsule-table">
          <thead><tr><th>Code</th><th>Capsule</th><th>Reward</th><th>Status</th></tr></thead>
          <tbody>${rows.map((code) => `
            <tr>
              <td class="capsule-code">${escapeHtml(code.code || code.id)}</td>
              <td>${escapeHtml(code.tierLabel || code.tier || 'Capsule')}</td>
              <td>${escapeHtml(rewardLabel(code))}</td>
              <td>${code.redeemed ? `Redeemed by ${escapeHtml(code.redeemedByEmail || 'user')}` : 'Ready'}</td>
            </tr>
          `).join('')}</tbody>
        </table>
      </div>
    `;
  } catch (err) {
    elements.recentTable.innerHTML = `<p style="color:#ff9db2;">Could not load recent codes: ${escapeHtml(err.message || 'error')}</p>`;
  }
}

function waitFrame() {
  return new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
}

async function qrDataUrl(text) {
  if (!window.QRCode) throw new Error('QR library failed to load.');
  const holder = document.createElement('div');
  holder.style.width = '180px';
  holder.style.height = '180px';
  elements.qrWork?.appendChild(holder);
  new window.QRCode(holder, {
    text,
    width: 180,
    height: 180,
    correctLevel: window.QRCode.CorrectLevel.M
  });
  await waitFrame();
  const canvas = holder.querySelector('canvas');
  const img = holder.querySelector('img');
  const dataUrl = canvas ? canvas.toDataURL('image/png') : img?.src;
  holder.remove();
  if (!dataUrl) throw new Error('Could not render QR code.');
  return dataUrl;
}

async function downloadGeneratedPdf() {
  if (!lastGeneratedCodes.length) {
    setStatus(elements.adminStatus, 'Generate capsule codes before downloading a PDF.', 'error');
    return;
  }
  if (!window.jspdf?.jsPDF) {
    setStatus(elements.adminStatus, 'PDF library failed to load.', 'error');
    return;
  }

  setStatus(elements.adminStatus, 'Building QR PDF...');
  try {
    const { jsPDF } = window.jspdf;
    const pdf = new jsPDF({ unit: 'pt', format: 'letter' });
    const pageWidth = pdf.internal.pageSize.getWidth();
    const pageHeight = pdf.internal.pageSize.getHeight();
    const margin = 36;
    const headerHeight = 42;
    const columns = 2;
    const rows = 3;
    const cellWidth = (pageWidth - margin * 2) / columns;
    const cellHeight = (pageHeight - margin * 2 - headerHeight) / rows;

    for (let i = 0; i < lastGeneratedCodes.length; i += 1) {
      if (i > 0 && i % (columns * rows) === 0) pdf.addPage();
      const indexOnPage = i % (columns * rows);
      if (indexOnPage === 0) {
        pdf.setFillColor(10, 12, 16);
        pdf.rect(0, 0, pageWidth, pageHeight, 'F');
        pdf.setTextColor(42, 255, 158);
        pdf.setFont('helvetica', 'bold');
        pdf.setFontSize(18);
        pdf.text('CAPSULE MACHINE QR CODES', margin, margin + 6);
        pdf.setTextColor(138, 155, 181);
        pdf.setFontSize(9);
        pdf.text('Cut out and place one QR code inside each physical Gatcha capsule.', margin, margin + 24);
      }

      const col = indexOnPage % columns;
      const row = Math.floor(indexOnPage / columns);
      const x = margin + col * cellWidth;
      const y = margin + headerHeight + row * cellHeight;
      const code = lastGeneratedCodes[i];
      const tier = TIERS[code.tier] || TIERS.common;
      const qr = await qrDataUrl(code.qrUrl);

      pdf.setDrawColor(42, 255, 158);
      pdf.setFillColor(26, 31, 42);
      pdf.roundedRect(x + 8, y + 8, cellWidth - 16, cellHeight - 16, 14, 14, 'FD');
      pdf.addImage(qr, 'PNG', x + 20, y + 24, 104, 104);
      pdf.setTextColor(255, 255, 255);
      pdf.setFont('helvetica', 'bold');
      pdf.setFontSize(10);
      pdf.text(code.code, x + 136, y + 46, { maxWidth: cellWidth - 154 });
      pdf.setTextColor(tier.color);
      pdf.setFontSize(12);
      pdf.text(`${tier.label} Capsule`, x + 136, y + 68, { maxWidth: cellWidth - 154 });
      pdf.setTextColor(210, 220, 235);
      pdf.setFont('helvetica', 'normal');
      pdf.setFontSize(9);
      pdf.text(rewardLabel(code), x + 136, y + 88, { maxWidth: cellWidth - 154 });
      pdf.setTextColor(138, 155, 181);
      pdf.setFontSize(7);
      pdf.text(code.qrUrl, x + 20, y + 146, { maxWidth: cellWidth - 40 });
    }

    const label = lastGeneratedCodes[0]?.batchId || `capsule-${Date.now()}`;
    pdf.save(`${label}-qr-codes.pdf`);
    setStatus(elements.adminStatus, 'PDF downloaded.', 'success');
  } catch (err) {
    setStatus(elements.adminStatus, `PDF failed: ${err.message || 'unknown error'}`, 'error');
  }
}

function setupEvents() {
  fillTierOptions();
  fillTierLegend();
  syncDefaultsForTier();
  elements.loginForm?.addEventListener('submit', handleLogin);
  elements.logoutBtn?.addEventListener('click', () => signOut(auth));
  elements.redeemForm?.addEventListener('submit', handleRedeem);
  elements.generateForm?.addEventListener('submit', handleGenerate);
  elements.downloadPdfBtn?.addEventListener('click', downloadGeneratedPdf);
  elements.tier?.addEventListener('change', () => {
    elements.tokenAmount?.removeAttribute('data-touched');
    elements.giftStars?.removeAttribute('data-touched');
    syncDefaultsForTier();
  });
  elements.tokenAmount?.addEventListener('input', () => { elements.tokenAmount.dataset.touched = '1'; });
  elements.giftStars?.addEventListener('input', () => { elements.giftStars.dataset.touched = '1'; });

  const codeFromUrl = new URLSearchParams(window.location.search).get('code');
  if (codeFromUrl && elements.redeemCode) {
    elements.redeemCode.value = codeFromUrl.trim().toUpperCase();
    setStatus(elements.redeemStatus, 'QR code loaded. Sign in, then press Redeem Capsule.', 'success');
  }
}

onAuthStateChanged(auth, async (user) => {
  if (!user) {
    setSignedInUi(null, null);
    elements.adminPanel?.classList.remove('active');
    setStatus(elements.authStatus, 'Sign in with your Capsule account to redeem capsule codes.');
    return;
  }

  try {
    const userData = await ensureUserProfile(user);
    setSignedInUi(user, userData);
    setStatus(elements.authStatus, 'Ready to redeem capsule QR codes.', 'success');
    const admin = isCapsuleAdmin(user, userData);
    elements.adminPanel?.classList.toggle('active', admin);
    if (admin) {
      setStatus(elements.adminStatus, 'Admin tools ready. Generate codes and download the QR PDF.', 'success');
      await renderRecentCodes();
    }
  } catch (err) {
    setSignedInUi(user, null);
    setStatus(elements.authStatus, `Signed in, but profile could not be loaded: ${err.message || 'error'}`, 'error');
  }
});

setupEvents();
