import { initializeApp, getApps } from "https://www.gstatic.com/firebasejs/9.22.1/firebase-app.js";
import {
  getAuth,
  createUserWithEmailAndPassword,
  signInWithEmailAndPassword,
  signOut,
  onAuthStateChanged,
  updateProfile
} from "https://www.gstatic.com/firebasejs/9.22.1/firebase-auth.js";
import {
  getFirestore,
  doc,
  getDoc,
  setDoc,
  collection,
  query,
  where,
  orderBy,
  limit,
  getDocs,
  writeBatch,
  serverTimestamp,
  runTransaction
} from "https://www.gstatic.com/firebasejs/9.22.1/firebase-firestore.js";

const ADMIN_EMAIL = 'chonhouliu@gmail.com';
const DEFAULT_AVATAR = 'https://t3.ftcdn.net/jpg/00/64/67/80/360_F_64678017_zUpiZFjj04cnLri7oADnyMH0XBYyQghG.jpg';
const CAPSULE_CONFIG_COLLECTION = 'capsuleConfigs';
const CAPSULE_CODE_COLLECTION = 'capsuleCodes';
const CAPSULE_ITEM_COLLECTION = 'capsuleItems';
const PDF_BATCH_SIZE = 100;

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

let currentUser = null;
let currentUserData = null;
let capsuleConfigs = [];
let lastGeneratedCodes = [];
let capsuleScanner = null;
let itemScanner = null;
let pendingCapsuleCode = null;
let pendingItemCode = null;

const $ = (id) => document.getElementById(id);

const elements = {
  accountPill: $('capsule-account-pill'),
  staffNav: $('staff-nav-link'),
  adminNav: $('admin-nav-link'),
  authStatus: $('capsule-auth-status'),
  loginForm: $('capsule-login-form'),
  loginEmail: $('capsule-login-email'),
  loginPassword: $('capsule-login-password'),
  signupForm: $('capsule-signup-form'),
  signupName: $('capsule-signup-name'),
  signupEmail: $('capsule-signup-email'),
  signupPassword: $('capsule-signup-password'),
  startScan: $('capsule-start-scan'),
  stopScan: $('capsule-stop-scan'),
  capsuleScanner: $('capsule-scanner'),
  redeemForm: $('capsule-redeem-form'),
  redeemCode: $('capsule-redeem-code'),
  redeemStatus: $('capsule-redeem-status'),
  previewTitle: $('capsule-preview-title'),
  previewImage: $('capsule-preview-image'),
  previewFallback: $('capsule-preview-fallback'),
  previewCode: $('capsule-preview-code'),
  revealPanel: $('capsule-reveal-panel'),
  revealStage: $('capsule-reveal-stage'),
  prizeImage: $('revealed-prize-image'),
  prizeFallback: $('revealed-prize-fallback'),
  prizeName: $('revealed-prize-name'),
  prizePercent: $('revealed-prize-percent'),
  oddsList: $('capsule-odds-list'),
  refreshItems: $('capsule-refresh-items'),
  itemsList: $('capsule-items-list'),
  staffSection: $('staff'),
  itemStartScan: $('item-start-scan'),
  itemStopScan: $('item-stop-scan'),
  itemLookupForm: $('item-lookup-form'),
  itemLookupCode: $('item-lookup-code'),
  itemLookupResult: $('item-lookup-result'),
  adminSection: $('admin'),
  configForm: $('capsule-config-form'),
  configId: $('capsule-config-id'),
  configName: $('capsule-config-name'),
  configImage: $('capsule-config-image'),
  prizesInput: $('capsule-prizes-input'),
  configClear: $('capsule-config-clear'),
  configStatus: $('capsule-config-status'),
  configList: $('capsule-config-list'),
  generateForm: $('capsule-generate-form'),
  generateSelect: $('capsule-generate-select'),
  batchLabel: $('capsule-batch-label'),
  downloadPdf: $('capsule-download-pdf'),
  adminStatus: $('capsule-admin-status'),
  generatedTable: $('capsule-generated-table'),
  recentTable: $('capsule-recent-table'),
  qrWork: $('capsule-qr-work')
};

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, (ch) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#039;'
  }[ch]));
}

function normalizeEmail(email) {
  return String(email || '').trim().toLowerCase();
}

function slugify(value) {
  return String(value || 'capsule')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 48) || 'capsule';
}

function setStatus(el, message, type = '') {
  if (!el) return;
  el.textContent = message;
  el.classList.remove('success', 'error', 'warning');
  if (type) el.classList.add(type);
}

function numberValue(value, fallback = 0) {
  const parsed = Number.parseFloat(String(value ?? '').trim());
  return Number.isFinite(parsed) ? parsed : fallback;
}

function randomFloat() {
  const cryptoApi = window.crypto || window.msCrypto;
  if (!cryptoApi?.getRandomValues) return Math.random();
  const bytes = new Uint32Array(1);
  cryptoApi.getRandomValues(bytes);
  return bytes[0] / 0x100000000;
}

function randomEightDigitCode() {
  const cryptoApi = window.crypto || window.msCrypto;
  if (cryptoApi?.getRandomValues) {
    const bytes = new Uint32Array(1);
    cryptoApi.getRandomValues(bytes);
    return String(10000000 + (bytes[0] % 90000000));
  }
  return String(Math.floor(10000000 + Math.random() * 90000000));
}

async function ensureUniqueDisplayId() {
  for (let attempt = 0; attempt < 80; attempt += 1) {
    const displayId = randomEightDigitCode().slice(0, 6);
    const snap = await getDocs(query(collection(db, 'users'), where('displayId', '==', displayId)));
    if (snap.empty) return displayId;
  }
  return randomEightDigitCode().slice(0, 6);
}

function capsuleUrlForCode(code) {
  const url = new URL('/capsule/', window.location.origin);
  url.searchParams.set('code', code);
  return url.toString();
}

function itemUrlForCode(code) {
  const url = new URL('/capsule/', window.location.origin);
  url.searchParams.set('item', code);
  return url.toString();
}

function parseEightDigitCode(value, queryKey) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try {
    const url = new URL(raw, window.location.origin);
    const fromQuery = url.searchParams.get(queryKey);
    if (fromQuery) {
      const match = String(fromQuery).match(/\d{8}/);
      if (match) return match[0];
    }
  } catch (_) {}
  const match = raw.match(/\d{8}/);
  return match ? match[0] : '';
}

function escapeSvgText(value) {
  return String(value ?? '').replace(/[&<>"']/g, (ch) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&apos;'
  }[ch]));
}

function defaultPrizeImage(name) {
  const safeName = escapeSvgText(String(name || 'Prize').slice(0, 24));
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="360" height="360" viewBox="0 0 360 360"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop stop-color="#2aff9e"/><stop offset="1" stop-color="#ff3d6c"/></linearGradient></defs><rect width="360" height="360" rx="58" fill="#111827"/><circle cx="180" cy="146" r="76" fill="url(#g)" opacity=".95"/><path d="M103 177h154v95a24 24 0 0 1-24 24H127a24 24 0 0 1-24-24z" fill="#ffffff" opacity=".14"/><path d="M180 80v210M101 177h158" stroke="#fff" stroke-width="16" stroke-linecap="round" opacity=".72"/><text x="180" y="331" text-anchor="middle" fill="#fff" font-size="24" font-family="Arial" font-weight="700">${safeName}</text></svg>`;
  return `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(svg)}`;
}

function defaultCapsuleImage(name) {
  const label = escapeSvgText(String(name || 'Capsule').slice(0, 22));
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="420" height="420" viewBox="0 0 420 420"><defs><radialGradient id="shine" cx=".32" cy=".23" r=".85"><stop stop-color="#fff"/><stop offset=".2" stop-color="#ffd6e0"/><stop offset=".55" stop-color="#ff3d6c"/><stop offset="1" stop-color="#7b1734"/></radialGradient><linearGradient id="band" x1="0" x2="1"><stop stop-color="#2aff9e"/><stop offset="1" stop-color="#00b8ff"/></linearGradient></defs><rect width="420" height="420" rx="72" fill="#080b12"/><circle cx="210" cy="184" r="132" fill="url(#shine)"/><rect x="83" y="171" width="254" height="36" rx="18" fill="url(#band)"/><circle cx="160" cy="120" r="34" fill="#fff" opacity=".42"/><circle cx="210" cy="184" r="132" fill="none" stroke="#fff" stroke-width="10" opacity=".25"/><text x="210" y="352" text-anchor="middle" fill="#fff" font-size="30" font-family="Arial" font-weight="800">${label}</text></svg>`;
  return `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(svg)}`;
}

function cleanPrize(raw, index) {
  const name = String(raw?.name || raw?.prizeName || `Prize ${index + 1}`).trim();
  const percentage = Math.max(0, numberValue(raw?.percentage ?? raw?.chance ?? raw?.chancePercent, 0));
  const imageUrl = String(raw?.imageUrl || raw?.prizeImageUrl || '').trim();
  if (!name || percentage <= 0) return null;
  return {
    name,
    percentage: Math.round(percentage * 100) / 100,
    imageUrl: imageUrl || defaultPrizeImage(name)
  };
}

function parsePrizeTextarea(value) {
  return String(value || '')
    .split(/\r?\n/)
    .map((line, index) => {
      const parts = line.split('|').map((part) => part.trim());
      return cleanPrize({ name: parts[0], percentage: parts[1], imageUrl: parts.slice(2).join('|') }, index);
    })
    .filter(Boolean);
}

function cleanPrizes(prizes) {
  const cleaned = Array.isArray(prizes)
    ? prizes.map((prize, index) => cleanPrize(prize, index)).filter(Boolean)
    : [];
  if (cleaned.length) return cleaned;
  return [
    { name: 'Common Plush', percentage: 45, imageUrl: defaultPrizeImage('Common Plush') },
    { name: 'Sticker Pack', percentage: 25, imageUrl: defaultPrizeImage('Sticker Pack') },
    { name: 'Mini Figure', percentage: 20, imageUrl: defaultPrizeImage('Mini Figure') },
    { name: 'Golden Ticket', percentage: 10, imageUrl: defaultPrizeImage('Golden Ticket') }
  ];
}

function cleanCapsuleConfig(id, data = {}) {
  const name = String(data.name || data.capsuleName || 'Lucky Capsule').trim();
  return {
    id: id || slugify(name),
    name,
    imageUrl: String(data.imageUrl || data.capsuleImageUrl || '').trim() || defaultCapsuleImage(name),
    prizes: cleanPrizes(data.prizes)
  };
}

function topFourPrizes(prizes) {
  return [...cleanPrizes(prizes)].sort((a, b) => b.percentage - a.percentage).slice(0, 4);
}

function totalChance(prizes) {
  return cleanPrizes(prizes).reduce((sum, prize) => sum + prize.percentage, 0);
}

function formatPercent(value) {
  const rounded = Math.round(Number(value || 0) * 100) / 100;
  return `${Number.isInteger(rounded) ? rounded.toFixed(0) : rounded.toFixed(2)}%`;
}

function pickPrize(prizes) {
  const options = cleanPrizes(prizes);
  const total = totalChance(options);
  let cursor = randomFloat() * total;
  for (const prize of options) {
    cursor -= prize.percentage;
    if (cursor <= 0) return prize;
  }
  return options[options.length - 1];
}

function prizeLinesForTextarea(prizes) {
  return cleanPrizes(prizes).map((prize) => `${prize.name} | ${prize.percentage} | ${prize.imageUrl || ''}`).join('\n');
}

function renderImage(imgEl, fallbackEl, src, alt) {
  if (!imgEl) return;
  const imageUrl = String(src || '').trim();
  if (imageUrl) {
    imgEl.src = imageUrl;
    imgEl.alt = alt || '';
    imgEl.hidden = false;
    if (fallbackEl) fallbackEl.hidden = true;
  } else {
    imgEl.removeAttribute('src');
    imgEl.hidden = true;
    if (fallbackEl) fallbackEl.hidden = false;
  }
}

function setAccountPill(user, userData) {
  if (!elements.accountPill) return;
  if (!user) {
    elements.accountPill.innerHTML = '<i class="fas fa-user"></i><span>Not signed in</span>';
    return;
  }
  elements.accountPill.innerHTML = `
    <img src="${escapeHtml(userData?.avatar || DEFAULT_AVATAR)}" alt="">
    <span>${escapeHtml(userData?.username || user.email || 'Signed in')}</span>
    <button type="button" id="capsule-inline-logout">Logout</button>
  `;
  $('capsule-inline-logout')?.addEventListener('click', () => signOut(auth));
}

function isCapsuleAdmin(user, userData) {
  const title = String(userData?.title || '').toLowerCase();
  return normalizeEmail(user?.email) === ADMIN_EMAIL ||
    userData?.isAdmin === true ||
    title === 'owner' ||
    title === 'admin';
}

function isStoreStaff(user, userData) {
  if (isCapsuleAdmin(user, userData)) return true;
  const title = String(userData?.title || '').toLowerCase();
  return userData?.isStaff === true ||
    userData?.staff === true ||
    ['staff', 'store staff', 'moderator', 'manager'].includes(title);
}

async function ensureUserProfile(user, preferredName = '') {
  const userRef = doc(db, 'users', user.uid);
  const snap = await getDoc(userRef);
  const owner = normalizeEmail(user.email) === ADMIN_EMAIL;
  if (!snap.exists()) {
    const profile = {
      email: user.email || '',
      emailLower: normalizeEmail(user.email),
      username: preferredName || user.displayName || (user.email || 'player').split('@')[0],
      avatar: DEFAULT_AVATAR,
      authUid: user.uid,
      authSource: 'firebase-auth',
      authProviders: ['password'],
      coins: owner ? 999999 : 0,
      stars: 0,
      title: owner ? 'Owner' : 'User',
      isAdmin: owner,
      badges: [],
      displayId: await ensureUniqueDisplayId(),
      ownedBannerIds: [],
      favoriteMovieIds: [],
      createdAt: serverTimestamp()
    };
    await setDoc(userRef, profile, { merge: true });
    return profile;
  }

  const data = snap.data() || {};
  const patch = {};
  if (!data.email) patch.email = user.email || '';
  if (!data.emailLower) patch.emailLower = normalizeEmail(user.email);
  if (!data.displayId || String(data.displayId).length !== 6) patch.displayId = await ensureUniqueDisplayId();
  if (owner && (data.isAdmin !== true || data.title !== 'Owner')) {
    patch.isAdmin = true;
    patch.title = 'Owner';
    patch.coins = 999999;
  }
  if (Object.keys(patch).length) {
    await setDoc(userRef, patch, { merge: true });
  }
  return { ...data, ...patch };
}

async function handleLogin(event) {
  event.preventDefault();
  const email = elements.loginEmail?.value.trim();
  const password = elements.loginPassword?.value;
  if (!email || !password) {
    setStatus(elements.authStatus, 'Enter your email and password.', 'error');
    return;
  }
  setStatus(elements.authStatus, 'Signing in...');
  try {
    await signInWithEmailAndPassword(auth, email, password);
    if (elements.loginPassword) elements.loginPassword.value = '';
    setStatus(elements.authStatus, 'Signed in.', 'success');
  } catch (err) {
    setStatus(elements.authStatus, `Login failed: ${err.message || 'check your details'}`, 'error');
  }
}

async function handleSignup(event) {
  event.preventDefault();
  const username = elements.signupName?.value.trim();
  const email = elements.signupEmail?.value.trim();
  const password = elements.signupPassword?.value;
  if (!username || !email || !password) {
    setStatus(elements.authStatus, 'Enter username, email, and password.', 'error');
    return;
  }
  setStatus(elements.authStatus, 'Creating your account...');
  try {
    const credential = await createUserWithEmailAndPassword(auth, email, password);
    await updateProfile(credential.user, { displayName: username });
    await ensureUserProfile(credential.user, username);
    if (elements.signupPassword) elements.signupPassword.value = '';
    setStatus(elements.authStatus, 'Account created. You are signed in.', 'success');
  } catch (err) {
    setStatus(elements.authStatus, `Signup failed: ${err.message || 'try again'}`, 'error');
  }
}

async function loadCapsuleConfigs() {
  try {
    const snap = await getDocs(collection(db, CAPSULE_CONFIG_COLLECTION));
    capsuleConfigs = snap.docs
      .map((d) => cleanCapsuleConfig(d.id, d.data()))
      .sort((a, b) => a.name.localeCompare(b.name));
  } catch (err) {
    capsuleConfigs = [];
    setStatus(elements.adminStatus, `Could not load capsule configs: ${err.message || 'error'}`, 'error');
  }
  if (!capsuleConfigs.length) {
    capsuleConfigs = [
      cleanCapsuleConfig('starter-capsule', {
        name: 'Starter Capsule',
        prizes: [
          { name: 'Common Plush', percentage: 45 },
          { name: 'Sticker Pack', percentage: 25 },
          { name: 'Mini Figure', percentage: 20 },
          { name: 'Golden Ticket', percentage: 10 }
        ]
      })
    ];
  }
  renderCapsuleConfigs();
  fillGenerateSelect();
}

function renderCapsuleConfigs() {
  if (!elements.configList) return;
  elements.configList.innerHTML = capsuleConfigs.map((config) => `
    <div class="config-row">
      <img src="${escapeHtml(config.imageUrl)}" alt="">
      <div>
        <strong>${escapeHtml(config.name)}</strong>
        <small>${topFourPrizes(config.prizes).map((prize) => `${escapeHtml(prize.name)} ${formatPercent(prize.percentage)}`).join(' - ')}</small>
      </div>
      <button type="button" class="capsule-btn config-edit-btn" data-config-id="${escapeHtml(config.id)}">Edit</button>
    </div>
  `).join('');
  elements.configList.querySelectorAll('.config-edit-btn').forEach((button) => {
    button.addEventListener('click', () => editCapsuleConfig(button.dataset.configId));
  });
}

function fillGenerateSelect() {
  if (!elements.generateSelect) return;
  elements.generateSelect.innerHTML = capsuleConfigs.map((config) => (
    `<option value="${escapeHtml(config.id)}">${escapeHtml(config.name)}</option>`
  )).join('');
}

function editCapsuleConfig(configId) {
  const config = capsuleConfigs.find((item) => item.id === configId);
  if (!config) return;
  if (elements.configId) elements.configId.value = config.id;
  if (elements.configName) elements.configName.value = config.name;
  if (elements.configImage) elements.configImage.value = config.imageUrl.startsWith('data:') ? '' : config.imageUrl;
  if (elements.prizesInput) elements.prizesInput.value = prizeLinesForTextarea(config.prizes);
  setStatus(elements.configStatus, `Editing ${config.name}.`, 'success');
}

function clearCapsuleConfigForm() {
  if (elements.configId) elements.configId.value = '';
  if (elements.configName) elements.configName.value = '';
  if (elements.configImage) elements.configImage.value = '';
  if (elements.prizesInput) {
    elements.prizesInput.value = [
      'Common Plush | 45 |',
      'Sticker Pack | 25 |',
      'Mini Figure | 20 |',
      'Golden Ticket | 10 |'
    ].join('\n');
  }
  setStatus(elements.configStatus, 'Ready to create a new capsule type.');
}

async function handleSaveCapsuleConfig(event) {
  event.preventDefault();
  if (!currentUser || !isCapsuleAdmin(currentUser, currentUserData)) {
    setStatus(elements.configStatus, 'Admin access is required.', 'error');
    return;
  }
  const name = elements.configName?.value.trim();
  const prizes = parsePrizeTextarea(elements.prizesInput?.value);
  if (!name) {
    setStatus(elements.configStatus, 'Enter a capsule type name.', 'error');
    return;
  }
  if (!prizes.length) {
    setStatus(elements.configStatus, 'Enter at least one prize with a percentage.', 'error');
    return;
  }
  const id = elements.configId?.value || slugify(name);
  const imageUrl = elements.configImage?.value.trim() || defaultCapsuleImage(name);
  const total = totalChance(prizes);
  const payload = {
    name,
    imageUrl,
    prizes,
    topPrizePreview: topFourPrizes(prizes).map((prize) => ({ name: prize.name, percentage: prize.percentage })),
    updatedBy: currentUser.uid,
    updatedByEmail: currentUser.email || '',
    updatedAt: serverTimestamp()
  };
  setStatus(elements.configStatus, 'Saving capsule...');
  try {
    await setDoc(doc(db, CAPSULE_CONFIG_COLLECTION, id), payload, { merge: true });
    await loadCapsuleConfigs();
    if (elements.configId) elements.configId.value = id;
    const type = Math.abs(total - 100) > 0.01 ? 'warning' : 'success';
    setStatus(elements.configStatus, `Saved ${name}. Prize percentages total ${formatPercent(total)}.`, type);
  } catch (err) {
    setStatus(elements.configStatus, `Save failed: ${err.message || 'unknown error'}`, 'error');
  }
}

function renderCapsulePreview(code, codeData) {
  const config = cleanCapsuleConfig(codeData.capsuleId || '', {
    name: codeData.capsuleName,
    imageUrl: codeData.capsuleImageUrl,
    prizes: codeData.prizes
  });
  if (elements.previewTitle) elements.previewTitle.textContent = config.name;
  if (elements.previewCode) elements.previewCode.textContent = code;
  renderImage(elements.previewImage, elements.previewFallback, config.imageUrl, config.name);
  if (elements.revealStage) elements.revealStage.classList.remove('opening', 'opened');
  renderImage(elements.prizeImage, elements.prizeFallback, '', '');
  if (elements.prizeName) elements.prizeName.textContent = 'Prize inside';
  if (elements.prizePercent) elements.prizePercent.textContent = 'Opening will reveal your exact prize percentage.';
  if (elements.oddsList) {
    elements.oddsList.innerHTML = `
      <strong>Most common possible prizes</strong>
      ${topFourPrizes(config.prizes).map((prize) => `
        <div class="odds-row">
          <span>${escapeHtml(prize.name)}</span>
          <b>${formatPercent(prize.percentage)}</b>
        </div>
      `).join('')}
    `;
  }
}

async function getCapsuleCode(code) {
  const snap = await getDoc(doc(db, CAPSULE_CODE_COLLECTION, code));
  if (!snap.exists()) throw new Error('That capsule code was not found.');
  const data = snap.data() || {};
  if (data.redeemed === true) throw new Error('That capsule code has already been opened.');
  return { id: snap.id, ...data };
}

async function handleRedeem(event) {
  event.preventDefault();
  const code = parseEightDigitCode(elements.redeemCode?.value, 'code');
  if (!currentUser) {
    pendingCapsuleCode = code || pendingCapsuleCode;
    setStatus(elements.redeemStatus, 'Sign in or create an account before opening a capsule.', 'error');
    location.hash = '#account';
    return;
  }
  if (!code) {
    setStatus(elements.redeemStatus, 'Enter or scan an 8 digit capsule code.', 'error');
    return;
  }
  await openCapsuleByCode(code);
}

async function openCapsuleByCode(code) {
  if (!currentUser) {
    pendingCapsuleCode = code;
    setStatus(elements.redeemStatus, 'Sign in or create an account before opening a capsule.', 'error');
    location.hash = '#account';
    return;
  }
  setStatus(elements.redeemStatus, 'Detecting capsule type...');
  try {
    const codeData = await getCapsuleCode(code);
    renderCapsulePreview(code, codeData);
    setStatus(elements.redeemStatus, `${codeData.capsuleName || 'Capsule'} detected. Opening capsule...`, 'success');
    await wait(450);
    elements.revealStage?.classList.add('opening');
    const prize = pickPrize(codeData.prizes);
    const item = await redeemCapsuleCode(code, codeData, prize);
    await wait(650);
    renderImage(elements.prizeImage, elements.prizeFallback, item.prizeImageUrl, item.prizeName);
    if (elements.prizeName) elements.prizeName.textContent = item.prizeName;
    if (elements.prizePercent) elements.prizePercent.textContent = `${formatPercent(item.chancePercent)} chance in this capsule`;
    elements.revealStage?.classList.add('opened');
    setStatus(elements.redeemStatus, `${item.prizeName} added to your Items with item code ${item.itemCode}.`, 'success');
    if (elements.redeemCode) elements.redeemCode.value = '';
    await renderUserItems();
  } catch (err) {
    setStatus(elements.redeemStatus, err.message || 'Could not open this capsule.', 'error');
  }
}

async function redeemCapsuleCode(code, codeData, prize) {
  for (let attempt = 0; attempt < 4; attempt += 1) {
    const itemCode = randomEightDigitCode();
    try {
      return await runTransaction(db, async (tx) => {
        const codeRef = doc(db, CAPSULE_CODE_COLLECTION, code);
        const userRef = doc(db, 'users', currentUser.uid);
        const itemRef = doc(db, CAPSULE_ITEM_COLLECTION, itemCode);
        const codeSnap = await tx.get(codeRef);
        const userSnap = await tx.get(userRef);
        const itemSnap = await tx.get(itemRef);
        if (itemSnap.exists()) {
          const collision = new Error('ITEM_CODE_COLLISION');
          collision.code = 'ITEM_CODE_COLLISION';
          throw collision;
        }
        if (!codeSnap.exists()) throw new Error('That capsule code was not found.');
        const liveCodeData = codeSnap.data() || {};
        if (liveCodeData.redeemed === true) throw new Error('That capsule code has already been opened.');
        const livePrizes = cleanPrizes(liveCodeData.prizes || codeData.prizes);
        const safePrize = cleanPrize(prize, 0) || pickPrize(livePrizes);
        const capsuleName = String(liveCodeData.capsuleName || codeData.capsuleName || 'Capsule');
        const capsuleImageUrl = String(liveCodeData.capsuleImageUrl || codeData.capsuleImageUrl || defaultCapsuleImage(capsuleName));
        const itemPayload = {
          itemCode,
          itemQrUrl: itemUrlForCode(itemCode),
          userId: currentUser.uid,
          userEmail: currentUser.email || '',
          capsuleCode: code,
          capsuleId: liveCodeData.capsuleId || codeData.capsuleId || '',
          capsuleName,
          capsuleImageUrl,
          prizeName: safePrize.name,
          prizeImageUrl: safePrize.imageUrl || defaultPrizeImage(safePrize.name),
          chancePercent: safePrize.percentage,
          status: 'owned',
          storeRedeemed: false,
          createdAt: serverTimestamp()
        };
        if (!userSnap.exists()) {
          tx.set(userRef, {
            email: currentUser.email || '',
            emailLower: normalizeEmail(currentUser.email),
            username: currentUser.displayName || (currentUser.email || 'player').split('@')[0],
            avatar: DEFAULT_AVATAR,
            title: normalizeEmail(currentUser.email) === ADMIN_EMAIL ? 'Owner' : 'User',
            isAdmin: normalizeEmail(currentUser.email) === ADMIN_EMAIL,
            badges: [],
            createdAt: serverTimestamp()
          }, { merge: true });
        }
        tx.set(itemRef, itemPayload);
        tx.update(codeRef, {
          redeemed: true,
          redeemedBy: currentUser.uid,
          redeemedByEmail: currentUser.email || '',
          redeemedAt: serverTimestamp(),
          prizeName: safePrize.name,
          prizeImageUrl: safePrize.imageUrl || defaultPrizeImage(safePrize.name),
          prizeChancePercent: safePrize.percentage,
          itemCode
        });
        return itemPayload;
      });
    } catch (err) {
      if (err.code === 'ITEM_CODE_COLLISION' || err.message === 'ITEM_CODE_COLLISION') continue;
      throw err;
    }
  }
  throw new Error('Could not create a unique item code. Please try again.');
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function renderUserItems() {
  if (!elements.itemsList) return;
  if (!currentUser) {
    elements.itemsList.innerHTML = '<p class="capsule-muted">Sign in to see your items.</p>';
    return;
  }
  elements.itemsList.innerHTML = '<p class="capsule-muted">Loading your items...</p>';
  try {
    const snap = await getDocs(query(
      collection(db, CAPSULE_ITEM_COLLECTION),
      where('userId', '==', currentUser.uid),
      limit(80)
    ));
    const items = snap.docs.map((d) => ({ id: d.id, ...d.data() })).sort((a, b) => {
      const at = a.createdAt?.toMillis ? a.createdAt.toMillis() : 0;
      const bt = b.createdAt?.toMillis ? b.createdAt.toMillis() : 0;
      return bt - at;
    });
    if (!items.length) {
      elements.itemsList.innerHTML = '<p class="capsule-muted">No items yet. Redeem a capsule QR code to collect one.</p>';
      return;
    }
    elements.itemsList.innerHTML = items.map((item) => `
      <article class="item-card ${item.storeRedeemed ? 'redeemed' : ''}">
        <div class="item-art">
          <img src="${escapeHtml(item.prizeImageUrl || defaultPrizeImage(item.prizeName))}" alt="${escapeHtml(item.prizeName || 'Prize')}">
        </div>
        <div class="item-body">
          <p class="capsule-kicker">${escapeHtml(item.capsuleName || 'Capsule')}</p>
          <h3>${escapeHtml(item.prizeName || 'Prize')}</h3>
          <p>${formatPercent(item.chancePercent)} chance - Item code <span class="eight-code inline">${escapeHtml(item.itemCode || item.id)}</span></p>
          <div class="item-qr" id="item-qr-${escapeHtml(item.itemCode || item.id)}"></div>
          <small>${item.storeRedeemed ? `Redeemed in store${item.storeRedeemedByEmail ? ` by ${escapeHtml(item.storeRedeemedByEmail)}` : ''}` : 'Show this QR code to store staff.'}</small>
        </div>
      </article>
    `).join('');
    await waitFrame();
    for (const item of items) {
      const code = item.itemCode || item.id;
      renderQrInto(`item-qr-${code}`, item.itemQrUrl || itemUrlForCode(code), 96);
    }
  } catch (err) {
    elements.itemsList.innerHTML = `<p class="capsule-error-text">Could not load items: ${escapeHtml(err.message || 'error')}</p>`;
  }
}

async function handleLookupItem(event) {
  event.preventDefault();
  const code = parseEightDigitCode(elements.itemLookupCode?.value, 'item');
  if (!code) {
    renderItemLookupError('Enter or scan an 8 digit item code.');
    return;
  }
  await lookupItemCode(code);
}

function renderItemLookupError(message) {
  if (elements.itemLookupResult) {
    elements.itemLookupResult.innerHTML = `<p class="capsule-error-text">${escapeHtml(message)}</p>`;
  }
}

async function lookupItemCode(code) {
  if (!currentUser || !isStoreStaff(currentUser, currentUserData)) {
    pendingItemCode = code;
    renderItemLookupError('Store staff sign-in is required to redeem item QR codes.');
    location.hash = '#account';
    return;
  }
  if (!elements.itemLookupResult) return;
  elements.itemLookupResult.innerHTML = '<p class="capsule-muted">Looking up item...</p>';
  try {
    const snap = await getDoc(doc(db, CAPSULE_ITEM_COLLECTION, code));
    if (!snap.exists()) {
      renderItemLookupError('Item code was not found.');
      return;
    }
    const item = { id: snap.id, ...snap.data() };
    elements.itemLookupResult.innerHTML = `
      <div class="staff-item-result ${item.storeRedeemed ? 'redeemed' : ''}">
        <img src="${escapeHtml(item.prizeImageUrl || defaultPrizeImage(item.prizeName))}" alt="">
        <div>
          <p class="capsule-kicker">${escapeHtml(item.capsuleName || 'Capsule')}</p>
          <h3>${escapeHtml(item.prizeName || 'Prize')}</h3>
          <p>Owner: ${escapeHtml(item.userEmail || item.userId || 'Unknown')}</p>
          <p>Chance: ${formatPercent(item.chancePercent)} - Item code <span class="eight-code inline">${escapeHtml(code)}</span></p>
          <p class="${item.storeRedeemed ? 'capsule-error-text' : 'capsule-success-text'}">${item.storeRedeemed ? 'Already redeemed in store.' : 'Ready for store redemption.'}</p>
          <button type="button" class="capsule-btn capsule-btn-primary" id="mark-item-redeemed" ${item.storeRedeemed ? 'disabled' : ''}>Mark Redeemed</button>
        </div>
      </div>
    `;
    $('mark-item-redeemed')?.addEventListener('click', () => markItemRedeemed(code));
  } catch (err) {
    renderItemLookupError(`Lookup failed: ${err.message || 'unknown error'}`);
  }
}

async function markItemRedeemed(code) {
  if (!currentUser || !isStoreStaff(currentUser, currentUserData)) {
    renderItemLookupError('Store staff sign-in is required.');
    return;
  }
  try {
    await runTransaction(db, async (tx) => {
      const itemRef = doc(db, CAPSULE_ITEM_COLLECTION, code);
      const snap = await tx.get(itemRef);
      if (!snap.exists()) throw new Error('Item code was not found.');
      const data = snap.data() || {};
      if (data.storeRedeemed === true) throw new Error('This item was already redeemed in store.');
      tx.update(itemRef, {
        storeRedeemed: true,
        status: 'store-redeemed',
        storeRedeemedBy: currentUser.uid,
        storeRedeemedByEmail: currentUser.email || '',
        storeRedeemedAt: serverTimestamp()
      });
    });
    await lookupItemCode(code);
    await renderUserItems();
  } catch (err) {
    renderItemLookupError(err.message || 'Could not redeem item.');
  }
}

async function handleGenerateCodes(event) {
  event.preventDefault();
  if (!currentUser || !isCapsuleAdmin(currentUser, currentUserData)) {
    setStatus(elements.adminStatus, 'Admin access is required to generate QR PDFs.', 'error');
    return;
  }
  const configId = elements.generateSelect?.value;
  const config = capsuleConfigs.find((item) => item.id === configId);
  if (!config) {
    setStatus(elements.adminStatus, 'Select a capsule type first.', 'error');
    return;
  }
  const batchLabel = elements.batchLabel?.value.trim() || `${config.name} batch`;
  const batchId = `batch-${Date.now()}-${randomEightDigitCode()}`;
  setStatus(elements.adminStatus, `Generating ${PDF_BATCH_SIZE} capsule QR codes...`);
  try {
    const batch = writeBatch(db);
    const generated = [];
    const used = new Set();
    for (let i = 0; i < PDF_BATCH_SIZE; i += 1) {
      let code = randomEightDigitCode();
      while (used.has(code) || (await getDoc(doc(db, CAPSULE_CODE_COLLECTION, code))).exists()) {
        code = randomEightDigitCode();
      }
      used.add(code);
      const payload = {
        code,
        qrUrl: capsuleUrlForCode(code),
        batchId,
        batchLabel,
        capsuleId: config.id,
        capsuleName: config.name,
        capsuleImageUrl: config.imageUrl,
        prizes: cleanPrizes(config.prizes),
        topPrizePreview: topFourPrizes(config.prizes).map((prize) => ({
          name: prize.name,
          percentage: prize.percentage
        })),
        redeemed: false,
        createdBy: currentUser.uid,
        createdByEmail: currentUser.email || '',
        createdAt: serverTimestamp()
      };
      batch.set(doc(db, CAPSULE_CODE_COLLECTION, code), payload);
      generated.push(payload);
    }
    await batch.commit();
    lastGeneratedCodes = generated;
    if (elements.downloadPdf) elements.downloadPdf.disabled = false;
    renderGeneratedCodes(generated);
    await renderRecentCodes();
    setStatus(elements.adminStatus, `Generated ${generated.length} ${config.name} QR codes. Download the PDF now.`, 'success');
  } catch (err) {
    setStatus(elements.adminStatus, `Generation failed: ${err.message || 'unknown error'}`, 'error');
  }
}

function renderGeneratedCodes(codes) {
  if (!elements.generatedTable) return;
  if (!codes.length) {
    elements.generatedTable.innerHTML = '<p class="capsule-muted">No generated codes yet.</p>';
    return;
  }
  elements.generatedTable.innerHTML = renderCodeTable(codes);
}

async function renderRecentCodes() {
  if (!elements.recentTable) return;
  if (!currentUser || !isCapsuleAdmin(currentUser, currentUserData)) {
    elements.recentTable.innerHTML = '';
    return;
  }
  try {
    const snap = await getDocs(query(collection(db, CAPSULE_CODE_COLLECTION), orderBy('createdAt', 'desc'), limit(40)));
    const rows = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
    elements.recentTable.innerHTML = rows.length ? renderCodeTable(rows) : '<p class="capsule-muted">No capsule codes generated yet.</p>';
  } catch (err) {
    elements.recentTable.innerHTML = `<p class="capsule-error-text">Could not load recent codes: ${escapeHtml(err.message || 'error')}</p>`;
  }
}

function renderCodeTable(codes) {
  return `
    <div class="capsule-table-wrap">
      <table class="capsule-table">
        <thead><tr><th>8 digit code</th><th>Type of Capsule</th><th>Top possible prizes</th><th>Status</th></tr></thead>
        <tbody>${codes.map((code) => `
          <tr>
            <td class="eight-code inline">${escapeHtml(code.code || code.id)}</td>
            <td>${escapeHtml(code.capsuleName || 'Capsule')}</td>
            <td>${topFourPrizes(code.prizes).map((prize) => `${escapeHtml(prize.name)} ${formatPercent(prize.percentage)}`).join('<br>')}</td>
            <td>${code.redeemed ? `Opened: ${escapeHtml(code.prizeName || 'Prize')}` : 'Ready'}</td>
          </tr>
        `).join('')}</tbody>
      </table>
    </div>
  `;
}

function waitFrame() {
  return new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
}

async function qrDataUrl(text, size = 160) {
  if (!window.QRCode) throw new Error('QR library failed to load.');
  const holder = document.createElement('div');
  holder.style.width = `${size}px`;
  holder.style.height = `${size}px`;
  elements.qrWork?.appendChild(holder);
  new window.QRCode(holder, {
    text,
    width: size,
    height: size,
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

function renderQrInto(id, text, size) {
  const el = $(id);
  if (!el || !window.QRCode) return;
  el.innerHTML = '';
  new window.QRCode(el, {
    text,
    width: size,
    height: size,
    correctLevel: window.QRCode.CorrectLevel.M
  });
}

async function downloadGeneratedPdf() {
  if (!lastGeneratedCodes.length) {
    setStatus(elements.adminStatus, 'Generate a 100 capsule batch first.', 'error');
    return;
  }
  if (!window.jspdf?.jsPDF) {
    setStatus(elements.adminStatus, 'PDF library failed to load.', 'error');
    return;
  }
  setStatus(elements.adminStatus, 'Building the QR PDF...');
  try {
    const { jsPDF } = window.jspdf;
    const pdf = new jsPDF({ unit: 'pt', format: 'letter' });
    const pageWidth = pdf.internal.pageSize.getWidth();
    const pageHeight = pdf.internal.pageSize.getHeight();
    const margin = 34;
    const columns = 2;
    const rows = 4;
    const cardWidth = (pageWidth - margin * 2 - 14) / columns;
    const cardHeight = (pageHeight - margin * 2 - 24) / rows;

    for (let i = 0; i < lastGeneratedCodes.length; i += 1) {
      if (i > 0 && i % (columns * rows) === 0) pdf.addPage();
      const index = i % (columns * rows);
      const col = index % columns;
      const row = Math.floor(index / columns);
      const x = margin + col * (cardWidth + 14);
      const y = margin + 24 + row * cardHeight;
      const code = lastGeneratedCodes[i];
      const qr = await qrDataUrl(code.qrUrl, 160);
      const prizes = topFourPrizes(code.prizes);

      if (index === 0) {
        pdf.setFillColor(8, 11, 18);
        pdf.rect(0, 0, pageWidth, pageHeight, 'F');
        pdf.setTextColor(42, 255, 158);
        pdf.setFont('helvetica', 'bold');
        pdf.setFontSize(15);
        pdf.text('CAPSULE REWARDS - 100 QR CODE BATCH', margin, margin - 7);
      }

      pdf.setDrawColor(42, 255, 158);
      pdf.setFillColor(18, 24, 36);
      pdf.roundedRect(x, y, cardWidth, cardHeight - 8, 12, 12, 'FD');
      pdf.addImage(qr, 'PNG', x + 12, y + 16, 88, 88);

      pdf.setTextColor(255, 255, 255);
      pdf.setFont('helvetica', 'bold');
      pdf.setFontSize(11);
      pdf.text(code.capsuleName || 'Capsule', x + 110, y + 30, { maxWidth: cardWidth - 124 });

      pdf.setTextColor(42, 255, 158);
      pdf.setFontSize(15);
      pdf.text(String(code.code), x + 110, y + 52, { maxWidth: cardWidth - 124 });

      pdf.setTextColor(180, 193, 214);
      pdf.setFont('helvetica', 'normal');
      pdf.setFontSize(7.5);
      pdf.text('4 most common possible prizes:', x + 110, y + 70, { maxWidth: cardWidth - 124 });
      prizes.forEach((prize, prizeIndex) => {
        pdf.text(`${prize.name} - ${formatPercent(prize.percentage)}`, x + 110, y + 84 + prizeIndex * 11, { maxWidth: cardWidth - 124 });
      });

      pdf.setTextColor(120, 135, 158);
      pdf.setFontSize(6.5);
      pdf.text('QR code on left. Scan to reveal and add item.', x + 12, y + cardHeight - 20, { maxWidth: cardWidth - 24 });
    }

    const label = lastGeneratedCodes[0]?.batchId || `capsule-${Date.now()}`;
    pdf.save(`${label}-100-qr-codes.pdf`);
    setStatus(elements.adminStatus, 'PDF downloaded.', 'success');
  } catch (err) {
    setStatus(elements.adminStatus, `PDF failed: ${err.message || 'unknown error'}`, 'error');
  }
}

async function startScanner(kind) {
  const isItem = kind === 'item';
  const holderId = isItem ? 'item-scanner' : 'capsule-scanner';
  const startButton = isItem ? elements.itemStartScan : elements.startScan;
  const stopButton = isItem ? elements.itemStopScan : elements.stopScan;
  if (!window.Html5Qrcode) {
    setStatus(isItem ? null : elements.redeemStatus, 'QR scanner library failed to load. Enter the code manually.', 'error');
    if (isItem) renderItemLookupError('QR scanner library failed to load. Enter the item code manually.');
    return;
  }
  await stopScanner(kind);
  const scanner = new window.Html5Qrcode(holderId);
  if (isItem) itemScanner = scanner;
  else capsuleScanner = scanner;
  startButton.disabled = true;
  stopButton.disabled = false;
  const onSuccess = async (decodedText) => {
    const code = parseEightDigitCode(decodedText, isItem ? 'item' : 'code');
    if (!code) return;
    await stopScanner(kind);
    if (isItem) {
      if (elements.itemLookupCode) elements.itemLookupCode.value = code;
      await lookupItemCode(code);
    } else {
      if (elements.redeemCode) elements.redeemCode.value = code;
      await openCapsuleByCode(code);
    }
  };
  try {
    await scanner.start(
      { facingMode: 'environment' },
      { fps: 10, qrbox: { width: 240, height: 240 } },
      onSuccess
    );
  } catch (err) {
    startButton.disabled = false;
    stopButton.disabled = true;
    if (isItem) renderItemLookupError(`Camera could not start: ${err.message || 'use manual entry'}`);
    else setStatus(elements.redeemStatus, `Camera could not start: ${err.message || 'use manual entry'}`, 'error');
  }
}

async function stopScanner(kind) {
  const isItem = kind === 'item';
  const scanner = isItem ? itemScanner : capsuleScanner;
  const startButton = isItem ? elements.itemStartScan : elements.startScan;
  const stopButton = isItem ? elements.itemStopScan : elements.stopScan;
  if (scanner) {
    try {
      await scanner.stop();
      await scanner.clear();
    } catch (_) {}
  }
  if (isItem) itemScanner = null;
  else capsuleScanner = null;
  if (startButton) startButton.disabled = false;
  if (stopButton) stopButton.disabled = true;
}

function setupEvents() {
  elements.loginForm?.addEventListener('submit', handleLogin);
  elements.signupForm?.addEventListener('submit', handleSignup);
  elements.redeemForm?.addEventListener('submit', handleRedeem);
  elements.startScan?.addEventListener('click', () => startScanner('capsule'));
  elements.stopScan?.addEventListener('click', () => stopScanner('capsule'));
  elements.refreshItems?.addEventListener('click', renderUserItems);
  elements.itemStartScan?.addEventListener('click', () => startScanner('item'));
  elements.itemStopScan?.addEventListener('click', () => stopScanner('item'));
  elements.itemLookupForm?.addEventListener('submit', handleLookupItem);
  elements.configForm?.addEventListener('submit', handleSaveCapsuleConfig);
  elements.configClear?.addEventListener('click', clearCapsuleConfigForm);
  elements.generateForm?.addEventListener('submit', handleGenerateCodes);
  elements.downloadPdf?.addEventListener('click', downloadGeneratedPdf);

  const params = new URLSearchParams(window.location.search);
  const code = parseEightDigitCode(params.get('code'), 'code');
  const item = parseEightDigitCode(params.get('item'), 'item');
  if (code) {
    pendingCapsuleCode = code;
    if (elements.redeemCode) elements.redeemCode.value = code;
    setStatus(elements.redeemStatus, 'Capsule QR code loaded. Sign in, then it will open.', 'success');
    location.hash = '#redeem';
  }
  if (item) {
    pendingItemCode = item;
    if (elements.itemLookupCode) elements.itemLookupCode.value = item;
    location.hash = '#staff';
  }
  clearCapsuleConfigForm();
}

onAuthStateChanged(auth, async (user) => {
  currentUser = user;
  if (!user) {
    currentUserData = null;
    setAccountPill(null, null);
    setStatus(elements.authStatus, 'Log in or sign up to redeem capsule QR codes.');
    elements.staffSection.hidden = true;
    elements.adminSection.hidden = true;
    if (elements.staffNav) elements.staffNav.hidden = true;
    if (elements.adminNav) elements.adminNav.hidden = true;
    await renderUserItems();
    return;
  }

  try {
    currentUserData = await ensureUserProfile(user);
    setAccountPill(user, currentUserData);
    setStatus(elements.authStatus, 'Signed in and ready to redeem.', 'success');
    const staff = isStoreStaff(user, currentUserData);
    const admin = isCapsuleAdmin(user, currentUserData);
    elements.staffSection.hidden = !staff;
    elements.adminSection.hidden = !admin;
    if (elements.staffNav) elements.staffNav.hidden = !staff;
    if (elements.adminNav) elements.adminNav.hidden = !admin;
    if (admin) {
      setStatus(elements.adminStatus, 'Admin tools ready. Set capsules, generate 100-code PDFs, and review recent codes.', 'success');
      await loadCapsuleConfigs();
      await renderRecentCodes();
    }
    await renderUserItems();
    if (pendingCapsuleCode) {
      const code = pendingCapsuleCode;
      pendingCapsuleCode = null;
      await openCapsuleByCode(code);
    }
    if (pendingItemCode && staff) {
      const item = pendingItemCode;
      pendingItemCode = null;
      await lookupItemCode(item);
    }
  } catch (err) {
    setAccountPill(user, null);
    setStatus(elements.authStatus, `Signed in, but profile failed to load: ${err.message || 'error'}`, 'error');
  }
});

setupEvents();
loadCapsuleConfigs();
