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
  increment,
  runTransaction
} from "https://www.gstatic.com/firebasejs/9.22.1/firebase-firestore.js";
import {
  getStorage,
  ref,
  uploadBytes,
  getDownloadURL
} from "https://www.gstatic.com/firebasejs/9.22.1/firebase-storage.js";

const ADMIN_EMAIL = 'chonhouliu@gmail.com';
const DEFAULT_AVATAR = 'https://t3.ftcdn.net/jpg/00/64/67/80/360_F_64678017_zUpiZFjj04cnLri7oADnyMH0XBYyQghG.jpg';
const CAPSULE_CONFIG_COLLECTION = 'capsuleConfigs';
const CAPSULE_CODE_COLLECTION = 'capsuleCodes';
const CAPSULE_ITEM_COLLECTION = 'capsuleItems';
const PDF_BATCH_SIZE = 100;
const MACHINE_CAPSULE_TYPES = 7;
const PAGE_PATHS = {
  home: '/capsule/',
  account: '/capsule/account.html',
  redeem: '/capsule/redeem.html',
  items: '/capsule/items.html',
  store: '/capsule/shop.html',
  staff: '/capsule/staff.html',
  admin: '/capsule/admin.html'
};
const CODE_LENGTH = 8;
const CODE_UPPER = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
const CODE_LOWER = 'abcdefghijkmnopqrstuvwxyz';
const CODE_NUMBER = '23456789';
const CODE_ALPHABET = `${CODE_UPPER}${CODE_LOWER}${CODE_NUMBER}`;

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
const storage = getStorage(app);

let currentUser = null;
let currentUserData = null;
let capsuleConfigs = [];
let lastGeneratedCodes = [];
let capsuleScanner = null;
let itemScanner = null;
let pendingCapsuleCode = null;
let pendingItemCode = null;
let pendingCapsuleOpen = null;
let capsuleOpeningInProgress = false;

const $ = (id) => document.getElementById(id);

const elements = {
  accountPill: $('capsule-account-pill'),
  staffNav: $('staff-nav-link'),
  adminNav: $('admin-nav-link'),
  sideStaffNav: $('side-staff-nav-link'),
  sideAdminNav: $('side-admin-nav-link'),
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
  capsuleCameraPermission: $('capsule-camera-permission'),
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
  prizePanel: $('revealed-prize-panel'),
  rewardBanner: $('reward-banner'),
  rewardBannerTitle: $('reward-banner-title'),
  openCapsuleBtn: $('capsule-open-button'),
  openCapsuleHint: $('capsule-open-hint'),
  oddsList: $('capsule-odds-list'),
  refreshItems: $('capsule-refresh-items'),
  itemsList: $('capsule-items-list'),
  tokenBalance: $('capsule-token-balance'),
  storeStatus: $('capsule-store-status'),
  staffSection: $('staff'),
  itemStartScan: $('item-start-scan'),
  itemStopScan: $('item-stop-scan'),
  itemCameraPermission: $('item-camera-permission'),
  itemLookupForm: $('item-lookup-form'),
  itemLookupCode: $('item-lookup-code'),
  itemLookupResult: $('item-lookup-result'),
  adminSection: $('admin'),
  configForm: $('capsule-config-form'),
  configId: $('capsule-config-id'),
  configName: $('capsule-config-name'),
  configChance: $('capsule-config-chance'),
  configImage: $('capsule-config-image'),
  configImageFile: $('capsule-config-image-file'),
  uploadCapsuleImage: $('capsule-upload-capsule-image'),
  prizesInput: $('capsule-prizes-input'),
  prizeBlocks: $('capsule-prize-blocks'),
  addPrizeBlock: $('capsule-add-prize-block'),
  configClear: $('capsule-config-clear'),
  configStatus: $('capsule-config-status'),
  configList: $('capsule-config-list'),
  generateForm: $('capsule-generate-form'),
  machineName: $('capsule-machine-name'),
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

function randomIndex(max) {
  const cryptoApi = window.crypto || window.msCrypto;
  if (cryptoApi?.getRandomValues) {
    const bytes = new Uint32Array(1);
    cryptoApi.getRandomValues(bytes);
    return bytes[0] % max;
  }
  return Math.floor(Math.random() * max);
}

function randomFromAlphabet(alphabet) {
  return alphabet[randomIndex(alphabet.length)];
}

function shuffleCode(chars) {
  const next = [...chars];
  for (let i = next.length - 1; i > 0; i -= 1) {
    const j = randomIndex(i + 1);
    [next[i], next[j]] = [next[j], next[i]];
  }
  return next.join('');
}

function randomMixedCode() {
  const chars = [
    randomFromAlphabet(CODE_UPPER),
    randomFromAlphabet(CODE_LOWER),
    randomFromAlphabet(CODE_NUMBER)
  ];
  while (chars.length < CODE_LENGTH) {
    chars.push(randomFromAlphabet(CODE_ALPHABET));
  }
  return shuffleCode(chars);
}

function randomNumericCode(length) {
  let code = '';
  for (let i = 0; i < length; i += 1) {
    code += String(randomIndex(10));
  }
  if (code[0] === '0') code = `${1 + randomIndex(9)}${code.slice(1)}`;
  return code;
}

async function ensureUniqueDisplayId() {
  for (let attempt = 0; attempt < 80; attempt += 1) {
    const displayId = randomNumericCode(6);
    const snap = await getDocs(query(collection(db, 'users'), where('displayId', '==', displayId)));
    if (snap.empty) return displayId;
  }
  return randomNumericCode(6);
}

function capsuleUrlForCode(code) {
  const url = new URL(PAGE_PATHS.redeem, window.location.origin);
  url.searchParams.set('code', code);
  return url.toString();
}

function itemUrlForCode(code) {
  const url = new URL(PAGE_PATHS.staff, window.location.origin);
  url.searchParams.set('item', code);
  return url.toString();
}

function parseMixedCode(value, queryKey) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try {
    const url = new URL(raw, window.location.origin);
    const fromQuery = url.searchParams.get(queryKey);
    if (fromQuery) {
      const match = String(fromQuery).match(/[A-Za-z0-9]{8}/);
      if (match) return match[0];
    }
  } catch (_) {}
  const match = raw.match(/[A-Za-z0-9]{8}/);
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

function defaultTokenImage(name = 'Tokens') {
  const safeName = escapeSvgText(String(name || 'Tokens').slice(0, 24));
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="360" height="360" viewBox="0 0 360 360"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop stop-color="#ffd84a"/><stop offset="1" stop-color="#ff8c2a"/></linearGradient></defs><rect width="360" height="360" rx="58" fill="#111827"/><circle cx="180" cy="156" r="86" fill="url(#g)"/><circle cx="180" cy="156" r="62" fill="none" stroke="#fff" stroke-width="14" opacity=".55"/><text x="180" y="174" text-anchor="middle" fill="#111827" font-size="58" font-family="Arial" font-weight="900">T</text><text x="180" y="320" text-anchor="middle" fill="#fff" font-size="26" font-family="Arial" font-weight="700">${safeName}</text></svg>`;
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
  const rewardType = String(raw?.rewardType || raw?.type || '').toLowerCase() === 'tokens' ? 'tokens' : 'prize';
  const tokenAmount = Math.max(0, Math.floor(numberValue(raw?.tokenAmount ?? raw?.tokens, 0)));
  const imageUrl = String(raw?.imageUrl || raw?.prizeImageUrl || '').trim();
  if (!name || percentage <= 0) return null;
  return {
    name,
    percentage: Math.round(percentage * 100) / 100,
    rewardType,
    tokenAmount: rewardType === 'tokens' ? Math.max(1, tokenAmount || 10) : 0,
    imageUrl: imageUrl || (rewardType === 'tokens' ? defaultTokenImage(name) : defaultPrizeImage(name))
  };
}

function parsePrizeTextarea(value) {
  return String(value || '')
    .split(/\r?\n/)
    .map((line, index) => {
      const parts = line.split('|').map((part) => part.trim());
      return cleanPrize({ name: parts[0], percentage: parts[1], imageUrl: parts[2], rewardType: parts[3], tokenAmount: parts[4] }, index);
    })
    .filter(Boolean);
}

function sanitizeFileName(name) {
  return String(name || 'image')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80) || 'image';
}

async function uploadImageFile(file, folder) {
  if (!file) return '';
  if (!currentUser) throw new Error('Sign in before uploading images.');
  if (!file.type || !file.type.startsWith('image/')) {
    throw new Error('Only image uploads are allowed.');
  }
  if (file.size > 5 * 1024 * 1024) {
    throw new Error('Image uploads must be 5 MB or smaller.');
  }
  const safeName = sanitizeFileName(file.name);
  const path = `capsule-rewards/${folder}/${currentUser.uid}/${Date.now()}-${randomMixedCode()}-${safeName}`;
  const storageRef = ref(storage, path);
  await uploadBytes(storageRef, file, { contentType: file.type });
  return getDownloadURL(storageRef);
}

function syncPrizeTextareaFromBlocks() {
  if (!elements.prizesInput || !elements.prizeBlocks) return;
  const lines = Array.from(elements.prizeBlocks.querySelectorAll('.prize-block')).map((block) => {
    const name = block.querySelector('[data-prize-field="name"]')?.value.trim() || '';
    const percentage = block.querySelector('[data-prize-field="percentage"]')?.value.trim() || '';
    const imageUrl = block.querySelector('[data-prize-field="imageUrl"]')?.value.trim() || '';
    const rewardType = block.querySelector('[data-prize-field="rewardType"]')?.value || 'prize';
    const tokenAmount = block.querySelector('[data-prize-field="tokenAmount"]')?.value.trim() || '';
    return `${name} | ${percentage} | ${imageUrl} | ${rewardType} | ${tokenAmount}`;
  });
  elements.prizesInput.value = lines.join('\n');
}

function prizeBlockTemplate(prize = {}, index = 0) {
  const safePrize = cleanPrize(prize, index) || {
    name: String(prize?.name || `Prize ${index + 1}`),
    percentage: Math.max(1, numberValue(prize?.percentage, 1)),
    imageUrl: String(prize?.imageUrl || '')
  };
  return `
    <div class="prize-block" data-prize-index="${index}">
      <div class="prize-block-top">
        <div class="prize-block-badge">Prize ${index + 1}</div>
        <button class="capsule-btn prize-remove-btn" type="button" data-prize-action="remove"><i class="fas fa-trash"></i> Remove</button>
      </div>
      <label>Prize title</label>
      <input data-prize-field="name" value="${escapeHtml(safePrize.name)}" placeholder="Prize title">
      <label>Drop percentage</label>
      <input data-prize-field="percentage" type="number" min="0.01" step="0.01" value="${escapeHtml(safePrize.percentage)}" placeholder="25">
      <label>Reward type</label>
      <select data-prize-field="rewardType">
        <option value="prize" ${safePrize.rewardType === 'tokens' ? '' : 'selected'}>Physical prize / Item QR</option>
        <option value="tokens" ${safePrize.rewardType === 'tokens' ? 'selected' : ''}>Store Tokens</option>
      </select>
      <label>Token amount (only for Store Tokens)</label>
      <input data-prize-field="tokenAmount" type="number" min="1" step="1" value="${escapeHtml(safePrize.tokenAmount || '')}" placeholder="25">
      <label>Prize image URL</label>
      <input data-prize-field="imageUrl" type="url" value="${escapeHtml(safePrize.imageUrl && !safePrize.imageUrl.startsWith('data:') ? safePrize.imageUrl : '')}" placeholder="https://...">
      <label>Or upload prize image</label>
      <div class="upload-row">
        <input data-prize-field="file" type="file" accept="image/*">
        <button class="capsule-btn" type="button" data-prize-action="upload"><i class="fas fa-upload"></i> Upload</button>
      </div>
    </div>
  `;
}

function renderPrizeBlocks(prizes) {
  if (!elements.prizeBlocks) return;
  const list = cleanPrizes(prizes).slice(0, 12);
  elements.prizeBlocks.innerHTML = list.map((prize, index) => prizeBlockTemplate(prize, index)).join('');
  syncPrizeTextareaFromBlocks();
}

function addPrizeBlock(prize = {}) {
  if (!elements.prizeBlocks) return;
  const index = elements.prizeBlocks.querySelectorAll('.prize-block').length;
  elements.prizeBlocks.insertAdjacentHTML('beforeend', prizeBlockTemplate(prize, index));
  renumberPrizeBlocks();
  syncPrizeTextareaFromBlocks();
}

function renumberPrizeBlocks() {
  if (!elements.prizeBlocks) return;
  elements.prizeBlocks.querySelectorAll('.prize-block').forEach((block, index) => {
    block.dataset.prizeIndex = String(index);
    const badge = block.querySelector('.prize-block-badge');
    if (badge) badge.textContent = `Prize ${index + 1}`;
  });
}

async function collectPrizesFromBlocks() {
  if (!elements.prizeBlocks) return parsePrizeTextarea(elements.prizesInput?.value);
  const blocks = Array.from(elements.prizeBlocks.querySelectorAll('.prize-block'));
  const prizes = [];
  for (let index = 0; index < blocks.length; index += 1) {
    const block = blocks[index];
    const name = block.querySelector('[data-prize-field="name"]')?.value.trim();
    const percentage = block.querySelector('[data-prize-field="percentage"]')?.value;
    const rewardType = block.querySelector('[data-prize-field="rewardType"]')?.value || 'prize';
    const tokenAmount = block.querySelector('[data-prize-field="tokenAmount"]')?.value;
    const imageInput = block.querySelector('[data-prize-field="imageUrl"]');
    const fileInput = block.querySelector('[data-prize-field="file"]');
    let imageUrl = imageInput?.value.trim() || '';
    if (fileInput?.files?.[0]) {
      setStatus(elements.configStatus, `Uploading image for ${name || `Prize ${index + 1}`}...`);
      imageUrl = await uploadImageFile(fileInput.files[0], 'prizes');
      if (imageInput) imageInput.value = imageUrl;
      fileInput.value = '';
    }
    const cleaned = cleanPrize({ name, percentage, imageUrl, rewardType, tokenAmount }, index);
    if (cleaned) prizes.push(cleaned);
  }
  syncPrizeTextareaFromBlocks();
  return prizes;
}

async function handlePrizeBlockClick(event) {
  const target = event.target?.closest ? event.target : null;
  const action = target?.closest('[data-prize-action]')?.dataset.prizeAction;
  if (!action) return;
  const block = target.closest('.prize-block');
  if (!block) return;
  if (action === 'remove') {
    block.remove();
    if (!elements.prizeBlocks?.querySelector('.prize-block')) addPrizeBlock({ name: 'New Prize', percentage: 100 });
    renumberPrizeBlocks();
    syncPrizeTextareaFromBlocks();
    return;
  }
  if (action === 'upload') {
    const fileInput = block.querySelector('[data-prize-field="file"]');
    const imageInput = block.querySelector('[data-prize-field="imageUrl"]');
    const name = block.querySelector('[data-prize-field="name"]')?.value.trim() || 'Prize';
    const file = fileInput?.files?.[0];
    if (!file) {
      setStatus(elements.configStatus, 'Choose an image file before uploading.', 'error');
      return;
    }
    try {
      setStatus(elements.configStatus, `Uploading image for ${name}...`);
      const url = await uploadImageFile(file, 'prizes');
      if (imageInput) imageInput.value = url;
      if (fileInput) fileInput.value = '';
      syncPrizeTextareaFromBlocks();
      setStatus(elements.configStatus, `Uploaded image for ${name}.`, 'success');
    } catch (err) {
      setStatus(elements.configStatus, `Upload failed: ${err.message || 'unknown error'}`, 'error');
    }
  }
}

async function handleCapsuleImageUpload() {
  const file = elements.configImageFile?.files?.[0];
  if (!file) {
    setStatus(elements.configStatus, 'Choose a capsule image file before uploading.', 'error');
    return;
  }
  try {
    setStatus(elements.configStatus, 'Uploading capsule image...');
    const url = await uploadImageFile(file, 'capsules');
    if (elements.configImage) elements.configImage.value = url;
    if (elements.configImageFile) elements.configImageFile.value = '';
    setStatus(elements.configStatus, 'Capsule image uploaded.', 'success');
  } catch (err) {
    setStatus(elements.configStatus, `Upload failed: ${err.message || 'unknown error'}`, 'error');
  }
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
  const capsuleChance = Math.max(0.01, numberValue(data.capsuleChance ?? data.obtainPercentage ?? data.machineChance, 1));
  return {
    id: id || slugify(name),
    name,
    capsuleChance: Math.round(capsuleChance * 100) / 100,
    imageUrl: String(data.imageUrl || data.capsuleImageUrl || '').trim() || defaultCapsuleImage(name),
    prizes: cleanPrizes(data.prizes)
  };
}

function defaultMachineCapsuleConfigs() {
  return [
    cleanCapsuleConfig('starter-capsule', {
      name: 'Starter Capsule',
      capsuleChance: 25,
      prizes: [
        { name: 'Common Plush', percentage: 45 },
        { name: 'Sticker Pack', percentage: 25 },
        { name: '10 Store Tokens', percentage: 20, rewardType: 'tokens', tokenAmount: 10 },
        { name: 'Golden Ticket', percentage: 10 }
      ]
    }),
    cleanCapsuleConfig('ocean-capsule', {
      name: 'Ocean Capsule',
      capsuleChance: 16,
      prizes: [
        { name: 'Shell Keychain', percentage: 40 },
        { name: 'Blue Sticker Pack', percentage: 25 },
        { name: '15 Store Tokens', percentage: 15, rewardType: 'tokens', tokenAmount: 15 },
        { name: 'Mini Dolphin', percentage: 20 },
      ]
    }),
    cleanCapsuleConfig('neon-capsule', {
      name: 'Neon Capsule',
      capsuleChance: 14,
      prizes: [
        { name: 'Glow Bracelet', percentage: 42 },
        { name: 'Neon Pin', percentage: 28 },
        { name: 'Light Charm', percentage: 20 },
        { name: 'Rainbow Ticket', percentage: 10 }
      ]
    }),
    cleanCapsuleConfig('sweet-capsule', {
      name: 'Sweet Capsule',
      capsuleChance: 14,
      prizes: [
        { name: 'Candy Charm', percentage: 43 },
        { name: 'Dessert Sticker', percentage: 27 },
        { name: 'Cupcake Figure', percentage: 20 },
        { name: 'Golden Candy', percentage: 10 }
      ]
    }),
    cleanCapsuleConfig('animal-capsule', {
      name: 'Animal Capsule',
      capsuleChance: 12,
      prizes: [
        { name: 'Paw Sticker', percentage: 44 },
        { name: 'Mini Pet', percentage: 26 },
        { name: 'Animal Pin', percentage: 20 },
        { name: 'Rare Dragon', percentage: 10 }
      ]
    }),
    cleanCapsuleConfig('space-capsule', {
      name: 'Space Capsule',
      capsuleChance: 11,
      prizes: [
        { name: 'Star Sticker', percentage: 41 },
        { name: 'Rocket Charm', percentage: 29 },
        { name: 'Astronaut Mini', percentage: 20 },
        { name: 'Meteor Ticket', percentage: 10 }
      ]
    }),
    cleanCapsuleConfig('gold-capsule', {
      name: 'Gold Capsule',
      capsuleChance: 8,
      prizes: [
        { name: 'Gold Sticker', percentage: 45 },
        { name: '50 Store Tokens', percentage: 25, rewardType: 'tokens', tokenAmount: 50 },
        { name: 'Gold Coin Charm', percentage: 25 },
        { name: 'Grand Prize', percentage: 5 }
      ]
    })
  ];
}

function sevenCapsuleTypes(configs = capsuleConfigs) {
  const defaults = defaultMachineCapsuleConfigs();
  const selected = [];
  const used = new Set();
  const addConfig = (config) => {
    if (!config?.id || used.has(config.id) || selected.length >= MACHINE_CAPSULE_TYPES) return;
    selected.push(config);
    used.add(config.id);
  };
  configs.forEach(addConfig);
  defaults.forEach(addConfig);
  return selected;
}

function topFourPrizes(prizes) {
  return [...cleanPrizes(prizes)].sort((a, b) => b.percentage - a.percentage).slice(0, 4);
}

function topThreePrizes(prizes) {
  return [...cleanPrizes(prizes)].sort((a, b) => b.percentage - a.percentage).slice(0, 3);
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

function setCapsuleOpenReady(ready, message = '') {
  if (elements.openCapsuleBtn) {
    elements.openCapsuleBtn.disabled = !ready;
    elements.openCapsuleBtn.classList.toggle('ready', ready);
  }
  if (elements.openCapsuleHint) {
    elements.openCapsuleHint.textContent = message || (ready ? 'Tap capsule to open' : 'Scan a capsule first');
  }
}

function resetRevealStage() {
  pendingCapsuleOpen = null;
  capsuleOpeningInProgress = false;
  elements.revealStage?.classList.remove('loaded', 'opening', 'opened');
  if (elements.rewardBanner) elements.rewardBanner.hidden = true;
  if (elements.rewardBannerTitle) elements.rewardBannerTitle.textContent = 'Reward revealed';
  setCapsuleOpenReady(false, 'Scan a capsule first');
  renderImage(elements.prizeImage, elements.prizeFallback, '', '');
  if (elements.prizeName) elements.prizeName.textContent = 'Prize inside';
  if (elements.prizePercent) elements.prizePercent.textContent = 'Scan to reveal the chance.';
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
      capsuleTokens: 0,
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
  if (!Object.prototype.hasOwnProperty.call(data, 'capsuleTokens')) patch.capsuleTokens = 0;
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
  capsuleConfigs = sevenCapsuleTypes(capsuleConfigs);
  renderCapsuleConfigs();
}

function renderCapsuleConfigs() {
  if (!elements.configList) return;
  elements.configList.innerHTML = sevenCapsuleTypes().map((config, index) => `
    <div class="config-row">
      <img src="${escapeHtml(config.imageUrl)}" alt="">
      <div>
        <strong>Type ${index + 1}: ${escapeHtml(config.name)}</strong>
        <small>Capsule chance ${formatPercent(config.capsuleChance)} - ${topFourPrizes(config.prizes).map((prize) => `${escapeHtml(prize.name)} ${formatPercent(prize.percentage)}${prize.rewardType === 'tokens' ? ` (${escapeHtml(prize.tokenAmount)} Tokens)` : ''}`).join(' - ')}</small>
      </div>
      <button type="button" class="capsule-btn config-edit-btn" data-config-id="${escapeHtml(config.id)}">Edit</button>
    </div>
  `).join('');
  elements.configList.querySelectorAll('.config-edit-btn').forEach((button) => {
    button.addEventListener('click', () => editCapsuleConfig(button.dataset.configId));
  });
}

function editCapsuleConfig(configId) {
  const config = capsuleConfigs.find((item) => item.id === configId);
  if (!config) return;
  if (elements.configId) elements.configId.value = config.id;
  if (elements.configName) elements.configName.value = config.name;
  if (elements.configChance) elements.configChance.value = String(config.capsuleChance || '');
  if (elements.configImage) elements.configImage.value = config.imageUrl.startsWith('data:') ? '' : config.imageUrl;
  if (elements.prizesInput) elements.prizesInput.value = prizeLinesForTextarea(config.prizes);
  renderPrizeBlocks(config.prizes);
  setStatus(elements.configStatus, `Editing ${config.name}.`, 'success');
}

function clearCapsuleConfigForm() {
  if (elements.configId) elements.configId.value = '';
  if (elements.configName) elements.configName.value = '';
  if (elements.configChance) elements.configChance.value = '14.29';
  if (elements.configImage) elements.configImage.value = '';
  if (elements.configImageFile) elements.configImageFile.value = '';
  const defaultPrizes = cleanPrizes([
    { name: 'Common Plush', percentage: 45 },
    { name: 'Sticker Pack', percentage: 25 },
    { name: 'Mini Figure', percentage: 20 },
    { name: 'Golden Ticket', percentage: 10 }
  ]);
  if (elements.prizesInput) elements.prizesInput.value = prizeLinesForTextarea(defaultPrizes);
  renderPrizeBlocks(defaultPrizes);
  setStatus(elements.configStatus, 'Ready to create a new capsule type.');
}

async function handleSaveCapsuleConfig(event) {
  event.preventDefault();
  if (!currentUser || !isCapsuleAdmin(currentUser, currentUserData)) {
    setStatus(elements.configStatus, 'Admin access is required.', 'error');
    return;
  }
  const name = elements.configName?.value.trim();
  if (!name) {
    setStatus(elements.configStatus, 'Enter a capsule type name.', 'error');
    return;
  }
  let prizes = [];
  try {
    prizes = await collectPrizesFromBlocks();
  } catch (err) {
    setStatus(elements.configStatus, `Image upload failed: ${err.message || 'unknown error'}`, 'error');
    return;
  }
  if (!prizes.length) {
    setStatus(elements.configStatus, 'Enter at least one prize with a percentage.', 'error');
    return;
  }
  const id = elements.configId?.value || slugify(name);
  let imageUrl = elements.configImage?.value.trim() || '';
  const capsuleChance = Math.max(0.01, numberValue(elements.configChance?.value, 1));
  const total = totalChance(prizes);
  const payload = {
    name,
    capsuleChance: Math.round(capsuleChance * 100) / 100,
    imageUrl: imageUrl || defaultCapsuleImage(name),
    prizes,
    topPrizePreview: topFourPrizes(prizes).map((prize) => ({ name: prize.name, percentage: prize.percentage })),
    updatedBy: currentUser.uid,
    updatedByEmail: currentUser.email || '',
    updatedAt: serverTimestamp()
  };
  setStatus(elements.configStatus, 'Saving capsule...');
  try {
    if (elements.configImageFile?.files?.[0]) {
      setStatus(elements.configStatus, 'Uploading capsule image...');
      imageUrl = await uploadImageFile(elements.configImageFile.files[0], 'capsules');
      if (elements.configImage) elements.configImage.value = imageUrl;
      elements.configImageFile.value = '';
      payload.imageUrl = imageUrl;
    }
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
  resetRevealStage();
  if (elements.previewTitle) elements.previewTitle.textContent = config.name;
  if (elements.previewCode) elements.previewCode.textContent = code;
  renderImage(elements.previewImage, elements.previewFallback, config.imageUrl, config.name);
  elements.revealStage?.classList.add('loaded');
  if (elements.prizeName) elements.prizeName.textContent = 'Capsule ready';
  if (elements.prizePercent) elements.prizePercent.textContent = 'Tap the capsule image above to open it.';
  if (elements.oddsList) {
    elements.oddsList.innerHTML = `
      <strong>Most common possible rewards</strong>
      ${topThreePrizes(config.prizes).map((prize) => `
        <div class="odds-row">
          <span>${escapeHtml(prize.name)}${prize.rewardType === 'tokens' ? ` (${escapeHtml(prize.tokenAmount)} Tokens)` : ''}</span>
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
  const code = parseMixedCode(elements.redeemCode?.value, 'code');
  if (!currentUser) {
    pendingCapsuleCode = code || pendingCapsuleCode;
    setStatus(elements.redeemStatus, 'Sign in or create an account before opening a capsule.', 'error');
    window.location.href = code ? `${PAGE_PATHS.account}?code=${encodeURIComponent(code)}` : PAGE_PATHS.account;
    return;
  }
  if (!code) {
    setStatus(elements.redeemStatus, 'Enter or scan an 8 character capsule code.', 'error');
    return;
  }
  await openCapsuleByCode(code);
}

async function openCapsuleByCode(code) {
  if (!currentUser) {
    pendingCapsuleCode = code;
    setStatus(elements.redeemStatus, 'Sign in or create an account before opening a capsule.', 'error');
    window.location.href = `${PAGE_PATHS.account}?code=${encodeURIComponent(code)}`;
    return;
  }
  resetRevealStage();
  setStatus(elements.redeemStatus, 'Detecting capsule type...');
  try {
    const codeData = await getCapsuleCode(code);
    renderCapsulePreview(code, codeData);
    pendingCapsuleOpen = { code, codeData };
    setCapsuleOpenReady(true, 'Tap capsule to open');
    setStatus(elements.redeemStatus, `${codeData.capsuleName || 'Capsule'} detected. Tap the capsule image to open it.`, 'success');
    if (elements.redeemCode) elements.redeemCode.value = '';
  } catch (err) {
    setStatus(elements.redeemStatus, err.message || 'Could not open this capsule.', 'error');
  }
}

async function revealPendingCapsule() {
  if (!pendingCapsuleOpen || capsuleOpeningInProgress) return;
  capsuleOpeningInProgress = true;
  setCapsuleOpenReady(false, 'Opening...');
  const { code, codeData } = pendingCapsuleOpen;
  setStatus(elements.redeemStatus, 'Opening capsule...');
  try {
    elements.revealStage?.classList.add('opening');
    const prize = pickPrize(codeData.prizes);
    const reward = await redeemCapsuleCode(code, codeData, prize);
    await wait(650);
    renderImage(elements.prizeImage, elements.prizeFallback, reward.prizeImageUrl, reward.prizeName);
    if (elements.rewardBanner) elements.rewardBanner.hidden = false;
    if (elements.rewardBannerTitle) elements.rewardBannerTitle.textContent = reward.rewardType === 'tokens'
      ? `${reward.tokenAmount} Store Tokens`
      : reward.prizeName;
    if (elements.prizeName) elements.prizeName.textContent = reward.rewardType === 'tokens' ? 'Token reward' : reward.prizeName;
    if (elements.prizePercent) {
      elements.prizePercent.textContent = reward.rewardType === 'tokens'
        ? `${formatPercent(reward.chancePercent)} chance - added to Token Shop balance`
        : `${formatPercent(reward.chancePercent)} chance - item QR created`;
    }
    elements.revealStage?.classList.add('opened');
    pendingCapsuleOpen = null;
    if (reward.rewardType === 'tokens') {
      if (currentUserData) currentUserData.capsuleTokens = Math.max(0, numberValue(currentUserData.capsuleTokens, 0)) + reward.tokenAmount;
      setStatus(elements.redeemStatus, `${reward.tokenAmount} Store Tokens added to your Token Shop balance.`, 'success');
      await renderTokenStore();
    } else {
      setStatus(elements.redeemStatus, `${reward.prizeName} added to your Items with item code ${reward.itemCode}.`, 'success');
      await renderUserItems();
    }
  } catch (err) {
    elements.revealStage?.classList.remove('opening');
    setCapsuleOpenReady(true, 'Try opening again');
    capsuleOpeningInProgress = false;
    setStatus(elements.redeemStatus, err.message || 'Could not open this capsule.', 'error');
    return;
  }
  capsuleOpeningInProgress = false;
}

async function redeemCapsuleCode(code, codeData, prize) {
  for (let attempt = 0; attempt < 4; attempt += 1) {
    let itemCode = randomMixedCode();
    try {
      return await runTransaction(db, async (tx) => {
        const codeRef = doc(db, CAPSULE_CODE_COLLECTION, code);
        const userRef = doc(db, 'users', currentUser.uid);
        const codeSnap = await tx.get(codeRef);
        const userSnap = await tx.get(userRef);
        if (!codeSnap.exists()) throw new Error('That capsule code was not found.');
        const liveCodeData = codeSnap.data() || {};
        if (liveCodeData.redeemed === true) throw new Error('That capsule code has already been opened.');
        const livePrizes = cleanPrizes(liveCodeData.prizes || codeData.prizes);
        const safePrize = cleanPrize(prize, 0) || pickPrize(livePrizes);
        const isTokenReward = safePrize.rewardType === 'tokens';
        const capsuleName = String(liveCodeData.capsuleName || codeData.capsuleName || 'Capsule');
        const capsuleImageUrl = String(liveCodeData.capsuleImageUrl || codeData.capsuleImageUrl || defaultCapsuleImage(capsuleName));
        let itemPayload = null;
        if (!isTokenReward) {
          const itemRef = doc(db, CAPSULE_ITEM_COLLECTION, itemCode);
          const itemSnap = await tx.get(itemRef);
          if (itemSnap.exists()) {
            const collision = new Error('ITEM_CODE_COLLISION');
            collision.code = 'ITEM_CODE_COLLISION';
            throw collision;
          }
          itemPayload = {
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
            rewardType: 'prize',
            status: 'owned',
            storeRedeemed: false,
            createdAt: serverTimestamp()
          };
          tx.set(itemRef, itemPayload);
        }
        const rewardPayload = itemPayload || {
          itemCode: '',
          itemQrUrl: '',
          userId: currentUser.uid,
          userEmail: currentUser.email || '',
          capsuleCode: code,
          capsuleId: liveCodeData.capsuleId || codeData.capsuleId || '',
          capsuleName,
          capsuleImageUrl,
          prizeName: safePrize.name,
          prizeImageUrl: safePrize.imageUrl || defaultTokenImage(safePrize.name),
          chancePercent: safePrize.percentage,
          rewardType: 'tokens',
          tokenAmount: safePrize.tokenAmount,
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
            capsuleTokens: isTokenReward ? safePrize.tokenAmount : 0,
            badges: [],
            createdAt: serverTimestamp()
          }, { merge: true });
        } else if (isTokenReward) {
          tx.update(userRef, { capsuleTokens: increment(safePrize.tokenAmount) });
        }
        const codeUpdate = {
          redeemed: true,
          redeemedBy: currentUser.uid,
          redeemedByEmail: currentUser.email || '',
          redeemedAt: serverTimestamp(),
          rewardType: safePrize.rewardType,
          prizeName: safePrize.name,
          prizeImageUrl: safePrize.imageUrl || (isTokenReward ? defaultTokenImage(safePrize.name) : defaultPrizeImage(safePrize.name)),
          prizeChancePercent: safePrize.percentage,
          tokenAmount: isTokenReward ? safePrize.tokenAmount : 0
        };
        if (!isTokenReward) codeUpdate.itemCode = itemCode;
        tx.update(codeRef, codeUpdate);
        return rewardPayload;
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

async function renderTokenStore() {
  if (!elements.tokenBalance && !elements.storeStatus) return;
  if (!currentUser) {
    if (elements.tokenBalance) elements.tokenBalance.textContent = '0';
    setStatus(elements.storeStatus, 'Sign in to load your Token balance.');
    return;
  }
  try {
    const snap = await getDoc(doc(db, 'users', currentUser.uid));
    const data = snap.exists() ? snap.data() : currentUserData || {};
    currentUserData = { ...(currentUserData || {}), ...data };
    const balance = Math.max(0, Math.floor(numberValue(data.capsuleTokens, 0)));
    if (elements.tokenBalance) elements.tokenBalance.textContent = String(balance);
    setStatus(elements.storeStatus, `Token balance loaded: ${balance} Token${balance === 1 ? '' : 's'}.`, 'success');
  } catch (err) {
    setStatus(elements.storeStatus, `Could not load Token balance: ${err.message || 'error'}`, 'error');
  }
}

async function handleLookupItem(event) {
  event.preventDefault();
  const code = parseMixedCode(elements.itemLookupCode?.value, 'item');
  if (!code) {
    renderItemLookupError('Enter or scan an 8 character item code.');
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
    window.location.href = `${PAGE_PATHS.account}?item=${encodeURIComponent(code)}`;
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
  const machineTypes = sevenCapsuleTypes();
  if (machineTypes.length !== MACHINE_CAPSULE_TYPES) {
    setStatus(elements.adminStatus, 'Set exactly 7 capsule types before printing a Gatcha Machine.', 'error');
    return;
  }
  const machineName = elements.machineName?.value.trim() || 'Gatcha Machine';
  const batchLabel = elements.batchLabel?.value.trim() || `${machineName} refill`;
  const batchId = `batch-${Date.now()}-${randomMixedCode()}`;
  const machineId = `machine-${Date.now()}-${randomMixedCode()}`;
  const slots = buildMachineSlots(machineTypes);
  setStatus(elements.adminStatus, `Generating ${machineName}: ${PDF_BATCH_SIZE} capsule QR codes across ${MACHINE_CAPSULE_TYPES} types...`);
  try {
    const batch = writeBatch(db);
    const generated = [];
    const used = new Set();
    for (let i = 0; i < slots.length; i += 1) {
      const config = slots[i];
      let code = randomMixedCode();
      while (used.has(code) || (await getDoc(doc(db, CAPSULE_CODE_COLLECTION, code))).exists()) {
        code = randomMixedCode();
      }
      used.add(code);
      const payload = {
        code,
        qrUrl: capsuleUrlForCode(code),
        batchId,
        batchLabel,
        gatchaMachineId: machineId,
        gatchaMachineName: machineName,
        machineSlot: i + 1,
        machineCapsuleCount: PDF_BATCH_SIZE,
        machineCapsuleTypeCount: MACHINE_CAPSULE_TYPES,
        capsuleId: config.id,
        capsuleName: config.name,
        capsuleChance: config.capsuleChance,
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
    setStatus(elements.adminStatus, `Generated ${machineName}: ${generated.length} capsule QR codes across ${MACHINE_CAPSULE_TYPES} types. Download the Machine PDF now.`, 'success');
  } catch (err) {
    setStatus(elements.adminStatus, `Generation failed: ${err.message || 'unknown error'}`, 'error');
  }
}

function buildMachineSlots(configs) {
  const types = sevenCapsuleTypes(configs);
  const reserved = Math.min(types.length, PDF_BATCH_SIZE);
  const remainingSlots = PDF_BATCH_SIZE - reserved;
  const total = types.reduce((sum, config) => sum + Math.max(0, numberValue(config.capsuleChance, 0)), 0) || types.length;
  const allocations = types.map((config) => {
    const exact = (Math.max(0, numberValue(config.capsuleChance, 0)) || 1) / total * remainingSlots;
    return { config, count: 1 + Math.floor(exact), remainder: exact % 1 };
  });
  let assigned = allocations.reduce((sum, entry) => sum + entry.count, 0);
  allocations.sort((a, b) => b.remainder - a.remainder);
  for (let i = 0; assigned < PDF_BATCH_SIZE; i += 1, assigned += 1) {
    allocations[i % allocations.length].count += 1;
  }
  const slots = [];
  allocations.forEach(({ config, count }) => {
    for (let i = 0; i < count; i += 1) {
      slots.push(config);
    }
  });
  return shuffleArray(slots);
}

function shuffleArray(values) {
  const next = [...values];
  for (let i = next.length - 1; i > 0; i -= 1) {
    const j = randomIndex(i + 1);
    [next[i], next[j]] = [next[j], next[i]];
  }
  return next;
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
        <thead><tr><th>8 character code</th><th>Gatcha Machine</th><th>Type of Capsule</th><th>Capsule chance</th><th>Top possible rewards</th><th>Status</th></tr></thead>
        <tbody>${codes.map((code) => `
          <tr>
            <td class="eight-code inline">${escapeHtml(code.code || code.id)}</td>
            <td>${escapeHtml(code.gatchaMachineName || code.batchLabel || 'Gatcha Machine')}${code.machineSlot ? `<br><small>Slot ${escapeHtml(code.machineSlot)}</small>` : ''}</td>
            <td>${escapeHtml(code.capsuleName || 'Capsule')}</td>
            <td>${formatPercent(code.capsuleChance || 0)}</td>
            <td>${topFourPrizes(code.prizes).map((prize) => `${escapeHtml(prize.name)} ${formatPercent(prize.percentage)}${prize.rewardType === 'tokens' ? ` (${escapeHtml(prize.tokenAmount)} Tokens)` : ''}`).join('<br>')}</td>
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

function sortedCodesForPdf(codes) {
  return [...codes].sort((a, b) => {
    const chanceDelta = numberValue(b.capsuleChance, 0) - numberValue(a.capsuleChance, 0);
    if (chanceDelta !== 0) return chanceDelta;
    const nameDelta = String(a.capsuleName || '').localeCompare(String(b.capsuleName || ''));
    if (nameDelta !== 0) return nameDelta;
    return numberValue(a.machineSlot, 0) - numberValue(b.machineSlot, 0);
  });
}

function truncatePdfText(value, maxLength) {
  const text = String(value || '').trim();
  if (text.length <= maxLength) return text;
  return `${text.slice(0, Math.max(0, maxLength - 3)).trim()}...`;
}

function formatPdfRewardLine(prize) {
  const rewardNote = prize.rewardType === 'tokens' ? ` (${prize.tokenAmount || 0} Tokens)` : '';
  return `${formatPercent(prize.percentage)} - ${truncatePdfText(`${prize.name}${rewardNote}`, 14)}`;
}

async function downloadGeneratedPdf() {
  if (!lastGeneratedCodes.length) {
    setStatus(elements.adminStatus, 'Generate a Gatcha Machine first.', 'error');
    return;
  }
  if (!window.jspdf?.jsPDF) {
    setStatus(elements.adminStatus, 'PDF library failed to load.', 'error');
    return;
  }
  setStatus(elements.adminStatus, 'Building the white QR PDF...');
  try {
    const { jsPDF } = window.jspdf;
    const pdf = new jsPDF({ unit: 'pt', format: 'letter', orientation: 'portrait' });
    const pageWidth = pdf.internal.pageSize.getWidth();
    const pageHeight = pdf.internal.pageSize.getHeight();
    const margin = 16;
    const headerHeight = 40;
    const columns = 4;
    const rows = 5;
    const gap = 8;
    const cardWidth = (pageWidth - margin * 2 - gap * (columns - 1)) / columns;
    const cardHeight = (pageHeight - margin * 2 - headerHeight - gap * (rows - 1)) / rows;
    const orderedCodes = sortedCodesForPdf(lastGeneratedCodes);

    for (let i = 0; i < orderedCodes.length; i += 1) {
      if (i > 0 && i % (columns * rows) === 0) pdf.addPage();
      const index = i % (columns * rows);
      const col = index % columns;
      const row = Math.floor(index / columns);
      const x = margin + col * (cardWidth + gap);
      const y = margin + headerHeight + row * (cardHeight + gap);
      const code = orderedCodes[i];
      const qr = await qrDataUrl(code.qrUrl, 160);
      const prizes = topThreePrizes(code.prizes);
      const machineName = orderedCodes[0]?.gatchaMachineName || 'Gatcha Machine';

      if (index === 0) {
        pdf.setFillColor(255, 255, 255);
        pdf.rect(0, 0, pageWidth, pageHeight, 'F');
        pdf.setTextColor(0, 0, 0);
        pdf.setFont('helvetica', 'bold');
        pdf.setFontSize(12);
        pdf.text(`${machineName.toUpperCase()} - 100 CAPSULE GATCHA MACHINE`, margin, margin + 2, { maxWidth: pageWidth - margin * 2 });
        pdf.setFont('helvetica', 'normal');
        pdf.setFontSize(7);
        pdf.text('White PDF. 4 QR codes per row, 5 rows per page. Ordered from lowest/common capsule to rarest. Each label shows the 3 most common rewards.', margin, margin + 14, { maxWidth: pageWidth - margin * 2 });
      }

      pdf.setDrawColor(0, 0, 0);
      pdf.setFillColor(255, 255, 255);
      pdf.rect(x, y, cardWidth, cardHeight, 'FD');
      const innerX = x + 4;
      const innerWidth = cardWidth - 8;
      const qrSize = Math.min(64, innerWidth);
      pdf.addImage(qr, 'PNG', x + (cardWidth - qrSize) / 2, y + 8, qrSize, qrSize);

      pdf.setTextColor(0, 0, 0);
      pdf.setFont('helvetica', 'bold');
      pdf.setFontSize(8);
      pdf.text(truncatePdfText(code.capsuleName || 'Capsule', 26), innerX, y + qrSize + 22, { maxWidth: innerWidth });
      pdf.setFontSize(10);
      pdf.text(String(code.code || ''), innerX, y + qrSize + 36, { maxWidth: innerWidth });

      pdf.setTextColor(40, 40, 40);
      pdf.setFont('helvetica', 'normal');
      pdf.setFontSize(7);
      prizes.forEach((prize, prizeIndex) => {
        pdf.text(formatPdfRewardLine(prize), innerX, y + qrSize + 56 + prizeIndex * 14, { maxWidth: innerWidth });
      });
    }

    const label = lastGeneratedCodes[0]?.batchId || `capsule-${Date.now()}`;
    pdf.save(`${label}-100-qr-codes.pdf`);
    setStatus(elements.adminStatus, 'PDF downloaded.', 'success');
  } catch (err) {
    setStatus(elements.adminStatus, `PDF failed: ${err.message || 'unknown error'}`, 'error');
  }
}

function setCameraPermissionStatus(kind, message, type = '') {
  const el = kind === 'item' ? elements.itemCameraPermission : elements.capsuleCameraPermission;
  if (!el) return;
  el.classList.remove('success', 'error');
  if (type) el.classList.add(type);
  const text = el.querySelector('span');
  if (text) text.textContent = message;
}

async function requestCameraPermission(kind) {
  if (!navigator.mediaDevices?.getUserMedia) {
    setCameraPermissionStatus(kind, 'This browser cannot request camera access here. Enter the 8 character code manually.', 'error');
    return false;
  }

  setCameraPermissionStatus(kind, 'Camera permission requested. Choose Allow to open the QR scanner.');
  try {
    const stream = await navigator.mediaDevices.getUserMedia({
      video: { facingMode: { ideal: 'environment' } },
      audio: false
    });
    stream.getTracks().forEach((track) => track.stop());
    setCameraPermissionStatus(kind, 'Camera allowed. Opening scanner now.', 'success');
    return true;
  } catch (err) {
    const denied = err?.name === 'NotAllowedError' || err?.name === 'PermissionDeniedError';
    setCameraPermissionStatus(
      kind,
      denied
        ? 'Camera permission was denied. Allow camera access in your browser or enter the code manually.'
        : `Camera unavailable: ${err.message || 'enter the code manually.'}`,
      'error'
    );
    return false;
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
  startButton.disabled = true;
  const allowed = await requestCameraPermission(kind);
  if (!allowed) {
    startButton.disabled = false;
    stopButton.disabled = true;
    return;
  }
  const scanner = new window.Html5Qrcode(holderId);
  if (isItem) itemScanner = scanner;
  else capsuleScanner = scanner;
  stopButton.disabled = false;
  const onSuccess = async (decodedText) => {
    const code = parseMixedCode(decodedText, isItem ? 'item' : 'code');
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

function currentPageId() {
  const path = window.location.pathname.replace(/\/+$/, '/');
  if (path.endsWith('/capsule/account.html')) return 'account';
  if (path.endsWith('/capsule/redeem.html')) return 'redeem';
  if (path.endsWith('/capsule/items.html')) return 'items';
  if (path.endsWith('/capsule/shop.html') || path.endsWith('/capsule/store.html')) return 'store';
  if (path.endsWith('/capsule/staff.html')) return 'staff';
  if (path.endsWith('/capsule/admin.html')) return 'admin';
  return 'home';
}

function syncActiveNavigation() {
  const activePage = currentPageId();
  document.querySelectorAll('.capsule-page').forEach((section) => {
    const active = !section.dataset.page || section.dataset.page === activePage;
    section.classList.toggle('active', active);
    section.setAttribute('aria-hidden', active ? 'false' : 'true');
  });
  document.querySelectorAll('[data-page-target]').forEach((link) => {
    link.classList.toggle('active', link.dataset.pageTarget === activePage);
    if (link.dataset.pageTarget === activePage) {
      link.setAttribute('aria-current', 'page');
    } else {
      link.removeAttribute('aria-current');
    }
  });
  document.body.dataset.activePage = activePage;
}

function setupEvents() {
  elements.loginForm?.addEventListener('submit', handleLogin);
  elements.signupForm?.addEventListener('submit', handleSignup);
  elements.redeemForm?.addEventListener('submit', handleRedeem);
  elements.openCapsuleBtn?.addEventListener('click', revealPendingCapsule);
  elements.startScan?.addEventListener('click', () => startScanner('capsule'));
  elements.stopScan?.addEventListener('click', () => stopScanner('capsule'));
  elements.refreshItems?.addEventListener('click', renderUserItems);
  elements.itemStartScan?.addEventListener('click', () => startScanner('item'));
  elements.itemStopScan?.addEventListener('click', () => stopScanner('item'));
  elements.itemLookupForm?.addEventListener('submit', handleLookupItem);
  elements.configForm?.addEventListener('submit', handleSaveCapsuleConfig);
  elements.configClear?.addEventListener('click', clearCapsuleConfigForm);
  elements.uploadCapsuleImage?.addEventListener('click', handleCapsuleImageUpload);
  elements.addPrizeBlock?.addEventListener('click', () => addPrizeBlock({ name: 'New Prize', percentage: 1 }));
  elements.prizeBlocks?.addEventListener('click', handlePrizeBlockClick);
  elements.prizeBlocks?.addEventListener('input', syncPrizeTextareaFromBlocks);
  elements.generateForm?.addEventListener('submit', handleGenerateCodes);
  elements.downloadPdf?.addEventListener('click', downloadGeneratedPdf);

  const params = new URLSearchParams(window.location.search);
  const code = parseMixedCode(params.get('code'), 'code');
  const item = parseMixedCode(params.get('item'), 'item');
  if (code) {
    pendingCapsuleCode = code;
    if (elements.redeemCode) elements.redeemCode.value = code;
    setStatus(elements.redeemStatus, 'Capsule QR code loaded. Sign in, then it will open.', 'success');
    if (currentPageId() !== 'redeem' && currentPageId() !== 'account') {
      window.location.href = `${PAGE_PATHS.redeem}?code=${encodeURIComponent(code)}`;
      return;
    }
  }
  if (item) {
    pendingItemCode = item;
    if (elements.itemLookupCode) elements.itemLookupCode.value = item;
    if (currentPageId() !== 'staff' && currentPageId() !== 'account') {
      window.location.href = `${PAGE_PATHS.staff}?item=${encodeURIComponent(item)}`;
      return;
    }
  }
  clearCapsuleConfigForm();
  syncActiveNavigation();
}

onAuthStateChanged(auth, async (user) => {
  currentUser = user;
  if (!user) {
    currentUserData = null;
    setAccountPill(null, null);
    setStatus(elements.authStatus, 'Log in or sign up to redeem capsule QR codes.');
    if (elements.staffSection) elements.staffSection.hidden = true;
    if (elements.adminSection) elements.adminSection.hidden = true;
    if (elements.staffNav) elements.staffNav.hidden = true;
    if (elements.adminNav) elements.adminNav.hidden = true;
    if (elements.sideStaffNav) elements.sideStaffNav.hidden = true;
    if (elements.sideAdminNav) elements.sideAdminNav.hidden = true;
    syncActiveNavigation();
    if (currentPageId() === 'staff' || currentPageId() === 'admin') {
      window.location.href = PAGE_PATHS.account;
      return;
    }
    await renderUserItems();
    await renderTokenStore();
    return;
  }

  try {
    currentUserData = await ensureUserProfile(user);
    setAccountPill(user, currentUserData);
    setStatus(elements.authStatus, 'Signed in and ready to redeem.', 'success');
    const staff = isStoreStaff(user, currentUserData);
    const admin = isCapsuleAdmin(user, currentUserData);
    if (elements.staffSection) elements.staffSection.hidden = !staff;
    if (elements.adminSection) elements.adminSection.hidden = !admin;
    if (elements.staffNav) elements.staffNav.hidden = !staff;
    if (elements.adminNav) elements.adminNav.hidden = !admin;
    if (elements.sideStaffNav) elements.sideStaffNav.hidden = !staff;
    if (elements.sideAdminNav) elements.sideAdminNav.hidden = !admin;
    syncActiveNavigation();
    if (currentPageId() === 'staff' && !staff) {
      window.location.href = PAGE_PATHS.items;
      return;
    }
    if (currentPageId() === 'admin' && !admin) {
      window.location.href = PAGE_PATHS.items;
      return;
    }
    if (admin) {
      setStatus(elements.adminStatus, 'Admin tools ready. Set 7 capsule types, generate a 100-capsule Gatcha Machine PDF, and review recent codes.', 'success');
      await loadCapsuleConfigs();
      await renderRecentCodes();
    }
    await renderUserItems();
    await renderTokenStore();
    if (pendingCapsuleCode) {
      const code = pendingCapsuleCode;
      pendingCapsuleCode = null;
      if (currentPageId() !== 'redeem') {
        window.location.href = `${PAGE_PATHS.redeem}?code=${encodeURIComponent(code)}`;
        return;
      }
      await openCapsuleByCode(code);
    }
    if (pendingItemCode && staff) {
      const item = pendingItemCode;
      pendingItemCode = null;
      if (currentPageId() !== 'staff') {
        window.location.href = `${PAGE_PATHS.staff}?item=${encodeURIComponent(item)}`;
        return;
      }
      await lookupItemCode(item);
    }
  } catch (err) {
    setAccountPill(user, null);
    setStatus(elements.authStatus, `Signed in, but profile failed to load: ${err.message || 'error'}`, 'error');
  }
});

setupEvents();
loadCapsuleConfigs();
