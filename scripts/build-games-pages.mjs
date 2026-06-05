#!/usr/bin/env node
/**
 * Splits games.html into real standalone page files under games/<route>/index.html
 * plus shared assets/css/games-universe.css and assets/js/games-universe/
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const SOURCE = path.join(ROOT, 'games.html');
const html = fs.readFileSync(SOURCE, 'utf8');

function slice(startMarker, endMarker) {
  const start = html.indexOf(startMarker);
  if (start === -1) throw new Error(`Missing marker: ${startMarker}`);
  const from = start;
  const end = endMarker ? html.indexOf(endMarker, start + startMarker.length) : html.length;
  if (end === -1) throw new Error(`Missing end marker after: ${startMarker}`);
  return html.slice(from, end).trim();
}

const css = slice('<style>', '</style>').replace(/^<style>\s*/, '').replace(/\s*<\/style>$/, '');
const jsRaw = html.match(/<script type="module">([\s\S]*)<\/script>\s*<\/body>/);
if (!jsRaw) throw new Error('Could not extract module script');
let js = jsRaw[1];

js = js.replace(
  /function getCurrentRoutePath\(\)[\s\S]*?function getCurrentPageId\(\)[\s\S]*?return PATH_TO_PAGE_ID\[path\] \|\| 'main-page';\s*\}/,
  `function getCurrentPageId() {
      return String(window.__GU_PAGE__ || 'main-page').trim() || 'main-page';
    }`
);

js = js.replace(
  /async function init\(\) \{[\s\S]*?hidePageLoading\(\);\s*\}/,
  `async function init() {
      applyFixedSiteTheme();
      const pageId = getCurrentPageId();
      if (pageId === 'contact' || pageId === 'main-page') {
        applyMainShellLayout(pageId);
      }
      hidePageLoading();
    }`
);

js = js.replace(/\bglobalSendChatBtn\.addEventListener/g, 'globalSendChatBtn?.addEventListener');
js = js.replace(/\bglobalChatInput\.addEventListener/g, 'globalChatInput?.addEventListener');
js = js.replace(/\bfriendSendChatBtn\.addEventListener/g, 'friendSendChatBtn?.addEventListener');
js = js.replace(/\bfriendChatInput\.addEventListener/g, 'friendChatInput?.addEventListener');
js = js.replace(/\baddFriendBtn\.addEventListener/g, 'addFriendBtn?.addEventListener');
js = js.replace(/document\.querySelector\('#gameModal \.close'\)\.addEventListener/g, "document.querySelector('#gameModal .close')?.addEventListener");
js = js.replace(/\blogoutBtn\.addEventListener/g, 'logoutBtn?.addEventListener');
js = js.replace(
  /document\.getElementById\('suggest-tab'\)\.addEventListener/g,
  "document.getElementById('suggest-tab')?.addEventListener"
);
js = js.replace(
  /document\.getElementById\('report-tab'\)\.addEventListener/g,
  "document.getElementById('report-tab')?.addEventListener"
);
js = js.replace(
  /document\.querySelectorAll\('\.sidebar-tabs \.tab-button, #home-tab, #contact-tab'\)\.forEach\([\s\S]*?\}\);\s*\n\s*document\.getElementById\('suggest-tab'\)/,
  "document.getElementById('suggest-tab')"
);
js = js.replace(
  /if \(isAuthRequiredPage\(getCurrentPageId\(\)\)\) \{\s*window\.location\.assign\('\/games\/dashboard'\);\s*\}/,
  `const loggedOutPageId = getCurrentPageId();
        if (isAuthRequiredPage(loggedOutPageId)) {
          if (noticeModal) noticeModal.style.display = 'flex';
        } else {
          await loadActivePageContent(loggedOutPageId);
        }`
);
js = js.replace(
  /function isAuthRequiredPage\(pageId\) \{[\s\S]*?return !publicPages\.has\(pageId\);\s*\}/,
  `function isAuthRequiredPage(pageId) {
      const publicPages = new Set(['main-page', 'movies-page', 'contact', 'view-profile-page']);
      return !publicPages.has(pageId);
    }`
);
js = js.replace(/\bcloseChanceModal\.addEventListener/g, 'closeChanceModal?.addEventListener');
js = js.replace(/\bcancelSendGiftInventoryBtn\.addEventListener/g, 'cancelSendGiftInventoryBtn?.addEventListener');
js = js.replace(/\bsaveSettingsBtn\.addEventListener/g, 'saveSettingsBtn?.addEventListener');
js = js.replace(
  /document\.getElementById\('signupBtn'\)\.addEventListener/g,
  "document.getElementById('signupBtn')?.addEventListener"
);
js = js.replace(
  /document\.getElementById\('showSignup'\)\.addEventListener/g,
  "document.getElementById('showSignup')?.addEventListener"
);
js = js.replace(
  /document\.getElementById\('showLogin'\)\.addEventListener/g,
  "document.getElementById('showLogin')?.addEventListener"
);
js = js.replace(
  /document\.getElementById\('closeLoginModal'\)\.addEventListener/g,
  "document.getElementById('closeLoginModal')?.addEventListener"
);
js = js.replace(
  /document\.getElementById\('closeSignupModal'\)\.addEventListener/g,
  "document.getElementById('closeSignupModal')?.addEventListener"
);
js = js.replace(
  /document\.getElementById\('noticeLoginBtn'\)\.addEventListener/g,
  "document.getElementById('noticeLoginBtn')?.addEventListener"
);
js = js.replace(
  /document\.getElementById\('noticeSignupBtn'\)\.addEventListener/g,
  "document.getElementById('noticeSignupBtn')?.addEventListener"
);
js = js.replace(
  /document\.getElementById\('noticeCancelBtn'\)\.addEventListener/g,
  "document.getElementById('noticeCancelBtn')?.addEventListener"
);
js = js.replace(
  /case 'main-page':[\s\S]*?case 'contact':[\s\S]*?break;/,
  `case 'main-page':
          if (shouldEagerLoadMainGames()) await mountMainPageGamesContent();
          else ensureMainPageGamesDeferredObserver();
          break;
        case 'contact':
          break;`
);

js = js.replace(
  /window\.addEventListener\('click', \(e\) => \{([\s\S]*?)\}\);/,
  `window.addEventListener('click', (e) => {
      if (confirmModal && e.target === confirmModal) confirmModal.style.display = 'none';
      if (packOpeningModal && e.target === packOpeningModal) {
        packOpeningModal.style.display = 'none';
        resetPackOpeningModalLayout();
      }
      if (sendGiftModal && e.target === sendGiftModal) sendGiftModal.style.display = 'none';
      if (sendingAnimationModal && e.target === sendingAnimationModal) sendingAnimationModal.style.display = 'none';
      if (chanceModal && e.target === chanceModal) chanceModal.style.display = 'none';
      if (sendGiftChatModal && e.target === sendGiftChatModal) sendGiftChatModal.style.display = 'none';
      if (sendGiftInventoryModal && e.target === sendGiftInventoryModal) sendGiftInventoryModal.style.display = 'none';
      if (sendCardModal && e.target === sendCardModal) { sendCardModal.style.display = 'none'; currentCard = null; }
      if (gameModal && e.target === gameModal) closeGameModal();
      if (movieInfoModal && e.target === movieInfoModal) closeMovieInfoModal();
      const ibm = document.getElementById('inv-blook-modal');
      if (ibm && e.target === ibm) closeInvBlookModal();
      const bdm = document.getElementById('badge-detail-modal');
      if (bdm && e.target === bdm) bdm.style.display = 'none';
    });`
);

js = js.replace(
  /document\.addEventListener\('keydown', \(e\) => \{[\s\S]*?if \(e\.key === 'Escape'\) \{[\s\S]*?\}\s*\}\);/,
  `document.addEventListener('keydown', (e) => {
      if (e.key !== 'Escape') return;
      if (confirmModal) confirmModal.style.display = 'none';
      if (packOpeningModal) { packOpeningModal.style.display = 'none'; resetPackOpeningModalLayout(); }
      if (sendGiftModal) sendGiftModal.style.display = 'none';
      if (sendingAnimationModal) sendingAnimationModal.style.display = 'none';
      if (chanceModal) chanceModal.style.display = 'none';
      if (sendGiftChatModal) sendGiftChatModal.style.display = 'none';
      if (sendGiftInventoryModal) sendGiftInventoryModal.style.display = 'none';
      if (sendCardModal) sendCardModal.style.display = 'none';
      closeInvBlookModal();
      if (loginModal) { loginModal.style.display = 'none'; resetLoginWizard(); }
      if (signupModal) signupModal.style.display = 'none';
      if (noticeModal) noticeModal.style.display = 'none';
      closeMovieInfoModal();
      closeGameModal();
      document.querySelectorAll('.staff-modal').forEach(m => m.style.display = 'none');
      const bpm = document.getElementById('banner-picker-modal');
      if (bpm) bpm.style.display = 'none';
      const bdm = document.getElementById('badge-detail-modal');
      if (bdm) bdm.style.display = 'none';
    });`
);

const shellBeforeMain = slice('<div class="site-bg-atlas"', '<!-- Sidebar -->');
const mainPage = slice('<!-- Main Page (Games) -->', '<!-- Movies Page -->');
const moviesPage = slice('<!-- Movies Page -->', '<!-- Profile Page');
const profilePage = slice('<!-- Profile Page (own profile - Blooket style) -->', '<!-- View Other User Profile Page');
const viewProfilePage = slice('<!-- View Other User Profile Page (navigated, not popup) -->', '<!-- History Page -->');
const historyPage = slice('<!-- History Page -->', '<!-- Shop Page -->');
const shopPage = slice('<!-- Shop Page -->', '<!-- Inventory Page');
const inventoryPage = slice('<!-- Inventory Page (Blooket My Blooks–style layout) -->', '<!-- Missions Page -->');
const missionsPage = slice('<!-- Missions Page -->', '<!-- Chat Page');
const chatPage = slice('<!-- Chat Page (Global Chat Only) -->', '<!-- Friends Page');
const friendsPage = slice('<!-- Friends Page (Friends list + Friend Chat) -->', '<!-- Staff Panel Page -->');
const staffPage = slice('<!-- Staff Panel Page -->', '<!-- Settings Page -->');
const settingsPage = slice('<!-- Settings Page -->', '<section class="contact-section"');
const contactSection = slice('<section class="contact-section"', '<!-- Notice Modal');
const sharedMainContentExtras = slice('<!-- Notice Modal (unlogged users) -->', '<!-- Staff Panel Modals -->')
  .replace(/\s*<\/div>\s*$/i, '');
const globalModals = slice('<!-- Staff Panel Modals -->', '<!-- Firebase SDK -->');

function makePageBlock(fragment, { active = true, extraClass = '' } = {}) {
  let block = fragment;
  if (extraClass) {
    block = block.replace(/\bclass="page([^"]*)"/, (m, rest) => {
      const classes = new Set(['page', ...rest.split(/\s+/).filter(Boolean), ...extraClass.split(/\s+/).filter(Boolean)]);
      if (active) classes.add('active');
      return `class="${[...classes].join(' ')}"`;
    });
  } else if (active) {
    block = block.replace(/\bclass="page([^"]*)"/, (m, rest) => {
      if (rest.includes('active')) return m;
      return `class="page${rest} active"`;
    });
  }
  return block;
}

function sidebar(activePageId, activeHeader) {
  let s = slice('<!-- Sidebar -->', '<!-- Main Content -->');
  s = s.replace(/class="tab-button active"/g, 'class="tab-button"');
  s = s.replace(
    new RegExp(`data-page="${activePageId}"`),
    `data-page="${activePageId}" class="tab-button active"`.replace('class="tab-button" class="tab-button active"', 'class="tab-button active"')
  );
  s = s.replace(/(<a class="tab-button" data-page="[^"]+")/g, (m, tag) => {
    if (tag.includes(`data-page="${activePageId}"`)) return tag.replace('class="tab-button"', 'class="tab-button active"');
    return m;
  });
  // fix double class issue - simpler approach
  s = slice('<!-- Sidebar -->', '<!-- Main Content -->');
  s = s.replace(/class="tab-button active"/g, 'class="tab-button"');
  s = s.replace(
    `data-page="${activePageId}"`,
    `data-page="${activePageId}"`
  );
  s = s.replace(
    new RegExp(`(<a class="tab-button" data-page="${activePageId}")`),
    '$1 active'.replace('tab-button" active', 'tab-button active"')
  );
  s = s.replace(`data-page="${activePageId}"`, (match, offset) => {
    const before = s.slice(Math.max(0, offset - 50), offset);
    if (before.includes('tab-button active')) return match;
    return match;
  });
  s = s.replace(
    new RegExp(`class="tab-button"([^>]*data-page="${activePageId}")`),
    `class="tab-button active"$1`
  );
  return s;
}

function buildHeader() {
  return fixNavPaths(slice('<header>', '</header>') + '</header>');
}

const HEAD = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{{TITLE}}</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link rel="preconnect" href="https://wsrv.nl" crossorigin>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <link href="https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800;900&family=Titan+One&display=swap" rel="stylesheet">
  <link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@24,400,0,0" rel="stylesheet">
  <link rel="stylesheet" href="/assets/css/games-universe.css">
</head>
<body>
`;

const FOOT = `
  <script type="module" src="/assets/js/games-universe/pages/{{ROUTE}}.js"></script>
</body>
</html>`;

const PAGES = [
  { route: 'dashboard', title: 'Game Universe - Dashboard', pageId: 'main-page', tab: 'main-page', content: makePageBlock(mainPage, { extraClass: 'main-page main-page-games-deferred' }) },
  { route: 'contact', title: 'Game Universe - Contact', pageId: 'contact', tab: 'contact', content: makePageBlock(mainPage, { active: false, extraClass: 'main-page main-page-games-deferred' }).replace('class="page main-page', 'class="page main-page" style="display:none" aria-hidden="true"') + '\n' + contactSection.replace('class="contact-section"', 'class="contact-section active"') },
  { route: 'movies', title: 'Game Universe - Movies', pageId: 'movies-page', tab: 'movies-page', content: makePageBlock(moviesPage) },
  { route: 'profile', title: 'Game Universe - Profile', pageId: 'profile-page', tab: 'profile-page', content: makePageBlock(profilePage) },
  { route: 'user', title: 'Game Universe - User Profile', pageId: 'view-profile-page', tab: null, content: makePageBlock(viewProfilePage) },
  { route: 'history', title: 'Game Universe - History', pageId: 'history-page', tab: 'history-page', content: makePageBlock(historyPage) },
  { route: 'shop', title: 'Game Universe - Shop', pageId: 'shop-page', tab: 'shop-page', content: makePageBlock(shopPage) },
  { route: 'inventory', title: 'Game Universe - Inventory', pageId: 'inventory-page', tab: 'inventory-page', content: makePageBlock(inventoryPage) },
  { route: 'missions', title: 'Game Universe - Missions', pageId: 'missions-page', tab: 'missions-page', content: makePageBlock(missionsPage) },
  { route: 'chat', title: 'Game Universe - Chat', pageId: 'chat-page', tab: 'chat-page', content: makePageBlock(chatPage) },
  { route: 'friends', title: 'Game Universe - Friends', pageId: 'friends-page', tab: 'friends-page', content: makePageBlock(friendsPage) },
  { route: 'settings', title: 'Game Universe - Settings', pageId: 'settings-page', tab: 'settings-page', content: makePageBlock(settingsPage) },
  { route: 'staff', title: 'Game Universe - Staff', pageId: 'staff-page', tab: 'staff-page', content: makePageBlock(staffPage) },
];

function fixNavPaths(htmlChunk) {
  return htmlChunk
    .replace(/href="games-main\.html"/g, 'href="/games/dashboard"')
    .replace(/href="games-home\.html"/g, 'href="/games/home"')
    .replace(/href="games-contact\.html"/g, 'href="/games/contact"')
    .replace(/href="games-movies\.html"/g, 'href="/games/movies"')
    .replace(/href="games-profile\.html"/g, 'href="/games/profile"')
    .replace(/href="games-history\.html"/g, 'href="/games/history"')
    .replace(/href="games-shop\.html"/g, 'href="/games/shop"')
    .replace(/href="games-inventory\.html"/g, 'href="/games/inventory"')
    .replace(/href="games-missions\.html"/g, 'href="/games/missions"')
    .replace(/href="games-chat\.html"/g, 'href="/games/chat"')
    .replace(/href="games-friends\.html"/g, 'href="/games/friends"')
    .replace(/href="games-settings\.html"/g, 'href="/games/settings"')
    .replace(/href="games-staff\.html"/g, 'href="/games/staff"');
}

function buildSidebar(activeTab) {
  let s = fixNavPaths(slice('<!-- Sidebar -->', '<!-- Main Content -->'));
  s = s.replace(/class="tab-button active"/g, 'class="tab-button"');
  if (activeTab) {
    s = s.replace(
      new RegExp(`(<a class="tab-button" data-page="${activeTab}")`),
      '<a class="tab-button active" data-page="' + activeTab + '"'
    );
  }
  return s;
}

fs.mkdirSync(path.join(ROOT, 'assets/css'), { recursive: true });
fs.mkdirSync(path.join(ROOT, 'assets/js/games-universe/pages'), { recursive: true });
fs.writeFileSync(path.join(ROOT, 'assets/css/games-universe.css'), css.trim() + '\n\n/* Standalone page files */\n.page { display: block !important; }\n.page-loading-overlay.hidden { display: none !important; }\n');
fs.writeFileSync(path.join(ROOT, 'assets/js/games-universe/app.js'), js.trim() + '\n');

for (const page of PAGES) {
  const dir = path.join(ROOT, 'games', page.route);
  fs.mkdirSync(dir, { recursive: true });
  const outPath = path.join(dir, 'index.html');
  const body = [
    shellBeforeMain,
    buildSidebar(page.tab),
    '  <!-- Main Content -->',
    '  <div class="main-content" id="main-content">',
    '    ' + buildHeader().split('\n').join('\n    '),
    '    ' + page.content.split('\n').join('\n    '),
    '    ' + sharedMainContentExtras.split('\n').join('\n    '),
    '  </div>',
    globalModals,
  ].join('\n');

  const file = HEAD.replace('{{TITLE}}', page.title) + body + FOOT.replace('{{ROUTE}}', page.route);
  fs.writeFileSync(outPath, file);

  const pageJs = `window.__GU_PAGE__ = '${page.pageId}';\nimport '../app.js';\n`;
  fs.writeFileSync(path.join(ROOT, 'assets/js/games-universe/pages', `${page.route}.js`), pageJs);
}

// /games entry -> dashboard
fs.writeFileSync(
  path.join(ROOT, 'games/index.html'),
  fs.readFileSync(path.join(ROOT, 'games/dashboard/index.html'), 'utf8')
);

// Legacy /games/home -> dashboard
const homeRedirect = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="refresh" content="0;url=/games/dashboard">
  <script>location.replace('/games/dashboard' + location.search + location.hash);</script>
  <title>Redirecting to Game Universe</title>
</head>
<body><p><a href="/games/dashboard">Continue to Game Universe</a></p></body>
</html>
`;
fs.mkdirSync(path.join(ROOT, 'games/home'), { recursive: true });
fs.writeFileSync(path.join(ROOT, 'games/home/index.html'), homeRedirect);

console.log(`Built ${PAGES.length} Game Universe pages + shared assets.`);
