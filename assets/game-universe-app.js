    import { initializeApp } from "https://www.gstatic.com/firebasejs/9.22.1/firebase-app.js";
    import { getAnalytics } from "https://www.gstatic.com/firebasejs/9.22.1/firebase-analytics.js";
    import { 
      getAuth,     
      createUserWithEmailAndPassword,     
      signInWithEmailAndPassword,     
      signOut,     
      onAuthStateChanged,     
      updatePassword,     
      updateProfile 
    } from "https://www.gstatic.com/firebasejs/9.22.1/firebase-auth.js";
    import { 
      getFirestore,     
      collection,     
      addDoc,     
      getDocs,     
      query,     
      where,     
      orderBy,     
      limit,     
      onSnapshot,     
      doc,     
      getDoc,     
      updateDoc,     
      setDoc,     // <-- ADDED setDoc import
      arrayUnion,     
      arrayRemove,     
      serverTimestamp,     
      writeBatch,    
      deleteDoc,    
      increment,
      runTransaction,
      deleteField
    } from "https://www.gstatic.com/firebasejs/9.22.1/firebase-firestore.js";
    import { 
      getStorage,     
      ref,     
      uploadBytes,     
      getDownloadURL 
    } from "https://www.gstatic.com/firebasejs/9.22.1/firebase-storage.js";

    // Firebase config
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

    // Initialize Firebase
    const app = initializeApp(firebaseConfig);
    const analytics = getAnalytics(app);
    const auth = getAuth(app);
    const db = getFirestore(app);
    const storage = getStorage(app);

    const GU_DASHBOARD_PANEL = typeof window !== 'undefined' && window.__GU_DASHBOARD_PANEL__ ? String(window.__GU_DASHBOARD_PANEL__) : '';
    const GU_IS_LEGACY_SINGLE_PAGE = GU_DASHBOARD_PANEL === '';
    const GU_DASHBOARD_BASE = !GU_IS_LEGACY_SINGLE_PAGE && typeof window !== 'undefined' && window.__GU_DASHBOARD_BASE__
      ? String(window.__GU_DASHBOARD_BASE__)
      : '';
    const GU_DASHBOARD_PAGE_BY_PANEL = {
      main: 'main-page',
      profile: 'profile-page',
      user: 'view-profile-page',
      history: 'history-page',
      shop: 'shop-page',
      inventory: 'inventory-page',
      missions: 'missions-page',
      chat: 'chat-page',
      friends: 'friends-page',
      staff: 'staff-page',
      settings: 'settings-page',
    };

    const DEFAULT_NEON_GREEN = '#2AFF9E';
    const DEFAULT_NEON_PINK = '#FF3D6C';
    const DEFAULT_THEME_GREY = '#8A9BB5';
    const DEFAULT_RARITY_ORDER = ['common','uncommon','rare','epic','legendary','mythical','chroma'];
    let effectiveRarityOrder = [...DEFAULT_RARITY_ORDER];
    let blookRarityDefs = {};
    const DEFAULT_RARITY_DEFS = {
      common: { solid: '#c0c0c0', isGradient: false, gradientColors: [], isRunning: false },
      uncommon: { solid: '#2e7d32', isGradient: false, gradientColors: [], isRunning: false },
      rare: { solid: '#1565c0', isGradient: false, gradientColors: [], isRunning: false },
      epic: { solid: '#6a1b9a', isGradient: false, gradientColors: [], isRunning: false },
      legendary: { solid: '#bf8f00', isGradient: false, gradientColors: [], isRunning: false },
      mythical: { solid: '#c62828', isGradient: false, gradientColors: [], isRunning: false },
      chroma: { solid: '#00bcd4', isGradient: false, gradientColors: [], isRunning: false }
    };
    function raritySortIndex(r) {
      const x = (r || 'common').toLowerCase();
      const i = effectiveRarityOrder.indexOf(x);
      return i === -1 ? 999 : i;
    }

    async function refreshRarityOrderFromServer() {
      try {
        const s = await getDoc(doc(db, 'siteConfig', 'settings'));
        if (s.exists()) {
          const d = s.data();
          const ro = d.blookRarityOrder;
          if (ro) {
            const arr = String(ro).split(',').map(x => x.trim().toLowerCase()).filter(Boolean);
            if (arr.length) effectiveRarityOrder = arr;
          }
          const rd = d.blookRarityDefs;
          if (rd && typeof rd === 'object') blookRarityDefs = { ...DEFAULT_RARITY_DEFS, ...rd };
          else blookRarityDefs = { ...DEFAULT_RARITY_DEFS };
        } else {
          blookRarityDefs = { ...DEFAULT_RARITY_DEFS };
        }
      } catch (e) {
        blookRarityDefs = { ...DEFAULT_RARITY_DEFS };
      }
    }

    function applyFixedSiteTheme() {
      document.documentElement.style.setProperty('--neon-green', DEFAULT_NEON_GREEN);
      document.documentElement.style.setProperty('--neon-pink', DEFAULT_NEON_PINK);
      document.documentElement.style.setProperty('--theme-grey', DEFAULT_THEME_GREY);
    }

    async function getStarBadgeRulesFromServer() {
      try {
        const s = await getDoc(doc(db, 'siteConfig', 'settings'));
        if (!s.exists()) return [];
        const r = s.data().starBadgeRules;
        if (!Array.isArray(r)) return [];
        const byName = new Map();
        r.forEach(x => {
          if (!x || !x.badgeName) return;
          const name = String(x.badgeName).trim();
          if (!name) return;
          const min = Math.max(0, Math.floor(parseInt(x.minStars, 10) || 0));
          const prev = byName.get(name);
          if (prev == null || min < prev) byName.set(name, min);
        });
        return Array.from(byName.entries()).map(([badgeName, minStars]) => ({ badgeName, minStars }));
      } catch (e) {
        return [];
      }
    }

    function computeStarBadgeUpdates(stars, currentBadges, rules) {
      const badges = Array.isArray(currentBadges) ? [...currentBadges] : [];
      const ruleMap = new Map();
      rules.forEach(rule => {
        const name = String(rule.badgeName || '').trim();
        if (!name) return;
        const min = Math.max(0, Number(rule.minStars) || 0);
        const prev = ruleMap.get(name);
        if (prev == null || min < prev) ruleMap.set(name, min);
      });
      const manual = badges.filter(b => !ruleMap.has(b));
      const autoNames = [...ruleMap.keys()].sort((a, b) => {
        const d = ruleMap.get(a) - ruleMap.get(b);
        return d !== 0 ? d : a.localeCompare(b);
      });
      const auto = autoNames.filter(name => stars >= ruleMap.get(name));
      const nextBadges = [...manual, ...auto];
      const changed = nextBadges.length !== badges.length || nextBadges.some((b, i) => b !== badges[i]);
      return { nextBadges, changed, toAdd: [], toRemove: [] };
    }

    async function syncStarBadgesForUser(uid, starCount) {
      if (!uid) return;
      const rules = await getStarBadgeRulesFromServer();
      if (!rules.length) return;
      try {
        await runTransaction(db, async tx => {
          const uref = doc(db, 'users', uid);
          const snap = await tx.get(uref);
          if (!snap.exists()) return;
          const d = snap.data();
          const stars = typeof starCount === 'number' && Number.isFinite(starCount)
            ? starCount
            : (typeof d.stars === 'number' ? d.stars : parseInt(d.stars, 10) || 0);
          const { nextBadges, changed } = computeStarBadgeUpdates(stars, d.badges, rules);
          if (changed) tx.update(uref, { badges: nextBadges });
        });
      } catch (e) {
        console.warn('syncStarBadgesForUser', e);
      }
    }

    function hidePageLoading() {
      const el = document.getElementById('page-loading-overlay');
      if (el) {
        el.classList.add('hidden');
        el.setAttribute('aria-busy', 'false');
      }
    }

    async function ensureUniqueDisplayId() {
      for (let attempt = 0; attempt < 80; attempt++) {
        const n = Math.floor(100000 + Math.random() * 900000);
        const idStr = String(n);
        const snap = await getDocs(query(collection(db, 'users'), where('displayId', '==', idStr)));
        if (snap.empty) return idStr;
      }
      return String(Math.floor(100000 + Math.random() * 900000));
    }

    async function ensureDisplayIdForUser(uid) {
      const ref = doc(db, 'users', uid);
      const u = await getDoc(ref);
      if (!u.exists()) return null;
      const d = u.data();
      if (d.displayId && String(d.displayId).length === 6) return d.displayId;
      const newId = await ensureUniqueDisplayId();
      await updateDoc(ref, { displayId: newId });
      return newId;
    }

    // DOM elements (abbreviated for brevity – all exist in the HTML)
    const noticeModal = document.getElementById('noticeModal');
    const loginModal = document.getElementById('loginModal');
    const signupModal = document.getElementById('signupModal');
    const confirmModal = document.getElementById('confirmModal');
    const userInfoDiv = document.getElementById('user-info');
    const userAvatar = document.getElementById('user-avatar');
    const userNameSpan = document.getElementById('user-name');
    const logoutBtn = document.getElementById('logout-btn');
    const avatarUpload = document.getElementById('avatarUpload');
    const avatarPreview = document.getElementById('avatarPreview');
    const sidebar = document.getElementById('sidebar');
    const mainContent = document.getElementById('main-content');
    const playCount = document.getElementById('play-count');
    const friendCount = document.getElementById('friend-count');
    const playHistoryList = document.getElementById('play-history-list');
    const shopPacks = document.getElementById('shop-packs');
    const shopBanners = document.getElementById('shop-banners');
    const inventoryContainer = document.getElementById('inventory-container');
    const friendsList = document.getElementById('friends-list');
    const globalChatContainer = document.getElementById('global-chat-container');
    const globalChatInput = document.getElementById('global-chat-input');
    const globalSendChatBtn = document.getElementById('global-send-chat-btn');
    const friendChatContainer = document.getElementById('friend-chat-container');
    const friendChatInput = document.getElementById('friend-chat-input');
    const friendSendChatBtn = document.getElementById('friend-send-chat-btn');
    const friendChatInputArea = document.getElementById('friend-chat-input-area');
    const friendChatHeader = document.getElementById('friend-chat-header');
    const addFriendBtn = document.getElementById('add-friend-btn');
    const friendEmailInput = document.getElementById('friend-email');
    let globalChatUnsubscribe = null;
    let friendChatUnsubscribe = null;
    const sendCardModal = document.getElementById('sendCardModal');
    const sendCardName = document.getElementById('send-card-name');
    const sendCardType = document.getElementById('send-card-type');
    const sendCardIcon = document.getElementById('send-card-icon');
    const recipientEmail = document.getElementById('recipient-email');
    const sendCardBtn = document.getElementById('send-card-btn');
    const cancelSendBtn = document.getElementById('cancel-send-btn');
    const notificationPopup = document.getElementById('notification-popup');
    const notificationIcon = document.getElementById('notification-icon');
    const notificationText = document.getElementById('notification-text');
    const coinsDisplay = document.getElementById('coins-display');
    const starsDisplay = document.getElementById('stars-display');
    const shopCoins = document.getElementById('shop-coins');
    const shopStars = document.getElementById('shop-stars');
    const giftWallContainer = document.getElementById('gift-wall-container');
    const packOpeningModal = document.getElementById('pack-opening-modal');
    const packOpeningModalInner = document.getElementById('pack-opening-modal-inner');
    const packOpeningModalBody = document.getElementById('pack-opening-modal-body');
    const packUnboxClickLayer = document.getElementById('pack-unbox-click-layer');
    const packUnboxDismissLayer = document.getElementById('pack-unbox-dismiss-layer');
    const packConfettiLayer = document.getElementById('pack-confetti-layer');
    const packUnboxBoost = document.getElementById('pack-unbox-boost');
    const openingModalBg = document.getElementById('opening-modal-bg');
    const giftAnimation = document.getElementById('gift-animation');
    const openedGiftName = document.getElementById('opened-gift-name');
    const openedGiftRarity = document.getElementById('opened-gift-rarity');
    const openedGiftChance = document.getElementById('opened-gift-chance');
    const packRevealCard = document.getElementById('pack-reveal-card');
    const packRevealBlookSlot = document.getElementById('pack-reveal-blook-slot');
    const packUnboxPackCenter = document.getElementById('pack-unbox-pack-center');
    const openedGiftStars = document.getElementById('opened-gift-stars');
    const sendGiftModal = document.getElementById('send-gift-modal');
    const sendGiftPreview = document.getElementById('send-gift-preview');
    const friendEmailSend = document.getElementById('friend-email-send');
    const confirmSendBtn = document.getElementById('confirm-send-btn');
    const cancelSendGiftBtn = document.getElementById('cancel-send-gift-btn');
    const sendingAnimationModal = document.getElementById('sending-animation-modal');
    const sendingModalBg = document.getElementById('sending-modal-bg');
    const sendingAnimation = document.getElementById('sending-animation');
    const sendingMessage = document.getElementById('sending-message');
    const chanceModal = document.getElementById('chance-modal');
    const chanceList = document.getElementById('chance-list');
    const closeChanceModal = document.getElementById('close-chance-modal');
    const userProfileModal = null; // removed - profiles use full-page navigation
    // Old modal user elements removed
    // Old modal stat elements removed — profiles use full-page navigation
    const modalGiftWall = document.getElementById('modal-gift-wall');
    const closeUserProfileModal = document.getElementById('close-user-profile-modal');
    const sendGiftChatModal = document.getElementById('send-gift-chat-modal');
    const sendGiftUserAvatar = document.getElementById('send-gift-user-avatar');
    const sendGiftUsername = document.getElementById('send-gift-username');
    const giftCardsList = document.getElementById('gift-cards-list');
    const closeSendGiftChatModal = document.getElementById('close-send-gift-chat-modal');
    const settingsPage = document.getElementById('settings-page');
    const currentAvatar = document.getElementById('current-avatar');
    const usernameInput = document.getElementById('username');
    const avatarUploadInput = document.getElementById('avatar-upload');
    const currentPassword = document.getElementById('current-password');
    const newPassword = document.getElementById('new-password');
    const confirmPassword = document.getElementById('confirm-password');
    const saveSettingsBtn = document.getElementById('save-settings-btn');
    const modalSendGiftBtn = document.getElementById('modal-send-gift-btn');
    const sendGiftInventoryModal = document.getElementById('send-gift-inventory-modal');
    const senderGiftCardsList = document.getElementById('sender-gift-cards-list');
    const cancelSendGiftInventoryBtn = document.getElementById('cancel-send-gift-inventory-btn');
    const missionsContainer = document.getElementById('missions-container');
    const profileAvatar = document.getElementById('profile-avatar');
    const profileUsername = document.getElementById('profile-username');
    const profileTitle = document.getElementById('profile-title');
    // profileEmail removed from profile page layout

    // Profile modal removed — profiles now use full-page navigation

    let pendingGame = null;
    let selectedFriend = null;
    let currentUser = null;
    let shopPacksUnsub = null;
    let shopBannersUnsub = null;
    let globalChatRenderedIds = new Set();
    let friendChatRenderedIds = new Set();
    let currentCard = null;
    let inventoryUnsubscribe = null;
    let giftWallUnsubscribe = null;
    let userUnsubscribe = null;
    let currentGiftCard = null;
    let sendingGiftCard = null;
    let selectedUserForGift = null;
    let selectedGiftForSend = null;
    let friendsData = [];

    // ========== Helper: show notification ==========
    function showNotification(message, type) {
      notificationText.textContent = message;
      notificationIcon.textContent = type === 'success' ? '✓' : '!';
      notificationIcon.className = `notification-icon ${type}`;
      notificationPopup.classList.add('show');
      setTimeout(() => notificationPopup.classList.remove('show'), 3000);
    }

    // ========== Save user profile ==========
    async function saveUserProfile(uid, email, username, avatarUrl) {
      const defaultAvatar = "https://t3.ftcdn.net/jpg/00/64/67/80/360_F_64678017_zUpiZFjj04cnLri7oADnyMH0XBYyQghG.jpg";
      const avatar = avatarUrl || defaultAvatar;
      let coins = 0;
      let title = "User";
      if (email === "chonhouliu@gmail.com") { coins = 999999; title = "Owner"; }
      const displayId = await ensureUniqueDisplayId();
      await setDoc(doc(db, "users", uid), {
        email,
        username: username || email.split('@')[0],
        avatar,
        coins,
        stars: 0,
        title,
        badges: [],
        displayId,
        ownedBannerIds: [],
        createdAt: serverTimestamp()
      }, { merge: true });
    }

    // ========== Load user profile with gradient title support ==========
    async function applyTitleStyleToElement(element, titleName) {
      if (!element) return;
      const name = titleName || 'User';
      element.textContent = name;
      element.classList.remove('gradient-title', 'running-gradient');
      element.style.background = '';
      element.style.backgroundSize = '';
      element.style.backgroundImage = '';
      element.style.webkitBackgroundClip = '';
      element.style.backgroundClip = '';
      element.style.webkitTextFillColor = '';
      element.style.color = '';
      element.style.animation = '';
      try {
        const titlesSnap = await getDocs(query(collection(db, 'titles'), where('name', '==', name)));
        if (!titlesSnap.empty) {
          const td = titlesSnap.docs[0].data();
          if (td.isGradient && td.gradientColors && td.gradientColors.length >= 2) {
            element.classList.add('gradient-title');
            const g = td.gradientColors.join(', ');
            element.style.background = `linear-gradient(135deg, ${g})`;
            element.style.webkitBackgroundClip = 'text';
            element.style.backgroundClip = 'text';
            element.style.webkitTextFillColor = 'transparent';
            element.style.color = 'transparent';
            if (td.isRunning) {
              element.classList.add('running-gradient');
              element.style.backgroundSize = '200% 200%';
            }
          } else if (td.color) {
            element.style.color = td.color;
            if (!element.classList.contains('up-friend-title-tag')) {
              element.style.background = `${td.color}20`;
            }
          }
        }
      } catch (e) {}
    }

    async function applyTitleStyle(element, titleName) {
      await applyTitleStyleToElement(element, titleName);
    }

    let starBadgeMetaCache = null;
    let starBadgeMetaCacheTime = 0;
    async function getStarBadgeMetaMap() {
      const now = Date.now();
      if (starBadgeMetaCache && now - starBadgeMetaCacheTime < 60000) return starBadgeMetaCache;
      const map = {};
      try {
        const snap = await getDocs(collection(db, 'badges'));
        snap.docs.forEach(d => {
          const x = d.data();
          if (x.name) map[x.name] = x;
        });
      } catch (e) {}
      starBadgeMetaCache = map;
      starBadgeMetaCacheTime = now;
      return map;
    }

    async function getStarBadgeRulesNameSet() {
      const rules = await getStarBadgeRulesFromServer();
      return new Set(rules.map(r => r.badgeName).filter(Boolean));
    }

    function pickStarSlotBadgeNames(badgeNames, max, ruleNameSet) {
      const arr = Array.isArray(badgeNames) ? badgeNames.filter(Boolean) : [];
      const ruleSet = ruleNameSet instanceof Set ? ruleNameSet : null;
      if (!ruleSet || !ruleSet.size) return [];
      const filtered = arr.filter(n => ruleSet.has(String(n).trim()));
      const uniq = [...new Set(filtered.map(n => String(n).trim()))];
      const cap = Math.max(0, Math.min(10, max || 1));
      return uniq.slice(0, cap);
    }

    async function renderStarSlotBadgesInto(wrapEl, fallbackEl, badgeNames, max) {
      if (!wrapEl) return;
      wrapEl.querySelectorAll('.star-slot-badge-img, .star-slot-badge-fa').forEach(n => n.remove());
      const ruleSet = await getStarBadgeRulesNameSet();
      const picks = pickStarSlotBadgeNames(badgeNames, max, ruleSet);
      if (!picks.length) {
        if (fallbackEl) fallbackEl.style.display = '';
        return;
      }
      if (fallbackEl) fallbackEl.style.display = 'none';
      const meta = await getStarBadgeMetaMap();
      picks.forEach(bn => {
        const bd = meta[bn];
        if (!bd) return;
        const icon = bd.icon || '';
        const isFa = /\bfa[srb]?\s+fa-/.test(icon) || /^fa[srb]?\s/.test(icon) || (icon.includes('fa-') && !/^https?:\/\//i.test(icon));
        if (isFa) {
          const safeFa = icon.split(/\s+/).filter(t => /^[a-zA-Z0-9_-]+$/.test(t)).join(' ');
          const iel = document.createElement('i');
          iel.className = `star-slot-badge-fa ${safeFa}`;
          iel.setAttribute('aria-hidden', 'true');
          iel.title = bn;
          iel.style.background = 'transparent';
          iel.style.color = bd.textColor || bd.bgColor || 'var(--neon-green)';
          wrapEl.appendChild(iel);
        } else if (/^https?:\/\//i.test(icon)) {
          const img = document.createElement('img');
          img.className = 'star-slot-badge-img';
          img.src = icon;
          img.alt = '';
          img.title = bn;
          wrapEl.appendChild(img);
        }
      });
      if (!wrapEl.querySelector('.star-slot-badge-img, .star-slot-badge-fa') && fallbackEl) fallbackEl.style.display = '';
    }

    async function refreshStarDisplayBadges(badgeNames) {
      await renderStarSlotBadgesInto(
        document.getElementById('sidebar-stars-icon-wrap'),
        document.getElementById('sidebar-stars-fallback-icon'),
        badgeNames,
        1
      );
      await renderStarSlotBadgesInto(
        document.getElementById('shop-stars-icon-wrap'),
        document.getElementById('shop-stars-fallback-icon'),
        badgeNames,
        1
      );
      await renderStarSlotBadgesInto(
        document.getElementById('profile-stars-icon-wrap'),
        document.getElementById('profile-stars-fallback-icon'),
        badgeNames,
        1
      );
      await renderStarSlotBadgesInto(
        document.getElementById('vp-stars-icon-wrap'),
        document.getElementById('vp-stars-fallback-icon'),
        badgeNames,
        1
      );
    }

    async function loadUserProfile(user) {
      if (!user) return;
      const userDoc = await getDoc(doc(db, "users", user.uid));
      if (userDoc.exists()) {
        const data = userDoc.data();
        userNameSpan.textContent = data.username || user.email;
        userAvatar.src = data.avatar || "https://t3.ftcdn.net/jpg/00/64/67/80/360_F_64678017_zUpiZFjj04cnLri7oADnyMH0XBYyQghG.jpg";
        profileAvatar.src = data.avatar || "https://t3.ftcdn.net/jpg/00/64/67/80/360_F_64678017_zUpiZFjj04cnLri7oADnyMH0XBYyQghG.jpg";
        profileUsername.textContent = data.username || user.email;
        applyTitleStyle(profileTitle, data.title || "User");
        renderProfileBadges(data.badges || []);
        refreshStarDisplayBadges(data.badges || []);
      } else {
        await saveUserProfile(user.uid, user.email, user.email.split('@')[0], null);
        await loadUserProfile(user);
        return;
      }
      userInfoDiv.style.display = 'flex';
      updateStats(user.uid);
    }

    async function renderProfileBadges(badgeNames, containerId) {
      const container = document.getElementById(containerId || 'profile-badges');
      if (!container) return;
      if (!badgeNames || badgeNames.length === 0) { container.innerHTML = ''; return; }
      try {
        const badgesSnap = await getDocs(collection(db, 'badges'));
        const allBadges = badgesSnap.docs.map(d => ({ id: d.id, ...d.data() }));
        container.innerHTML = '';
        badgeNames.forEach(bn => {
          const bd = allBadges.find(b => b.name === bn);
          if (!bd) return;
          const icon = bd.icon || '';
          const isFa = /\bfa[srb]?\s+fa-/.test(icon) || /^fa[srb]?\s/.test(icon) || (icon.includes('fa-') && !/^https?:\/\//i.test(icon));
          if (isFa) {
            const safeFa = icon.split(/\s+/).filter(t => /^[a-zA-Z0-9_-]+$/.test(t)).join(' ');
            const wrap = document.createElement('button');
            wrap.type = 'button';
            wrap.className = 'up-badge-fa';
            wrap.style.background = bd.bgColor || 'var(--neon-green)';
            wrap.style.color = bd.textColor || '#000';
            wrap.setAttribute('aria-label', bd.name || 'Badge');
            const iel = document.createElement('i');
            iel.className = safeFa;
            iel.setAttribute('aria-hidden', 'true');
            wrap.appendChild(iel);
            wrap.addEventListener('click', () => openBadgeDetailModal(bd));
            container.appendChild(wrap);
          } else {
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.className = 'up-badge-icon';
            btn.style.background = bd.bgColor || 'rgba(0,0,0,0.25)';
            btn.setAttribute('aria-label', bd.name || 'Badge');
            const img = document.createElement('img');
            img.src = /^https?:\/\//i.test(icon) ? icon : '';
            img.alt = '';
            if (!img.src) {
              btn.innerHTML = '<i class="fas fa-award" aria-hidden="true" style="font-size:0.95rem;color:#000;"></i>';
            } else btn.appendChild(img);
            btn.addEventListener('click', () => openBadgeDetailModal(bd));
            container.appendChild(btn);
          }
        });
      } catch(e) { container.innerHTML = ''; }
    }

    // ========== Update stats (plays, friends) ==========
    async function updateStats(uid) {
      const playsSnap = await getDocs(query(collection(db, "plays"), where("userId", "==", uid)));
      if (playCount) playCount.textContent = playsSnap.size;
      const friendsSnap = await getDocs(query(collection(db, "friends"), where("userId", "==", uid)));
      if (friendCount) friendCount.textContent = friendsSnap.size;
    }

    // ========== Auth state observer with owner enforcement ==========
    onAuthStateChanged(auth, async (user) => {
      currentUser = user;
      if (user) {
        const userDoc = await getDoc(doc(db, "users", user.uid));
        if (!userDoc.exists()) {
          await saveUserProfile(user.uid, user.email, user.email.split('@')[0], null);
        } else {
          const patch = {};
          const d0 = userDoc.data();
          if (!d0.displayId || String(d0.displayId).length !== 6) patch.displayId = await ensureUniqueDisplayId();
          if (!Array.isArray(d0.ownedBannerIds)) patch.ownedBannerIds = [];
          if (Object.keys(patch).length) await updateDoc(doc(db, "users", user.uid), patch);
          if (user.email === "chonhouliu@gmail.com") {
            await updateDoc(doc(db, "users", user.uid), { coins: 999999, title: "Owner" });
          }
        }
        const banCheck = await getDoc(doc(db, "users", user.uid));
        if (banCheck.exists()) {
          const bd = banCheck.data();
          if (bd.banStatus === 'perm' || (bd.banStatus === 'temp' && bd.banUntil && new Date(bd.banUntil.toDate ? bd.banUntil.toDate() : bd.banUntil) > new Date())) {
            const reason = bd.modReason || 'No reason provided';
            const until = bd.banStatus === 'perm' ? 'permanently' : `until ${new Date(bd.banUntil.toDate ? bd.banUntil.toDate() : bd.banUntil).toLocaleString()}`;
            alert(`Your account has been banned ${until}.\nReason: ${reason}`);
            await signOut(auth);
            return;
          }
          if (bd.banStatus === 'temp' && bd.banUntil && new Date(bd.banUntil.toDate ? bd.banUntil.toDate() : bd.banUntil) <= new Date()) {
            await updateDoc(doc(db, "users", user.uid), { banStatus: 'none', banUntil: null });
          }
        }
        await loadUserProfile(user);
        await ensureDefaultTitlesExist();
        await checkStaffAccess(user);
        if (pendingGame) { showConfirmModal(pendingGame); pendingGame = null; }
        sidebar.classList.add('active');
        mainContent.classList.add('sidebar-active');
        const activePage = GU_IS_LEGACY_SINGLE_PAGE
          ? (document.querySelector('.page.active')?.id || 'main-page')
          : (GU_DASHBOARD_PAGE_BY_PANEL[GU_DASHBOARD_PANEL] || 'main-page');
        switch (activePage) {
          case 'profile-page': loadProfilePage(user.uid); break;
          case 'history-page': loadPlayHistory(user.uid); break;
          case 'shop-page': loadShopPacks(); loadUserBalance(user.uid); break;
          case 'inventory-page': loadInventory(user.uid); loadFriends(user.uid); break;
          case 'missions-page': loadMissions(user.uid); break;
          case 'chat-page': loadGlobalChat(); break;
          case 'friends-page': loadFriends(user.uid); break;
          case 'settings-page': loadSettings(user.uid); break;
          case 'staff-page': loadStaffPanel(); break;
        }
      } else {
        currentUserPermissions = [];
        currentUserTitle = 'User';
        applyNonStaffMediaUi();
        userInfoDiv.style.display = 'none';
        sidebar.classList.remove('active');
        mainContent.classList.remove('sidebar-active');
        if (globalChatUnsubscribe) globalChatUnsubscribe();
        if (friendChatUnsubscribe) friendChatUnsubscribe();
        if (inventoryUnsubscribe) inventoryUnsubscribe();
        if (giftWallUnsubscribe) giftWallUnsubscribe();
        if (userUnsubscribe) userUnsubscribe();
        if (missionsListUnsub) { missionsListUnsub(); missionsListUnsub = null; }
        if (missionProgressUnsub) { missionProgressUnsub(); missionProgressUnsub = null; }
      }
      if (GU_DASHBOARD_PANEL === 'user') {
        initViewProfilePageFromUrl().catch(() => {});
      }
    });

    // ========== Sign up, login, logout (unchanged) ==========
    avatarUpload?.addEventListener('change', () => {
      const f = avatarUpload.files[0];
      if (f && avatarPreview) {
        avatarPreview.src = URL.createObjectURL(f);
        avatarPreview.style.display = 'block';
      }
    });

    document.getElementById('signupBtn')?.addEventListener('click', async () => {
      const username = document.getElementById('signupUsername').value.trim();
      const email = document.getElementById('signupEmail').value.trim();
      const password = document.getElementById('signupPassword').value;
      const urlField = document.getElementById('signupAvatarUrl');
      const urlAvatar = (canUserPasteImageLinks() && urlField) ? urlField.value.trim() : '';
      const avatarFile = avatarUpload.files[0];
      let avatarUrl = urlAvatar || null;
      if (avatarFile) {
        try {
          const storageRef = ref(storage, `avatars/${Date.now()}_${avatarFile.name}`);
          const snapshot = await uploadBytes(storageRef, avatarFile);
          avatarUrl = await getDownloadURL(snapshot.ref);
        } catch (error) { showNotification("Error uploading avatar: " + error.message, "error"); return; }
      }
      if (!username || !email || !password) { showNotification("Please fill all required fields", "error"); return; }
      try {
        const cred = await createUserWithEmailAndPassword(auth, email, password);
        await saveUserProfile(cred.user.uid, email, username, avatarUrl);
        signupModal.style.display = 'none';
        showNotification("Account created successfully!", "success");
      } catch (error) { showNotification(error.message, "error"); }
    });

    document.getElementById('loginBtn')?.addEventListener('click', async () => {
      const email = document.getElementById('loginEmail').value.trim();
      const password = document.getElementById('loginPassword').value;
      if (!email || !password) { showNotification("Enter email and password", "error"); return; }
      try {
        await signInWithEmailAndPassword(auth, email, password);
        loginModal.style.display = 'none';
        showNotification("Login successful!", "success");
      } catch (error) { showNotification(error.message, "error"); }
    });

    logoutBtn.addEventListener('click', async () => { await signOut(auth); showNotification("Logged out", "success"); });

    document.getElementById('showSignup')?.addEventListener('click', () => { loginModal.style.display = 'none'; signupModal.style.display = 'flex'; });
    document.getElementById('showLogin')?.addEventListener('click', () => { signupModal.style.display = 'none'; loginModal.style.display = 'flex'; });
    document.getElementById('closeLoginModal')?.addEventListener('click', () => { loginModal.style.display = 'none'; });
    document.getElementById('closeSignupModal')?.addEventListener('click', () => { signupModal.style.display = 'none'; });
    document.getElementById('noticeLoginBtn')?.addEventListener('click', () => { noticeModal.style.display = 'none'; loginModal.style.display = 'flex'; });
    document.getElementById('noticeSignupBtn')?.addEventListener('click', () => { noticeModal.style.display = 'none'; signupModal.style.display = 'flex'; });
    document.getElementById('noticeCancelBtn')?.addEventListener('click', () => { noticeModal.style.display = 'none'; pendingGame = null; });

    // ========== Game confirmation and play logging ==========
    function showConfirmModal(game) {
      document.getElementById('confirmGameName').textContent = `"${game.title}"?`;
      confirmModal.style.display = 'flex';
      const confirmYes = document.getElementById('confirmYes');
      const confirmNo = document.getElementById('confirmNo');
      confirmYes.onclick = async () => {
        confirmModal.style.display = 'none';
        await logGamePlay(game.id, game.title);
        openGameModal(game.title, game.url);
      };
      confirmNo.onclick = () => { confirmModal.style.display = 'none'; };
    }

    function openGameModal(title, url) {
      document.getElementById('modalGameTitle').textContent = title;
      document.getElementById('gameFrame').src = url;
      document.getElementById('gameModal').style.display = 'block';
      document.body.style.overflow = 'hidden';
    }

    async function logGamePlay(gameId, gameTitle) {
      if (!currentUser) return;
      try {
        const durationSec = 300;
        await addDoc(collection(db, "plays"), { gameId, gameTitle, userId: currentUser.uid, userEmail: currentUser.email, duration: durationSec, timestamp: serverTimestamp() });
        updateStats(currentUser.uid);
        updateMissionProgress('gametime', durationSec);
      } catch (error) { console.error("Error logging game play: ", error); }
    }

    async function handleGameClick(gameId, gameTitle, gameUrl) {
      if (!currentUser) { pendingGame = { id: gameId, title: gameTitle, url: gameUrl }; noticeModal.style.display = 'flex'; return; }
      showConfirmModal({ id: gameId, title: gameTitle, url: gameUrl });
    }

    // ========== Games rendering (unchanged) ==========
    async function getGames() {
      try {
        const gamesSnapshot = await getDocs(collection(db, "games"));
        const games = gamesSnapshot.docs.map(d => ({ id: d.id, ...d.data() }));
        const playCounts = {};
        const playsSnapshot = await getDocs(collection(db, "plays"));
        playsSnapshot.forEach(d => playCounts[d.data().gameId] = (playCounts[d.data().gameId] || 0) + 1);
        games.sort((a, b) => (playCounts[b.id]||0) - (playCounts[a.id]||0));
        const topGames = games.slice(0, 5).map((g, i)=>({...g, rank: i+1}));
        const newGames = games.slice(5, 12);
        const allGames = games;
        return { topGames, newGames, allGames };
      } catch(e) { console.error(e); return { topGames: [], newGames: [], allGames: [] }; }
    }

    function renderTopGamesCarousel(topGames) {
      const container = document.getElementById('topGamesCarousel');
      container.innerHTML = '';
      topGames.forEach((game, idx) => {
        const slide = document.createElement('div');
        slide.className = `carousel-slide ${idx===0?'active': ''}`;
        slide.innerHTML = `
          <div class="slide-background" style="background-image: url('${game.image}')"></div>
          <div class="slide-content">
            <div class="slide-info">
              <h2>${game.title}</h2>
              <div class="game-meta"><span><i class="fas fa-star"></i> ${game.rating||'N/A'}</span><span><i class="${game.multiplayer?'fas fa-users': 'fas fa-user'}"></i> ${game.multiplayer?'Multiplayer': 'Single Player'}</span><span><i class="fas fa-trophy"></i> TOP ${game.rank}</span></div>
              <p>${game.description||'No description available.'}</p>
              <button class="slide-play-button" data-url="${game.url}" data-id="${game.id}" data-title="${game.title}">PLAY NOW</button>
            </div>
          </div>
        `;
        container.appendChild(slide);
      });
      const nav = document.createElement('div'); nav.className='carousel-nav';
      topGames.forEach((_, i)=>{ const dot=document.createElement('div'); dot.className=`carousel-dot ${i===0?'active': ''}`; dot.addEventListener('click', ()=>showSlide(i)); nav.appendChild(dot); });
      container.appendChild(nav);
      document.querySelectorAll('.slide-play-button').forEach(btn => btn.addEventListener('click', (e) => { e.preventDefault(); handleGameClick(btn.dataset.id, btn.dataset.title, btn.dataset.url); }));
      startCarousel();
    }

    let currentSlide=0, interval;
    function showSlide(i){ const slides=document.querySelectorAll('.carousel-slide'), dots=document.querySelectorAll('.carousel-dot'); if(i>=slides.length)i=0; if(i<0)i=slides.length-1; slides.forEach(s=>s.classList.remove('active')); dots.forEach(d=>d.classList.remove('active')); slides[i].classList.add('active'); dots[i].classList.add('active'); currentSlide=i; }
    function nextSlide(){ showSlide(currentSlide+1); }
    function startCarousel(){ clearInterval(interval); interval=setInterval(nextSlide, 5000); }

    function createGameCard(game) {
      const card = document.createElement('div');
      card.className = 'game-card';
      card.style.backgroundImage = `url('${game.image}')`;
      card.dataset.id = game.id;
      card.dataset.title = game.title;
      card.dataset.url = game.url;
      const rating = game.rating || 3;
      const starsHtml = [1, 2, 3, 4, 5].map(s => `<i class="fas fa-star star ${s <= Math.floor(rating) ? 'filled' : ''}"></i>`).join('');
      card.innerHTML = `<div class="card-overlay"><div class="game-name">${game.title}</div><div class="card-meta"><i class="${game.multiplayer ? 'fas fa-users mode-multi' : 'fas fa-user mode-single'}"></i><div class="card-rating"><div class="stars">${starsHtml}</div><span class="rating-value">${rating.toFixed(1)}</span></div></div></div>`;
      card.addEventListener('click', () => handleGameClick(game.id, game.title, game.url));
      return card;
    }

    function createCategoryRow(title, games, container) {
      if(!games.length) return;
      const limited = games.slice(0, 7);
      const row = document.createElement('div');
      row.className = 'category-row';
      row.innerHTML = `<h2 class="category-title">${title}</h2><div class="carousel-track-container"><div class="carousel-track"></div><div class="track-nav prev"><i class="fas fa-chevron-left"></i></div><div class="track-nav next"><i class="fas fa-chevron-right"></i></div></div>`;
      const track = row.querySelector('.carousel-track');
      limited.forEach(game => track.appendChild(createGameCard(game)));
      container.appendChild(row);
      const prev = row.querySelector('.prev'), next = row.querySelector('.next');
      prev.addEventListener('click', () => track.scrollBy({ left: -260, behavior: 'smooth' }));
      next.addEventListener('click', () => track.scrollBy({ left: 260, behavior: 'smooth' }));
      let isDown=false, startX, scrollLeft;
      track.addEventListener('mousedown', (e)=>{ isDown=true; track.style.cursor='grabbing'; startX=e.pageX-track.offsetLeft; scrollLeft=track.scrollLeft; });
      track.addEventListener('mouseleave', ()=>{ isDown=false; track.style.cursor='grab'; });
      track.addEventListener('mouseup', ()=>{ isDown=false; track.style.cursor='grab'; });
      track.addEventListener('mousemove', (e)=>{ if(!isDown) return; e.preventDefault(); const x=e.pageX-track.offsetLeft; const walk=(x-startX)*1.5; track.scrollLeft=scrollLeft-walk; });
      track.addEventListener('touchstart', (e)=>{ isDown=true; startX=e.touches[0].pageX-track.offsetLeft; scrollLeft=track.scrollLeft; });
      track.addEventListener('touchend', ()=>{ isDown=false; });
      track.addEventListener('touchmove', (e)=>{ if(!isDown) return; const x=e.touches[0].pageX-track.offsetLeft; const walk=(x-startX)*1.5; track.scrollLeft=scrollLeft-walk; });
    }

    function renderCategoryBrowsing(gamesData) {
      const container = document.getElementById('categoryBrowsingSection');
      container.innerHTML = '';
      createCategoryRow('🔥 TOP', gamesData.topGames, container);
      createCategoryRow('🆕 NEW', gamesData.newGames, container);
      const tagGroups = {};
      gamesData.allGames.forEach(g => { if(g.tags) g.tags.forEach(t=>{ if(!tagGroups[t]) tagGroups[t]=[]; tagGroups[t].push(g); }); });
      Object.keys(tagGroups).sort().forEach(tag => { createCategoryRow(tag.toUpperCase(), tagGroups[tag], container); });
    }

    function renderFullGamesList(allGames) {
      const grid = document.getElementById('fullGamesGrid');
      const searchInput = document.getElementById('gameSearchInput');
      const sortedGames = [...allGames].sort((a, b) => a.title.localeCompare(b.title));
      const filterAndRender = () => {
        const query = searchInput.value.toLowerCase().trim();
        const filtered = sortedGames.filter(g => g.title.toLowerCase().includes(query));
        grid.innerHTML = filtered.map(game => `
          <div class="full-game-item" data-id="${game.id}" data-url="${game.url}" data-title="${game.title}">
            <div class="full-game-banner" style="background-image: url('${game.image}')"></div>
            <div class="full-game-info">
              <div class="full-game-title">${game.title}</div>
              <div class="full-game-meta"><span><i class="fas fa-star"></i> ${game.rating||'N/A'}</span><span><i class="${game.multiplayer?'fas fa-users': 'fas fa-user'}"></i> ${game.multiplayer?'Multiplayer': 'Single Player'}</span></div>
            </div>
          </div>
        `).join('');
        document.querySelectorAll('.full-game-item').forEach(el => { el.addEventListener('click', () => handleGameClick(el.dataset.id, el.dataset.title, el.dataset.url)); });
      };
      filterAndRender();
      searchInput.addEventListener('input', filterAndRender);
    }

    // ========== Tab switching (home/contact) ==========
    const homeTab = document.getElementById('home-tab');
    const contactTab = document.getElementById('contact-tab');
    const categorySection = document.getElementById('categoryBrowsingSection');
    const fullGamesListSec = document.getElementById('fullGamesList');
    const contactSectionEl = document.getElementById('contact-section');
    homeTab?.addEventListener('click', (e) => {
      e.preventDefault();
      if (categorySection) categorySection.style.display = 'block';
      if (fullGamesListSec) fullGamesListSec.style.display = 'block';
      contactSectionEl?.classList.remove('active');
      homeTab.classList.add('active');
      contactTab?.classList.remove('active');
    });
    contactTab?.addEventListener('click', (e) => {
      e.preventDefault();
      if (categorySection) categorySection.style.display = 'none';
      if (fullGamesListSec) fullGamesListSec.style.display = 'none';
      contactSectionEl?.classList.add('active');
      contactTab.classList.add('active');
      homeTab?.classList.remove('active');
    });
    document.getElementById('suggest-tab')?.addEventListener('click', () => {
      document.getElementById('suggest-form-container').style.display = 'block';
      document.getElementById('report-form-container').style.display = 'none';
    });
    document.getElementById('report-tab')?.addEventListener('click', () => {
      document.getElementById('suggest-form-container').style.display = 'none';
      document.getElementById('report-form-container').style.display = 'block';
    });

    document.getElementById('suggestGameForm')?.addEventListener('submit', async (e) => {
      e.preventDefault();
      const msgEl = document.getElementById('suggestMessage');
      const title = document.getElementById('gameTitle')?.value.trim();
      let bannerUrl = canUserPasteImageLinks() ? (document.getElementById('gameBanner')?.value.trim() || '') : '';
      const link = document.getElementById('gameLink')?.value.trim();
      const file = document.getElementById('gameBannerFile')?.files?.[0];
      if (!title || !link) {
        if (msgEl) { msgEl.textContent = 'Please fill title and game link.'; msgEl.style.color = '#FF3D6C'; }
        return;
      }
      if (file) {
        try {
          const storageRef = ref(storage, `suggestions/banners/${Date.now()}_${file.name}`);
          const snapshot = await uploadBytes(storageRef, file);
          bannerUrl = await getDownloadURL(snapshot.ref);
        } catch (err) {
          if (msgEl) { msgEl.textContent = 'Banner upload failed: ' + err.message; msgEl.style.color = '#FF3D6C'; }
          return;
        }
      }
      if (!bannerUrl) {
        if (msgEl) { msgEl.textContent = 'Upload a banner image for your suggestion.'; msgEl.style.color = '#FF3D6C'; }
        return;
      }
      try {
        await addDoc(collection(db, 'gameSuggestions'), {
          suggesterName: document.getElementById('suggestName')?.value.trim(),
          suggesterEmail: document.getElementById('suggestEmail')?.value.trim(),
          gameTitle: title,
          gameBanner: bannerUrl,
          gameLink: link,
          gameDescription: document.getElementById('gameDescription')?.value.trim(),
          createdAt: serverTimestamp()
        });
        if (msgEl) { msgEl.textContent = 'Thanks! Your suggestion was sent.'; msgEl.style.color = 'var(--neon-green)'; }
        e.target.reset();
      } catch (err) {
        if (msgEl) { msgEl.textContent = 'Could not submit: ' + err.message; msgEl.style.color = '#FF3D6C'; }
      }
    });

    // ========== Sidebar navigation ==========
    document.querySelector('.sidebar')?.addEventListener('click', (ev) => {
      const button = ev.target.closest('.tab-button');
      if (!button || !document.getElementById('sidebar')?.contains(button)) return;
      const pageId = button.getAttribute('data-page');
      if (!pageId) return;
      if (!currentUser && pageId !== 'main-page') {
        ev.preventDefault();
        noticeModal.style.display = 'flex';
        return;
      }
      if (!GU_IS_LEGACY_SINGLE_PAGE) return;
      ev.preventDefault();
      document.querySelectorAll('.tab-button').forEach(btn => btn.classList.remove('active'));
      document.querySelectorAll('.page').forEach(page => page.classList.remove('active'));
      button.classList.add('active');
      const pageEl = document.getElementById(pageId);
      if (pageEl) pageEl.classList.add('active');
      switch (pageId) {
        case 'profile-page': loadProfilePage(currentUser.uid); break;
        case 'history-page': loadPlayHistory(currentUser.uid); break;
        case 'shop-page': loadShopPacks(); loadUserBalance(currentUser.uid); break;
        case 'inventory-page': loadInventory(currentUser.uid); loadFriends(currentUser.uid); break;
        case 'missions-page': loadMissions(currentUser.uid); break;
        case 'chat-page': loadGlobalChat(); break;
        case 'friends-page': loadFriends(currentUser.uid); break;
        case 'settings-page': loadSettings(currentUser.uid); break;
        case 'staff-page': loadStaffPanel(); break;
      }
    });

    // ========== Load profile page ==========
    async function loadProfilePage(userId) {
      try {
        await populateProfileLayout(userId, {
          avatar: 'profile-avatar', banner: 'profile-banner', bannerWrap: 'profile-banner-wrap',
          username: 'profile-username', title: 'profile-title',
          badges: 'profile-badges',
          starsVal: 'profile-stars-val', playtimeVal: 'profile-playtime-val',
          playCount: 'play-count', friendCount: 'friend-count',
          blookCount: 'profile-blook-count', blookTotal: 'profile-blook-total',
          blookProgress: 'profile-blook-progress',
          topBlooks: 'profile-top-blooks', friendsList: 'profile-friends-list',
        });
        setupOwnProfileBannerPicker(userId);
      } catch(e) { console.error(e); }
    }

    async function populateProfileLayout(userId, ids) {
      const defaultAvatar = "https://t3.ftcdn.net/jpg/00/64/67/80/360_F_64678017_zUpiZFjj04cnLri7oADnyMH0XBYyQghG.jpg";
      const userDoc = await getDoc(doc(db, "users", userId));
      if (!userDoc.exists()) return;
      const data = userDoc.data();

      document.getElementById(ids.avatar).src = data.avatar || defaultAvatar;
      if (ids.bannerWrap) {
        const bannerWrap = document.getElementById(ids.bannerWrap);
        const bannerImg = document.getElementById(ids.banner);
        if (data.banner) {
          bannerWrap.classList.add('has-custom-banner');
          bannerImg.src = data.banner;
        } else {
          bannerWrap.classList.remove('has-custom-banner');
          bannerImg.src = '';
        }
      }
      document.getElementById(ids.username).textContent = data.username || data.email;
      applyTitleStyle(document.getElementById(ids.title), data.title || "User");
      renderProfileBadges(data.badges || [], ids.badges);
      refreshStarDisplayBadges(data.badges || []);
      document.getElementById(ids.starsVal).textContent = data.stars || 0;

      const playsSnap = await getDocs(query(collection(db, "plays"), where("userId", "==", userId)));
      document.getElementById(ids.playCount).textContent = playsSnap.size;
      let totalSec = 0;
      playsSnap.forEach(d => { totalSec += d.data().duration || 300; });
      const hours = Math.floor(totalSec / 3600);
      const mins = Math.floor((totalSec % 3600) / 60);
      document.getElementById(ids.playtimeVal).textContent = `${hours}h ${mins}m`;

      const friendsSnap = await getDocs(query(collection(db, "friends"), where("userId", "==", userId)));
      document.getElementById(ids.friendCount).textContent = friendsSnap.size;

      const blooksSnap = await getDocs(query(collection(db, "inventory"), where("userId", "==", userId)));
      const ownedNames = new Set(blooksSnap.docs.map(d => d.data().itemName));
      const blookUnique = ownedNames.size;
      const blookCopies = blooksSnap.size;
      document.getElementById(ids.blookCount).textContent = blookCopies;
      document.getElementById(ids.blookTotal).textContent = blookUnique;
      const blookBarPct = Math.min(100, blookUnique * (100 / 45));
      document.getElementById(ids.blookProgress).style.width = `${blookBarPct}%`;

      const top5 = blooksSnap.docs.sort((a, b) => (b.data().starsGained || 0) - (a.data().starsGained || 0)).slice(0, 5);
      document.getElementById(ids.topBlooks).innerHTML = top5.length ? top5.map(d => {
        const bl = d.data();
        return `<div class="up-blook-card"><div class="up-blook-img"><img src="${bl.imageUrl || 'https://via.placeholder.com/52'}" alt="${bl.itemName}"></div><div class="up-blook-name">${bl.itemName}</div><div class="up-blook-rarity">${bl.rarity}</div></div>`;
      }).join('') : '<div style="color:var(--text-secondary);font-size:0.8rem;">No blooks yet</div>';

      const friendIds = friendsSnap.docs.map(d => d.data().friendId);
      const friendsContainer = document.getElementById(ids.friendsList);
      if (friendIds.length === 0) {
        friendsContainer.innerHTML = '<div style="color:var(--text-secondary);font-size:0.8rem;">No friends yet</div>';
      } else {
        const batches = [];
        for (let i = 0; i < friendIds.length; i += 10) {
          batches.push(friendIds.slice(i, i + 10));
        }
        let allFriends = [];
        for (const batch of batches) {
          const snap = await getDocs(query(collection(db, "users"), where("__name__", "in", batch)));
          snap.docs.forEach(d => allFriends.push({ id: d.id, ...d.data() }));
        }
        friendsContainer.innerHTML = '';
        allFriends.forEach(f => {
          const row = document.createElement('div');
          row.className = 'up-friend-row';
          row.dataset.uid = f.id;
          const av = document.createElement('div');
          av.className = 'up-friend-avatar';
          const avImg = document.createElement('img');
          avImg.src = f.avatar || defaultAvatar;
          avImg.alt = f.username || '';
          av.appendChild(avImg);
          const ban = document.createElement('div');
          ban.className = `up-friend-banner${f.banner ? ' has-custom-banner' : ''}`;
          const pat = document.createElement('div');
          pat.className = 'up-friend-pattern';
          ban.appendChild(pat);
          if (f.banner) {
            const bg = document.createElement('img');
            bg.className = 'up-friend-banner-bg';
            bg.src = f.banner;
            bg.alt = '';
            ban.appendChild(bg);
          }
          const textWrap = document.createElement('div');
          textWrap.className = 'up-friend-banner-text';
          const nameSp = document.createElement('span');
          nameSp.className = 'up-friend-name';
          nameSp.textContent = f.username || f.email || '';
          const titleSp = document.createElement('span');
          titleSp.className = 'up-friend-title-tag';
          textWrap.appendChild(nameSp);
          textWrap.appendChild(titleSp);
          ban.appendChild(textWrap);
          row.appendChild(av);
          row.appendChild(ban);
          friendsContainer.appendChild(row);
          applyTitleStyleToElement(titleSp, f.title || 'User');
          row.addEventListener('click', () => navigateToUserProfile(f.id));
        });
      }
    }

    let bannerPickerSetupUid = null;
    function setupOwnProfileBannerPicker(userId) {
      if (!currentUser || userId !== currentUser.uid) return;
      const wrap = document.getElementById('profile-banner-wrap');
      if (!wrap) return;
      wrap.classList.add('profile-banner-clickable');
      if (bannerPickerSetupUid === userId) return;
      bannerPickerSetupUid = userId;
      wrap.addEventListener('click', () => openBannerPickerModal(userId));
    }

    async function openBannerPickerModal(userId) {
      const modal = document.getElementById('banner-picker-modal');
      const grid = document.getElementById('banner-picker-grid');
      if (!modal || !grid) return;
      const u = await getDoc(doc(db, 'users', userId));
      if (!u.exists()) return;
      const d = u.data();
      const ownedIds = d.ownedBannerIds || [];
      const packsSnap = await getDocs(collection(db, 'packs'));
      const byId = {};
      packsSnap.docs.forEach(docu => { if (docu.data().shopType === 'banner') byId[docu.id] = { id: docu.id, ...docu.data() }; });
      grid.innerHTML = '';
      const def = document.createElement('div');
      def.className = 'banner-picker-item default-pattern';
      def.dataset.banner = '';
      def.textContent = 'Default pattern';
      grid.appendChild(def);
      ownedIds.forEach(pid => {
        const p = byId[pid];
        if (!p) return;
        const url = p.bannerImageUrl || p.backgroundImage || '';
        if (!url) return;
        const el = document.createElement('div');
        el.className = 'banner-picker-item';
        el.dataset.banner = url;
        el.innerHTML = `<img src="${escapeHtml(url)}" alt="">`;
        grid.appendChild(el);
      });
      const curBanner = d.banner || '';
      grid.querySelectorAll('.banner-picker-item').forEach(x => {
        x.classList.toggle('selected', (x.dataset.banner || '') === curBanner);
      });
      grid.querySelectorAll('.banner-picker-item').forEach(el => {
        el.addEventListener('click', async () => {
          const url = el.dataset.banner || '';
          const ref = doc(db, 'users', userId);
          if (url) await updateDoc(ref, { banner: url });
          else await updateDoc(ref, { banner: null });
          grid.querySelectorAll('.banner-picker-item').forEach(x => x.classList.remove('selected'));
          el.classList.add('selected');
          await loadProfilePage(userId);
          showNotification(url ? 'Banner updated' : 'Using default banner', 'success');
        });
      });
      modal.style.display = 'flex';
    }

    document.getElementById('banner-picker-close')?.addEventListener('click', () => {
      const m = document.getElementById('banner-picker-modal');
      if (m) m.style.display = 'none';
    });
    document.getElementById('banner-picker-modal')?.addEventListener('click', (e) => {
      if (e.target.id === 'banner-picker-modal') e.target.style.display = 'none';
    });

    // ========== Load play history with simple timestamp ==========
    async function loadPlayHistory(userId) {
      try {
        const playsQuery = query(collection(db, "plays"), where("userId", "==", userId), orderBy("timestamp", "desc"), limit(10));
        const unsubscribe = onSnapshot(playsQuery, (snapshot) => {
          playHistoryList.innerHTML = '';
          if (snapshot.empty) { playHistoryList.innerHTML = '<p>No play history yet</p>'; return; }
          snapshot.forEach(doc => {
            const data = doc.data();
            const timestamp = data.timestamp?.toDate ? data.timestamp.toDate() : new Date(data.timestamp);
            const formattedTime = timestamp.toLocaleString(); // simple local date+time
            const historyItem = document.createElement('div');
            historyItem.className = 'history-item';
            historyItem.innerHTML = `<img src="${data.gameImage || 'https://placehold.co/60x40'}" alt="${data.gameTitle}"><div class="history-info"><div class="history-title">${data.gameTitle}</div><div class="history-time">${formattedTime}</div></div>`;
            playHistoryList.appendChild(historyItem);
          });
        });
      } catch(e) { console.error(e); }
    }

    // ========== Shop packs & banners ==========
    async function loadShopPacks() {
      try {
        if (shopPacksUnsub) { shopPacksUnsub(); shopPacksUnsub = null; }
        const packsQuery = query(collection(db, "packs"), orderBy("createdAt", "desc"));
        shopPacksUnsub = onSnapshot(packsQuery, (snapshot) => {
          shopPacks.innerHTML = '';
          if (snapshot.empty) { shopPacks.innerHTML = '<p style="color:var(--text-secondary);">No packs available</p>'; return; }
          let anyPack = false;
          snapshot.forEach(docSnap => {
            const pack = { id: docSnap.id, ...docSnap.data() };
            if (pack.shopType === 'banner') return;
            anyPack = true;
            const card = buildMarketPackCardEl({
              name: pack.name,
              price: pack.price,
              pack,
              artUrl: marketPackArtUrl(pack),
              patternUrl: marketPackPatternUrl(pack),
              onCardClick: () => purchasePack(pack),
              onInfoClick: () => showChanceModal(pack)
            });
            shopPacks.appendChild(card);
          });
          if (!anyPack) shopPacks.innerHTML = '<p style="color:var(--text-secondary);">No packs available</p>';
        });
        loadShopBannerOffers();
      } catch(e) { console.error(e); }
    }

    function buildShopBannerRowCard(bannerPack) {
      const wrap = document.createElement('div');
      wrap.className = 'shop-banner-card';
      const imgUrl = (bannerPack.bannerImageUrl || bannerPack.backgroundImage || '').trim();
      const vis = document.createElement('div');
      vis.className = 'shop-banner-card-visual';
      if (imgUrl) {
        const im = document.createElement('img');
        im.src = imgUrl;
        im.alt = bannerPack.name || 'Banner';
        im.loading = 'lazy';
        vis.appendChild(im);
      } else {
        vis.innerHTML = '<i class="fas fa-image" style="font-size:1.6rem;color:rgba(255,255,255,0.35);"></i>';
      }
      const foot = document.createElement('div');
      foot.className = 'shop-banner-card-foot';
      foot.innerHTML = `<div class="shop-banner-card-name">${escapeHtml(bannerPack.name || 'Banner')}</div><div class="shop-banner-card-price"><i class="fas fa-coins"></i> ${escapeHtml(String(bannerPack.price ?? 0))}</div>`;
      wrap.appendChild(vis);
      wrap.appendChild(foot);
      wrap.addEventListener('click', () => purchaseBannerOffer(bannerPack));
      return wrap;
    }

    async function loadShopBannerOffers() {
      if (shopBannersUnsub) { shopBannersUnsub(); shopBannersUnsub = null; }
      if (!shopBanners) return;
      const packsQuery = query(collection(db, "packs"), orderBy("createdAt", "desc"));
      shopBannersUnsub = onSnapshot(packsQuery, (snapshot) => {
        shopBanners.innerHTML = '';
        const banners = [];
        snapshot.forEach(d => { const p = { id: d.id, ...d.data() }; if (p.shopType === 'banner') banners.push(p); });
        if (banners.length === 0) {
          shopBanners.innerHTML = '<p style="color:var(--text-secondary);grid-column:1/-1;">No banners in the shop yet</p>';
          return;
        }
        banners.forEach(b => shopBanners.appendChild(buildShopBannerRowCard(b)));
      });
    }

    async function purchaseBannerOffer(bannerPack) {
      if (!currentUser) return;
      try {
        const userDoc = await getDoc(doc(db, "users", currentUser.uid));
        if (!userDoc.exists()) return;
        const userData = userDoc.data();
        const owned = userData.ownedBannerIds || [];
        if (owned.includes(bannerPack.id)) { showNotification('You already own this banner', 'error'); return; }
        if ((userData.coins || 0) < bannerPack.price) { showNotification('Not enough coins!', 'error'); return; }
        const url = bannerPack.bannerImageUrl || bannerPack.backgroundImage || '';
        await updateDoc(doc(db, "users", currentUser.uid), {
          coins: userData.coins - bannerPack.price,
          ownedBannerIds: arrayUnion(bannerPack.id)
        });
        updateMissionProgress('spending', bannerPack.price);
        loadUserBalance(currentUser.uid);
        showNotification('Banner unlocked! Equip it from your profile banner.', 'success');
      } catch (e) { showNotification('Error: ' + e.message, 'error'); }
    }

    function showChanceModal(pack) {
      chanceList.innerHTML = '';
      if (pack.items && pack.items.length > 0) {
        pack.items.forEach(item => {
          const chanceItem = document.createElement('div');
          chanceItem.className = 'chance-item';
          chanceItem.innerHTML = `<div class="chance-name">${item.name}</div><div class="chance-percent">${item.chance}%</div>`;
          chanceList.appendChild(chanceItem);
        });
      } else { chanceList.innerHTML = '<div class="chance-item">No items in this pack</div>'; }
      chanceModal.style.display = 'flex';
    }
    closeChanceModal.addEventListener('click', () => { chanceModal.style.display = 'none'; });

    // ========== Purchase pack and award stars immediately ==========
    async function purchasePack(pack) {
      if (!currentUser) return;
      try {
        const userDoc = await getDoc(doc(db, "users", currentUser.uid));
        if (!userDoc.exists()) { showNotification("User data not found", "error"); return; }
        const userData = userDoc.data();
        if (userData.coins < pack.price) { showNotification("Not enough coins!", "error"); return; }
        await updateDoc(doc(db, "users", currentUser.uid), { coins: userData.coins - pack.price });
        updateMissionProgress('spending', pack.price);
        loadUserBalance(currentUser.uid);
        openPack(pack, currentUser.uid);
      } catch(e) { showNotification("Error purchasing pack: " + e.message, "error"); }
    }

    // Get item based on weighted chance
    function getItemFromPack(pack) {
      const totalChance = pack.items.reduce((sum, item) => sum + item.chance, 0);
      let random = Math.random() * totalChance;
      
      for (const item of pack.items) {
        if (random < item.chance) {
          return item;
        }
        random -= item.chance;
      }
      
      // Fallback to first item
      return pack.items[0];
    }

    // Get stars based on rarity
    function getStarsForRarity(rarity) {
      const starValues = {
        'common': 5,   
        'uncommon': 15,   
        'rare': 25,   
        'epic': 75,   
        'legendary': 150,   
        'chroma': 300,   
        'mythical': 1000
      };
      return starValues[rarity] || 0;
    }

    async function openPack(pack, userId) {
      const item = getItemFromPack(pack);
      const starsGained = typeof item.starsGained === 'number' && item.starsGained >= 0
        ? item.starsGained
        : getStarsForRarity(item.rarity);
      
      // Award stars immediately to the user
      await updateDoc(doc(db, "users", userId), { stars: increment(starsGained) });
      const uAfter = await getDoc(doc(db, 'users', userId));
      const newStars = uAfter.exists() ? (uAfter.data().stars || 0) : 0;
      syncStarBadgesForUser(userId, newStars);

      currentGiftCard = { 
        ...item,    
        packId: pack.id,    
        packName: pack.name,    
        starsGained: starsGained 
      };
      
      showPackOpeningModal(pack, currentGiftCard);
    }

    function getRarityDef(r) {
      const key = (r || 'common').toLowerCase();
      return blookRarityDefs[key] || DEFAULT_RARITY_DEFS[key] || { solid: '#ffffff', isGradient: false, gradientColors: [], isRunning: false };
    }
    function getRarityColor(rarity) {
      const d = getRarityDef(rarity);
      return d.solid || '#ffffff';
    }
    function getRarityColorCss(rarity) {
      const d = getRarityDef(rarity);
      if (d.isGradient && d.gradientColors && d.gradientColors.length >= 2) {
        const g = d.gradientColors.join(', ');
        const base = `linear-gradient(135deg, ${g})`;
        if (d.isRunning) {
          return `${base} 0% 50% / 200% 200%; -webkit-background-clip: text; background-clip: text; -webkit-text-fill-color: transparent; color: transparent; animation: runGradient 3s linear infinite`;
        }
        return `${base}; -webkit-background-clip: text; background-clip: text; -webkit-text-fill-color: transparent; color: transparent`;
      }
      return d.solid || '#ffffff';
    }

    /** Full-area modal background from rarity (not text-clipped). */
    function getRarityBackgroundGradientCss(rarity) {
      const d = getRarityDef(rarity);
      if (d.isGradient && d.gradientColors && d.gradientColors.length >= 2) {
        const g = d.gradientColors.join(', ');
        return {
          image: `linear-gradient(145deg, ${g}, #080c14 78%, #030509 100%)`,
          size: d.isRunning ? '220% 220%' : '100% 100%',
          repeat: 'no-repeat',
          position: d.isRunning ? '0% 50%' : 'center',
          animated: !!d.isRunning
        };
      }
      const c = d.solid || '#1a2233';
      return {
        image: `linear-gradient(165deg, ${c} 0%, #0d1218 45%, #030509 100%)`,
        size: '100% 100%',
        repeat: 'no-repeat',
        position: 'center',
        animated: false
      };
    }

    let packConfettiInterval = null;
    let packConfettiStopTimer = null;
    let packAutoUnboxTimer = null;

    function hexToRgbTriplet(hex) {
      const s = String(hex || '').trim();
      if (!s.startsWith('#')) return null;
      if (s.length === 4) {
        const r = parseInt(s[1] + s[1], 16), g = parseInt(s[2] + s[2], 16), b = parseInt(s[3] + s[3], 16);
        return Number.isFinite(r) ? [r, g, b] : null;
      }
      if (s.length === 7) {
        const r = parseInt(s.slice(1, 3), 16), g = parseInt(s.slice(3, 5), 16), b = parseInt(s.slice(5, 7), 16);
        return Number.isFinite(r) ? [r, g, b] : null;
      }
      return null;
    }
    function mixRgb(a, b, t) {
      return [
        Math.round(a[0] + (b[0] - a[0]) * t),
        Math.round(a[1] + (b[1] - a[1]) * t),
        Math.round(a[2] + (b[2] - a[2]) * t)
      ];
    }
    function rgbToHex(rgb) {
      return '#' + rgb.map(x => Math.max(0, Math.min(255, x)).toString(16).padStart(2, '0')).join('');
    }

    /** Deep → light palette for confetti from rarity definition. */
    function getRarityConfettiColors(rarity) {
      const d = getRarityDef(rarity);
      let deep;
      let light;
      if (d.isGradient && d.gradientColors && d.gradientColors.length >= 2) {
        const parsed = d.gradientColors.map(c => hexToRgbTriplet(c)).filter(Boolean);
        if (parsed.length >= 2) {
          deep = parsed[0];
          light = parsed[parsed.length - 1];
        }
      }
      if (!deep || !light) {
        const base = hexToRgbTriplet(d.solid) || [140, 150, 165];
        deep = base.map(c => Math.max(0, Math.floor(c * 0.28)));
        light = base.map(c => Math.min(255, Math.floor(c + (255 - c) * 0.5)));
      }
      const palette = [];
      for (let i = 0; i <= 7; i++) {
        palette.push(rgbToHex(mixRgb(deep, light, i / 7)));
      }
      palette.push('rgba(255,255,255,0.92)');
      return palette;
    }

    function stopPackConfetti() {
      if (packConfettiInterval) {
        clearInterval(packConfettiInterval);
        packConfettiInterval = null;
      }
      if (packConfettiStopTimer) {
        clearTimeout(packConfettiStopTimer);
        packConfettiStopTimer = null;
      }
      if (packConfettiLayer) {
        packConfettiLayer.innerHTML = '';
        packConfettiLayer.style.display = 'none';
      }
    }

    function spawnPackConfettiBurst(n, colorPalette) {
      if (!packConfettiLayer) return;
      const colors = Array.isArray(colorPalette) && colorPalette.length
        ? colorPalette
        : ['#2AFF9E', '#FF3D6C', '#FFC107', '#42A5F5', '#AB47BC', '#ffffff'];
      const count = Math.max(1, Math.floor(n));
      for (let i = 0; i < count; i++) {
        const el = document.createElement('div');
        el.className = 'pack-confetti-piece';
        el.style.left = `${Math.random() * 100}%`;
        el.style.background = colors[Math.floor(Math.random() * colors.length)];
        el.style.width = `${6 + Math.random() * 10}px`;
        el.style.height = `${6 + Math.random() * 10}px`;
        el.style.borderRadius = Math.random() > 0.45 ? '50%' : '2px';
        const dur = 2.2 + Math.random() * 2.2;
        el.style.setProperty('--cf-dx', `${(Math.random() - 0.5) * 160}px`);
        el.style.setProperty('--cf-rot', `${420 + Math.random() * 540}deg`);
        el.style.animation = `packConfettiFall ${dur}s linear forwards`;
        packConfettiLayer.appendChild(el);
        setTimeout(() => { try { el.remove(); } catch (e) {} }, dur * 1000 + 120);
      }
    }

    function startPackConfetti(durationMs, colorPalette) {
      stopPackConfetti();
      if (packConfettiLayer) packConfettiLayer.style.display = 'block';
      spawnPackConfettiBurst(32, colorPalette);
      packConfettiInterval = setInterval(() => spawnPackConfettiBurst(18, colorPalette), 360);
      packConfettiStopTimer = setTimeout(() => stopPackConfetti(), durationMs || 15000);
    }

    function ymdUTC(d) {
      return d.toISOString().slice(0, 10);
    }

    function normalizeStaffRarityDef(raw) {
      const solid = raw && raw.solid ? String(raw.solid) : '#c0c0c0';
      const isGradient = !!(raw && (raw.isGradient === true || raw.isGradient === 'true'));
      let gradientColors = [];
      if (raw && Array.isArray(raw.gradientColors)) gradientColors = raw.gradientColors.map(String).filter(Boolean);
      else if (raw && typeof raw.gradientColors === 'string') gradientColors = String(raw.gradientColors).split(',').map(x => x.trim()).filter(Boolean);
      const isRunning = !!(raw && (raw.isRunning === true || raw.isRunning === 'true'));
      return { solid, isGradient, gradientColors, isRunning };
    }

    function toggleStaffRarityGradUI() {
      const g = document.getElementById('staff-rarity-def-gradient')?.value === 'true';
      const gw = document.getElementById('staff-rarity-def-grad-wrap');
      const rw = document.getElementById('staff-rarity-def-run-wrap');
      if (gw) gw.style.display = g ? 'block' : 'none';
      if (rw) rw.style.display = g ? 'block' : 'none';
    }

    function renderStaffRarityDefsList() {
      const list = document.getElementById('staff-rarity-defs-list');
      if (!list) return;
      const keysSet = new Set([...Object.keys(DEFAULT_RARITY_DEFS), ...Object.keys(blookRarityDefs || {})]);
      const keys = Array.from(keysSet);
      keys.sort((a, b) => {
        const ia = raritySortIndex(a);
        const ib = raritySortIndex(b);
        if (ia !== ib) return ia - ib;
        return a.localeCompare(b);
      });
      list.innerHTML = keys.map(k => {
        const def = normalizeStaffRarityDef(getRarityDef(k));
        const gradHint = def.isGradient ? (def.gradientColors.slice(0, 3).join(', ') || '—') : 'solid';
        const anim = def.isGradient && def.isRunning ? ' · animated' : '';
        return `<div class="staff-user-row" style="margin-bottom:8px;align-items:center;">
          <div style="flex:1;min-width:0;"><strong>${escapeHtml(k)}</strong>
          <span style="font-size:0.75rem;color:var(--text-secondary);display:block;">${escapeHtml(gradHint)}${anim}</span></div>
          <button type="button" class="staff-btn staff-btn-primary staff-btn-sm staff-rarity-def-edit" data-rarity-key="${escapeHtml(k)}">Edit</button>
          <button type="button" class="staff-btn staff-btn-danger staff-btn-sm staff-rarity-def-del" data-rarity-key="${escapeHtml(k)}">Delete</button>
        </div>`;
      }).join('');
      list.querySelectorAll('.staff-rarity-def-edit').forEach(b => b.addEventListener('click', () => openStaffRarityDefModal('edit', b.dataset.rarityKey)));
      list.querySelectorAll('.staff-rarity-def-del').forEach(b => b.addEventListener('click', () => deleteStaffRarityDef(b.dataset.rarityKey)));
    }

    async function deleteStaffRarityDef(key) {
      const k = (key || '').toLowerCase().trim();
      if (!k || !confirm(`Remove custom style for "${k}"? Built-in default will be used.`)) return;
      try {
        await updateDoc(doc(db, 'siteConfig', 'settings'), {
          [`blookRarityDefs.${k}`]: deleteField(),
          updatedAt: serverTimestamp()
        });
        await refreshRarityOrderFromServer();
        renderStaffRarityDefsList();
        if (inventoryItems.length) renderInventory();
        showNotification('Rarity style removed', 'success');
      } catch (e) {
        showNotification('Delete failed: ' + e.message, 'error');
      }
    }

    function openStaffRarityDefModal(mode, key) {
      const modal = document.getElementById('staff-rarity-def-modal');
      const keyInp = document.getElementById('staff-rarity-def-key-input');
      const keyHid = document.getElementById('staff-rarity-def-key');
      const delBtn = document.getElementById('staff-rarity-def-delete');
      const title = document.getElementById('staff-rarity-def-modal-title');
      if (!modal || !keyInp || !keyHid) return;
      if (mode === 'add') {
        if (title) title.textContent = 'Add rarity style';
        keyHid.value = '';
        keyInp.value = '';
        keyInp.disabled = false;
        document.getElementById('staff-rarity-def-solid').value = '#c0c0c0';
        document.getElementById('staff-rarity-def-gradient').value = 'false';
        document.getElementById('staff-rarity-def-grad-colors').value = '';
        document.getElementById('staff-rarity-def-running').value = 'false';
        if (delBtn) delBtn.style.display = 'none';
        toggleStaffRarityGradUI();
        modal.style.display = 'flex';
        return;
      }
      const k = (key || '').toLowerCase().trim();
      const def = normalizeStaffRarityDef(getRarityDef(k));
      if (title) title.textContent = 'Edit rarity style';
      keyHid.value = k;
      keyInp.value = k;
      keyInp.disabled = true;
      const solidEl = document.getElementById('staff-rarity-def-solid');
      if (solidEl) solidEl.value = /^#[0-9a-fA-F]{6}$/.test(def.solid) ? def.solid : '#c0c0c0';
      document.getElementById('staff-rarity-def-gradient').value = def.isGradient ? 'true' : 'false';
      document.getElementById('staff-rarity-def-grad-colors').value = def.gradientColors.join(', ');
      document.getElementById('staff-rarity-def-running').value = def.isRunning ? 'true' : 'false';
      if (delBtn) delBtn.style.display = 'inline-flex';
      toggleStaffRarityGradUI();
      modal.style.display = 'flex';
    }

    async function saveStaffRarityDef() {
      const keyHid = (document.getElementById('staff-rarity-def-key')?.value || '').trim().toLowerCase();
      let key = keyHid;
      if (!key) {
        key = (document.getElementById('staff-rarity-def-key-input')?.value || '').trim().toLowerCase().replace(/[^a-z0-9_-]+/g, '');
      }
      if (!key) { showNotification('Enter a rarity id (letters, numbers, _ or -)', 'error'); return; }
      const solid = document.getElementById('staff-rarity-def-solid')?.value || '#c0c0c0';
      const isGradient = document.getElementById('staff-rarity-def-gradient')?.value === 'true';
      const gradStr = document.getElementById('staff-rarity-def-grad-colors')?.value || '';
      const gradientColors = gradStr.split(',').map(x => x.trim()).filter(Boolean);
      const isRunning = document.getElementById('staff-rarity-def-running')?.value === 'true';
      if (isGradient && gradientColors.length < 2) { showNotification('Gradient needs at least two hex colors', 'error'); return; }
      const payload = {
        solid,
        isGradient,
        gradientColors: isGradient ? gradientColors : [],
        isRunning: !!(isGradient && isRunning)
      };
      try {
        const sref = doc(db, 'siteConfig', 'settings');
        const prevSnap = await getDoc(sref);
        const prevDefs = prevSnap.exists() && typeof prevSnap.data().blookRarityDefs === 'object'
          ? { ...prevSnap.data().blookRarityDefs }
          : {};
        prevDefs[key] = payload;
        await setDoc(sref, {
          blookRarityDefs: prevDefs,
          updatedAt: serverTimestamp()
        }, { merge: true });
        await refreshRarityOrderFromServer();
        renderStaffRarityDefsList();
        if (inventoryItems.length) renderInventory();
        document.getElementById('staff-rarity-def-modal').style.display = 'none';
        showNotification('Rarity style saved', 'success');
      } catch (e) {
        showNotification('Save failed: ' + e.message, 'error');
      }
    }

    async function loadStaffRaritySitePanel() {
      await refreshRarityOrderFromServer();
      renderStaffRarityDefsList();
      try {
        const s = await getDoc(doc(db, 'siteConfig', 'settings'));
        const d = s.exists() ? s.data() : {};
        const dr = d.dailyReward || {};
        const cEl = document.getElementById('staff-daily-reward-coins');
        const sEl = document.getElementById('staff-daily-reward-stars');
        if (cEl) cEl.value = dr.coins != null ? dr.coins : 50;
        if (sEl) sEl.value = dr.stars != null ? dr.stars : 10;
      } catch (e) {}
    }

    let staffStarBadgeRulesDraft = [];

    function renderStaffStarBadgeRulesList() {
      const list = document.getElementById('staff-star-badge-rules-list');
      if (!list) return;
      if (!staffStarBadgeRulesDraft.length) {
        list.innerHTML = '<p style="font-size:0.82rem;color:var(--text-secondary);">No rules yet. Add at least one badge and minimum stars.</p>';
        return;
      }
      const sorted = [...staffStarBadgeRulesDraft].sort((a, b) => a.minStars - b.minStars || a.badgeName.localeCompare(b.badgeName));
      list.innerHTML = sorted.map(r => `<div class="staff-user-row" style="margin-bottom:8px;align-items:center;">
          <div style="flex:1;"><strong>${escapeHtml(r.badgeName)}</strong> <span style="font-size:0.8rem;color:var(--text-secondary);">≥ ${r.minStars} stars</span></div>
          <button type="button" class="staff-btn staff-btn-danger staff-btn-sm staff-star-badge-del" data-badge="${escapeHtml(r.badgeName)}" data-min="${r.minStars}">Remove</button>
        </div>`).join('');
      list.querySelectorAll('.staff-star-badge-del').forEach(btn => {
        btn.addEventListener('click', () => {
          const bn = btn.dataset.badge;
          const mn = parseInt(btn.dataset.min, 10);
          staffStarBadgeRulesDraft = staffStarBadgeRulesDraft.filter(x => !(x.badgeName === bn && x.minStars === mn));
          renderStaffStarBadgeRulesList();
        });
      });
    }

    async function loadStaffStarBadgesPanel() {
      const pick = document.getElementById('staff-star-badge-pick');
      const status = document.getElementById('staff-star-badge-status');
      if (status) status.textContent = '';
      try {
        const s = await getDoc(doc(db, 'siteConfig', 'settings'));
        const rules = s.exists() && Array.isArray(s.data().starBadgeRules) ? s.data().starBadgeRules : [];
        staffStarBadgeRulesDraft = rules
          .filter(x => x && x.badgeName && (typeof x.minStars === 'number' || x.minStars != null))
          .map(x => ({ badgeName: String(x.badgeName).trim(), minStars: Math.max(0, parseInt(x.minStars, 10) || 0) }))
          .filter(x => x.badgeName);
        const snap = await getDocs(collection(db, 'badges'));
        const names = snap.docs.map(d => d.data().name).filter(Boolean).sort();
        if (pick) {
          const cur = pick.value;
          pick.innerHTML = '<option value="">Select badge…</option>' + names.map(n => `<option value="${escapeHtml(n)}">${escapeHtml(n)}</option>`).join('');
          if (cur && names.includes(cur)) pick.value = cur;
        }
        renderStaffStarBadgeRulesList();
      } catch (e) {
        if (status) status.textContent = 'Could not load: ' + e.message;
      }
    }

    async function refreshDailyRewardUI(userId) {
      const btn = document.getElementById('daily-reward-claim-btn');
      const status = document.getElementById('daily-reward-status');
      const desc = document.getElementById('daily-reward-desc');
      if (!btn || !userId) return;
      let coins = 50;
      let stars = 10;
      try {
        const s = await getDoc(doc(db, 'siteConfig', 'settings'));
        if (s.exists()) {
          const dr = s.data().dailyReward;
          if (dr && typeof dr === 'object') {
            const c = parseInt(dr.coins, 10);
            const st = parseInt(dr.stars, 10);
            if (Number.isFinite(c)) coins = Math.max(0, c);
            if (Number.isFinite(st)) stars = Math.max(0, st);
          }
        }
      } catch (e) {}
      if (desc) desc.textContent = `Claim ${coins} coins and ${stars} stars once per calendar day (UTC).`;
      try {
        const u = await getDoc(doc(db, 'users', userId));
        const last = u.exists() ? u.data().lastDailyRewardDate : null;
        const today = ymdUTC(new Date());
        const claimed = last === today;
        btn.disabled = claimed;
        btn.textContent = claimed ? 'Come back tomorrow' : 'Claim';
        if (status) status.textContent = claimed ? `Claimed for ${today}` : 'Ready to claim';
      } catch (e) {
        btn.disabled = true;
        if (status) status.textContent = 'Could not load status';
      }
    }

    async function claimDailyReward() {
      if (!currentUser) return;
      const uid = currentUser.uid;
      let coins = 50;
      let stars = 10;
      try {
        const s = await getDoc(doc(db, 'siteConfig', 'settings'));
        if (s.exists()) {
          const dr = s.data().dailyReward;
          if (dr && typeof dr === 'object') {
            const c = parseInt(dr.coins, 10);
            const st = parseInt(dr.stars, 10);
            if (Number.isFinite(c)) coins = Math.max(0, c);
            if (Number.isFinite(st)) stars = Math.max(0, st);
          }
        }
      } catch (e) {}
      const today = ymdUTC(new Date());
      try {
        await runTransaction(db, async tx => {
          const uref = doc(db, 'users', uid);
          const snap = await tx.get(uref);
          if (!snap.exists()) throw new Error('nouser');
          const d = snap.data();
          if (d.lastDailyRewardDate === today) throw new Error('already');
          tx.update(uref, {
            coins: (d.coins || 0) + coins,
            stars: (d.stars || 0) + stars,
            lastDailyRewardDate: today
          });
        });
        showNotification(`Daily reward: +${coins} coins, +${stars} stars`, 'success');
        loadUserBalance(uid);
        refreshDailyRewardUI(uid);
        syncStarBadgesForUser(uid);
      } catch (e) {
        if (e.message === 'already') showNotification('Already claimed today', 'error');
        else if (e.message === 'nouser') showNotification('User not found', 'error');
        else showNotification('Could not claim: ' + (e.message || 'error'), 'error');
      }
    }

    function openBadgeDetailModal(bd) {
      const modal = document.getElementById('badge-detail-modal');
      const wrap = document.getElementById('badge-detail-icon-wrap');
      const nm = document.getElementById('badge-detail-name');
      const dc = document.getElementById('badge-detail-desc');
      if (!modal || !wrap) return;
      wrap.innerHTML = '';
      wrap.style.background = bd.bgColor || 'var(--neon-green)';
      const icon = bd.icon || '';
      const isFa = /\bfa[srb]?\s+fa-/.test(icon) || /^fa[srb]?\s/.test(icon) || (icon.includes('fa-') && !/^https?:\/\//i.test(icon));
      if (isFa) {
        const safeFa = icon.replace(/[^a-zA-Z0-9_\- ]/g, '').trim();
        const iel = document.createElement('i');
        iel.className = safeFa;
        iel.setAttribute('aria-hidden', 'true');
        iel.style.fontSize = '1.75rem';
        iel.style.color = bd.textColor || '#000';
        wrap.appendChild(iel);
      } else if (/^https?:\/\//i.test(icon)) {
        const img = document.createElement('img');
        img.src = icon;
        img.alt = bd.name || '';
        img.style.width = '100%';
        img.style.height = '100%';
        img.style.objectFit = 'contain';
        img.style.borderRadius = '10px';
        wrap.appendChild(img);
      } else {
        wrap.innerHTML = '<i class="fas fa-award" style="font-size:1.75rem;color:#000;"></i>';
      }
      if (nm) nm.textContent = bd.name || 'Badge';
      if (dc) dc.textContent = bd.description || 'No description';
      modal.style.display = 'flex';
    }

    let packUnboxClickHandler = null;
    let packUnboxDismissHandler = null;

    function resetPackOpeningModalLayout() {
      if (packAutoUnboxTimer) {
        clearTimeout(packAutoUnboxTimer);
        packAutoUnboxTimer = null;
      }
      stopPackConfetti();
      packOpeningModalInner?.classList.remove('pack-unbox-celebrate', 'pack-unbox-peek-mood');
      if (packOpeningModalInner) {
        ['--pack-inner', '--pack-outer', '--pack-aurora-inner', '--pack-aurora-outer', '--pack-aurora-rare-a', '--pack-aurora-rare-b', '--pack-aurora-rare-c'].forEach(p => packOpeningModalInner.style.removeProperty(p));
      }
      if (openingModalBg) {
        openingModalBg.classList.remove('pack-rarity-celebrate', 'pack-bg-rarity-animated', 'pack-bg-rip-flash');
        openingModalBg.style.animation = '';
      }
      if (packUnboxClickLayer && packUnboxClickHandler) {
        packUnboxClickLayer.removeEventListener('click', packUnboxClickHandler);
        packUnboxClickHandler = null;
      }
      if (packUnboxDismissLayer && packUnboxDismissHandler) {
        packUnboxDismissLayer.removeEventListener('click', packUnboxDismissHandler);
        packUnboxDismissHandler = null;
      }
      if (packUnboxClickLayer) packUnboxClickLayer.style.display = 'none';
      if (packUnboxDismissLayer) packUnboxDismissLayer.style.display = 'none';
      if (packRevealCard) packRevealCard.classList.remove('pack-unbox-reveal-anim');
      if (packUnboxBoost) packUnboxBoost.style.display = 'none';
      packOpeningModalInner?.classList.remove('pack-opening-modal-unbox');
      packOpeningModalBody?.classList.remove('pack-unbox-body', 'pack-unbox-revealed');
      if (packUnboxPackCenter) packUnboxPackCenter.style.display = '';
      giftAnimation.classList.remove('pack-unbox-stage', 'torn', 'pack-unbox-peek', 'pack-unbox-cover-drop', 'pack-unbox-reveal', 'pack-unbox-anim');
      giftAnimation.innerHTML = '';
      if (packRevealBlookSlot) packRevealBlookSlot.innerHTML = '';
      if (packRevealCard) {
        packRevealCard.style.backgroundImage = '';
        packRevealCard.style.backgroundColor = '';
      }
      openingModalBg.style.backgroundImage = '';
      openingModalBg.style.backgroundSize = '';
      openingModalBg.style.backgroundRepeat = '';
      openingModalBg.style.backgroundPosition = '';
    }

    function applyPackUnboxBackdropVars(innerHex, outerHex, rarity) {
      const root = packOpeningModalInner;
      if (!root) return;
      const ir = hexToRgbTriplet(innerHex) || [90, 106, 138];
      const or = hexToRgbTriplet(outerHex) || [26, 34, 51];
      const rr = hexToRgbTriplet(getRarityColor(rarity)) || [42, 255, 158];
      const lit = mixRgb(rr, [255, 255, 255], 0.55);
      const toR = (rgb, a) => `rgba(${rgb[0]},${rgb[1]},${rgb[2]},${a})`;
      root.style.setProperty('--pack-inner', innerHex);
      root.style.setProperty('--pack-outer', outerHex);
      root.style.setProperty('--pack-aurora-inner', toR(ir, 0.44));
      root.style.setProperty('--pack-aurora-outer', toR(or, 0.52));
      root.style.setProperty('--pack-aurora-rare-a', toR(rr, 0.48));
      root.style.setProperty('--pack-aurora-rare-b', toR(lit, 0.18));
      root.style.setProperty('--pack-aurora-rare-c', toR(rr, 0.14));
    }

    function showPackOpeningModal(pack, item) {
      resetPackOpeningModalLayout();

      const fc = marketPackFrameColors(pack);
      const inner = fc.inner || '#5a6a8a';
      const outer = fc.outer || '#1a2233';
      applyPackUnboxBackdropVars(inner, outer, item.rarity);
      openingModalBg.style.backgroundImage = `linear-gradient(168deg, ${inner} 0%, ${outer} 42%, #030509 88%)`;
      openingModalBg.style.backgroundSize = '220% 220%';
      openingModalBg.style.backgroundRepeat = 'no-repeat';
      openingModalBg.style.backgroundPosition = '40% 45%';

      packOpeningModalInner?.classList.add('pack-opening-modal-unbox');
      packOpeningModalBody?.classList.add('pack-unbox-body');

      openedGiftName.textContent = item.name || '';
      const rKey = (item.rarity || 'common').toLowerCase();
      openedGiftRarity.textContent = (item.rarity || 'common').toUpperCase();
      openedGiftRarity.className = 'pack-reveal-rarity';
      openedGiftRarity.style.cssText = `color:${getRarityColor(rKey)};background:none;-webkit-text-fill-color:initial;`;
      const pct = item.chance != null && item.chance !== '' ? Number(item.chance) : null;
      openedGiftChance.textContent = pct != null && Number.isFinite(pct) ? `${pct}%` : '—';
      openedGiftStars.textContent = `Stars: ${item.starsGained != null ? item.starsGained : 0}`;
      const revealUrl = (pack.revealCardBgUrl || '').trim();
      if (packRevealCard) {
        if (revealUrl) {
          packRevealCard.style.backgroundColor = '#0b0e14';
          packRevealCard.style.backgroundImage = `url(${JSON.stringify(revealUrl)})`;
          packRevealCard.style.backgroundSize = 'cover';
        } else {
          packRevealCard.style.backgroundColor = '#0b0e14';
          packRevealCard.style.backgroundSize = '100% 100%';
          packRevealCard.style.backgroundImage = `linear-gradient(155deg, ${inner} 0%, ${outer} 55%, #05070c 100%)`;
        }
      }
      if (packRevealBlookSlot) {
        packRevealBlookSlot.innerHTML = '';
        if (item.imageUrl) {
          const im = document.createElement('img');
          im.src = item.imageUrl;
          im.alt = item.name || '';
          packRevealBlookSlot.appendChild(im);
        } else {
          packRevealBlookSlot.innerHTML = `<i class="fas fa-gift" style="font-size:3rem;color:${getRarityColor(item.rarity)};"></i>`;
        }
      }
      openedGiftName.style.opacity = '0';
      openedGiftRarity.style.opacity = '0';
      openedGiftChance.style.opacity = '0';
      openedGiftStars.style.opacity = '0';

      if (packUnboxClickLayer) packUnboxClickLayer.style.display = 'block';
      if (packUnboxBoost) packUnboxBoost.style.display = 'flex';

      giftAnimation.innerHTML = '';
      giftAnimation.classList.add('pack-unbox-stage');

      const artUrl = (marketPackArtUrl(pack) || (pack.backgroundImage || '').trim() || (pack.items?.find(it => it.imageUrl)?.imageUrl) || 'https://placehold.co/280x280/1a1f2e/8899aa?text=Pack').replace(/\\/g, '/');

      const stack = document.createElement('div');
      stack.className = 'pack-unbox-stack';

      const setStackSize = (nw, nh) => {
        const maxW = Math.min(window.innerWidth * 0.92, 440);
        const maxH = Math.min(window.innerHeight * 0.62, 560);
        const w = nw > 0 ? nw : 280;
        const h = nh > 0 ? nh : 280;
        const scale = Math.min(1, maxW / w, maxH / h) * 1.3;
        stack.style.width = `${Math.round(w * scale)}px`;
        stack.style.height = `${Math.round(h * scale)}px`;
      };

      const blookUnder = document.createElement('div');
      blookUnder.className = 'pack-unbox-blook-under';
      const blookClip = document.createElement('div');
      blookClip.className = 'pack-unbox-blook-clip';
      if (item.imageUrl) {
        const bi = document.createElement('img');
        bi.src = item.imageUrl;
        bi.alt = item.name || '';
        blookClip.appendChild(bi);
      } else {
        const ic = document.createElement('i');
        ic.className = 'fas fa-gift pack-unbox-blook-icon';
        ic.style.fontSize = 'min(26vw,6.5rem)';
        ic.style.color = getRarityColor(item.rarity);
        blookClip.appendChild(ic);
      }
      blookUnder.appendChild(blookClip);

      const makeHalfImg = () => {
        const im = document.createElement('img');
        im.src = artUrl;
        im.alt = '';
        im.draggable = false;
        return im;
      };

      const halfBot = document.createElement('div');
      halfBot.className = 'pack-unbox-half pack-unbox-half-bot';
      halfBot.appendChild(makeHalfImg());

      const halfTop = document.createElement('div');
      halfTop.className = 'pack-unbox-half pack-unbox-half-top';
      halfTop.appendChild(makeHalfImg());

      stack.appendChild(blookUnder);
      stack.appendChild(halfBot);
      stack.appendChild(halfTop);

      const finishRevealCard = () => {
        packOpeningModalBody?.classList.add('pack-unbox-revealed');
        if (packUnboxPackCenter) packUnboxPackCenter.style.display = 'none';
        openedGiftName.style.opacity = '1';
        openedGiftRarity.style.opacity = '1';
        openedGiftChance.style.opacity = '1';
        openedGiftStars.style.opacity = '1';
        if (packRevealCard) packRevealCard.classList.add('pack-unbox-reveal-anim');
        if (packUnboxDismissLayer) {
          packUnboxDismissLayer.style.display = 'block';
          packUnboxDismissHandler = async (e) => {
            e.preventDefault();
            e.stopPropagation();
            if (!currentUser || !currentGiftCard) {
              packOpeningModal.style.display = 'none';
              resetPackOpeningModalLayout();
              return;
            }
            try {
              await addDoc(collection(db, 'inventory'), {
                userId: currentUser.uid,
                packId: currentGiftCard.packId,
                packName: currentGiftCard.packName,
                itemName: currentGiftCard.name,
                rarity: currentGiftCard.rarity,
                imageUrl: currentGiftCard.imageUrl || null,
                starsGained: currentGiftCard.starsGained,
                timestamp: serverTimestamp()
              });
              packOpeningModal.style.display = 'none';
              resetPackOpeningModalLayout();
              showNotification(`You received: ${currentGiftCard.name}!`, 'success');
            } catch (err) {
              showNotification('Error collecting item: ' + (err.message || 'error'), 'error');
            }
          };
          packUnboxDismissLayer.addEventListener('click', packUnboxDismissHandler);
        }
      };

      const applyRarityCelebrateBg = () => {
        const bg = getRarityBackgroundGradientCss(item.rarity);
        if (openingModalBg) {
          openingModalBg.classList.add('pack-rarity-celebrate');
          openingModalBg.classList.toggle('pack-bg-rarity-animated', !!bg.animated);
          openingModalBg.style.backgroundImage = bg.image;
          openingModalBg.style.backgroundSize = bg.size;
          openingModalBg.style.backgroundRepeat = bg.repeat;
          openingModalBg.style.backgroundPosition = bg.position;
          openingModalBg.style.animation = '';
        }
        packOpeningModalInner?.classList.add('pack-unbox-celebrate');
        packOpeningModalInner?.classList.remove('pack-unbox-peek-mood');
      };

      const firstRip = () => {
        if (giftAnimation.classList.contains('torn')) return;
        giftAnimation.classList.add('torn', 'pack-unbox-peek');
        packOpeningModalInner?.classList.add('pack-unbox-peek-mood');
        if (openingModalBg) {
          openingModalBg.classList.remove('pack-bg-rip-flash');
          void openingModalBg.offsetWidth;
          openingModalBg.classList.add('pack-bg-rip-flash');
          setTimeout(() => { openingModalBg?.classList.remove('pack-bg-rip-flash'); }, 700);
        }
        if (packUnboxBoost) packUnboxBoost.style.display = 'none';
      };

      const secondReveal = () => {
        if (!giftAnimation.classList.contains('torn') || giftAnimation.classList.contains('pack-unbox-cover-drop')) return;
        giftAnimation.classList.add('pack-unbox-cover-drop');
        giftAnimation.classList.remove('pack-unbox-peek');
        applyRarityCelebrateBg();
        const confettiColors = getRarityConfettiColors(item.rarity);
        startPackConfetti(15000, confettiColors);
        setTimeout(finishRevealCard, 620);
      };

      packUnboxClickHandler = (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (!giftAnimation.classList.contains('torn')) {
          firstRip();
          const preConfettiMs = 1000;
          if (packAutoUnboxTimer) clearTimeout(packAutoUnboxTimer);
          packAutoUnboxTimer = setTimeout(() => {
            packAutoUnboxTimer = null;
            secondReveal();
          }, preConfettiMs);
        }
      };

      const probe = new Image();
      probe.onload = () => {
        setStackSize(probe.naturalWidth, probe.naturalHeight);
        giftAnimation.appendChild(stack);
        if (packUnboxClickLayer) packUnboxClickLayer.addEventListener('click', packUnboxClickHandler);
      };
      probe.onerror = () => {
        setStackSize(280, 280);
        giftAnimation.appendChild(stack);
        if (packUnboxClickLayer) packUnboxClickLayer.addEventListener('click', packUnboxClickHandler);
      };
      probe.src = artUrl;

      packOpeningModal.style.display = 'flex';
    }

    // ========== Blooket-style Inventory ==========
    let currentInventoryFilter = 'all';
    let inventoryItems = [];
    let invBlookDetailContext = null;
    let packBuilderItems = [];
    let staffEditUserInitialBanner = '';

    function invSellPriceForStars(stars) {
      const n = Number(stars) || 0;
      return Math.max(1, Math.floor(n * 0.5));
    }

    function closeInvBlookModal() {
      const m = document.getElementById('inv-blook-modal');
      if (m) m.style.display = 'none';
      invBlookDetailContext = null;
    }

    function populateRecipientSelectForInvSend() {
      if (!recipientEmail) return;
      recipientEmail.innerHTML = '<option value="">Select a friend</option>';
      (friendsData || []).forEach(f => {
        const o = document.createElement('option');
        o.value = f.id;
        o.textContent = f.username || f.email || f.id;
        recipientEmail.appendChild(o);
      });
    }

    function openInvBlookDetail(rep, copies) {
      invBlookDetailContext = { rep, copies: copies || [rep] };
      const r = (rep.rarity || 'common').toLowerCase();
      const qty = (copies && copies.length) ? copies.length : 1;
      const stars = rep.starsGained || 0;
      const sell = invSellPriceForStars(stars);
      const rc = getRarityColor(r);

      const vis = document.getElementById('inv-blook-modal-visual');
      if (vis) {
        vis.innerHTML = rep.imageUrl
          ? `<img src="${escapeHtml(rep.imageUrl)}" alt="">`
          : `<span class="inv-blook-ph" style="color:${rc};"><i class="fas fa-dragon"></i></span>`;
      }
      const nm = document.getElementById('inv-blook-modal-name');
      if (nm) nm.textContent = rep.itemName || 'Blook';
      const rr = document.getElementById('inv-blook-modal-rarity');
      if (rr) {
        rr.textContent = r;
        rr.style.color = rc;
        rr.style.border = `1px solid ${rc}55`;
        rr.style.background = `${rc}22`;
      }
      const pk = document.getElementById('inv-blook-modal-pack');
      if (pk) pk.textContent = rep.packName || '—';
      const q = document.getElementById('inv-blook-modal-qty');
      if (q) q.textContent = String(qty);
      const st = document.getElementById('inv-blook-modal-stars');
      if (st) st.textContent = String(stars);
      const sl = document.getElementById('inv-blook-modal-sell');
      if (sl) sl.textContent = `${sell} tokens (each)`;

      const m = document.getElementById('inv-blook-modal');
      if (m) m.style.display = 'flex';
    }

    async function sellOneInvBlookFromDetail() {
      if (!currentUser || !invBlookDetailContext) return;
      const { copies } = invBlookDetailContext;
      const docId = copies[0].id;
      const rep = copies[0];
      const price = invSellPriceForStars(rep.starsGained || 0);
      if (!confirm(`Sell one ${rep.itemName || 'blook'} for ${price} tokens?`)) return;
      try {
        const ref = doc(db, 'inventory', docId);
        const uref = doc(db, 'users', currentUser.uid);
        await runTransaction(db, async (transaction) => {
          const snap = await transaction.get(ref);
          if (!snap.exists()) throw new Error('missing');
          transaction.delete(ref);
          transaction.update(uref, { coins: increment(price) });
        });
        showNotification(`Sold for ${price} tokens`, 'success');
        closeInvBlookModal();
      } catch (e) {
        showNotification('Could not sell: ' + (e.message || 'error'), 'error');
      }
    }

    function openInvSendFromDetail() {
      if (!invBlookDetailContext || !currentUser) return;
      const { copies } = invBlookDetailContext;
      const rep = copies[0];
      currentCard = {
        id: rep.id,
        name: rep.itemName,
        rarity: rep.rarity,
        image: rep.imageUrl || '',
        starsGained: rep.starsGained || 0
      };
      closeInvBlookModal();
      populateRecipientSelectForInvSend();
      if (sendCardName) sendCardName.textContent = rep.itemName || 'Blook';
      if (sendCardType) {
        sendCardType.innerHTML = `${escapeHtml((rep.rarity || '').toString())} · <i class="fas fa-star"></i> ${rep.starsGained || 0} · Send 1 copy`;
      }
      if (sendCardIcon) {
        sendCardIcon.innerHTML = rep.imageUrl
          ? `<img src="${escapeHtml(rep.imageUrl)}" alt="" style="width:100%;height:100%;object-fit:contain;border-radius:8px;">`
          : '🎁';
      }
      if (sendCardModal) sendCardModal.style.display = 'flex';
    }

    function openSendCardModal() {
      if (!currentCard) return;
      populateRecipientSelectForInvSend();
      if (sendCardName) sendCardName.textContent = currentCard.name || 'Blook';
      if (sendCardType) {
        sendCardType.innerHTML = `${escapeHtml((currentCard.rarity || '').toString())} · <i class="fas fa-star"></i> ${currentCard.starsGained || 0}`;
      }
      if (sendCardIcon) {
        sendCardIcon.innerHTML = currentCard.image
          ? `<img src="${escapeHtml(currentCard.image)}" alt="" style="width:100%;height:100%;object-fit:contain;border-radius:8px;">`
          : '🎁';
      }
      if (sendCardModal) sendCardModal.style.display = 'flex';
    }

    async function staffAppendBlookToPackDoc(packId, entry) {
      const d = await getDoc(doc(db, 'packs', packId));
      if (!d.exists()) { showNotification('Pack not found', 'error'); return false; }
      const items = [...(d.data().items || []), entry];
      await updateDoc(doc(db, 'packs', packId), { items, updatedAt: serverTimestamp() });
      return true;
    }

    function renderPackBuilderItems() {
      const list = document.getElementById('staff-pack-items-list');
      if (!list) return;
      if (packBuilderItems.length === 0) {
        list.innerHTML = '<span style="color:var(--text-secondary);font-size:0.8rem;">No blooks yet — click Add blook</span>';
        return;
      }
      list.innerHTML = packBuilderItems.map((item, i) => {
        const thumb = item.imageUrl
          ? `<img src="${escapeHtml(item.imageUrl)}" alt="">`
          : '<span style="width:28px;height:28px;border-radius:8px;background:rgba(255,255,255,0.08);display:inline-flex;align-items:center;justify-content:center;font-size:0.65rem;color:var(--text-secondary);"><i class="fas fa-cube"></i></span>';
        const label = escapeHtml(item.name || '(unnamed)');
        return `<div class="staff-blook-chip-wrap" style="display:inline-flex;align-items:center;gap:2px;"><button type="button" class="staff-blook-chip" data-idx="${i}">${thumb}<span>${label}</span></button><button type="button" class="staff-blook-chip-del" data-idx="${i}" title="Remove">×</button></div>`;
      }).join('');
      list.querySelectorAll('.staff-blook-chip').forEach(btn => {
        btn.addEventListener('click', () => { openStaffBlookEditor(parseInt(btn.dataset.idx, 10)); });
      });
      list.querySelectorAll('.staff-blook-chip-del').forEach(btn => {
        btn.addEventListener('click', (e) => {
          e.stopPropagation();
          packBuilderItems.splice(parseInt(btn.dataset.idx, 10), 1);
          renderPackBuilderItems();
        });
      });
    }

    async function openStaffBlookEditor(idx) {
      const modal = document.getElementById('staff-blook-editor-modal');
      if (!modal) return;
      const isNew = idx < 0;
      document.getElementById('staff-blook-editor-idx').value = String(idx);
      document.getElementById('staff-blook-editor-title').textContent = isNew ? 'Add blook' : 'Edit blook';
      const RAR_OPTS = effectiveRarityOrder.length ? effectiveRarityOrder : DEFAULT_RARITY_ORDER;
      const raritySel = document.getElementById('staff-blook-editor-rarity');
      raritySel.innerHTML = RAR_OPTS.map(r => `<option value="${r}">${r}</option>`).join('');
      const currentPackId = document.getElementById('staff-pack-id').value;
      const snap = await getDocs(collection(db, 'packs'));
      const packs = snap.docs.map(d => ({ id: d.id, ...d.data() })).filter(p => p.shopType !== 'banner');
      const packSel = document.getElementById('staff-blook-editor-pack');
      let opts = '';
      if (!currentPackId) {
        opts += `<option value="__DRAFT__">This pack (draft — save pack first to move blooks elsewhere)</option>`;
      }
      packs.forEach(p => {
        opts += `<option value="${p.id}" ${p.id === currentPackId ? 'selected' : ''}>${escapeHtml(p.name || p.id)}</option>`;
      });
      if (!opts) opts = '<option value="">No item packs in database</option>';
      packSel.innerHTML = opts;
      const item = !isNew ? packBuilderItems[idx] : null;
      if (item) {
        document.getElementById('staff-blook-editor-name').value = item.name || '';
        raritySel.value = (item.rarity || 'common').toLowerCase();
        document.getElementById('staff-blook-editor-stars').value = item.starsGained ?? 0;
        document.getElementById('staff-blook-editor-chance').value = item.chance ?? 0;
        document.getElementById('staff-blook-editor-image').value = item.imageUrl || '';
        if (currentPackId) packSel.value = currentPackId;
        else if (packSel.querySelector('option[value="__DRAFT__"]')) packSel.value = '__DRAFT__';
      } else {
        document.getElementById('staff-blook-editor-name').value = '';
        raritySel.value = RAR_OPTS[0] || 'common';
        document.getElementById('staff-blook-editor-stars').value = 0;
        document.getElementById('staff-blook-editor-chance').value = 0;
        document.getElementById('staff-blook-editor-image').value = '';
        if (!currentPackId && packSel.querySelector('option[value="__DRAFT__"]')) packSel.value = '__DRAFT__';
        else if (currentPackId) packSel.value = currentPackId;
      }
      const delBtn = document.getElementById('staff-blook-editor-delete');
      if (delBtn) delBtn.style.display = isNew ? 'none' : 'inline-block';
      modal.style.display = 'flex';
    }

    async function applyStaffBlookEditor() {
      const idx = parseInt(document.getElementById('staff-blook-editor-idx').value, 10);
      const name = document.getElementById('staff-blook-editor-name').value.trim();
      if (!name) { showNotification('Enter a blook name', 'error'); return; }
      const targetPack = document.getElementById('staff-blook-editor-pack').value;
      const currentPackId = document.getElementById('staff-pack-id').value;
      const rarityVal = document.getElementById('staff-blook-editor-rarity').value;
      const entry = {
        name,
        rarity: rarityVal,
        starsGained: parseFloat(document.getElementById('staff-blook-editor-stars').value) || 0,
        chance: parseFloat(document.getElementById('staff-blook-editor-chance').value) || 0,
        imageUrl: document.getElementById('staff-blook-editor-image').value.trim(),
        rarityColor: getRarityColor(rarityVal)
      };
      const inThisPack = currentPackId ? (targetPack === currentPackId) : (targetPack === '__DRAFT__');

      if (idx < 0) {
        if (inThisPack) {
          packBuilderItems.push(entry);
          renderPackBuilderItems();
        } else {
          const ok = await staffAppendBlookToPackDoc(targetPack, entry);
          if (ok) showNotification('Blook added to selected pack', 'success');
        }
      } else {
        if (inThisPack) {
          packBuilderItems[idx] = { ...packBuilderItems[idx], ...entry };
          renderPackBuilderItems();
        } else {
          packBuilderItems.splice(idx, 1);
          renderPackBuilderItems();
          const ok = await staffAppendBlookToPackDoc(targetPack, entry);
          if (ok) showNotification('Blook moved to the selected pack. Save this pack to remove it from here permanently.', 'success');
        }
      }
      document.getElementById('staff-blook-editor-modal').style.display = 'none';
    }

    function renderInventory() {
      if (!inventoryContainer) return;
      inventoryContainer.innerHTML = '';
      const packTabs = document.getElementById('inv-pack-tabs');

      if (inventoryItems.length === 0) {
        if (packTabs) packTabs.innerHTML = '';
        inventoryContainer.innerHTML = '<p class="empty-message">No blooks yet — open some packs!</p>';
        return;
      }

      const packCounts = {};
      inventoryItems.forEach(item => {
        const pn = item.packName || 'Unknown pack';
        packCounts[pn] = (packCounts[pn] || 0) + 1;
      });
      const packNames = Object.keys(packCounts).sort();
      if (!packNames.includes(currentInventoryFilter) && currentInventoryFilter !== 'all') {
        currentInventoryFilter = 'all';
      }

      if (packTabs) {
        packTabs.innerHTML = '';
        const mkTab = (label, filter, count) => {
          const b = document.createElement('button');
          b.type = 'button';
          b.className = `inv-pack-tab${currentInventoryFilter === filter ? ' active' : ''}`;
          b.dataset.filter = filter;
          b.textContent = count != null ? `${label} (${count})` : label;
          b.addEventListener('click', () => {
            currentInventoryFilter = filter;
            renderInventory();
          });
          packTabs.appendChild(b);
        };
        mkTab('All', 'all', inventoryItems.length);
        packNames.forEach(pn => mkTab(pn, pn, packCounts[pn]));
      }

      const filtered = currentInventoryFilter === 'all' ? inventoryItems : inventoryItems.filter(i => (i.packName || 'Unknown pack') === currentInventoryFilter);
      const byName = {};
      filtered.forEach(item => {
        const key = item.itemName || '?';
        if (!byName[key]) byName[key] = [];
        byName[key].push(item);
      });

      const rows = Object.keys(byName).map(k => {
        const copies = byName[k];
        const rep = copies.reduce((a, b) => (raritySortIndex(a.rarity) > raritySortIndex(b.rarity) ? a : b));
        return { copies, rep, count: copies.length };
      });
      rows.sort((a, b) => {
        const rd = raritySortIndex(a.rep.rarity) - raritySortIndex(b.rep.rarity);
        if (rd !== 0) return rd;
        return (a.rep.itemName || '').localeCompare(b.rep.itemName || '');
      });

      rows.forEach(({ rep, copies, count }) => {
        const r = (rep.rarity || 'common').toLowerCase();
        const rc = getRarityColor(r);
        const cell = document.createElement('div');
        cell.className = 'inv-blook-cell';
        cell.innerHTML = `
          ${count > 1 ? `<span class="inv-blook-qty-badge" style="color:${rc};border-color:${rc}44;">${count}</span>` : ''}
          <div class="inv-blook-cell-inner">
            ${rep.imageUrl ? `<img src="${escapeHtml(rep.imageUrl)}" alt="">` : `<span class="inv-blook-ph" style="color:${rc};"><i class="fas fa-dragon"></i></span>`}
          </div>
          <div class="inv-blook-cell-name" title="${escapeHtml(rep.itemName)}">${escapeHtml(rep.itemName)}</div>
        `;
        cell.addEventListener('click', () => openInvBlookDetail(rep, copies));
        inventoryContainer.appendChild(cell);
      });
    }

    async function loadInventory(userId) {
      if (inventoryUnsubscribe) inventoryUnsubscribe();
      const inventoryQuery = query(collection(db, "inventory"), where("userId", "==", userId));
      inventoryUnsubscribe = onSnapshot(inventoryQuery, (snapshot) => {
        inventoryItems = [];
        snapshot.forEach(d => {
          inventoryItems.push({ id: d.id, ...d.data() });
        });
        renderInventory();
      }, (error) => {
        console.error("Inventory listener error: ", error);
        inventoryContainer.innerHTML = `<p class="error-message">Error loading inventory: ${error.message}</p>`;
      });
    }

    async function useItem(item) {
      if (!currentUser) return;
      try {
        // For now, just delete the item when used
        await deleteDoc(doc(db, "inventory", item.id));
        showNotification(`Used item: ${item.name}!`, "success");
      } catch(e) { showNotification("Error using item: " + e.message, "error"); }
    }

    let missionsListUnsub = null;
    let missionProgressUnsub = null;
    let missionsRenderCache = [];
    let missionProgressMap = {};
    let missionsPageUserId = null;

    function renderMissionsList() {
      missionsContainer.innerHTML = '';
      if (!missionsRenderCache.length) {
        missionsContainer.innerHTML = '<div class="no-missions">No missions available yet. Check back later!</div>';
        return;
      }
      const sorted = [...missionsRenderCache].sort((a, b) => (a.createdAt?.seconds || 0) - (b.createdAt?.seconds || 0));
      sorted.forEach(mission => {
        const progDoc = missionProgressMap[`${mission.id}`] || {};
        const rawProg = typeof progDoc.progress === 'number' ? progDoc.progress : 0;
        const target = Math.max(1, mission.target || 1);
        const progressVal = Math.min(rawProg, target);
        const progressPercent = Math.min(100, progressVal / target * 100);
        const userClaimed = !!progDoc.claimed;
        const isCompleted = progressVal >= target && !userClaimed;

        const missionCard = document.createElement('div');
        missionCard.className = 'mission-card';
        missionCard.innerHTML = `
          <div class="mission-header">
            <div class="mission-title">${escapeHtml(mission.title)}</div>
            <div class="mission-type ${escapeHtml(mission.type || 'gametime')}">${escapeHtml((mission.type || 'gametime').toUpperCase())}</div>
          </div>
          <div class="mission-description">${escapeHtml(mission.description || '')}</div>
          <div class="mission-progress">
            <div class="progress-bar">
              <div class="progress-fill" style="width: ${progressPercent}%"></div>
            </div>
            <div class="progress-text">${progressVal} / ${target}</div>
          </div>
          <div class="mission-rewards">
            <div class="reward-item">
              <i class="fas fa-coins reward-icon"></i>
              <span>${mission.rewardCoins || 0} coins</span>
            </div>
            <div class="reward-item">
              <i class="fas fa-star reward-icon"></i>
              <span>${mission.rewardStars || 0} stars</span>
            </div>
          </div>
          <button class="claim-btn" ${userClaimed || !isCompleted ? 'disabled' : ''}>${userClaimed ? 'CLAIMED' : (isCompleted ? 'CLAIM REWARD' : 'IN PROGRESS')}</button>
        `;

        const claimBtn = missionCard.querySelector('.claim-btn');
        if (isCompleted && !userClaimed) {
          const uid = missionsPageUserId || currentUser?.uid;
          if (uid) claimBtn.addEventListener('click', () => claimMissionReward(mission, uid));
        }
        missionsContainer.appendChild(missionCard);
      });
    }

    async function loadMissions(userId) {
      try {
        missionsPageUserId = userId || null;
        refreshDailyRewardUI(userId);
        if (missionsListUnsub) missionsListUnsub();
        if (missionProgressUnsub) missionProgressUnsub();
        missionsListUnsub = onSnapshot(query(collection(db, "missions"), orderBy("createdAt", "desc")), (snapshot) => {
          missionsRenderCache = snapshot.docs.map(d => ({ id: d.id, ...d.data() }));
          renderMissionsList();
        });
        missionProgressUnsub = onSnapshot(query(collection(db, "userMissionProgress"), where("userId", "==", userId)), (snap) => {
          missionProgressMap = {};
          snap.forEach(d => {
            const data = d.data();
            if (data.missionId) missionProgressMap[data.missionId] = data;
          });
          renderMissionsList();
        });
      } catch(e) {
        console.error(e);
        missionsContainer.innerHTML = '<div class="no-missions">Error loading missions. Please try again later.</div>';
      }
    }

    async function updateMissionProgress(type, amount) {
      if (!currentUser || !amount || amount <= 0) return;
      try {
        const mq = query(collection(db, "missions"), where("type", "==", type));
        const msnap = await getDocs(mq);
        for (const d of msnap.docs) {
          const mission = d.data();
          const target = Math.max(1, mission.target || 1);
          const progRef = doc(db, "userMissionProgress", `${currentUser.uid}_${d.id}`);
          await runTransaction(db, async (transaction) => {
            const p = await transaction.get(progRef);
            const cur = p.exists() ? p.data() : {};
            if (cur.claimed) return;
            const prev = typeof cur.progress === 'number' ? cur.progress : 0;
            const next = Math.min(prev + amount, target);
            transaction.set(progRef, {
              userId: currentUser.uid,
              missionId: d.id,
              progress: next,
              claimed: false,
              updatedAt: serverTimestamp()
            }, { merge: true });
          });
        }
      } catch (e) { console.warn('Mission progress update:', e); }
    }

    async function claimMissionReward(mission, userId) {
      try {
        const progRef = doc(db, "userMissionProgress", `${userId}_${mission.id}`);
        await runTransaction(db, async (transaction) => {
          const p = await transaction.get(progRef);
          const cur = p.exists() ? p.data() : {};
          if (cur.claimed) throw new Error('already');
          const target = Math.max(1, mission.target || 1);
          const prog = typeof cur.progress === 'number' ? cur.progress : 0;
          if (prog < target) throw new Error('notdone');
          const userRef = doc(db, "users", userId);
          const u = await transaction.get(userRef);
          if (!u.exists()) throw new Error('nouser');
          const userData = u.data();
          transaction.update(userRef, {
            coins: (userData.coins || 0) + (mission.rewardCoins || 0),
            stars: (userData.stars || 0) + (mission.rewardStars || 0)
          });
          transaction.set(progRef, {
            userId,
            missionId: mission.id,
            progress: target,
            claimed: true,
            updatedAt: serverTimestamp()
          }, { merge: true });
        });
        showNotification(`Mission completed! You received ${mission.rewardCoins || 0} coins and ${mission.rewardStars || 0} stars!`, "success");
        loadUserBalance(userId);
        syncStarBadgesForUser(userId);
      } catch(e) {
        if (e.message === 'already') showNotification('Reward already claimed', 'error');
        else if (e.message === 'notdone') showNotification('Mission not complete yet', 'error');
        else showNotification("Error claiming reward: " + e.message, "error");
      }
    }

    let friendsListUnsub = null;
    let friendRequestsUnsub = null;

    // ========== Friends List ==========
    async function loadFriends(userId) {
      try {
        if (friendsListUnsub) { friendsListUnsub(); friendsListUnsub = null; }
        if (friendRequestsUnsub) { friendRequestsUnsub(); friendRequestsUnsub = null; }
        const reqBox = document.getElementById('friend-requests-incoming');
        const friendsQuery = query(collection(db, "friends"), where("userId", "==", userId));
        friendsListUnsub = onSnapshot(friendsQuery, async (snapshot) => {
          friendsList.innerHTML = '';
          friendsData = [];
          if (snapshot.empty) {
            friendsList.innerHTML = '<p>No friends yet</p>';
          } else {
            const friendIds = snapshot.docs.map(doc => doc.data().friendId);
            for (let i = 0; i < friendIds.length; i += 10) {
              const batch = friendIds.slice(i, i + 10);
              const usersSnapshot = await getDocs(query(collection(db, "users"), where("__name__", "in", batch)));
              usersSnapshot.forEach(docu => {
                const friend = { id: docu.id, ...docu.data() };
                friendsData.push(friend);
                const friendItem = document.createElement('div');
                friendItem.className = `friend-item${selectedFriend && selectedFriend.id === friend.id ? ' active' : ''}`;
                friendItem.innerHTML = `<div class="friend-avatar">${friend.avatar ? `<img src="${friend.avatar}" alt="${friend.username}" style="width: 100%;height: 100%;object-fit: cover;border-radius: 50%;">` : '👤'}</div><div class="friend-name">${escapeHtml(friend.username || '')}</div>`;
                friendItem.addEventListener('click', () => {
                  document.querySelectorAll('.friend-item').forEach(el => el.classList.remove('active'));
                  friendItem.classList.add('active');
                  selectedFriend = friend;
                  loadFriendChat();
                });
                friendsList.appendChild(friendItem);
              });
            }
          }
        });
        friendRequestsUnsub = onSnapshot(
          query(collection(db, 'friendRequests'), where('toUserId', '==', userId), where('status', '==', 'pending')),
          async (snap) => {
            if (!reqBox) return;
            reqBox.innerHTML = '';
            if (snap.empty) return;
            const title = document.createElement('div');
            title.style.cssText = 'font-weight:800;font-size:0.82rem;color:var(--neon-green);margin-bottom:8px;';
            title.textContent = 'Friend requests';
            reqBox.appendChild(title);
            for (const d of snap.docs) {
              const r = d.data();
              const fromId = r.fromUserId;
              if (!fromId) continue;
              const udoc = await getDoc(doc(db, 'users', fromId));
              const un = udoc.exists() ? (udoc.data().username || fromId) : fromId;
              const row = document.createElement('div');
              row.style.cssText = 'display:flex;align-items:center;justify-content:space-between;gap:8px;padding:8px 10px;background:rgba(0,0,0,0.25);border-radius:10px;margin-bottom:8px;border:1px solid rgba(42,255,158,0.15);';
              row.innerHTML = `<span style="font-size:0.88rem;">${escapeHtml(un)}</span><span style="display:flex;gap:6px;"><button type="button" class="staff-btn staff-btn-primary staff-btn-sm fr-acc" data-req="${d.id}" data-from="${fromId}">Accept</button><button type="button" class="staff-btn staff-btn-sm fr-dec" data-req="${d.id}">Decline</button></span>`;
              reqBox.appendChild(row);
            }
            reqBox.querySelectorAll('.fr-acc').forEach(b => b.addEventListener('click', () => acceptFriendRequest(b.dataset.req, b.dataset.from)));
            reqBox.querySelectorAll('.fr-dec').forEach(b => b.addEventListener('click', () => declineFriendRequest(b.dataset.req)));
          }
        );
      } catch(e) { console.error(e); }
    }

    function formatMessageTime(timestamp) {
      if (!timestamp) return 'Just now';
      let date;
      if (typeof timestamp.toDate === 'function') date = timestamp.toDate();
      else if (typeof timestamp.seconds === 'number') date = new Date(timestamp.seconds * 1000 + Math.floor((timestamp.nanoseconds || 0) / 1e6));
      else date = new Date(timestamp);
      if (Number.isNaN(date.getTime())) return 'Just now';
      const now = new Date();
      const diffMs = now - date;
      const diffSec = Math.floor(diffMs / 1000);
      if (diffSec < 300 || diffMs < 0) return 'Just now';

      const startToday = new Date(now.getFullYear(), now.getMonth(), now.getDate());
      const startMsgDay = new Date(date.getFullYear(), date.getMonth(), date.getDate());
      const dayDiff = Math.round((startToday - startMsgDay) / 86400000);

      const hm = date.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', hour12: false });
      if (dayDiff === 0) return hm;
      if (dayDiff === 1) return `Yesterday at ${hm}`;

      const startOfWeek = new Date(startToday);
      startOfWeek.setDate(startToday.getDate() - startToday.getDay());
      const weekdayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
      if (date >= startOfWeek && date < startToday) {
        return weekdayNames[date.getDay()];
      }

      const yy = date.getFullYear().toString().slice(-2);
      const mm = (date.getMonth() + 1).toString().padStart(2, '0');
      const dd = date.getDate().toString().padStart(2, '0');
      const hh = date.getHours().toString().padStart(2, '0');
      const min = date.getMinutes().toString().padStart(2, '0');
      return `${yy}/${mm}/${dd} ${hh}:${min}`;
    }

    let chatMentionDir = null;
    let chatMentionDirPromise = null;
    async function ensureChatMentionDirectory() {
      if (chatMentionDir) return chatMentionDir;
      if (chatMentionDirPromise) return chatMentionDirPromise;
      chatMentionDirPromise = (async () => {
        const dir = {};
        try {
          const [usersSnap, titlesSnap] = await Promise.all([
            getDocs(collection(db, 'users')),
            getDocs(collection(db, 'titles'))
          ]);
          const titlesByName = {};
          titlesSnap.docs.forEach(d => { const t = d.data(); if (t.name) titlesByName[t.name] = t; });
          usersSnap.docs.forEach(d => {
            const u = d.data();
            const un = (u.username || '').trim();
            if (!un) return;
            const key = un.toLowerCase();
            let titleStyle = 'color:var(--neon-green);font-weight:800;';
            const td = titlesByName[u.title || 'User'];
            if (td?.isGradient && td.gradientColors?.length >= 2) {
              titleStyle = `background:linear-gradient(90deg,${td.gradientColors.join(',')});-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;font-weight:900;${td.isRunning ? 'background-size:300% 100%;animation:runGradient 3s linear infinite;' : ''}`;
            } else if (td?.color) titleStyle = `color:${td.color};font-weight:800;`;
            dir[key] = { id: d.id, display: un, titleStyle };
          });
        } catch (e) { console.warn('chat mention dir', e); }
        chatMentionDir = dir;
        return dir;
      })();
      return chatMentionDirPromise;
    }

    async function refreshChatMentionDirectory() {
      chatMentionDir = null;
      chatMentionDirPromise = null;
      return ensureChatMentionDirectory();
    }

    function mergeChatMentionEntry(username, userId, titleStyle) {
      const un = (username || '').trim();
      if (!un || !userId) return;
      if (!chatMentionDir) chatMentionDir = {};
      const key = un.toLowerCase();
      chatMentionDir[key] = { id: userId, display: un, titleStyle: titleStyle || 'color:var(--neon-green);font-weight:800;' };
    }

    async function resolveUsernameTitleStyle(username) {
      const q = (username || '').trim();
      if (!q) return 'color:var(--neon-green);font-weight:800;';
      try {
        const usersSnap = await getDocs(query(collection(db, 'users'), where('username', '==', q)));
        let udoc = usersSnap.empty ? null : usersSnap.docs[0];
        if (!udoc) {
          const lower = q.toLowerCase();
          const all = await getDocs(collection(db, 'users'));
          udoc = all.docs.find(d => (d.data().username || '').toLowerCase() === lower) || null;
        }
        if (!udoc) return 'color:var(--neon-green);font-weight:800;';
        const u = udoc.data();
        const titlesSnap = await getDocs(query(collection(db, 'titles'), where('name', '==', u.title || 'User')));
        if (titlesSnap.empty) return 'color:var(--neon-green);font-weight:800;';
        const td = titlesSnap.docs[0].data();
        if (td.isGradient && td.gradientColors?.length >= 2) {
          return `background:linear-gradient(90deg,${td.gradientColors.join(',')});-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;font-weight:900;${td.isRunning ? 'background-size:300% 100%;animation:runGradient 3s linear infinite;' : ''}`;
        }
        if (td.color) return `color:${td.color};font-weight:800;`;
      } catch (e) { console.warn('resolveUsernameTitleStyle', e); }
      return 'color:var(--neon-green);font-weight:800;';
    }

    async function renderChatMessageBodyHtml(rawText) {
      const text = rawText == null ? '' : String(rawText);
      await ensureChatMentionDirectory();
      const dir = chatMentionDir || {};
      const re = /@([\w.\-]{2,40})/g;
      let out = '';
      let last = 0;
      let m;
      while ((m = re.exec(text)) !== null) {
        out += escapeHtml(text.slice(last, m.index));
        const rawHandle = m[1];
        const key = rawHandle.toLowerCase();
        let info = dir[key];
        if (!info) {
          const uid = await findUserIdByUsername(rawHandle);
          if (uid) {
            const ts = await resolveUsernameTitleStyle(rawHandle);
            const display = (await getChatUserData(uid))?.username || rawHandle;
            info = { id: uid, display: String(display).trim() || rawHandle, titleStyle: ts };
            mergeChatMentionEntry(info.display, uid, ts);
          }
        }
        if (info) {
          out += `<span class="chat-at-mention" role="link" tabindex="0" data-user-id="${escapeHtml(info.id)}" style="${info.titleStyle}">@${escapeHtml(info.display)}</span>`;
        } else {
          out += escapeHtml(m[0]);
        }
        last = m.index + m[0].length;
      }
      out += escapeHtml(text.slice(last));
      return out;
    }

    let chatUserCache = {};
    async function getChatUserData(uid) {
      if (chatUserCache[uid]) return chatUserCache[uid];
      try {
        const u = await getDoc(doc(db, 'users', uid));
        if (u.exists()) { chatUserCache[uid] = u.data(); return u.data(); }
      } catch(e) {}
      return null;
    }

    function startChatComposerLoading(wrapEl) {
      if (!wrapEl) return;
      wrapEl.classList.add('chat-composer-loading');
      const overlay = wrapEl.querySelector('.chat-loading-full');
      if (overlay) {
        overlay.classList.remove('hidden');
        overlay.setAttribute('aria-busy', 'true');
      }
      wrapEl.querySelectorAll('.chat-input input[type="text"], .chat-input button').forEach(el => {
        if (el && el.type !== 'file') el.disabled = true;
      });
    }

    function getChatComposerWrap(chatContainerEl) {
      return chatContainerEl && chatContainerEl.closest('.chat-composer-wrap, .friend-chat-composer-wrap');
    }

    function scrollChatToBottom(chatContainerEl) {
      if (!chatContainerEl) return;
      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          chatContainerEl.scrollTop = chatContainerEl.scrollHeight;
        });
      });
    }

    function finishChatComposerLoading(chatContainerEl) {
      const wrapEl = getChatComposerWrap(chatContainerEl);
      if (!wrapEl) return;
      const overlay = wrapEl.querySelector('.chat-loading-full');
      const scrollAndReveal = () => {
        chatContainerEl.scrollTop = chatContainerEl.scrollHeight;
        if (overlay) {
          overlay.classList.add('hidden');
          overlay.setAttribute('aria-busy', 'false');
        }
        wrapEl.classList.remove('chat-composer-loading');
        wrapEl.querySelectorAll('.chat-input input[type="text"], .chat-input button').forEach(el => {
          if (el && el.type !== 'file') el.disabled = false;
        });
        wrapEl.dataset.chatReady = 'true';
      };
      requestAnimationFrame(() => {
        requestAnimationFrame(scrollAndReveal);
      });
    }

    async function buildChatMessageElement(docSnap, options) {
      const opts = options || {};
      const msg = docSnap.data();
      const div = document.createElement('div');
      div.className = `chat-message ${msg.senderId === currentUser?.uid ? 'user' : 'friend'}`;
      div.dataset.msgId = docSnap.id;
      div.dataset.senderId = msg.senderId || '';
      if (msg.type === 'system') {
        div.innerHTML = `<div class="message-content">${escapeHtml(msg.message || '')}</div><div class="message-time">${formatMessageTime(msg.timestamp)}</div>`;
        return div;
      }
      const userData = await getChatUserData(msg.senderId);
      let titleStyle = 'color:var(--neon-green);';
      let topBadgeHtml = '';
      if (userData) {
        try {
          const titlesSnap = await getDocs(query(collection(db, 'titles'), where('name', '==', userData.title || 'User')));
          if (!titlesSnap.empty) {
            const td = titlesSnap.docs[0].data();
            if (td.isGradient && td.gradientColors?.length >= 2) {
              titleStyle = `background:linear-gradient(90deg,${td.gradientColors.join(',')});-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-weight:900;${td.isRunning ? 'background-size:300% 100%;animation:runGradient 3s linear infinite;' : ''}`;
            } else if (td.color) {
              titleStyle = `color:${td.color};`;
            }
          }
        } catch(e) {}
        if (userData.badges?.length > 0) {
          try {
            const badgesSnap = await getDocs(collection(db, 'badges'));
            const bd = badgesSnap.docs.map(d=>d.data()).find(b=>b.name===userData.badges[0]);
            if (bd?.icon && /^https?:\/\//i.test(bd.icon)) topBadgeHtml = `<img class="chat-msg-badge" src="${escapeHtml(bd.icon)}" alt="${escapeHtml(bd.name)}" title="${escapeHtml(bd.name)}">`;
          } catch(e) {}
        }
      }
      const safeName = escapeHtml(msg.senderName || '');
      const av = escapeHtml(msg.senderAvatar || 'https://t3.ftcdn.net/jpg/00/64/67/80/360_F_64678017_zUpiZFjj04cnLri7oADnyMH0XBYyQghG.jpg');
      const hideHeader = !!opts.groupWithPrevious;
      if (hideHeader) div.classList.add('chat-msg-grouped');
      const timeStr = formatMessageTime(msg.timestamp);
      const imgUrl = (msg.imageUrl || '').trim();
      const safeImg = imgUrl && /^https?:\/\//i.test(imgUrl) ? escapeHtml(imgUrl) : '';
      div.innerHTML = `
        <div class="chat-msg-header">
          <img class="chat-avatar" src="${av}" alt="${safeName}" data-user-id="${escapeHtml(msg.senderId)}" data-username="${safeName}">
          <span class="message-sender clickable-username" style="${titleStyle}" data-user-id="${escapeHtml(msg.senderId)}" data-username="${safeName}">${safeName}</span>
          ${topBadgeHtml}
          <span class="message-time">${escapeHtml(timeStr)}</span>
        </div>
        <div class="message-content chat-msg-body"></div>
        ${hideHeader ? `<div class="message-time message-time-inline">${escapeHtml(timeStr)}</div>` : ''}
      `;
      const body = div.querySelector('.chat-msg-body');
      if (body) {
        body.innerHTML = '';
        if (safeImg) {
          const im = document.createElement('img');
          im.className = 'chat-msg-img';
          im.src = imgUrl;
          im.alt = 'Chat image';
          im.loading = 'lazy';
          im.addEventListener('click', () => window.open(imgUrl, '_blank', 'noopener'));
          body.appendChild(im);
        }
        const text = (msg.message || '').trim();
        if (text) {
          const span = document.createElement('span');
          span.className = 'chat-msg-text';
          span.innerHTML = await renderChatMessageBodyHtml(text);
          body.appendChild(span);
        }
      }
      div.querySelectorAll('.clickable-username, .chat-avatar').forEach(el => {
        el.addEventListener('click', () => showUserProfileModal(el.dataset.userId, el.dataset.username));
      });
      div.querySelectorAll('.chat-at-mention').forEach(el => {
        const uid = el.dataset.userId;
        const open = () => { if (uid) navigateToUserProfile(uid); };
        el.addEventListener('click', open);
        el.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); open(); } });
      });
      return div;
    }

    async function syncChatFromSnapshot(container, snapshot, renderedIdSet) {
      renderedIdSet.clear();
      container.querySelectorAll('.chat-message').forEach(n => n.remove());
      const docs = snapshot.docs.slice().sort((a, b) => {
        const ta = a.data().timestamp?.toMillis ? a.data().timestamp.toMillis() : 0;
        const tb = b.data().timestamp?.toMillis ? b.data().timestamp.toMillis() : 0;
        return ta - tb;
      });
      let prevSender = null;
      for (const d of docs) {
        renderedIdSet.add(d.id);
        const msg = d.data();
        const group = prevSender && msg.senderId === prevSender && msg.type !== 'system';
        container.appendChild(await buildChatMessageElement(d, { groupWithPrevious: group }));
        if (msg.type !== 'system') prevSender = msg.senderId || null;
        else prevSender = null;
      }
      const wrapEl = getChatComposerWrap(container);
      if (wrapEl && wrapEl.classList.contains('chat-composer-loading')) {
        finishChatComposerLoading(container);
      } else {
        scrollChatToBottom(container);
      }
    }

    async function applyChatSnapshot(container, snapshot, renderedIdSet) {
      const fromCache = snapshot.metadata && snapshot.metadata.fromCache === true;
      const hasLocalAdds = snapshot.docChanges().some(c => c.type === 'added');
      if (fromCache && hasLocalAdds) {
        await syncChatFromSnapshot(container, snapshot, renderedIdSet);
        return;
      }
      const changes = snapshot.docChanges();
      for (const ch of changes) {
        if (ch.type === 'removed') {
          const el = container.querySelector(`[data-msg-id="${ch.doc.id}"]`);
          if (el) el.remove();
          renderedIdSet.delete(ch.doc.id);
        }
      }
      const added = changes.filter(c => c.type === 'added').map(c => c.doc);
      added.sort((a, b) => {
        const ta = a.data().timestamp?.toMillis ? a.data().timestamp.toMillis() : 0;
        const tb = b.data().timestamp?.toMillis ? b.data().timestamp.toMillis() : 0;
        return ta - tb;
      });
      let appendedAny = false;
      for (const d of added) {
        if (renderedIdSet.has(d.id)) continue;
        renderedIdSet.add(d.id);
        const msg = d.data();
        const last = container.querySelector('.chat-message:last-of-type');
        const group = last && last.dataset.senderId && msg.senderId === last.dataset.senderId && msg.type !== 'system';
        container.appendChild(await buildChatMessageElement(d, { groupWithPrevious: group }));
        appendedAny = true;
      }
      const wrapEl = getChatComposerWrap(container);
      if (wrapEl && wrapEl.classList.contains('chat-composer-loading')) {
        finishChatComposerLoading(container);
      } else if (appendedAny) {
        scrollChatToBottom(container);
      }
    }

    // ========== Global Chat ==========
    async function loadGlobalChat() {
      if (globalChatUnsubscribe) globalChatUnsubscribe();
      globalChatRenderedIds = new Set();
      const wrap = document.getElementById('global-chat-composer-wrap');
      startChatComposerLoading(wrap);
      globalChatContainer.innerHTML = '';
      await refreshChatMentionDirectory();
      const chatQuery = query(collection(db, "chats"), where("type", "==", "global"), orderBy("timestamp", "desc"), limit(50));
      globalChatUnsubscribe = onSnapshot(chatQuery, (snapshot) => {
        applyChatSnapshot(globalChatContainer, snapshot, globalChatRenderedIds).catch(e => console.warn(e));
      });
    }

    async function isUserMuted() {
      if (!currentUser) return false;
      const ud = await getDoc(doc(db, "users", currentUser.uid));
      if (!ud.exists()) return false;
      const d = ud.data();
      if (d.muteStatus === 'perm') { showNotification('You are permanently muted.', 'error'); return true; }
      if (d.muteStatus === 'temp' && d.muteUntil) {
        const until = new Date(d.muteUntil.toDate ? d.muteUntil.toDate() : d.muteUntil);
        if (until > new Date()) { showNotification(`You are muted until ${until.toLocaleString()}.`, 'error'); return true; }
        else { await updateDoc(doc(db, "users", currentUser.uid), { muteStatus: 'none', muteUntil: null }); }
      }
      return false;
    }

    async function uploadChatImageFile(file) {
      if (!file || !currentUser) return null;
      const storageRef = ref(storage, `chatImages/${currentUser.uid}_${Date.now()}_${file.name.replace(/[^\w.\-]+/g, '_')}`);
      const snapshot = await uploadBytes(storageRef, file);
      return getDownloadURL(snapshot.ref);
    }

    function promptChatImageUrl() {
      if (!canUserPasteImageLinks()) {
        showNotification('Only staff can attach images by URL.', 'error');
        return null;
      }
      const raw = window.prompt('Image URL (https://...)');
      if (raw == null) return null;
      const u = raw.trim();
      if (!u) return null;
      if (!/^https?:\/\//i.test(u)) {
        showNotification('Please use an http(s) image URL.', 'error');
        return null;
      }
      return u;
    }

    async function sendGlobalChatMessage() {
      const message = globalChatInput.value.trim();
      if (!currentUser) return;
      if (await isUserMuted()) return;
      if (!message) return;
      const userDoc = await getDoc(doc(db, "users", currentUser.uid));
      const userData = userDoc.data();
      await addDoc(collection(db, "chats"), {
        message,
        senderId: currentUser.uid,
        senderName: userData.username || currentUser.email,
        senderAvatar: userData.avatar || "https://t3.ftcdn.net/jpg/00/64/67/80/360_F_64678017_zUpiZFjj04cnLri7oADnyMH0XBYyQghG.jpg",
        timestamp: serverTimestamp(),
        type: 'global'
      });
      globalChatInput.value = '';
      refreshChatMentionDirectory().catch(() => {});
      updateMissionProgress('chatting', 1);
    }

    async function sendGlobalChatWithImage(imageUrl, caption) {
      if (!currentUser || !imageUrl) return;
      if (await isUserMuted()) return;
      const userDoc = await getDoc(doc(db, "users", currentUser.uid));
      const userData = userDoc.data();
      await addDoc(collection(db, "chats"), {
        message: caption || '',
        imageUrl,
        senderId: currentUser.uid,
        senderName: userData.username || currentUser.email,
        senderAvatar: userData.avatar || "https://t3.ftcdn.net/jpg/00/64/67/80/360_F_64678017_zUpiZFjj04cnLri7oADnyMH0XBYyQghG.jpg",
        timestamp: serverTimestamp(),
        type: 'global'
      });
      globalChatInput.value = '';
      refreshChatMentionDirectory().catch(() => {});
      updateMissionProgress('chatting', 1);
    }

    // ========== Friend Chat ==========
    async function loadFriendChat() {
      if (friendChatUnsubscribe) friendChatUnsubscribe();
      const fWrap = document.getElementById('friend-chat-composer-wrap');
      if (!selectedFriend) {
        friendChatHeader.innerHTML = '<span class="friend-chat-placeholder">Select a friend to start chatting</span>';
        friendChatContainer.innerHTML = '';
        friendChatInputArea.style.display = 'none';
        fWrap?.classList.remove('is-visible');
        document.getElementById('friend-chat-input-loading')?.classList.add('hidden');
        if (fWrap) fWrap.classList.remove('chat-composer-loading');
        return;
      }
      fWrap?.classList.add('is-visible');
      friendChatHeader.innerHTML = `${selectedFriend.avatar ? `<img src="${selectedFriend.avatar}" alt="${selectedFriend.username}">` : '<i class="fas fa-user"></i>'} <span>${selectedFriend.username}</span>`;
      friendChatInputArea.style.display = 'flex';
      friendChatRenderedIds = new Set();
      startChatComposerLoading(fWrap);
      friendChatContainer.innerHTML = '';
      await refreshChatMentionDirectory();
      const uids = [currentUser.uid, selectedFriend.id].sort();
      const chatId = `${uids[0]}_${uids[1]}`;
      const chatQuery = query(collection(db, "chats"), where("chatId", "==", chatId), orderBy("timestamp", "desc"), limit(50));
      friendChatUnsubscribe = onSnapshot(chatQuery, (snapshot) => {
        applyChatSnapshot(friendChatContainer, snapshot, friendChatRenderedIds).catch(e => console.warn(e));
      });
    }

    async function sendFriendChatMessage() {
      const message = friendChatInput.value.trim();
      if (!currentUser || !selectedFriend) return;
      if (await isUserMuted()) return;
      if (!message) return;
      const userDoc = await getDoc(doc(db, "users", currentUser.uid));
      const userData = userDoc.data();
      const uids = [currentUser.uid, selectedFriend.id].sort();
      await addDoc(collection(db, "chats"), {
        message,
        senderId: currentUser.uid,
        senderName: userData.username || currentUser.email,
        senderAvatar: userData.avatar || "https://t3.ftcdn.net/jpg/00/64/67/80/360_F_64678017_zUpiZFjj04cnLri7oADnyMH0XBYyQghG.jpg",
        timestamp: serverTimestamp(),
        type: 'private',
        chatId: `${uids[0]}_${uids[1]}`,
        participants: [currentUser.uid, selectedFriend.id]
      });
      friendChatInput.value = '';
      refreshChatMentionDirectory().catch(() => {});
      updateMissionProgress('chatting', 1);
    }

    async function sendFriendChatWithImage(imageUrl, caption) {
      if (!currentUser || !selectedFriend || !imageUrl) return;
      if (await isUserMuted()) return;
      const userDoc = await getDoc(doc(db, "users", currentUser.uid));
      const userData = userDoc.data();
      const uids = [currentUser.uid, selectedFriend.id].sort();
      await addDoc(collection(db, "chats"), {
        message: caption || '',
        imageUrl,
        senderId: currentUser.uid,
        senderName: userData.username || currentUser.email,
        senderAvatar: userData.avatar || "https://t3.ftcdn.net/jpg/00/64/67/80/360_F_64678017_zUpiZFjj04cnLri7oADnyMH0XBYyQghG.jpg",
        timestamp: serverTimestamp(),
        type: 'private',
        chatId: `${uids[0]}_${uids[1]}`,
        participants: [currentUser.uid, selectedFriend.id]
      });
      friendChatInput.value = '';
      refreshChatMentionDirectory().catch(() => {});
      updateMissionProgress('chatting', 1);
    }

    async function findUserIdByUsername(username) {
      const q = (username || '').trim();
      if (!q) return null;
      const exact = await getDocs(query(collection(db, 'users'), where('username', '==', q)));
      if (!exact.empty) return exact.docs[0].id;
      const lower = q.toLowerCase();
      const all = await getDocs(collection(db, 'users'));
      const hit = all.docs.find(d => (d.data().username || '').toLowerCase() === lower);
      return hit ? hit.id : null;
    }

    async function sendFriendRequestToUserId(targetUid) {
      if (!currentUser || !targetUid || targetUid === currentUser.uid) return;
      const dup = await getDocs(query(collection(db, 'friendRequests'),
        where('fromUserId', '==', currentUser.uid),
        where('toUserId', '==', targetUid),
        where('status', '==', 'pending')));
      if (!dup.empty) { showNotification('Request already sent', 'error'); return; }
      const already = await getDocs(query(collection(db, 'friends'), where('userId', '==', currentUser.uid), where('friendId', '==', targetUid)));
      if (!already.empty) { showNotification('Already friends', 'error'); return; }
      await addDoc(collection(db, 'friendRequests'), {
        fromUserId: currentUser.uid,
        toUserId: targetUid,
        status: 'pending',
        createdAt: serverTimestamp()
      });
      showNotification('Friend request sent', 'success');
    }

    async function addFriend() {
      const raw = friendEmailInput.value.trim();
      if (!raw || !currentUser) return;
      const friendId = await findUserIdByUsername(raw);
      if (!friendId) { showNotification('User not found', 'error'); return; }
      await sendFriendRequestToUserId(friendId);
      friendEmailInput.value = '';
    }

    async function acceptFriendRequest(reqId, fromUid) {
      if (!currentUser || !fromUid) return;
      try {
        const batch = writeBatch(db);
        batch.update(doc(db, 'friendRequests', reqId), { status: 'accepted', respondedAt: serverTimestamp() });
        batch.set(doc(collection(db, 'friends')), { userId: currentUser.uid, friendId: fromUid, createdAt: serverTimestamp() });
        batch.set(doc(collection(db, 'friends')), { userId: fromUid, friendId: currentUser.uid, createdAt: serverTimestamp() });
        await batch.commit();
        showNotification('You are now friends', 'success');
        loadFriends(currentUser.uid);
        updateStats(currentUser.uid);
      } catch (e) { showNotification('Could not accept: ' + e.message, 'error'); }
    }

    async function declineFriendRequest(reqId) {
      try {
        await updateDoc(doc(db, 'friendRequests', reqId), { status: 'declined', respondedAt: serverTimestamp() });
        loadFriends(currentUser.uid);
      } catch (e) { showNotification(e.message, 'error'); }
    }

    // ===== Navigate to user profile page (full page, not popup) =====
    let previousPageId = null;
    let viewProfileTargetUid = null;
    async function initViewProfilePageFromUrl() {
      const params = new URLSearchParams(window.location.search || '');
      const uid = (params.get('uid') || '').trim();
      if (!uid) { showNotification('User not found', 'error'); return; }
      viewProfileTargetUid = uid;
      const addBtn = document.getElementById('vp-add-friend-btn');
      try {
        await populateProfileLayout(uid, {
          avatar: 'vp-avatar', banner: 'vp-banner', bannerWrap: 'vp-banner-wrap',
          username: 'vp-username', title: 'vp-title',
          badges: 'vp-badges',
          starsVal: 'vp-stars', playtimeVal: 'vp-playtime',
          playCount: 'vp-plays', friendCount: 'vp-friends',
          blookCount: 'vp-blook-count', blookTotal: 'vp-blook-total',
          blookProgress: 'vp-blook-progress',
          topBlooks: 'vp-top-blooks', friendsList: 'vp-friends-list',
        });
        if (addBtn && currentUser) {
          if (uid === currentUser.uid) addBtn.style.display = 'none';
          else {
            addBtn.style.display = 'inline-flex';
            const fr = await getDocs(query(collection(db, 'friends'), where('userId', '==', currentUser.uid), where('friendId', '==', uid)));
            addBtn.textContent = fr.empty ? 'Add friend' : 'Friends';
            addBtn.disabled = !fr.empty;
          }
        }
      } catch (e) { console.error(e); showNotification('Could not load profile', 'error'); }
    }
    async function navigateToUserProfile(userId) {
      if (!userId) return;
      if (!GU_IS_LEGACY_SINGLE_PAGE && GU_DASHBOARD_BASE) {
        if (currentUser && userId === currentUser.uid) {
          window.location.href = `${GU_DASHBOARD_BASE}/profile/`;
        } else {
          window.location.href = `${GU_DASHBOARD_BASE}/user/?uid=${encodeURIComponent(userId)}`;
        }
        return;
      }
      if (currentUser && userId === currentUser.uid) {
        document.querySelectorAll('.tab-button').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
        document.querySelector('[data-page="profile-page"]').classList.add('active');
        document.getElementById('profile-page').classList.add('active');
        loadProfilePage(currentUser.uid);
        return;
      }
      previousPageId = document.querySelector('.page.active')?.id || 'main-page';
      document.querySelectorAll('.tab-button').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
      document.getElementById('view-profile-page').classList.add('active');
      viewProfileTargetUid = userId;
      const addBtn = document.getElementById('vp-add-friend-btn');
      try {
        await populateProfileLayout(userId, {
          avatar: 'vp-avatar', banner: 'vp-banner', bannerWrap: 'vp-banner-wrap',
          username: 'vp-username', title: 'vp-title',
          badges: 'vp-badges',
          starsVal: 'vp-stars', playtimeVal: 'vp-playtime',
          playCount: 'vp-plays', friendCount: 'vp-friends',
          blookCount: 'vp-blook-count', blookTotal: 'vp-blook-total',
          blookProgress: 'vp-blook-progress',
          topBlooks: 'vp-top-blooks', friendsList: 'vp-friends-list',
        });
        if (addBtn && currentUser) {
          if (userId === currentUser.uid) addBtn.style.display = 'none';
          else {
            addBtn.style.display = 'inline-flex';
            const fr = await getDocs(query(collection(db, 'friends'), where('userId', '==', currentUser.uid), where('friendId', '==', userId)));
            addBtn.textContent = fr.empty ? 'Add friend' : 'Friends';
            addBtn.disabled = !fr.empty;
          }
        }
      } catch(e) { console.error(e); showNotification("Could not load profile", "error"); }
    }
    function showUserProfileModal(userId, username) { navigateToUserProfile(userId); }
    window.showUserModal = showUserProfileModal;

    document.getElementById('vp-back-btn')?.addEventListener('click', () => {
      if (!GU_IS_LEGACY_SINGLE_PAGE) {
        window.history.back();
        return;
      }
      document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
      const target = previousPageId || 'main-page';
      document.getElementById(target).classList.add('active');
      const tabBtn = document.querySelector(`[data-page="${target}"]`);
      if (tabBtn) { document.querySelectorAll('.tab-button').forEach(b => b.classList.remove('active')); tabBtn.classList.add('active'); }
      previousPageId = null;
      viewProfileTargetUid = null;
    });
    document.getElementById('vp-add-friend-btn')?.addEventListener('click', async () => {
      if (!currentUser || !viewProfileTargetUid || viewProfileTargetUid === currentUser.uid) return;
      await sendFriendRequestToUserId(viewProfileTargetUid);
    });

    // ========== Send gift from chat (fully implemented) ==========
    async function sendGiftToFriend(recipientId, cardId, card) {
      if (!currentUser) return;
      try {
        const cardDoc = await getDoc(doc(db, "inventory", cardId));
        if (!cardDoc.exists()) { showNotification("Item not found", "error"); return; }
        const cardData = cardDoc.data();
        const batch = writeBatch(db);
        batch.delete(doc(db, "inventory", cardId));
        const recipientRef = doc(db, "users", recipientId);
        const recipientSnap = await getDoc(recipientRef);
        const newStars = (recipientSnap.data().stars || 0) + (cardData.starsGained || 0);
        batch.update(recipientRef, { stars: newStars });
        await batch.commit();
        syncStarBadgesForUser(recipientId, newStars);
        await addDoc(collection(db, "giftwall"), {
          recipientId, senderId: currentUser.uid, senderName: currentUser.displayName || currentUser.email,    
          itemName: cardData.itemName, rarity: cardData.rarity, imageUrl: cardData.imageUrl || null,    
          value: cardData.starsGained || 0, stars: cardData.starsGained || 0, timestamp: serverTimestamp()
        });
        showNotification(`Gift sent! ${cardData.itemName} delivered.`, "success");
        if (recipientId === currentUser.uid) {
          loadUserBalance(currentUser.uid);
        }
      } catch(e) { showNotification("Error sending gift: " + e.message, "error"); }
    }

    // Send gift button (removed with old modal - kept as no-op)
    modalSendGiftBtn?.addEventListener('click', async () => {
      if (selectedUserForGift) {
        await loadSenderGiftCards(currentUser.uid);
        sendGiftInventoryModal.style.display = 'flex';
      }
    });

    async function loadSenderGiftCards(userId) {
      try {
        const invSnap = await getDocs(query(collection(db, "inventory"), where("userId", "==", userId)));
        senderGiftCardsList.innerHTML = '';
        if (invSnap.empty) { senderGiftCardsList.innerHTML = '<p>No items available</p>'; return; }
        invSnap.forEach(doc => {
          const card = { id: doc.id, ...doc.data() };
          const cardElement = document.createElement('div');
          cardElement.className = 'gift-card-item';
          cardElement.innerHTML = `
            <div class="gift-card-item-icon ${card.rarity}">${card.imageUrl ? `<img src="${card.imageUrl}" alt="${card.itemName}" style="width: 100%;height: 100%;object-fit: cover;border-radius: 10px;">` : '<i class="fas fa-gift"></i>'}</div>
            <div class="gift-card-item-info"><div class="gift-card-item-name">${card.itemName}</div><div class="gift-card-item-rarity">${card.rarity}</div><div class="gift-card-item-value">Stars: ${card.starsGained || 0}</div></div>
          `;
          cardElement.addEventListener('click', async () => {
            if (selectedUserForGift && selectedUserForGift.id) {
              await sendGiftToFriend(selectedUserForGift.id, card.id, card);
              sendGiftInventoryModal.style.display = 'none';
              if (userProfileModal) userProfileModal.style.display = 'none';
            }
          });
          senderGiftCardsList.appendChild(cardElement);
        });
      } catch(e) { showNotification("Error loading items", "error"); console.error(e); }
    }

    cancelSendGiftInventoryBtn.addEventListener('click', () => { sendGiftInventoryModal.style.display = 'none'; });

    // ========== Load user balance (coins & stars) ==========
    async function loadUserBalance(userId) {
      if (userUnsubscribe) userUnsubscribe();
      userUnsubscribe = onSnapshot(doc(db, "users", userId), (docSnap) => {
        if (docSnap.exists()) {
          const data = docSnap.data();
          coinsDisplay.textContent = data.coins || 0;
          starsDisplay.textContent = data.stars || 0;
          shopCoins.textContent = data.coins || 0;
          shopStars.textContent = data.stars || 0;
          const itd = document.getElementById('inv-tokens-display');
          if (itd) itd.textContent = data.coins || 0;
          refreshStarDisplayBadges(data.badges || []);
        }
      });
    }

    // ========== Gift wall (received gifts) ==========
    async function loadGiftWall(userId) {
      if (giftWallUnsubscribe) giftWallUnsubscribe();
      const giftWallQuery = query(collection(db, "giftwall"), where("recipientId", "==", userId), orderBy("timestamp", "desc"));
      giftWallUnsubscribe = onSnapshot(giftWallQuery, (snapshot) => {
        giftWallContainer.innerHTML = '';
        if (snapshot.empty) { giftWallContainer.innerHTML = '<p>No gifts received yet</p>'; return; }
        snapshot.forEach(doc => {
          const gift = doc.data();
          const giftCard = document.createElement('div');
          giftCard.className = `gift-card ${gift.rarity}`;
          giftCard.innerHTML = `
            <div class="gift-icon">${gift.imageUrl ? `<img src="${gift.imageUrl}" alt="${gift.itemName}" style="width: 100%;height: 100%;object-fit: cover;border-radius: 50%;">` : '<i class="fas fa-gift"></i>'}</div>
            <div class="gift-name">${gift.itemName}</div>
            <div class="gift-sender">From: ${gift.senderName}</div>
            <div class="gift-stats"><div class="gift-stat"><div class="gift-stat-value">${gift.value}</div><div class="gift-stat-label">Value</div></div><div class="gift-stat"><div class="gift-stat-value">${gift.stars}</div><div class="gift-stat-label">Stars</div></div></div>
          `;
          giftWallContainer.appendChild(giftCard);
        });
      });
    }

    // ========== Settings ==========
    async function loadSettings(userId) {
      const userDoc = await getDoc(doc(db, "users", userId));
      if (userDoc.exists()) {
        currentAvatar.src = userDoc.data().avatar || "https://t3.ftcdn.net/jpg/00/64/67/80/360_F_64678017_zUpiZFjj04cnLri7oADnyMH0XBYyQghG.jpg";
        usernameInput.value = userDoc.data().username || "";
      }
      const urlIn = document.getElementById('avatar-url-input');
      if (urlIn) urlIn.value = '';
      applyNonStaffMediaUi();
    }

    saveSettingsBtn.addEventListener('click', async () => {
      if (!currentUser) return;
      try {
        const updates = {};
        const newUsername = usernameInput.value.trim();
        if (newUsername && newUsername !== currentUser.email.split('@')[0]) {
          updates.username = newUsername;
        }
        
        const newPasswordValue = newPassword.value;
        const confirmNewPasswordValue = confirmPassword.value;
        
        if (newPasswordValue || confirmNewPasswordValue) {
          if (newPasswordValue !== confirmNewPasswordValue) {
            showNotification("New passwords do not match", "error");
            return;
          }
          if (newPasswordValue.length < 6) {
            showNotification("Password must be at least 6 characters", "error");
            return;
          }
          const currentPasswordValue = currentPassword.value;
          if (!currentPasswordValue) {
            showNotification("Please enter your current password", "error");
            return;
          }
          // Re-authenticate user before updating password
          try {
            await updatePassword(currentUser, newPasswordValue);
            showNotification("Password updated successfully", "success");
          } catch (error) {
            showNotification("Error updating password: " + error.message, "error");
            return;
          }
        }
        
        const avatarUrlField = canUserPasteImageLinks() ? document.getElementById('avatar-url-input')?.value.trim() : '';
        if (avatarUrlField) updates.avatar = avatarUrlField;

        const avatarFile = avatarUploadInput.files[0];
        if (avatarFile) {
          try {
            const storageRef = ref(storage, `avatars/${currentUser.uid}_${Date.now()}_${avatarFile.name}`);
            const snapshot = await uploadBytes(storageRef, avatarFile);
            const avatarUrl = await getDownloadURL(snapshot.ref);
            updates.avatar = avatarUrl;
          } catch (error) {
            showNotification("Error uploading avatar: " + error.message, "error");
            return;
          }
        }
        
        if (Object.keys(updates).length > 0) {
          await updateDoc(doc(db, "users", currentUser.uid), updates);
          showNotification("Settings updated successfully", "success");
          // Reload user profile to reflect changes
          await loadUserProfile(currentUser);
        } else {
          showNotification("No changes to save", "info");
        }
      } catch (error) {
        showNotification("Error updating settings: " + error.message, "error");
      }
    });

    // ========== Permission System ==========
    const ALL_PERMISSIONS = [
      { key: 'manage_games', label: 'Manage Games', desc: 'Add, edit, delete games' },
      { key: 'manage_tags', label: 'Manage Tags', desc: 'Add and delete tags' },
      { key: 'manage_packs', label: 'Manage Packs & Items', desc: 'Create, edit, delete packs' },
      { key: 'manage_users', label: 'Manage Users', desc: 'Edit user profiles, coins, stars' },
      { key: 'manage_titles', label: 'Manage Titles', desc: 'Create and edit titles' },
      { key: 'manage_badges', label: 'Manage Badges', desc: 'Create and assign badges' },
      { key: 'view_plays', label: 'View Play Logs & IP', desc: 'View play history and IP' },
      { key: 'delete_plays', label: 'Delete Play Records', desc: 'Remove play records' },
      { key: 'view_chats', label: 'View All Chats', desc: 'View global and private chats' },
      { key: 'delete_chats', label: 'Delete Chat Messages', desc: 'Remove chat messages' },
      { key: 'manage_inventory', label: 'Manage User Inventory', desc: 'View and delete user items' },
      { key: 'assign_titles', label: 'Assign Titles to Users', desc: 'Change user titles' },
      { key: 'assign_badges', label: 'Assign Badges to Users', desc: 'Give badges to users' },
      { key: 'custom_avatar', label: 'Set Custom Avatar for Users', desc: 'Change any user avatar' },
      { key: 'create_gradient_titles', label: 'Create Gradient Titles', desc: 'Create titles with gradient colors' },
      { key: 'manage_permissions', label: 'Manage Title Permissions', desc: 'Edit permissions on any title' },
      { key: 'mute_users', label: 'Mute Users', desc: 'Temp or perm mute users from chat' },
      { key: 'ban_users', label: 'Ban Users', desc: 'Temp or perm ban users from logging in' },
      { key: 'view_dashboard', label: 'View Staff Dashboard', desc: 'See stats overview' },
      { key: 'staff_access', label: 'Staff Panel Access', desc: 'Can open the Staff Panel' },
    ];

    let currentUserPermissions = [];
    let currentUserTitle = 'User';

    async function ensureDefaultTitlesExist() {
      const snap = await getDocs(collection(db, "titles"));
      const existing = snap.docs.map(d => d.data().name);
      const defaults = [
        { name: 'Owner', color: '#FFD700', isGradient: true, gradientColors: ['#FFD700','#FF6B35','#FF1744'], priority: 1000, permissions: ALL_PERMISSIONS.map(p => p.key) },
        { name: 'Admin', color: '#FF3D6C', isGradient: false, gradientColors: [], priority: 500, permissions: ALL_PERMISSIONS.filter(p => p.key !== 'manage_permissions' && p.key !== 'create_gradient_titles').map(p => p.key) },
        { name: 'Moderator', color: '#42A5F5', isGradient: false, gradientColors: [], priority: 100, permissions: ['staff_access','view_dashboard','manage_users','view_plays','view_chats','delete_chats','assign_titles','assign_badges','mute_users','ban_users'] },
        { name: 'User', color: '#2AFF9E', isGradient: false, gradientColors: [], priority: 0, permissions: [] },
      ];
      for (const def of defaults) {
        if (!existing.includes(def.name)) {
          await addDoc(collection(db, "titles"), { ...def, createdAt: serverTimestamp() });
        }
      }
    }

    async function getUserPermissions(uid) {
      const userDoc = await getDoc(doc(db, "users", uid));
      if (!userDoc.exists()) return [];
      const title = userDoc.data().title || 'User';
      currentUserTitle = title;
      const titlesSnap = await getDocs(query(collection(db, "titles"), where("name", "==", title)));
      if (titlesSnap.empty) return [];
      return titlesSnap.docs[0].data().permissions || [];
    }

    function hasPermission(perm) {
      return currentUserPermissions.includes(perm) || currentUserTitle === 'Owner';
    }

    async function checkStaffAccess(user) {
      currentUserPermissions = await getUserPermissions(user.uid);
      const staffTab = document.getElementById('staff-panel-tab');
      if (hasPermission('staff_access')) {
        staffTab.style.display = 'flex';
      } else {
        staffTab.style.display = 'none';
      }
      applyNonStaffMediaUi();
    }

    function canUserPasteImageLinks() {
      return hasPermission('staff_access');
    }

    function applyNonStaffMediaUi() {
      const staff = canUserPasteImageLinks();
      document.querySelectorAll('.staff-only-url-field').forEach(el => {
        el.style.display = staff ? '' : 'none';
      });
      document.querySelectorAll('.staff-only-signup-url').forEach(el => {
        el.style.display = staff ? 'block' : 'none';
      });
      document.querySelectorAll('.staff-only-suggest-banner-url').forEach(el => {
        el.style.display = staff ? '' : 'none';
      });
      document.querySelectorAll('.chat-link-btn').forEach(btn => {
        if (staff) btn.classList.add('visible');
        else btn.classList.remove('visible');
      });
    }

    function escapeHtml(str) { if(!str) return ''; return String(str).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }

    function parseRgbaColorStop(str) {
      if (!str || typeof str !== 'string') return null;
      const m = str.match(/rgba?\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/i);
      if (m) return '#' + [m[1], m[2], m[3]].map(x => (+x).toString(16).padStart(2, '0')).join('');
      const m2 = str.match(/#([a-f0-9]{6})/i);
      return m2 ? ('#' + m2[1].toLowerCase()) : null;
    }
    function hexToBlooketColorStop(hex, pct) {
      const h = String(hex || '').replace('#', '');
      if (h.length !== 6) return null;
      const r = parseInt(h.slice(0, 2), 16), g = parseInt(h.slice(2, 4), 16), b = parseInt(h.slice(4, 6), 16);
      return `rgba(${r},${g},${b},1) ${pct}`;
    }
    function marketPackFrameColors(pack) {
      if (!pack) return { inner: '#c8c8c8', outer: '#6e6e6e' };
      let inner = parseRgbaColorStop(pack.innerColor) || (typeof pack.packInnerColor === 'string' && pack.packInnerColor.startsWith('#') ? pack.packInnerColor : null);
      let outer = parseRgbaColorStop(pack.outerColor) || (typeof pack.packOuterColor === 'string' && pack.packOuterColor.startsWith('#') ? pack.packOuterColor : null);
      if (!inner || !outer) {
        const order = ['common', 'uncommon', 'rare', 'epic', 'legendary', 'mythical', 'chroma'];
        const topRarity = (pack.items || []).reduce((best, it) => {
          const r = (it.rarity || '').toLowerCase();
          return order.indexOf(r) > order.indexOf(best) ? r : best;
        }, 'common');
        const fall = {
          common: { i: '#c8c8c8', o: '#6e6e6e' },
          uncommon: { i: '#81c784', o: '#2e7d32' },
          rare: { i: '#64b5f6', o: '#1565c0' },
          epic: { i: '#ce93d8', o: '#6a1b9a' },
          legendary: { i: '#ffe082', o: '#bf8f00' },
          mythical: { i: '#ef9a9a', o: '#b71c1c' },
          chroma: { i: '#80deea', o: '#00838f' }
        };
        const f = fall[topRarity] || fall.common;
        inner = inner || f.i;
        outer = outer || f.o;
      }
      return { inner, outer };
    }
    function marketPackArtUrl(pack) {
      const a = (pack.packArtUrl || pack.packUrl || '').trim();
      if (a) return a;
      const bg = (pack.backgroundImage || '').trim();
      if (bg) return bg;
      return (pack.items && pack.items.find(it => it.imageUrl)?.imageUrl) || '';
    }
    function marketPackPatternUrl(pack) {
      return (pack.patternUrl || pack.packBackground || '').trim();
    }
    function buildMarketPackCardEl({ name, price, pack, inner, outer, artUrl, patternUrl, onCardClick, onInfoClick }) {
      const wrap = document.createElement('div');
      wrap.className = 'market-pack-card';
      let ci = inner, co = outer;
      if (pack && (!ci || !co)) {
        const fc = marketPackFrameColors(pack);
        ci = ci || fc.inner;
        co = co || fc.outer;
      }
      if (!ci || !co) {
        const fc = marketPackFrameColors(null);
        ci = ci || fc.inner;
        co = co || fc.outer;
      }
      wrap.style.setProperty('--pack-inner', ci);
      wrap.style.setProperty('--pack-outer', co);
      const art = artUrl
        ? `<img class="market-pack-art" src="${escapeHtml(artUrl)}" alt="">`
        : '<div class="market-pack-art-placeholder"><i class="fas fa-image"></i></div>';
      const infoBtn = onInfoClick
        ? '<button type="button" class="market-pack-info" title="Drop rates">?</button>'
        : '';
      wrap.innerHTML = `
        ${infoBtn}
        <div class="market-pack-frame">
          <div class="market-pack-inner">
            <div class="market-pack-pattern"></div>
            ${art}
          </div>
        </div>
        <div class="market-pack-footer">
          <span class="market-pack-name">${escapeHtml(name || 'Pack')}</span>
          <div class="market-pack-price"><i class="fas fa-coins"></i> ${price}</div>
        </div>`;
      const patEl = wrap.querySelector('.market-pack-pattern');
      if (patternUrl && patEl) patEl.style.backgroundImage = `url(${JSON.stringify(String(patternUrl))})`;
      const ib = wrap.querySelector('.market-pack-info');
      if (ib && onInfoClick) ib.addEventListener('click', (e) => { e.stopPropagation(); onInfoClick(e); });
      wrap.addEventListener('click', () => onCardClick && onCardClick());
      return wrap;
    }
    function updateStaffPackMarketPreview() {
      const box = document.getElementById('staff-pack-market-preview');
      if (!box) return;
      const name = document.getElementById('staff-pack-name')?.value?.trim() || 'Preview';
      const price = parseInt(document.getElementById('staff-pack-price')?.value, 10);
      const inner = document.getElementById('staff-pack-color-inner')?.value || '#c8c8c8';
      const outer = document.getElementById('staff-pack-color-outer')?.value || '#6e6e6e';
      const art = document.getElementById('staff-pack-art-url')?.value?.trim() || '';
      const pat = document.getElementById('staff-pack-pattern-url')?.value?.trim() || '';
      box.innerHTML = '';
      const el = buildMarketPackCardEl({
        name,
        price: Number.isFinite(price) ? price : 0,
        inner, outer, artUrl: art, patternUrl: pat, pack: null,
        onCardClick: () => {}, onInfoClick: () => {}
      });
      box.appendChild(el);
    }
    function updateStaffBannerShopPreview() {
      const box = document.getElementById('staff-banner-shop-preview');
      if (!box) return;
      const name = document.getElementById('staff-banner-shop-name')?.value?.trim() || 'Banner';
      const price = parseInt(document.getElementById('staff-banner-shop-price')?.value, 10);
      const inner = document.getElementById('staff-banner-shop-inner')?.value || '#5a7a9a';
      const outer = document.getElementById('staff-banner-shop-outer')?.value || '#1a2a3a';
      const art = document.getElementById('staff-banner-shop-url')?.value?.trim() || '';
      box.innerHTML = '';
      const el = buildMarketPackCardEl({
        name,
        price: Number.isFinite(price) ? price : 0,
        inner, outer, artUrl: art, patternUrl: '', pack: null,
        onCardClick: () => {}, onInfoClick: () => {}
      });
      box.appendChild(el);
    }

    // ========== Staff Panel Main ==========
    async function loadStaffPanel() {
      if (!hasPermission('staff_access')) return;
      setupStaffSubTabs();
      loadStaffDashboard();
    }

    function setupStaffSubTabs() {
      document.querySelectorAll('.staff-sub-tab[data-staff-tab]').forEach(tab => {
        tab.addEventListener('click', () => {
          document.querySelectorAll('.staff-sub-tab[data-staff-tab]').forEach(t => t.classList.remove('active'));
          tab.classList.add('active');
          document.querySelectorAll('.staff-section').forEach(s => s.classList.remove('active'));
          const target = document.getElementById(tab.dataset.staffTab);
          if (target) target.classList.add('active');
          switch(tab.dataset.staffTab) {
            case 'staff-dashboard': loadStaffDashboard(); break;
            case 'staff-games': loadStaffGames(); break;
            case 'staff-tags': loadStaffTags(); break;
            case 'staff-plays': loadStaffPlays(); break;
            case 'staff-packs': loadStaffPacks(); break;
            case 'staff-users': loadStaffUsers(); break;
            case 'staff-titles': loadStaffTitles(); break;
            case 'staff-badges': loadStaffBadges(); break;
            case 'staff-missions': loadStaffMissions(); break;
            case 'staff-stars': break;
            case 'staff-star-badges': loadStaffStarBadgesPanel(); break;
            case 'staff-chatview': loadStaffChatViewer(); break;
            case 'staff-rarity': loadStaffRaritySitePanel(); break;
          }
        });
      });
    }

    // ========== Staff Dashboard ==========
    async function loadStaffDashboard() {
      if (!hasPermission('view_dashboard')) return;
      const [gamesSnap, usersSnap, playsSnap, packsSnap, titlesSnap, badgesSnap] = await Promise.all([
        getDocs(collection(db,"games")), getDocs(collection(db,"users")),
        getDocs(collection(db,"plays")), getDocs(collection(db,"packs")),
        getDocs(collection(db,"titles")), getDocs(collection(db,"badges"))
      ]);
      document.getElementById('staff-stats-grid').innerHTML = `
        <div class="staff-stat-card"><h4>Total Games</h4><div class="staff-stat-number">${gamesSnap.size}</div></div>
        <div class="staff-stat-card"><h4>Registered Users</h4><div class="staff-stat-number">${usersSnap.size}</div></div>
        <div class="staff-stat-card"><h4>Play Records</h4><div class="staff-stat-number">${playsSnap.size}</div></div>
        <div class="staff-stat-card"><h4>Packs</h4><div class="staff-stat-number">${packsSnap.size}</div></div>
        <div class="staff-stat-card"><h4>Titles</h4><div class="staff-stat-number">${titlesSnap.size}</div></div>
        <div class="staff-stat-card"><h4>Badges</h4><div class="staff-stat-number">${badgesSnap.size}</div></div>
      `;
    }

    // ========== Staff Games ==========
    let staffGamesCache = [];
    async function loadStaffGames() {
      if (!hasPermission('manage_games')) { document.getElementById('staff-games-table').innerHTML = '<p style="color:var(--neon-pink);">No permission.</p>'; return; }
      const snap = await getDocs(collection(db,"games"));
      staffGamesCache = snap.docs.map(d => ({id:d.id,...d.data()}));
      renderStaffGames(staffGamesCache);
    }
    function renderStaffGames(games) {
      document.getElementById('staff-games-table').innerHTML = `<table class="staff-table"><thead><tr><th>Title</th><th>Tags</th><th>Rating</th><th>Multi</th><th>Actions</th></tr></thead><tbody>${games.map(g=>`<tr><td><strong>${escapeHtml(g.title)}</strong></td><td>${(g.tags||[]).map(t=>`<span class="staff-badge">${escapeHtml(t)}</span>`).join(' ')}</td><td>${g.rating||3}</td><td>${g.multiplayer?'Yes':'No'}</td><td><button class="staff-btn staff-btn-primary staff-btn-sm sg-edit" data-id="${g.id}">Edit</button> <button class="staff-btn staff-btn-danger staff-btn-sm sg-del" data-id="${g.id}">Del</button></td></tr>`).join('')}</tbody></table>`;
      document.querySelectorAll('.sg-edit').forEach(b=>b.addEventListener('click',()=>openStaffEditGame(b.dataset.id)));
      document.querySelectorAll('.sg-del').forEach(b=>b.addEventListener('click',async()=>{if(confirm('Delete game?')){await deleteDoc(doc(db,'games',b.dataset.id));loadStaffGames();loadStaffDashboard();}}));
    }
    async function openStaffEditGame(id) {
      const d = await getDoc(doc(db,'games',id));
      if(!d.exists()) return;
      const data = d.data();
      document.getElementById('staff-game-id').value = id;
      document.getElementById('staff-game-title').value = data.title||'';
      document.getElementById('staff-game-desc').value = data.description||'';
      document.getElementById('staff-game-image').value = data.image||'';
      document.getElementById('staff-game-url').value = data.url||'';
      document.getElementById('staff-game-rating').value = data.rating||3;
      document.getElementById('staff-game-multi').value = data.multiplayer?'true':'false';
      document.getElementById('staff-game-tags').value = (data.tags||[]).join(', ');
      document.getElementById('staff-game-modal-title').textContent = 'Edit Game';
      document.getElementById('staff-game-modal').style.display = 'flex';
    }

    // ========== Staff Tags ==========
    async function loadStaffTags() {
      if (!hasPermission('manage_tags')) { document.getElementById('staff-tags-list').innerHTML = '<p style="color:var(--neon-pink);">No permission.</p>'; return; }
      const snap = await getDocs(collection(db,'tags'));
      const tags = snap.docs.map(d=>({id:d.id,name:d.data().name}));
      document.getElementById('staff-tags-list').innerHTML = tags.map(t=>`<div class="staff-badge" style="font-size:0.85rem;padding:6px 14px;">${escapeHtml(t.name)} <i class="fas fa-trash-alt st-del-tag" data-id="${t.id}" style="cursor:pointer;color:var(--neon-pink);margin-left:8px;"></i></div>`).join('');
      document.querySelectorAll('.st-del-tag').forEach(el=>el.addEventListener('click',async()=>{if(confirm('Delete tag?')){await deleteDoc(doc(db,'tags',el.dataset.id));loadStaffTags();}}));
    }

    // ========== Staff Plays ==========
    async function loadStaffPlays() {
      if (!hasPermission('view_plays')) { document.querySelector('#staff-plays-table tbody').innerHTML = '<tr><td colspan="5" style="color:var(--neon-pink);">No permission.</td></tr>'; return; }
      const snap = await getDocs(query(collection(db,'plays'),orderBy('timestamp','desc')));
      const plays = snap.docs.map(d=>{const data=d.data();return{id:d.id,...data,timestamp:data.timestamp?.toDate?.()||new Date()};});
      renderStaffPlays(plays);
    }
    function renderStaffPlays(plays, filter='') {
      const filtered = plays.filter(p=>(p.userEmail||p.userId||'').toLowerCase().includes(filter)||(p.gameTitle||'').toLowerCase().includes(filter));
      document.querySelector('#staff-plays-table tbody').innerHTML = filtered.map(p=>`<tr><td>${escapeHtml(p.userEmail||p.userId||'?')}</td><td>${escapeHtml(p.gameTitle)}</td><td>${p.timestamp.toLocaleString()}</td><td>${p.ipAddress||'N/A'}</td><td>${hasPermission('delete_plays')?`<button class="staff-btn staff-btn-danger staff-btn-sm sp-del" data-id="${p.id}">Del</button>`:''}</td></tr>`).join('');
      document.querySelectorAll('.sp-del').forEach(b=>b.addEventListener('click',async()=>{if(confirm('Delete play record?')){await deleteDoc(doc(db,'plays',b.dataset.id));loadStaffPlays();loadStaffDashboard();}}));
    }

    // ========== Staff Packs & banner shop ==========
    async function loadStaffPacks() {
      if (!hasPermission('manage_packs')) {
        document.getElementById('staff-packs-list').innerHTML = '<p style="color:var(--neon-pink);">No permission.</p>';
        document.getElementById('staff-banner-shop-list').innerHTML = '';
        return;
      }
      const snap = await getDocs(collection(db,'packs'));
      const all = snap.docs.map(d=>({id:d.id,...d.data()}));
      const banners = all.filter(p => p.shopType === 'banner');
      const packs = all.filter(p => p.shopType !== 'banner');
      document.getElementById('staff-banner-shop-list').innerHTML = banners.length ? banners.map(p=>{
        const img = p.bannerImageUrl || p.backgroundImage || '';
        return `<div class="staff-card" style="margin-bottom:10px;display:flex;align-items:center;gap:12px;"><div style="width:120px;height:44px;border-radius:8px;overflow:hidden;background:#1a2a3a;flex-shrink:0;">${img?`<img src="${escapeHtml(img)}" style="width:100%;height:100%;object-fit:cover;">`:''}</div><div style="flex:1;"><strong>${escapeHtml(p.name)}</strong> — ${p.price} coins</div><div><button class="staff-btn staff-btn-primary staff-btn-sm sbn-edit" data-id="${p.id}">Edit</button> <button class="staff-btn staff-btn-danger staff-btn-sm sbn-del" data-id="${p.id}">Del</button></div></div>`;
      }).join('') : '<p style="color:var(--text-secondary);font-size:0.85rem;">No shop banners.</p>';
      document.querySelectorAll('.sbn-edit').forEach(b=>b.addEventListener('click',()=>openStaffBannerShopModal(b.dataset.id)));
      document.querySelectorAll('.sbn-del').forEach(b=>b.addEventListener('click',async()=>{if(confirm('Delete banner from shop?')){await deleteDoc(doc(db,'packs',b.dataset.id));loadStaffPacks();loadStaffDashboard();}}));
      document.getElementById('staff-packs-list').innerHTML = packs.map(p=>`<div class="staff-card" style="margin-bottom:12px;"><div class="staff-card-header"><h3>${escapeHtml(p.name)}</h3><div><button class="staff-btn staff-btn-primary staff-btn-sm spk-edit" data-id="${p.id}">Edit</button> <button class="staff-btn staff-btn-danger staff-btn-sm spk-del" data-id="${p.id}">Del</button></div></div><div style="font-size:0.85rem;"><strong>Price:</strong> ${p.price} coins | <strong>Items:</strong> ${p.items?.length||0}</div><div style="margin-top:8px;">${(p.items||[]).map(i=>`<span class="staff-badge">${escapeHtml(i.name)} (${i.rarity} ${i.chance}%)</span>`).join(' ')}</div></div>`).join('');
      document.querySelectorAll('.spk-edit').forEach(b=>b.addEventListener('click',()=>openStaffEditPack(b.dataset.id)));
      document.querySelectorAll('.spk-del').forEach(b=>b.addEventListener('click',async()=>{if(confirm('Delete pack?')){await deleteDoc(doc(db,'packs',b.dataset.id));loadStaffPacks();loadStaffDashboard();}}));
    }
    async function openStaffBannerShopModal(id) {
      document.getElementById('staff-banner-shop-id').value = id || '';
      document.getElementById('staff-banner-shop-modal-title').textContent = id ? 'Edit shop banner' : 'New shop banner';
      document.getElementById('staff-banner-shop-name').value = '';
      document.getElementById('staff-banner-shop-price').value = '500';
      document.getElementById('staff-banner-shop-url').value = '';
      document.getElementById('staff-banner-shop-file').value = '';
      if (id) {
        const d = await getDoc(doc(db,'packs',id));
        if (d.exists()) {
          const x = d.data();
          document.getElementById('staff-banner-shop-name').value = x.name||'';
          document.getElementById('staff-banner-shop-price').value = x.price||0;
          document.getElementById('staff-banner-shop-url').value = x.bannerImageUrl || x.backgroundImage || '';
          const bfc = marketPackFrameColors(x);
          const bi = document.getElementById('staff-banner-shop-inner');
          const bo = document.getElementById('staff-banner-shop-outer');
          if (bi) bi.value = bfc.inner;
          if (bo) bo.value = bfc.outer;
        }
      } else {
        document.getElementById('staff-banner-shop-inner').value = '#5a7a9a';
        document.getElementById('staff-banner-shop-outer').value = '#1a2a3a';
      }
      document.getElementById('staff-banner-shop-modal').style.display = 'flex';
      updateStaffBannerShopPreview();
    }
    async function openStaffEditPack(id) {
      const d = await getDoc(doc(db,'packs',id));
      if(!d.exists()) return;
      const data = d.data();
      document.getElementById('staff-pack-id').value = id;
      document.getElementById('staff-pack-name').value = data.name||'';
      document.getElementById('staff-pack-price').value = data.price||0;
      const fc = marketPackFrameColors(data);
      const innerEl = document.getElementById('staff-pack-color-inner');
      const outerEl = document.getElementById('staff-pack-color-outer');
      if (innerEl) innerEl.value = fc.inner;
      if (outerEl) outerEl.value = fc.outer;
      const artEl = document.getElementById('staff-pack-art-url');
      const patEl = document.getElementById('staff-pack-pattern-url');
      if (artEl) {
        const ex = (data.packArtUrl || data.packUrl || '').trim();
        artEl.value = ex || (data.backgroundImage || '').trim();
      }
      if (patEl) patEl.value = (data.patternUrl || data.packBackground || '').trim();
      const revEl = document.getElementById('staff-pack-reveal-bg-url');
      if (revEl) revEl.value = (data.revealCardBgUrl || '').trim();
      document.getElementById('staff-pack-bg').value = data.backgroundImage||'';
      document.getElementById('staff-pack-items-json').value = JSON.stringify(data.items||[],null,2);
      packBuilderItems = JSON.parse(JSON.stringify(data.items || []));
      renderPackBuilderItems();
      document.getElementById('staff-pack-edit-mode').value = 'builder';
      document.getElementById('staff-pack-builder-area').style.display = 'block';
      document.getElementById('staff-pack-json-area').style.display = 'none';
      document.getElementById('staff-pack-modal-title').textContent = 'Edit Pack';
      document.getElementById('staff-pack-modal').style.display = 'flex';
      updateStaffPackMarketPreview();
    }

    // ========== Staff Users ==========
    async function loadStaffUsers() {
      if (!hasPermission('manage_users')) { document.getElementById('staff-users-table').innerHTML = '<p style="color:var(--neon-pink);">No permission.</p>'; return; }
      const snap = await getDocs(collection(db,'users'));
      const users = snap.docs.map(d=>({id:d.id,...d.data()}));
      renderStaffUsers(users);
    }
    function renderStaffUsers(users, filter='') {
      const q = filter.toLowerCase().trim();
      const filtered = users.filter(u => {
        const idStr = u.displayId != null ? String(u.displayId) : '';
        return (u.username||'').toLowerCase().includes(q) || (u.email||'').toLowerCase().includes(q) || idStr.includes(q);
      });
      document.getElementById('staff-users-table').innerHTML = `<table class="staff-table"><thead><tr><th>User</th><th>ID</th><th>Email</th><th>Coins</th><th>Stars</th><th>Title</th><th>Status</th><th>Actions</th></tr></thead><tbody>${filtered.map(u=>{const muteTag=u.muteStatus&&u.muteStatus!=='none'?`<span style="color:#FFB347;font-size:0.7rem;">🔇${u.muteStatus}</span>`:'';const banTag=u.banStatus&&u.banStatus!=='none'?`<span style="color:#FF3D6C;font-size:0.7rem;">🚫${u.banStatus}</span>`:'';const did=(u.displayId!=null&&String(u.displayId).length===6)?String(u.displayId):'—';return `<tr><td>${escapeHtml(u.username||'?')}</td><td style="font-weight:800;">${escapeHtml(did)}</td><td>${escapeHtml(u.email)}</td><td>${u.coins||0}</td><td>${u.stars||0}</td><td>${escapeHtml(u.title||'User')}</td><td>${muteTag} ${banTag}${!muteTag&&!banTag?'<span style="color:var(--neon-green);font-size:0.7rem;">✓</span>':''}</td><td><button class="staff-btn staff-btn-primary staff-btn-sm su-edit" data-id="${u.id}">Edit</button></td></tr>`;}).join('')}</tbody></table>`;
      document.querySelectorAll('.su-edit').forEach(b=>b.addEventListener('click',()=>openStaffEditUser(b.dataset.id)));
    }
    async function openStaffEditUser(uid) {
      const userDoc = await getDoc(doc(db,'users',uid));
      if(!userDoc.exists()) return;
      const data = userDoc.data();
      document.getElementById('staff-edit-user-id').value = uid;
      const didEl = document.getElementById('staff-edit-display-id');
      if (didEl) {
        let did = data.displayId != null ? String(data.displayId) : '';
        if (!did || did.length !== 6) {
          ensureDisplayIdForUser(uid).then(nid => { if (didEl && nid) didEl.value = nid; });
          didEl.value = '…';
        } else didEl.value = did;
      }
      document.getElementById('staff-edit-username').value = data.username||'';
      document.getElementById('staff-edit-coins').value = data.coins||0;
      document.getElementById('staff-edit-stars').value = data.stars||0;
      document.getElementById('staff-edit-avatar').value = '';
      staffEditUserInitialBanner = data.banner || '';
      const bUrl = document.getElementById('staff-edit-banner-url');
      if (bUrl) bUrl.value = data.banner || '';
      const bFile = document.getElementById('staff-edit-banner-file');
      if (bFile) bFile.value = '';
      document.getElementById('staff-edit-mute').value = data.muteStatus || 'none';
      document.getElementById('staff-edit-ban').value = data.banStatus || 'none';
      document.getElementById('staff-edit-mod-reason').value = data.modReason || '';
      document.getElementById('staff-mute-until-group').style.display = data.muteStatus === 'temp' ? 'block' : 'none';
      document.getElementById('staff-ban-until-group').style.display = data.banStatus === 'temp' ? 'block' : 'none';
      if (data.muteUntil) { try { document.getElementById('staff-edit-mute-until').value = new Date(data.muteUntil.toDate ? data.muteUntil.toDate() : data.muteUntil).toISOString().slice(0,16); } catch(e){} }
      if (data.banUntil) { try { document.getElementById('staff-edit-ban-until').value = new Date(data.banUntil.toDate ? data.banUntil.toDate() : data.banUntil).toISOString().slice(0,16); } catch(e){} }
      const titleSelect = document.getElementById('staff-edit-user-title');
      const titlesSnap = await getDocs(collection(db,'titles'));
      titleSelect.innerHTML = titlesSnap.docs.map(d=>{const t=d.data();return `<option value="${escapeHtml(t.name)}" ${t.name===(data.title||'User')?'selected':''}>${escapeHtml(t.name)}</option>`;}).join('');
      const userBadges = data.badges || [];
      const badgesSnap = await getDocs(collection(db,'badges'));
      const allBadges = badgesSnap.docs.map(d=>({id:d.id,...d.data()}));
      document.getElementById('staff-user-badges-list').innerHTML = userBadges.length ? userBadges.map(bn=>{const bd=allBadges.find(b=>b.name===bn);return `<span class="user-badge" style="background:${bd?bd.bgColor:'#333'};color:${bd?bd.textColor:'#fff'};">${bd&&bd.icon?`<img src="${bd.icon}" style="width:14px;height:14px;object-fit:contain;vertical-align:middle;">`:''} ${escapeHtml(bn)} <i class="fas fa-times sub-rm-badge" data-badge="${escapeHtml(bn)}" style="cursor:pointer;margin-left:4px;"></i></span>`;}).join(' ') : '<span style="color:var(--text-secondary);font-size:0.8rem;">No badges</span>';
      const addBadgeSelect = document.getElementById('staff-user-add-badge');
      addBadgeSelect.innerHTML = '<option value="">Select badge...</option>' + allBadges.filter(b=>!userBadges.includes(b.name)).map(b=>`<option value="${escapeHtml(b.name)}">${escapeHtml(b.name)}</option>`).join('');
      if(hasPermission('manage_inventory')) {
        const invSnap = await getDocs(query(collection(db,'inventory'),where('userId','==',uid)));
        document.getElementById('staff-user-inventory').innerHTML = invSnap.docs.map(d=>{const it=d.data();return `<div style="background:rgba(0,0,0,0.3);border-radius:10px;padding:8px;margin-bottom:6px;display:flex;justify-content:space-between;align-items:center;"><div><strong>${escapeHtml(it.itemName)}</strong> <span class="staff-badge">${it.rarity}</span></div><button class="staff-btn staff-btn-danger staff-btn-sm sui-del" data-id="${d.id}" data-uid="${uid}">Del</button></div>`;}).join('')||'<span style="color:var(--text-secondary);font-size:0.8rem;">Empty inventory</span>';
        document.querySelectorAll('.sui-del').forEach(b=>b.addEventListener('click',async()=>{if(confirm('Delete item?')){await deleteDoc(doc(db,'inventory',b.dataset.id));openStaffEditUser(b.dataset.uid);}}));
      }
      document.querySelectorAll('.sub-rm-badge').forEach(b=>b.addEventListener('click',async()=>{
        await updateDoc(doc(db,'users',uid),{badges:arrayRemove(b.dataset.badge)});
        openStaffEditUser(uid);
      }));
      document.getElementById('staff-user-modal').style.display = 'flex';
    }

    // ========== Staff Titles ==========
    async function loadStaffTitles() {
      if (!hasPermission('manage_titles')) { document.getElementById('staff-titles-list').innerHTML = '<p style="color:var(--neon-pink);">No permission.</p>'; return; }
      const snap = await getDocs(collection(db,'titles'));
      const titles = snap.docs.map(d=>({id:d.id,...d.data()})).sort((a,b)=>(b.priority||0)-(a.priority||0));
      document.getElementById('staff-titles-list').innerHTML = titles.map(t=>{
        const style = t.isGradient && t.gradientColors?.length>=2 ? `background:linear-gradient(135deg,${t.gradientColors.join(',')});-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-weight:900;` : `color:${t.color||'#2AFF9E'};`;
        return `<div class="staff-card" style="margin-bottom:12px;"><div class="staff-card-header"><h3><span style="${style}">${escapeHtml(t.name)}</span> <span style="font-size:0.75rem;color:var(--text-secondary);">(Priority: ${t.priority||0})</span></h3><div><button class="staff-btn staff-btn-primary staff-btn-sm stt-edit" data-id="${t.id}">Edit</button> ${t.name!=='Owner'&&t.name!=='User'?`<button class="staff-btn staff-btn-danger staff-btn-sm stt-del" data-id="${t.id}">Del</button>`:''}</div></div><div style="font-size:0.8rem;color:var(--text-secondary);">Permissions: ${(t.permissions||[]).length ? (t.permissions||[]).join(', ') : 'None'}</div></div>`;
      }).join('');
      document.querySelectorAll('.stt-edit').forEach(b=>b.addEventListener('click',()=>openStaffEditTitle(b.dataset.id)));
      document.querySelectorAll('.stt-del').forEach(b=>b.addEventListener('click',async()=>{if(confirm('Delete title?')){await deleteDoc(doc(db,'titles',b.dataset.id));loadStaffTitles();}}));
    }
    async function openStaffEditTitle(id) {
      const d = await getDoc(doc(db,'titles',id));
      if(!d.exists()) return;
      const data = d.data();
      document.getElementById('staff-title-id').value = id;
      document.getElementById('staff-title-name').value = data.name||'';
      document.getElementById('staff-title-color').value = data.color||'#2AFF9E';
      document.getElementById('staff-title-gradient').value = data.isGradient?'true':'false';
      document.getElementById('staff-title-gradient-colors').value = (data.gradientColors||[]).join(', ');
      document.getElementById('staff-title-running').value = data.isRunning?'true':'false';
      document.getElementById('staff-title-priority').value = data.priority||0;
      document.getElementById('staff-gradient-colors-group').style.display = data.isGradient?'block':'none';
      updateGradientPreview();
      renderTitlePermissions(data.permissions||[]);
      document.getElementById('staff-title-modal-title').textContent = 'Edit Title';
      document.getElementById('staff-title-modal').style.display = 'flex';
    }
    function renderTitlePermissions(activePerms) {
      const container = document.getElementById('staff-title-perms');
      const canEditPerms = hasPermission('manage_permissions');
      container.innerHTML = ALL_PERMISSIONS.map(p=>`<label class="title-perm-item"><input type="checkbox" value="${p.key}" ${activePerms.includes(p.key)?'checked':''} ${canEditPerms?'':'disabled'}> <span>${p.label}</span></label>`).join('');
    }
    function updateGradientPreview() {
      const colors = document.getElementById('staff-title-gradient-colors').value.split(',').map(c=>c.trim()).filter(c=>c);
      const preview = document.getElementById('staff-gradient-preview');
      if(colors.length>=2) {
        preview.style.background = `linear-gradient(135deg, ${colors.join(', ')})`;
        preview.textContent = document.getElementById('staff-title-name').value || 'Preview';
      } else {
        preview.style.background = '#333';
        preview.textContent = 'Need 2+ colors';
      }
    }

    // ========== Staff Badges ==========
    async function loadStaffBadges() {
      if (!hasPermission('manage_badges')) { document.getElementById('staff-badges-list').innerHTML = '<p style="color:var(--neon-pink);">No permission.</p>'; return; }
      const snap = await getDocs(collection(db,'badges'));
      const badges = snap.docs.map(d=>({id:d.id,...d.data()}));
      document.getElementById('staff-badges-list').innerHTML = badges.map(b=>`<div class="staff-card" style="margin-bottom:12px;display:flex;align-items:center;justify-content:space-between;"><div style="display:flex;align-items:center;gap:12px;"><span class="user-badge" style="background:${b.bgColor};color:${b.textColor};font-size:0.85rem;padding:6px 14px;">${b.icon?`<img src="${b.icon}" style="width:18px;height:18px;object-fit:contain;vertical-align:middle;">`:''} ${escapeHtml(b.name)}</span><span style="font-size:0.8rem;color:var(--text-secondary);">${escapeHtml(b.description||'')}</span></div><div><button class="staff-btn staff-btn-primary staff-btn-sm sb-edit" data-id="${b.id}">Edit</button> <button class="staff-btn staff-btn-danger staff-btn-sm sb-del" data-id="${b.id}">Del</button></div></div>`).join('')||'<p style="color:var(--text-secondary);">No badges created yet.</p>';
      document.querySelectorAll('.sb-edit').forEach(b=>b.addEventListener('click',()=>openStaffEditBadge(b.dataset.id)));
      document.querySelectorAll('.sb-del').forEach(b=>b.addEventListener('click',async()=>{if(confirm('Delete badge?')){await deleteDoc(doc(db,'badges',b.dataset.id));loadStaffBadges();}}));
    }
    async function openStaffEditBadge(id) {
      const d = await getDoc(doc(db,'badges',id));
      if(!d.exists()) return;
      const data = d.data();
      document.getElementById('staff-badge-id').value = id;
      document.getElementById('staff-badge-name').value = data.name||'';
      document.getElementById('staff-badge-icon').value = data.icon||'fas fa-star';
      document.getElementById('staff-badge-bg').value = data.bgColor||'#2AFF9E';
      document.getElementById('staff-badge-text-color').value = data.textColor||'#000000';
      document.getElementById('staff-badge-desc').value = data.description||'';
      document.getElementById('staff-badge-modal-title').textContent = 'Edit Badge';
      document.getElementById('staff-badge-modal').style.display = 'flex';
    }

    // ========== Staff Chat Viewer ==========
    let cvUnsubscribe = null;
    let cvMode = 'global';
    async function loadStaffChatViewer() {
      if (!hasPermission('view_chats')) {
        document.getElementById('cv-messages').innerHTML = '<p style="color:var(--neon-pink);">No permission.</p>';
        return;
      }
      document.getElementById('cv-global-btn').classList.add('active');
      document.getElementById('cv-private-btn').classList.remove('active');
      cvMode = 'global';
      loadCvGlobal();
    }
    async function loadCvGlobal() {
      if (cvUnsubscribe) cvUnsubscribe();
      document.getElementById('cv-sidebar').innerHTML = '<div style="color:var(--text-secondary);font-size:0.8rem;padding:8px;">Global chat — all messages</div>';
      const chatQuery = query(collection(db,'chats'),where('type','==','global'),orderBy('timestamp','desc'),limit(100));
      cvUnsubscribe = onSnapshot(chatQuery, snap => {
        const msgs = snap.docs.map(d=>({id:d.id,...d.data()})).reverse();
        document.getElementById('cv-messages').innerHTML = msgs.map(m=>`<div class="chat-viewer-msg"><span class="cv-sender">${escapeHtml(m.senderName||'?')}</span><span class="cv-time">${m.timestamp?formatMessageTime(m.timestamp):''}</span><br>${escapeHtml(m.message)}${hasPermission('delete_chats')?` <i class="fas fa-trash-alt cv-del-msg" data-id="${m.id}" style="cursor:pointer;color:var(--neon-pink);font-size:0.7rem;float:right;margin-top:4px;"></i>`:''}</div>`).join('');
        document.querySelectorAll('.cv-del-msg').forEach(b=>b.addEventListener('click',async()=>{if(confirm('Delete message?')){await deleteDoc(doc(db,'chats',b.dataset.id));}}));
        document.getElementById('cv-messages').scrollTop = document.getElementById('cv-messages').scrollHeight;
      });
    }
    async function loadCvPrivate() {
      if (cvUnsubscribe) cvUnsubscribe();
      cvUnsubscribe = null;
      const chatsSnap = await getDocs(query(collection(db,'chats'),where('type','==','private'),orderBy('timestamp','desc'),limit(500)));
      const chatIds = new Set();
      const chatMeta = {};
      chatsSnap.docs.forEach(d=>{
        const data = d.data();
        if(data.chatId && !chatIds.has(data.chatId)){
          chatIds.add(data.chatId);
          chatMeta[data.chatId] = { participants: data.participants||[], lastMsg: data.message, senderName: data.senderName };
        }
      });
      const sidebar = document.getElementById('cv-sidebar');
      if(chatIds.size === 0) { sidebar.innerHTML = '<div style="color:var(--text-secondary);font-size:0.8rem;padding:8px;">No private chats found</div>'; document.getElementById('cv-messages').innerHTML=''; return; }
      const userCache = {};
      async function getUsername(uid) {
        if(userCache[uid]) return userCache[uid];
        const u = await getDoc(doc(db,'users',uid));
        const name = u.exists() ? (u.data().username||u.data().email) : uid;
        userCache[uid] = name;
        return name;
      }
      let sidebarHtml = '';
      for(const [cid, meta] of Object.entries(chatMeta)) {
        const names = await Promise.all((meta.participants||[]).map(uid=>getUsername(uid)));
        sidebarHtml += `<div class="chat-viewer-user cv-priv-chat" data-chatid="${cid}">${names.join(' & ')}</div>`;
      }
      sidebar.innerHTML = sidebarHtml;
      document.querySelectorAll('.cv-priv-chat').forEach(el=>el.addEventListener('click',()=>{
        document.querySelectorAll('.cv-priv-chat').forEach(e=>e.classList.remove('active'));
        el.classList.add('active');
        loadCvPrivateChat(el.dataset.chatid);
      }));
      document.getElementById('cv-messages').innerHTML = '<div style="color:var(--text-secondary);padding:20px;text-align:center;">Select a conversation</div>';
    }
    async function loadCvPrivateChat(chatId) {
      if(cvUnsubscribe) cvUnsubscribe();
      const chatQuery = query(collection(db,'chats'),where('chatId','==',chatId),orderBy('timestamp','desc'),limit(100));
      cvUnsubscribe = onSnapshot(chatQuery, snap=>{
        const msgs = snap.docs.map(d=>({id:d.id,...d.data()})).reverse();
        document.getElementById('cv-messages').innerHTML = msgs.map(m=>`<div class="chat-viewer-msg"><span class="cv-sender">${escapeHtml(m.senderName||'?')}</span><span class="cv-time">${m.timestamp?formatMessageTime(m.timestamp):''}</span><br>${escapeHtml(m.message)}${hasPermission('delete_chats')?` <i class="fas fa-trash-alt cv-del-msg" data-id="${m.id}" style="cursor:pointer;color:var(--neon-pink);font-size:0.7rem;float:right;margin-top:4px;"></i>`:''}</div>`).join('');
        document.querySelectorAll('.cv-del-msg').forEach(b=>b.addEventListener('click',async()=>{if(confirm('Delete message?')){await deleteDoc(doc(db,'chats',b.dataset.id));}}));
        document.getElementById('cv-messages').scrollTop = document.getElementById('cv-messages').scrollHeight;
      });
    }

    // ========== Staff Missions ==========
    async function loadStaffMissions() {
      const container = document.getElementById('staff-missions-list');
      try {
        const snap = await getDocs(query(collection(db,'missions'),orderBy('createdAt','desc')));
        const missions = snap.docs.map(d=>({id:d.id,...d.data()}));
        container.innerHTML = missions.length ? missions.map(m=>`<div class="staff-card" style="margin-bottom:10px;"><div class="staff-card-header"><h3 style="font-size:0.9rem;">${escapeHtml(m.title)}</h3><div><button class="staff-btn staff-btn-primary staff-btn-sm sm-edit" data-id="${m.id}">Edit</button> <button class="staff-btn staff-btn-danger staff-btn-sm sm-del" data-id="${m.id}">Delete</button></div></div><div style="font-size:0.8rem;color:var(--text-secondary);">${escapeHtml(m.description||'')} | Type: ${m.type} | Target: ${m.target} | Reward: ${m.rewardCoins||0} coins, ${m.rewardStars||0} stars</div></div>`).join('') : '<p style="color:var(--text-secondary);">No missions.</p>';
        container.querySelectorAll('.sm-edit').forEach(b=>b.addEventListener('click',async()=>{
          const d = await getDoc(doc(db,'missions',b.dataset.id));
          if(!d.exists()) return;
          const x = d.data();
          document.getElementById('staff-mission-form-id').value = b.dataset.id;
          document.getElementById('staff-mission-title').value = x.title||'';
          document.getElementById('staff-mission-desc').value = x.description||'';
          document.getElementById('staff-mission-type').value = x.type||'gametime';
          document.getElementById('staff-mission-target').value = x.target||10;
          document.getElementById('staff-mission-rcoins').value = x.rewardCoins||0;
          document.getElementById('staff-mission-rstars').value = x.rewardStars||0;
          document.getElementById('staff-mission-form-modal').style.display='flex';
        }));
        container.querySelectorAll('.sm-del').forEach(b=>b.addEventListener('click',async()=>{if(confirm('Delete mission?')){await deleteDoc(doc(db,'missions',b.dataset.id));loadStaffMissions();}}));
      } catch(e) { container.innerHTML = '<p style="color:var(--neon-pink);">Error loading missions.</p>'; }
    }

    async function findUserByStaffLookup(raw) {
      const q = raw.trim();
      if (!q) return null;
      if (/^\d{6}$/.test(q)) {
        const s = await getDocs(query(collection(db,'users'),where('displayId','==',q)));
        if (!s.empty) return s.docs[0].id;
      }
      if (q.includes('@')) {
        const s = await getDocs(query(collection(db,'users'),where('email','==',q)));
        if (!s.empty) return s.docs[0].id;
      }
      const all = await getDocs(collection(db,'users'));
      const low = q.toLowerCase();
      let m = all.docs.find(d => (d.data().username||'').toLowerCase() === low);
      if (!m) m = all.docs.find(d => (d.data().username||'').toLowerCase().includes(low));
      return m ? m.id : null;
    }

    // ========== Staff Panel Event Listeners ==========
    function setupStaffEventListeners() {
      // Games
      document.getElementById('staff-add-game-btn')?.addEventListener('click',()=>{document.getElementById('staff-game-id').value='';document.getElementById('staff-game-form').reset();document.getElementById('staff-game-modal-title').textContent='Add Game';document.getElementById('staff-game-modal').style.display='flex';});
      document.getElementById('staff-close-game-modal')?.addEventListener('click',()=>document.getElementById('staff-game-modal').style.display='none');
      document.getElementById('staff-game-form')?.addEventListener('submit',async(e)=>{
        e.preventDefault();
        const id=document.getElementById('staff-game-id').value;
        const tagsArr=document.getElementById('staff-game-tags').value.split(',').map(t=>t.trim()).filter(t=>t);
        let imageUrl = document.getElementById('staff-game-image').value.trim();
        const imgFile = document.getElementById('staff-game-image-file')?.files?.[0];
        if (imgFile) {
          try {
            const storageRef = ref(storage, `games/covers/${Date.now()}_${imgFile.name}`);
            const snapshot = await uploadBytes(storageRef, imgFile);
            imageUrl = await getDownloadURL(snapshot.ref);
          } catch(err) { showNotification('Image upload failed: '+err.message,'error'); return; }
        }
        if (!imageUrl) { showNotification('Add a cover image URL or upload a file','error'); return; }
        const gameData={title:document.getElementById('staff-game-title').value,description:document.getElementById('staff-game-desc').value,image:imageUrl,url:document.getElementById('staff-game-url').value,rating:parseFloat(document.getElementById('staff-game-rating').value),multiplayer:document.getElementById('staff-game-multi').value==='true',tags:tagsArr,updatedAt:serverTimestamp()};
        if(id) await updateDoc(doc(db,'games',id),gameData); else await addDoc(collection(db,'games'),{...gameData,createdAt:serverTimestamp()});
        document.getElementById('staff-game-modal').style.display='none';
        loadStaffGames();loadStaffDashboard();
        showNotification('Game saved!','success');
      });
      document.getElementById('staff-search-game')?.addEventListener('input',e=>{const t=e.target.value.toLowerCase();renderStaffGames(staffGamesCache.filter(g=>g.title.toLowerCase().includes(t)));});

      // Tags
      document.getElementById('staff-add-tag-btn')?.addEventListener('click',()=>document.getElementById('staff-tag-modal').style.display='flex');
      document.getElementById('staff-close-tag-modal')?.addEventListener('click',()=>document.getElementById('staff-tag-modal').style.display='none');
      document.getElementById('staff-save-tag-btn')?.addEventListener('click',async()=>{const n=document.getElementById('staff-new-tag-name').value.trim();if(n){await addDoc(collection(db,'tags'),{name:n});document.getElementById('staff-tag-modal').style.display='none';document.getElementById('staff-new-tag-name').value='';loadStaffTags();showNotification('Tag created!','success');}});

      // Packs
      document.getElementById('staff-pack-add-item-btn')?.addEventListener('click', () => openStaffBlookEditor(-1));
      document.getElementById('staff-blook-editor-cancel')?.addEventListener('click', () => { document.getElementById('staff-blook-editor-modal').style.display = 'none'; });
      document.getElementById('staff-blook-editor-save')?.addEventListener('click', () => applyStaffBlookEditor());
      document.getElementById('staff-blook-editor-delete')?.addEventListener('click', () => {
        const idx = parseInt(document.getElementById('staff-blook-editor-idx').value, 10);
        if (idx >= 0) { packBuilderItems.splice(idx, 1); renderPackBuilderItems(); }
        document.getElementById('staff-blook-editor-modal').style.display = 'none';
      });
      document.getElementById('staff-clear-banner-btn')?.addEventListener('click', () => {
        document.getElementById('staff-edit-banner-url').value = '';
        const f = document.getElementById('staff-edit-banner-file');
        if (f) f.value = '';
      });
      document.getElementById('staff-pack-edit-mode')?.addEventListener('change', e => {
        const isJson = e.target.value === 'json';
        document.getElementById('staff-pack-builder-area').style.display = isJson ? 'none' : 'block';
        document.getElementById('staff-pack-json-area').style.display = isJson ? 'block' : 'none';
        if (isJson) {
          document.getElementById('staff-pack-items-json').value = JSON.stringify(packBuilderItems, null, 2);
        } else {
          try { packBuilderItems = JSON.parse(document.getElementById('staff-pack-items-json').value) || []; } catch(e) { packBuilderItems = []; }
          renderPackBuilderItems();
        }
      });
      document.getElementById('staff-add-pack-btn')?.addEventListener('click',()=>{
        document.getElementById('staff-pack-id').value='';
        document.getElementById('staff-pack-form').reset();
        document.getElementById('staff-pack-modal-title').textContent='Create Pack';
        const pi = document.getElementById('staff-pack-color-inner'); if (pi) pi.value = '#c8c8c8';
        const po = document.getElementById('staff-pack-color-outer'); if (po) po.value = '#6e6e6e';
        document.getElementById('staff-pack-art-url') && (document.getElementById('staff-pack-art-url').value = '');
        document.getElementById('staff-pack-pattern-url') && (document.getElementById('staff-pack-pattern-url').value = '');
        document.getElementById('staff-pack-reveal-bg-url') && (document.getElementById('staff-pack-reveal-bg-url').value = '');
        packBuilderItems=[];renderPackBuilderItems();
        document.getElementById('staff-pack-edit-mode').value='builder';
        document.getElementById('staff-pack-builder-area').style.display='block';
        document.getElementById('staff-pack-json-area').style.display='none';
        document.getElementById('staff-pack-modal').style.display='flex';
        updateStaffPackMarketPreview();
      });
      ['staff-pack-name','staff-pack-price','staff-pack-color-inner','staff-pack-color-outer','staff-pack-art-url','staff-pack-pattern-url','staff-pack-reveal-bg-url'].forEach(id => {
        document.getElementById(id)?.addEventListener('input', updateStaffPackMarketPreview);
        document.getElementById(id)?.addEventListener('change', updateStaffPackMarketPreview);
      });
      ['staff-banner-shop-name','staff-banner-shop-price','staff-banner-shop-inner','staff-banner-shop-outer','staff-banner-shop-url'].forEach(id => {
        document.getElementById(id)?.addEventListener('input', updateStaffBannerShopPreview);
        document.getElementById(id)?.addEventListener('change', updateStaffBannerShopPreview);
      });
      document.getElementById('staff-close-pack-modal')?.addEventListener('click',()=>document.getElementById('staff-pack-modal').style.display='none');
      document.getElementById('staff-pack-form')?.addEventListener('submit',async(e)=>{
        e.preventDefault();
        const id=document.getElementById('staff-pack-id').value;
        const mode=document.getElementById('staff-pack-edit-mode').value;
        let items=[];
        if(mode==='json'){try{items=JSON.parse(document.getElementById('staff-pack-items-json').value);}catch(err){showNotification('Invalid JSON','error');return;}}
        else{items=packBuilderItems.filter(it=>it.name);}
        let bg = document.getElementById('staff-pack-bg').value.trim();
        const bgFile = document.getElementById('staff-pack-bg-file')?.files?.[0];
        if (bgFile) {
          try {
            const storageRef = ref(storage, `packs/bg/${Date.now()}_${bgFile.name}`);
            const snapshot = await uploadBytes(storageRef, bgFile);
            bg = await getDownloadURL(snapshot.ref);
          } catch(err) { showNotification('Background upload failed: '+err.message,'error'); return; }
        }
        const innerHex = document.getElementById('staff-pack-color-inner')?.value || '#c8c8c8';
        const outerHex = document.getElementById('staff-pack-color-outer')?.value || '#6e6e6e';
        const packArt = document.getElementById('staff-pack-art-url')?.value.trim() || '';
        const pattern = document.getElementById('staff-pack-pattern-url')?.value.trim() || '';
        const revealBg = document.getElementById('staff-pack-reveal-bg-url')?.value.trim() || '';
        const packData={
          name:document.getElementById('staff-pack-name').value,
          price:parseInt(document.getElementById('staff-pack-price').value),
          backgroundImage:bg,
          items,
          packInnerColor: innerHex,
          packOuterColor: outerHex,
          innerColor: hexToBlooketColorStop(innerHex, 0),
          outerColor: hexToBlooketColorStop(outerHex, 100),
          packArtUrl: packArt,
          patternUrl: pattern,
          revealCardBgUrl: revealBg,
          updatedAt:serverTimestamp()
        };
        if(id) await updateDoc(doc(db,'packs',id),packData); else await addDoc(collection(db,'packs'),{...packData,createdAt:serverTimestamp()});
        document.getElementById('staff-pack-modal').style.display='none';
        loadStaffPacks();loadStaffDashboard();
        showNotification('Pack saved!','success');
      });

      document.getElementById('staff-add-banner-shop-btn')?.addEventListener('click', () => openStaffBannerShopModal(''));
      document.getElementById('staff-close-banner-shop-modal')?.addEventListener('click', () => document.getElementById('staff-banner-shop-modal').style.display='none');
      document.getElementById('staff-save-banner-shop-btn')?.addEventListener('click', async () => {
        const id = document.getElementById('staff-banner-shop-id').value;
        let url = document.getElementById('staff-banner-shop-url').value.trim();
        const f = document.getElementById('staff-banner-shop-file')?.files?.[0];
        if (f) {
          try {
            const storageRef = ref(storage, `shop/banners/${Date.now()}_${f.name}`);
            const snapshot = await uploadBytes(storageRef, f);
            url = await getDownloadURL(snapshot.ref);
          } catch(err) { showNotification('Upload failed: '+err.message,'error'); return; }
        }
        if (!url) { showNotification('Add image URL or upload','error'); return; }
        const name = document.getElementById('staff-banner-shop-name').value.trim();
        const price = parseInt(document.getElementById('staff-banner-shop-price').value, 10) || 0;
        const bInner = document.getElementById('staff-banner-shop-inner')?.value || '#5a7a9a';
        const bOuter = document.getElementById('staff-banner-shop-outer')?.value || '#1a2a3a';
        const data = {
          name, price, shopType: 'banner', bannerImageUrl: url, backgroundImage: url, items: [],
          packInnerColor: bInner, packOuterColor: bOuter,
          innerColor: hexToBlooketColorStop(bInner, 0), outerColor: hexToBlooketColorStop(bOuter, 100),
          updatedAt: serverTimestamp()
        };
        if (id) await updateDoc(doc(db,'packs',id), data);
        else await addDoc(collection(db,'packs'), { ...data, createdAt: serverTimestamp() });
        document.getElementById('staff-banner-shop-modal').style.display='none';
        loadStaffPacks(); loadStaffDashboard();
        showNotification('Shop banner saved','success');
      });

      const openRarityOrderModal = async () => {
        const modal = document.getElementById('staff-rarity-order-modal');
        const ta = document.getElementById('staff-rarity-order-input');
        const cur = await getDoc(doc(db,'siteConfig','settings'));
        ta.value = cur.exists() && cur.data().blookRarityOrder ? cur.data().blookRarityOrder : DEFAULT_RARITY_ORDER.join(',');
        modal.style.display = 'flex';
      };
      document.getElementById('staff-rarity-order-open')?.addEventListener('click', openRarityOrderModal);
      document.getElementById('staff-set-rarity-order-btn')?.addEventListener('click', openRarityOrderModal);
      document.getElementById('staff-rarity-order-cancel')?.addEventListener('click', () => document.getElementById('staff-rarity-order-modal').style.display='none');
      document.getElementById('staff-rarity-order-save')?.addEventListener('click', async () => {
        const v = document.getElementById('staff-rarity-order-input').value.trim();
        await setDoc(doc(db,'siteConfig','settings'), { blookRarityOrder: v, updatedAt: serverTimestamp() }, { merge: true });
        await refreshRarityOrderFromServer();
        renderPackBuilderItems();
        if (inventoryItems.length) renderInventory();
        document.getElementById('staff-rarity-order-modal').style.display='none';
        showNotification('Rarity order saved','success');
      });

      document.getElementById('staff-daily-reward-save')?.addEventListener('click', async () => {
        const coins = Math.max(0, parseInt(document.getElementById('staff-daily-reward-coins')?.value, 10) || 0);
        const stars = Math.max(0, parseInt(document.getElementById('staff-daily-reward-stars')?.value, 10) || 0);
        try {
          await setDoc(doc(db, 'siteConfig', 'settings'), {
            dailyReward: { coins, stars },
            updatedAt: serverTimestamp()
          }, { merge: true });
          if (currentUser) refreshDailyRewardUI(currentUser.uid);
          showNotification('Daily reward amounts saved', 'success');
        } catch (e) { showNotification('Save failed: ' + e.message, 'error'); }
      });
      document.getElementById('staff-rarity-def-add')?.addEventListener('click', () => openStaffRarityDefModal('add'));
      document.getElementById('staff-rarity-def-gradient')?.addEventListener('change', toggleStaffRarityGradUI);
      document.getElementById('staff-rarity-def-save')?.addEventListener('click', () => saveStaffRarityDef());
      document.getElementById('staff-rarity-def-cancel')?.addEventListener('click', () => { document.getElementById('staff-rarity-def-modal').style.display = 'none'; });
      document.getElementById('staff-rarity-def-delete')?.addEventListener('click', async () => {
        const k = (document.getElementById('staff-rarity-def-key')?.value || '').trim().toLowerCase();
        if (!k) return;
        document.getElementById('staff-rarity-def-modal').style.display = 'none';
        await deleteStaffRarityDef(k);
      });

      // Plays filter
      document.getElementById('staff-filter-plays')?.addEventListener('input',async e=>{const snap=await getDocs(query(collection(db,'plays'),orderBy('timestamp','desc')));const plays=snap.docs.map(d=>{const data=d.data();return{id:d.id,...data,timestamp:data.timestamp?.toDate?.()||new Date()};});renderStaffPlays(plays,e.target.value.toLowerCase());});

      // Users
      document.getElementById('staff-search-user')?.addEventListener('input',async e=>{const snap=await getDocs(collection(db,'users'));const users=snap.docs.map(d=>({id:d.id,...d.data()}));renderStaffUsers(users,e.target.value);});
      document.getElementById('staff-close-user-modal')?.addEventListener('click',()=>document.getElementById('staff-user-modal').style.display='none');
      document.getElementById('staff-edit-mute')?.addEventListener('change',e=>{document.getElementById('staff-mute-until-group').style.display=e.target.value==='temp'?'block':'none';});
      document.getElementById('staff-edit-ban')?.addEventListener('change',e=>{document.getElementById('staff-ban-until-group').style.display=e.target.value==='temp'?'block':'none';});
      document.getElementById('staff-update-user-btn')?.addEventListener('click',async()=>{
        const uid=document.getElementById('staff-edit-user-id').value;
        const updates={username:document.getElementById('staff-edit-username').value,coins:parseInt(document.getElementById('staff-edit-coins').value),stars:parseInt(document.getElementById('staff-edit-stars').value)};
        if(hasPermission('assign_titles')) updates.title=document.getElementById('staff-edit-user-title').value;
        const avatarVal=document.getElementById('staff-edit-avatar').value.trim();
        if(avatarVal && hasPermission('custom_avatar')) updates.avatar=avatarVal;
        const bannerUrlField = document.getElementById('staff-edit-banner-url')?.value.trim() || '';
        const bannerFile = document.getElementById('staff-edit-banner-file')?.files?.[0];
        let bannerVal = bannerUrlField;
        if (bannerFile) {
          try {
            const storageRef = ref(storage, `profile-banners/${uid}_${Date.now()}_${bannerFile.name}`);
            const snapshot = await uploadBytes(storageRef, bannerFile);
            bannerVal = await getDownloadURL(snapshot.ref);
          } catch (err) { showNotification('Banner upload failed: '+err.message,'error'); return; }
        }
        if (bannerVal) updates.banner = bannerVal;
        else if (!bannerFile && bannerUrlField === '' && staffEditUserInitialBanner) updates.banner = null;
        updates.muteStatus=document.getElementById('staff-edit-mute').value;
        updates.banStatus=document.getElementById('staff-edit-ban').value;
        updates.modReason=document.getElementById('staff-edit-mod-reason').value;
        if(updates.muteStatus==='temp'){const v=document.getElementById('staff-edit-mute-until').value;if(v) updates.muteUntil=new Date(v);else updates.muteUntil=null;} else {updates.muteUntil=null;}
        if(updates.banStatus==='temp'){const v=document.getElementById('staff-edit-ban-until').value;if(v) updates.banUntil=new Date(v);else updates.banUntil=null;} else {updates.banUntil=null;}
        await updateDoc(doc(db,'users',uid),updates);
        if (typeof updates.stars === 'number') syncStarBadgesForUser(uid, updates.stars);
        document.getElementById('staff-user-modal').style.display='none';
        loadStaffUsers();
        showNotification('User updated!','success');
      });
      document.getElementById('staff-assign-badge-btn')?.addEventListener('click',async()=>{
        const uid=document.getElementById('staff-edit-user-id').value;
        const badge=document.getElementById('staff-user-add-badge').value;
        if(badge && uid){await updateDoc(doc(db,'users',uid),{badges:arrayUnion(badge)});openStaffEditUser(uid);showNotification('Badge assigned!','success');}
      });

      // Titles
      document.getElementById('staff-add-title-btn')?.addEventListener('click',()=>{document.getElementById('staff-title-id').value='';document.getElementById('staff-title-name').value='';document.getElementById('staff-title-color').value='#2AFF9E';document.getElementById('staff-title-gradient').value='false';document.getElementById('staff-title-gradient-colors').value='';document.getElementById('staff-title-priority').value='0';document.getElementById('staff-gradient-colors-group').style.display='none';renderTitlePermissions([]);document.getElementById('staff-title-modal-title').textContent='Create Title';document.getElementById('staff-title-modal').style.display='flex';});
      document.getElementById('staff-close-title-modal')?.addEventListener('click',()=>document.getElementById('staff-title-modal').style.display='none');
      document.getElementById('staff-title-gradient')?.addEventListener('change',e=>{document.getElementById('staff-gradient-colors-group').style.display=e.target.value==='true'?'block':'none';});
      document.getElementById('staff-title-gradient-colors')?.addEventListener('input',updateGradientPreview);
      document.getElementById('staff-title-name')?.addEventListener('input',updateGradientPreview);
      document.getElementById('staff-save-title-btn')?.addEventListener('click',async()=>{
        const id=document.getElementById('staff-title-id').value;
        const isGradient=document.getElementById('staff-title-gradient').value==='true';
        if(isGradient && !hasPermission('create_gradient_titles')){showNotification('No permission to create gradient titles','error');return;}
        const gradientColors=document.getElementById('staff-title-gradient-colors').value.split(',').map(c=>c.trim()).filter(c=>c);
        const perms=Array.from(document.querySelectorAll('#staff-title-perms input:checked')).map(i=>i.value);
        const isRunning=document.getElementById('staff-title-running').value==='true';
        const titleData={name:document.getElementById('staff-title-name').value,color:document.getElementById('staff-title-color').value,isGradient,isRunning,gradientColors,priority:parseInt(document.getElementById('staff-title-priority').value)||0,permissions:perms,updatedAt:serverTimestamp()};
        if(id) await updateDoc(doc(db,'titles',id),titleData); else await addDoc(collection(db,'titles'),{...titleData,createdAt:serverTimestamp()});
        document.getElementById('staff-title-modal').style.display='none';
        loadStaffTitles();
        showNotification('Title saved!','success');
      });

      // Badges
      document.getElementById('staff-add-badge-btn')?.addEventListener('click',()=>{document.getElementById('staff-badge-id').value='';document.getElementById('staff-badge-name').value='';document.getElementById('staff-badge-icon').value='';const bif=document.getElementById('staff-badge-icon-file');if(bif)bif.value='';document.getElementById('staff-badge-bg').value='#2AFF9E';document.getElementById('staff-badge-text-color').value='#000000';document.getElementById('staff-badge-desc').value='';document.getElementById('staff-badge-modal-title').textContent='Create Badge';document.getElementById('staff-badge-modal').style.display='flex';});
      document.getElementById('staff-close-badge-modal')?.addEventListener('click',()=>document.getElementById('staff-badge-modal').style.display='none');
      document.getElementById('staff-save-badge-btn')?.addEventListener('click',async()=>{
        const id=document.getElementById('staff-badge-id').value;
        let icon = document.getElementById('staff-badge-icon').value.trim();
        const iconFile = document.getElementById('staff-badge-icon-file')?.files?.[0];
        if (iconFile) {
          try {
            const storageRef = ref(storage, `badges/icons/${Date.now()}_${iconFile.name}`);
            const snapshot = await uploadBytes(storageRef, iconFile);
            icon = await getDownloadURL(snapshot.ref);
          } catch(err) { showNotification('Icon upload failed: '+err.message,'error'); return; }
        }
        const isFa = /\bfa[srb]?\s+fa-/.test(icon) || /^fa[srb]?\s/.test(icon) || icon.includes('fa-');
        if (!iconFile && !icon) { showNotification('Set icon URL or upload an image','error'); return; }
        if (!iconFile && !isFa && !/^https?:\/\//i.test(icon)) { showNotification('Icon must be a URL, upload, or Font Awesome classes','error'); return; }
        const badgeData={name:document.getElementById('staff-badge-name').value,icon,bgColor:document.getElementById('staff-badge-bg').value,textColor:document.getElementById('staff-badge-text-color').value,description:document.getElementById('staff-badge-desc').value,updatedAt:serverTimestamp()};
        if(id) await updateDoc(doc(db,'badges',id),badgeData); else await addDoc(collection(db,'badges'),{...badgeData,createdAt:serverTimestamp()});
        starBadgeMetaCache = null;
        document.getElementById('staff-badge-modal').style.display='none';
        loadStaffBadges();
        showNotification('Badge saved!','success');
      });

      // Chat Viewer
      document.getElementById('cv-global-btn')?.addEventListener('click',()=>{document.getElementById('cv-global-btn').classList.add('active');document.getElementById('cv-private-btn').classList.remove('active');cvMode='global';loadCvGlobal();});
      document.getElementById('cv-private-btn')?.addEventListener('click',()=>{document.getElementById('cv-private-btn').classList.add('active');document.getElementById('cv-global-btn').classList.remove('active');cvMode='private';loadCvPrivate();});

      // Quick actions
      document.getElementById('staff-quick-add-game')?.addEventListener('click',()=>document.getElementById('staff-add-game-btn')?.click());
      document.getElementById('staff-quick-add-pack')?.addEventListener('click',()=>document.getElementById('staff-add-pack-btn')?.click());
      document.getElementById('staff-quick-add-title')?.addEventListener('click',()=>document.getElementById('staff-add-title-btn')?.click());

      // Missions
      document.getElementById('staff-add-mission-btn')?.addEventListener('click', () => {
        document.getElementById('staff-mission-form-id').value = '';
        document.getElementById('staff-mission-title').value = '';
        document.getElementById('staff-mission-desc').value = '';
        document.getElementById('staff-mission-type').value = 'gametime';
        document.getElementById('staff-mission-target').value = '300';
        document.getElementById('staff-mission-rcoins').value = '100';
        document.getElementById('staff-mission-rstars').value = '50';
        document.getElementById('staff-mission-form-modal').style.display = 'flex';
      });
      document.getElementById('staff-mission-form-cancel')?.addEventListener('click', () => document.getElementById('staff-mission-form-modal').style.display = 'none');
      document.getElementById('staff-mission-form-save')?.addEventListener('click', async () => {
        const mid = document.getElementById('staff-mission-form-id').value;
        const payload = {
          title: document.getElementById('staff-mission-title').value.trim(),
          description: document.getElementById('staff-mission-desc').value.trim(),
          type: document.getElementById('staff-mission-type').value,
          target: parseInt(document.getElementById('staff-mission-target').value, 10) || 1,
          rewardCoins: parseInt(document.getElementById('staff-mission-rcoins').value, 10) || 0,
          rewardStars: parseInt(document.getElementById('staff-mission-rstars').value, 10) || 0,
          updatedAt: serverTimestamp()
        };
        if (!payload.title) { showNotification('Mission needs a title','error'); return; }
        if (mid) await updateDoc(doc(db,'missions',mid), payload);
        else await addDoc(collection(db,'missions'), { ...payload, createdAt: serverTimestamp() });
        document.getElementById('staff-mission-form-modal').style.display = 'none';
        loadStaffMissions();
        showNotification('Mission saved','success');
      });

      document.getElementById('staff-mod-apply-btn')?.addEventListener('click', async () => {
        const lookup = document.getElementById('staff-mod-lookup')?.value || '';
        const action = document.getElementById('staff-mod-action')?.value || 'open';
        const reason = document.getElementById('staff-mod-reason-quick')?.value.trim() || '';
        const uid = await findUserByStaffLookup(lookup);
        if (!uid) { showNotification('User not found — try 6-digit ID, email, or exact username','error'); return; }
        if (action === 'open') { openStaffEditUser(uid); return; }
        const updates = { modReason: reason };
        const now = new Date();
        const addHours = (h) => new Date(now.getTime() + h * 3600000);
        const addDays = (d) => new Date(now.getTime() + d * 86400000);
        if (action === 'mute_1h') {
          if (!hasPermission('mute_users')) { showNotification('No mute permission','error'); return; }
          Object.assign(updates, { muteStatus: 'temp', muteUntil: addHours(1), banStatus: 'none', banUntil: null });
        } else if (action === 'mute_24h') {
          if (!hasPermission('mute_users')) { showNotification('No mute permission','error'); return; }
          Object.assign(updates, { muteStatus: 'temp', muteUntil: addHours(24), banStatus: 'none', banUntil: null });
        } else if (action === 'mute_perm') {
          if (!hasPermission('mute_users')) { showNotification('No mute permission','error'); return; }
          Object.assign(updates, { muteStatus: 'perm', muteUntil: null, banStatus: 'none', banUntil: null });
        } else if (action === 'unmute') {
          if (!hasPermission('mute_users')) { showNotification('No mute permission','error'); return; }
          Object.assign(updates, { muteStatus: 'none', muteUntil: null });
        } else if (action === 'ban_24h') {
          if (!hasPermission('ban_users')) { showNotification('No ban permission','error'); return; }
          Object.assign(updates, { banStatus: 'temp', banUntil: addHours(24), muteStatus: 'none', muteUntil: null });
        } else if (action === 'ban_7d') {
          if (!hasPermission('ban_users')) { showNotification('No ban permission','error'); return; }
          Object.assign(updates, { banStatus: 'temp', banUntil: addDays(7), muteStatus: 'none', muteUntil: null });
        } else if (action === 'ban_perm') {
          if (!hasPermission('ban_users')) { showNotification('No ban permission','error'); return; }
          Object.assign(updates, { banStatus: 'perm', banUntil: null, muteStatus: 'none', muteUntil: null });
        } else if (action === 'unban') {
          if (!hasPermission('ban_users')) { showNotification('No ban permission','error'); return; }
          Object.assign(updates, { banStatus: 'none', banUntil: null });
        }
        await updateDoc(doc(db,'users',uid), updates);
        showNotification('Moderation updated','success');
        loadStaffUsers();
      });

      // Stars
      document.getElementById('staff-stars-apply-btn')?.addEventListener('click', async () => {
        const email = document.getElementById('staff-stars-email').value.trim();
        const action = document.getElementById('staff-stars-action').value;
        const amount = parseInt(document.getElementById('staff-stars-amount').value) || 0;
        if (!email) { showNotification('Enter email','error'); return; }
        const snap = await getDocs(query(collection(db,'users'),where('email','==',email)));
        if (snap.empty) { showNotification('User not found','error'); return; }
        const uid = snap.docs[0].id;
        const current = snap.docs[0].data().stars || 0;
        let newVal = current;
        if (action === 'set') newVal = amount;
        else if (action === 'add') newVal = current + amount;
        else if (action === 'remove') newVal = Math.max(0, current - amount);
        await updateDoc(doc(db,'users',uid),{stars:newVal});
        syncStarBadgesForUser(uid, newVal);
        document.getElementById('staff-stars-result').innerHTML = `<span style="color:var(--neon-green);">✓ ${snap.docs[0].data().username}: ${current} → ${newVal} stars</span>`;
        showNotification('Stars updated!','success');
      });

      document.getElementById('staff-star-badge-add-row')?.addEventListener('click', () => {
        const pick = document.getElementById('staff-star-badge-pick');
        const minEl = document.getElementById('staff-star-badge-min');
        const name = (pick?.value || '').trim();
        const minStars = Math.max(0, parseInt(minEl?.value, 10) || 0);
        if (!name) { showNotification('Choose a badge', 'error'); return; }
        if (staffStarBadgeRulesDraft.some(r => r.badgeName === name)) { showNotification('That badge already has a rule', 'error'); return; }
        staffStarBadgeRulesDraft.push({ badgeName: name, minStars });
        renderStaffStarBadgeRulesList();
      });
      document.getElementById('staff-star-badge-save-rules')?.addEventListener('click', async () => {
        const status = document.getElementById('staff-star-badge-status');
        const byName = new Map();
        staffStarBadgeRulesDraft.forEach(r => {
          const prev = byName.get(r.badgeName);
          if (!prev || r.minStars < prev.minStars) byName.set(r.badgeName, r);
        });
        const deduped = Array.from(byName.values());
        try {
          await setDoc(doc(db, 'siteConfig', 'settings'), {
            starBadgeRules: deduped,
            updatedAt: serverTimestamp()
          }, { merge: true });
          staffStarBadgeRulesDraft = deduped;
          renderStaffStarBadgeRulesList();
          if (status) status.textContent = 'Saved. Use “Re-apply to all users” if you changed thresholds.';
          showNotification('Star badge rules saved', 'success');
        } catch (e) {
          showNotification('Save failed: ' + e.message, 'error');
        }
      });
      document.getElementById('staff-star-badge-resync-all')?.addEventListener('click', async () => {
        const status = document.getElementById('staff-star-badge-status');
        if (!confirm('Re-apply star badge rules to every user? This may take a moment.')) return;
        if (status) status.textContent = 'Working…';
        try {
          const rules = await getStarBadgeRulesFromServer();
          if (!rules.length) {
            if (status) status.textContent = 'No rules saved yet.';
            return;
          }
          const us = await getDocs(collection(db, 'users'));
          let n = 0;
          for (const d of us.docs) {
            const stars = typeof d.data().stars === 'number' ? d.data().stars : parseInt(d.data().stars, 10) || 0;
            await syncStarBadgesForUser(d.id, stars);
            n++;
            if (n % 40 === 0 && status) status.textContent = `Processed ${n} users…`;
          }
          if (status) status.textContent = `Done. Updated ${n} users.`;
          showNotification('Star badges re-applied for all users', 'success');
        } catch (e) {
          if (status) status.textContent = 'Error: ' + e.message;
          showNotification('Re-apply failed: ' + e.message, 'error');
        }
      });

      // Close staff modals on backdrop click
      document.querySelectorAll('.staff-modal').forEach(m=>m.addEventListener('click',e=>{if(e.target===m) m.style.display='none';}));
    }
    if (document.getElementById('staff-game-modal')) setupStaffEventListeners();

    // ========== Initialize the app ==========
    async function init() {
      await refreshRarityOrderFromServer();
      applyFixedSiteTheme();
      if (document.getElementById('topGamesCarousel')) {
        const gamesData = await getGames();
        renderTopGamesCarousel(gamesData.topGames);
        renderCategoryBrowsing(gamesData);
        renderFullGamesList(gamesData.allGames);
      }
      hidePageLoading();
    }

    init().catch(() => hidePageLoading());
    setTimeout(hidePageLoading, 12000);

    // ========== Event Listeners ==========
    document.getElementById('daily-reward-claim-btn')?.addEventListener('click', () => claimDailyReward());
    document.getElementById('badge-detail-close')?.addEventListener('click', () => {
      const m = document.getElementById('badge-detail-modal');
      if (m) m.style.display = 'none';
    });

    globalSendChatBtn?.addEventListener('click', sendGlobalChatMessage);
    globalChatInput?.addEventListener('keypress', (e) => { if (e.key === 'Enter') sendGlobalChatMessage(); });
    friendSendChatBtn?.addEventListener('click', sendFriendChatMessage);
    friendChatInput?.addEventListener('keypress', (e) => { if (e.key === 'Enter') sendFriendChatMessage(); });

    document.getElementById('global-chat-attach-btn')?.addEventListener('click', () => document.getElementById('global-chat-file')?.click());
    document.getElementById('global-chat-file')?.addEventListener('change', async (e) => {
      const f = e.target.files?.[0];
      e.target.value = '';
      if (!f || !currentUser) return;
      if (await isUserMuted()) return;
      try {
        const url = await uploadChatImageFile(f);
        if (url) await sendGlobalChatWithImage(url, globalChatInput.value.trim());
      } catch (err) { showNotification('Image upload failed: ' + (err.message || 'error'), 'error'); }
    });
    document.getElementById('global-chat-link-btn')?.addEventListener('click', async () => {
      const url = promptChatImageUrl();
      if (!url || !currentUser) return;
      if (await isUserMuted()) return;
      try { await sendGlobalChatWithImage(url, globalChatInput.value.trim()); } catch (err) { showNotification(err.message || 'Send failed', 'error'); }
    });

    document.getElementById('friend-chat-attach-btn')?.addEventListener('click', () => document.getElementById('friend-chat-file')?.click());
    document.getElementById('friend-chat-file')?.addEventListener('change', async (e) => {
      const f = e.target.files?.[0];
      e.target.value = '';
      if (!f || !currentUser || !selectedFriend) return;
      if (await isUserMuted()) return;
      try {
        const url = await uploadChatImageFile(f);
        if (url) await sendFriendChatWithImage(url, friendChatInput.value.trim());
      } catch (err) { showNotification('Image upload failed: ' + (err.message || 'error'), 'error'); }
    });
    document.getElementById('friend-chat-link-btn')?.addEventListener('click', async () => {
      const url = promptChatImageUrl();
      if (!url || !currentUser || !selectedFriend) return;
      if (await isUserMuted()) return;
      try { await sendFriendChatWithImage(url, friendChatInput.value.trim()); } catch (err) { showNotification(err.message || 'Send failed', 'error'); }
    });
    addFriendBtn?.addEventListener('click', addFriend);

    document.getElementById('inv-blook-modal-close')?.addEventListener('click', closeInvBlookModal);
    document.getElementById('inv-blook-sell-btn')?.addEventListener('click', () => sellOneInvBlookFromDetail());
    document.getElementById('inv-blook-send-btn')?.addEventListener('click', () => openInvSendFromDetail());
    cancelSendBtn?.addEventListener('click', () => { if (sendCardModal) sendCardModal.style.display = 'none'; currentCard = null; });
    sendCardBtn?.addEventListener('click', async () => {
      if (!currentUser || !currentCard) return;
      const recipientId = recipientEmail?.value;
      if (!recipientId) { showNotification('Choose a friend to send to', 'error'); return; }
      try {
        await sendGiftToFriend(recipientId, currentCard.id, currentCard);
        if (sendCardModal) sendCardModal.style.display = 'none';
        currentCard = null;
      } catch (e) { showNotification('Send failed: ' + (e.message || 'error'), 'error'); }
    });

    // Close modals when clicking outside
    window.addEventListener('click', (e) => {
      if (e.target === confirmModal) confirmModal.style.display = 'none';
      if (e.target === packOpeningModal) {
        packOpeningModal.style.display = 'none';
        resetPackOpeningModalLayout();
      }
      if (e.target === sendGiftModal) sendGiftModal.style.display = 'none';
      if (e.target === sendingAnimationModal) sendingAnimationModal.style.display = 'none';
      if (e.target === chanceModal) chanceModal.style.display = 'none';
      if (e.target === sendGiftChatModal) sendGiftChatModal.style.display = 'none';
      if (e.target === sendGiftInventoryModal) sendGiftInventoryModal.style.display = 'none';
      if (e.target === sendCardModal) { sendCardModal.style.display = 'none'; currentCard = null; }
      const ibm = document.getElementById('inv-blook-modal');
      if (e.target === ibm) closeInvBlookModal();
      const bdm = document.getElementById('badge-detail-modal');
      if (e.target === bdm) bdm.style.display = 'none';
    });

    // Close modals with Escape key
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') {
        confirmModal.style.display = 'none';
        packOpeningModal.style.display = 'none';
        resetPackOpeningModalLayout();
        sendGiftModal.style.display = 'none';
        sendingAnimationModal.style.display = 'none';
        chanceModal.style.display = 'none';
        sendGiftChatModal.style.display = 'none';
        sendGiftInventoryModal.style.display = 'none';
        if (sendCardModal) sendCardModal.style.display = 'none';
        closeInvBlookModal();
        loginModal.style.display = 'none';
        signupModal.style.display = 'none';
        noticeModal.style.display = 'none';
        document.querySelectorAll('.staff-modal').forEach(m => m.style.display = 'none');
        const bpm = document.getElementById('banner-picker-modal');
        if (bpm) bpm.style.display = 'none';
        const bdm = document.getElementById('badge-detail-modal');
        if (bdm) bdm.style.display = 'none';
      }
    });

    // Close game modal
    document.querySelector('#gameModal .close')?.addEventListener('click', () => {
      document.getElementById('gameModal').style.display = 'none';
      document.body.style.overflow = 'auto';
    });
