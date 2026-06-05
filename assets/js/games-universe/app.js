// Import required Firebase modules
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
    const auth = getAuth(app);
    const db = getFirestore(app);
    const storage = getStorage(app);
    window.addEventListener('load', () => {
      try { getAnalytics(app); } catch (_) { /* analytics optional */ }
    }, { once: true });

    const DEFAULT_NEON_GREEN = '#2AFF9E';
    const DEFAULT_NEON_PINK = '#FF3D6C';
    const DEFAULT_THEME_GREY = '#8A9BB5';
    const DEFAULT_RARITY_ORDER = ['common','uncommon','rare','epic','legendary','mythical','chroma'];
    let effectiveRarityOrder = [...DEFAULT_RARITY_ORDER];
    let blookRarityDefs = {};
    /** Unknown rarities only — all real styles come from admin `blookRarityDefs`. */
    const FALLBACK_RARITY_DEF = { solid: '#a8b0c8', isGradient: false, gradientColors: [], isRunning: false };
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
          blookRarityDefs = rd && typeof rd === 'object' ? { ...rd } : {};
        } else {
          blookRarityDefs = {};
        }
      } catch (e) {
        blookRarityDefs = {};
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
      const badges = [...new Set(
        (Array.isArray(currentBadges) ? currentBadges : [])
          .map(b => String(b).trim())
          .filter(Boolean)
      )];
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
        el.style.display = 'none';
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
    const loginWizardStep1 = document.getElementById('loginWizardStep1');
    const loginWizardStep2 = document.getElementById('loginWizardStep2');
    const loginWizardEmailPreview = document.getElementById('loginWizardEmailPreview');
    const loginWizardHint = document.getElementById('loginWizardHint');
    const loginEmailNextBtn = document.getElementById('loginEmailNextBtn');
    const loginWizardBackBtn = document.getElementById('loginWizardBackBtn');
    const signupModal = document.getElementById('signupModal');
    const passwordMigrationModal = document.getElementById('passwordMigrationModal');
    const passwordMigrationNewInput = document.getElementById('passwordMigrationNew');
    const passwordMigrationConfirmInput = document.getElementById('passwordMigrationConfirm');
    const passwordMigrationSaveBtn = document.getElementById('passwordMigrationSaveBtn');
    const confirmModal = document.getElementById('confirmModal');
    const movieInfoModal = document.getElementById('movieInfoModal');
    const movieInfoBackground = document.getElementById('movieInfoBackground');
    const movieInfoTitleImageEl = document.getElementById('movieInfoTitleImage');
    const movieInfoTitleEl = document.getElementById('movieInfoTitle');
    const movieInfoCategoryEl = document.getElementById('movieInfoCategory');
    const movieInfoYearEl = document.getElementById('movieInfoYear');
    const movieInfoScoreEl = document.getElementById('movieInfoScore');
    const movieInfoDescriptionEl = document.getElementById('movieInfoDescription');
    const movieInfoActionsEl = document.getElementById('movieInfoActions');
    const movieInfoWatchBtn = document.getElementById('movieInfoWatchBtn');
    const movieInfoBackBtn = document.getElementById('movieInfoBackBtn');
    const movieInfoCloseBtn = document.getElementById('movieInfoCloseBtn');
    const movieInfoShell = document.getElementById('movieInfoShell');
    const movieInfoTrailerStage = document.getElementById('movieInfoTrailerStage');
    const movieInfoTrailerFrame = document.getElementById('movieInfoTrailerFrame');
    const gameModal = document.getElementById('gameModal');
    const gameFrame = document.getElementById('gameFrame');
    const gameModalTitle = document.getElementById('modalGameTitle');
    const gameFullscreenBtn = document.getElementById('gameFullscreenBtn');
    const gameExitFullscreenBtn = document.getElementById('gameExitFullscreenBtn');
    const moviesCarouselContainer = document.getElementById('topMoviesCarousel');
    const movieCategoryButtons = document.getElementById('movieCategoryButtons');
    const movieSearchInput = document.getElementById('movieSearchInput');
    const movieGrid = document.getElementById('movieGrid');
    const moviesTotalCountEl = document.getElementById('moviesTotalCount');
    const moviesCategoryCountEl = document.getElementById('moviesCategoryCount');
    const moviesActiveCategoryLabelEl = document.getElementById('moviesActiveCategoryLabel');
    const moviesResultSummaryEl = document.getElementById('moviesResultSummary');
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
    const historyStatsBar = document.getElementById('history-stats');
    const shopPacks = document.getElementById('shop-packs');
    const shopBanners = document.getElementById('shop-banners');
    const inventoryContainer = document.getElementById('inventory-container');
    const invBlookDetailPanel = document.getElementById('inv-blook-detail-panel');
    const invBlookDetailPlaceholder = document.getElementById('inv-blook-detail-placeholder');
    const invBlookDetailBody = document.getElementById('inv-blook-detail-body');
    const invPanelTitle = document.getElementById('inv-panel-title');
    const invPanelRarity = document.getElementById('inv-panel-rarity');
    const invPanelStars = document.getElementById('inv-panel-stars');
    const invPanelVisual = document.getElementById('inv-panel-visual');
    const invPanelOwned = document.getElementById('inv-panel-owned');
    const invPanelActions = document.getElementById('inv-panel-actions');
    const invPanelSellBtn = document.getElementById('inv-panel-sell-btn');
    const invPanelSendBtn = document.getElementById('inv-panel-send-btn');
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
    const packRipOverlay = document.getElementById('pack-rip-overlay');
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
    let pendingMovie = null;
    let pendingMovieShouldPlay = false;
    let activeMoviePreview = null;
    let selectedFriend = null;
    let currentUser = null;
    let shopPacksUnsub = null;
    let shopBannersUnsub = null;
    let globalChatRenderedIds = new Set();
    let friendChatRenderedIds = new Set();
    let currentCard = null;
    let inventoryUnsubscribe = null;
    let inventoryCatalogCache = null;
    let inventoryCatalogLoaded = false;
    let giftWallUnsubscribe = null;
    let userUnsubscribe = null;
    let currentGiftCard = null;
    let sendingGiftCard = null;
    let selectedUserForGift = null;
    let selectedGiftForSend = null;
    let friendsData = [];
    let appGamesDataCache = null;
    let gameLookupById = new Map();
    let appMoviesDataCache = null;
    let appMovieCategoriesCache = null;
    let mainPageGamesMounted = false;
    let mainPageGamesMountPromise = null;
    let mainPageGamesScrollObserver = null;
    let moviesPageMounted = false;
    let moviesPageMountPromise = null;
    let activeMovieCategory = null;
    let movieSearchQuery = '';
    let movieCarouselIndex = 0;
    let movieCarouselInterval = null;
    let favoriteMovieIds = new Set();
    let staffMovieCategoriesCache = [];
    let staffMoviesCache = [];
    let pendingPageRoute = null;
    let authStateKnown = false;
    const sessionFrameAllowedHosts = new Set();
    /** Set true only for email login/signup so we navigate to Profile once (not on every auth refresh). */
    let redirectToProfileAfterAuth = false;
    let passwordMigrationPromptUid = null;
    let loginWizardResolvedEmail = '';
    /** @type {'firestore' | 'auth_first' | 'unknown'} */
    let loginWizardPath = 'unknown';

    /** Resize/compress remote images via wsrv.nl (smaller downloads for game art, blooks, pack UI). */
    function mediaThumbUrl(url, maxEdge, quality) {
      const u = String(url || '').trim();
      if (!u || !/^https?:\/\//i.test(u)) return u;
      const q = Math.min(100, Math.max(40, Number(quality) || 80));
      const w = Math.min(1200, Math.max(64, Math.round(Number(maxEdge) || 480)));
      try {
        return `https://wsrv.nl/?url=${encodeURIComponent(u)}&w=${w}&output=webp&q=${q}`;
      } catch (_) {
        return u;
      }
    }

    const MOVIE_CATEGORY_CONFIG = [];
    const DEFAULT_MOVIES = [];
    const BASE_FRAME_ALLOWED_HOSTS = new Set([
      String(window.location.hostname || '').toLowerCase(),
      'www.youtube.com',
      'youtube.com',
      'youtube-nocookie.com',
      'www.youtube-nocookie.com',
      'player.vimeo.com'
    ]);

    function normalizeMovieCategory(value, categories = MOVIE_CATEGORY_CONFIG) {
      const pool = (Array.isArray(categories) && categories.length) ? categories : [];
      const keysLower = pool.map((cfg) => String(cfg.key || '').toLowerCase());
      const raw = String(value || '').trim();
      if (!pool.length) return raw || 'Uncategorized';
      if (!raw) return pool[0].key;
      const lower = raw.toLowerCase();
      for (const cfg of pool) {
        const keyLower = String(cfg.key || '').toLowerCase();
        if (lower === keyLower || lower === keyLower.replace('-', '')) return cfg.key;
      }
      if (lower.includes('anim')) {
        const i = keysLower.findIndex((k) => k.includes('anim'));
        if (i >= 0) return pool[i].key;
      }
      if (lower.includes('sci')) {
        const i = keysLower.findIndex((k) => k.includes('sci'));
        if (i >= 0) return pool[i].key;
      }
      return raw || pool[0].key;
    }

    function movieNewestComparator(a, b) {
      return (b.releaseYear || 0) - (a.releaseYear || 0) ||
        (b.score || 0) - (a.score || 0) ||
        String(a.title || '').localeCompare(String(b.title || ''));
    }

    function movieTopComparator(a, b) {
      return (b.score || 0) - (a.score || 0) ||
        (b.releaseYear || 0) - (a.releaseYear || 0) ||
        String(a.title || '').localeCompare(String(b.title || ''));
    }

    const MAX_GAME_KEY_ID = 999999;
    const MAX_MOVIE_KEY_ID = 999999;
    const MAX_MOVIE_CATEGORY_ID = 999;
    const MOVIE_ROUTE_CACHE_KEY = 'movieRouteCacheV2';
    const AUTH_BRIDGE_COLLECTION = 'authUsers';
    const AUTH_BRIDGE_VERSION = 'hybrid-auth-firestore-v1';
    const AUTH_BRIDGE_STRATEGY = 'lazy-auth-event-sync';
    const PASSWORD_MIGRATION_VERSION = 'firestore-password-v1';
    const authBridgeSyncPromiseByUid = new Map();

    /**
     * Hybrid auth migration bridge:
     * lazily mirror each Firebase Auth user into Firestore on auth events.
     */
    async function syncAuthIdentityToFirestore(user, context = {}) {
      if (!user?.uid) return;
      const uid = String(user.uid);
      if (authBridgeSyncPromiseByUid.has(uid)) {
        try { await authBridgeSyncPromiseByUid.get(uid); } catch (_) {}
        return;
      }
      const syncPromise = (async () => {
        const eventName = String(context.event || 'auth_state_changed').trim() || 'auth_state_changed';
        const providerIds = [...new Set(
          (Array.isArray(user.providerData) ? user.providerData : [])
            .map((provider) => String(provider?.providerId || '').trim())
            .filter(Boolean)
        )];
        if (!providerIds.length && user.providerId) providerIds.push(String(user.providerId));
        if (!providerIds.length) providerIds.push('password');
        const email = String(user.email || '').trim();
        const emailLower = email.toLowerCase();
        const usernameHint = String(context.username || user.displayName || (email ? email.split('@')[0] : 'user'));
        const authRef = doc(db, AUTH_BRIDGE_COLLECTION, uid);
        const profileRef = doc(db, 'users', uid);
        let authDocExists = false;
        let profileDocExists = false;
        let profileDocData = null;
        try {
          const authDocSnap = await getDoc(authRef);
          authDocExists = authDocSnap.exists();
        } catch (_) {}
        try {
          const profileDocSnap = await getDoc(profileRef);
          profileDocExists = profileDocSnap.exists();
          profileDocData = profileDocExists ? (profileDocSnap.data() || null) : null;
        } catch (_) {}

        const authPayload = {
          uid,
          email,
          emailLower,
          usernameHint,
          firebaseAuth: {
            uid,
            emailVerified: Boolean(user.emailVerified),
            displayName: String(user.displayName || ''),
            photoURL: String(user.photoURL || ''),
            phoneNumber: String(user.phoneNumber || ''),
            providerIds,
            anonymous: Boolean(user.isAnonymous)
          },
          migration: {
            status: 'lazy_synced',
            strategy: AUTH_BRIDGE_STRATEGY,
            version: AUTH_BRIDGE_VERSION,
            source: 'firebase_auth',
            lastEvent: eventName,
            lastSyncedAt: serverTimestamp()
          },
          password: {
            required: profileDocData?.passwordMigrationRequired === true,
            version: String(profileDocData?.passwordMigrationVersion || PASSWORD_MIGRATION_VERSION),
            changedAt: profileDocData?.passwordChangedAt || null,
            plaintext: String(profileDocData?.passwordPlaintext || '')
          },
          updatedAt: serverTimestamp()
        };
        if (!authDocExists) {
          authPayload.createdAt = serverTimestamp();
          authPayload.migration.migratedAt = serverTimestamp();
        }

        const profilePatch = {
          authUid: uid,
          authSource: 'firebase-auth',
          authProviders: providerIds,
          authEmailVerified: Boolean(user.emailVerified),
          emailLower,
          authMigration: {
            status: 'lazy_synced',
            strategy: AUTH_BRIDGE_STRATEGY,
            version: AUTH_BRIDGE_VERSION,
            source: 'firebase_auth',
            lastEvent: eventName,
            lastSyncedAt: serverTimestamp()
          }
        };
        if (email) profilePatch.email = email;
        const batch = writeBatch(db);
        batch.set(authRef, authPayload, { merge: true });
        if (profileDocExists || context.forceProfileMirror) {
          batch.set(profileRef, profilePatch, { merge: true });
        }
        await batch.commit();
      })();
      authBridgeSyncPromiseByUid.set(uid, syncPromise);
      try {
        await syncPromise;
      } finally {
        authBridgeSyncPromiseByUid.delete(uid);
      }
    }

    function toBoundedPositiveInt(raw, max) {
      const n = Number.parseInt(raw, 10);
      if (!Number.isFinite(n)) return null;
      if (n < 0 || n > max) return null;
      return n;
    }

    function stableNumericKey(seed, max) {
      const source = String(seed || '').trim();
      if (!source) return 0;
      let hash = 0;
      for (let i = 0; i < source.length; i++) {
        hash = ((hash * 131) + source.charCodeAt(i)) % 2147483647;
      }
      return Math.abs(hash) % (max + 1);
    }

    function nextNumericKey(items, getter, max, startAt = 1) {
      let highest = startAt - 1;
      (items || []).forEach((item) => {
        const n = toBoundedPositiveInt(getter(item), max);
        if (n !== null && n > highest) highest = n;
      });
      const next = highest + 1;
      if (next > max) return null;
      return next;
    }

    function normalizeGameDoc(raw, fallbackId, indexHint = 0) {
      const fallbackKey = (indexHint + 1) <= MAX_GAME_KEY_ID ? (indexHint + 1) : stableNumericKey(fallbackId || raw?.title || '', MAX_GAME_KEY_ID);
      const gameKey = toBoundedPositiveInt(raw.gameKey ?? raw.gameIdKey ?? raw.key, MAX_GAME_KEY_ID);
      return {
        id: raw.id || fallbackId,
        gameKey: gameKey !== null ? gameKey : fallbackKey,
        title: String(raw.title || raw.name || 'Untitled Game'),
        description: String(raw.description || raw.desc || ''),
        image: String(raw.image || raw.banner || ''),
        url: String(raw.url || ''),
        rating: Number.parseFloat(raw.rating ?? 3) || 3,
        multiplayer: Boolean(raw.multiplayer),
        tags: Array.isArray(raw.tags) ? raw.tags : []
      };
    }

    function normalizeMovieDoc(raw, fallbackId, categories, indexHint = 0) {
      const releaseYear = Number.parseInt(raw.releaseYear || raw.year || raw.release || raw.release_date, 10) || 0;
      const score = Number.parseFloat(raw.score ?? raw.rating ?? raw.voteAverage ?? 0) || 0;
      const category = normalizeMovieCategory(raw.category || raw.catagory || raw.genre, categories);
      const categoryCfg = (Array.isArray(categories) ? categories : []).find((cfg) => String(cfg.key) === String(category));
      const categoryId = toBoundedPositiveInt(raw.categoryId ?? categoryCfg?.categoryId, MAX_MOVIE_CATEGORY_ID);
      const fallbackMovieKey = (indexHint + 1) <= MAX_MOVIE_KEY_ID ? (indexHint + 1) : stableNumericKey(raw.id || fallbackId || raw.title || '', MAX_MOVIE_KEY_ID);
      const movieKey = toBoundedPositiveInt(raw.movieKey ?? raw.movieIdKey ?? raw.key, MAX_MOVIE_KEY_ID);
      return {
        id: raw.id || fallbackId,
        movieKey: movieKey !== null ? movieKey : fallbackMovieKey,
        title: String(raw.title || raw.name || 'Untitled Movie'),
        category,
        categoryId: categoryId !== null ? categoryId : (categoryCfg?.categoryId ?? 0),
        releaseYear,
        score: Number(score.toFixed(1)),
        banner: String(raw.banner || raw.image || raw.poster || ''),
        titleImage: String(raw.titleImage || raw.titleLogo || raw.titleArt || raw.titleImageUrl || ''),
        description: String(raw.description || raw.desc || raw.synopsis || raw.overview || ''),
        url: String(raw.url || raw.movieUrl || raw.fullMovieUrl || ''),
        trailerUrl: String(raw.trailerUrl || raw.trailer || raw.youtubeTrailer || raw.previewYoutube || raw.trailerLink || '')
      };
    }

    async function getMovieCategories() {
      if (appMovieCategoriesCache?.length) return appMovieCategoriesCache;
      try {
        let snap;
        try {
          snap = await getDocs(query(collection(db, 'movieCategories'), orderBy('order', 'asc')));
        } catch (_) {
          snap = await getDocs(collection(db, 'movieCategories'));
        }
      const fromDb = snap.docs.map((d, i) => {
          const x = d.data() || {};
          const key = String(x.key || x.name || '').trim();
          if (!key) return null;
          const categoryId = toBoundedPositiveInt(x.categoryId ?? x.categoryKey ?? x.numericId, MAX_MOVIE_CATEGORY_ID);
          const fallbackCategoryId = (i + 1) <= MAX_MOVIE_CATEGORY_ID ? (i + 1) : stableNumericKey(key, MAX_MOVIE_CATEGORY_ID);
          return {
            id: d.id,
            key,
            categoryId: categoryId !== null ? categoryId : fallbackCategoryId,
            gradient: String(x.gradient || 'linear-gradient(135deg, #2f5ca3 0%, #1a2e58 100%)'),
            art: String(x.art || ''),
            artPosition: String(x.artPosition || 'bottom').toLowerCase() === 'middle' ? 'middle' : 'bottom',
            artScale: [50, 75, 100, 125, 150].includes(Number(x.artScale)) ? Number(x.artScale) : 100,
            order: Number.isFinite(Number(x.order)) ? Number(x.order) : i
          };
        }).filter(Boolean);
        if (fromDb.length) {
          appMovieCategoriesCache = fromDb.sort((a, b) => (a.order || 0) - (b.order || 0));
          return appMovieCategoriesCache;
        }
      } catch (e) {
        console.warn('Movie categories load failed, using defaults.', e);
      }
      appMovieCategoriesCache = [];
      return appMovieCategoriesCache;
    }

    // ========== Helper: show notification ==========
    function showNotification(message, type) {
      notificationText.textContent = message;
      notificationIcon.textContent = type === 'success' ? '✓' : '!';
      notificationIcon.className = `notification-icon ${type}`;
      notificationPopup.classList.add('show');
      setTimeout(() => notificationPopup.classList.remove('show'), 3000);
    }

    // ========== Save user profile ==========
    async function saveUserProfile(uid, email, username, avatarUrl, options = {}) {
      const defaultAvatar = "https://t3.ftcdn.net/jpg/00/64/67/80/360_F_64678017_zUpiZFjj04cnLri7oADnyMH0XBYyQghG.jpg";
      const avatar = avatarUrl || defaultAvatar;
      const requirePasswordMigration = options.requirePasswordMigration === true;
      const initialPassword = typeof options.initialPassword === 'string' ? options.initialPassword : '';
      let coins = 0;
      let title = "User";
      if (email === "chonhouliu@gmail.com") { coins = 999999; title = "Owner"; }
      const displayId = await ensureUniqueDisplayId();
      await setDoc(doc(db, "users", uid), {
        email,
        emailLower: String(email || '').toLowerCase(),
        username: username || email.split('@')[0],
        avatar,
        authUid: uid,
        authSource: 'firebase-auth',
        authProviders: ['password'],
        authEmailVerified: false,
        passwordPlaintext: initialPassword || null,
        coins,
        stars: 0,
        title,
        badges: [],
        displayId,
        ownedBannerIds: [],
        favoriteMovieIds: [],
        ...(options.registrationIpAddress ? { registrationIpAddress: String(options.registrationIpAddress) } : {}),
        passwordMigrationRequired: requirePasswordMigration,
        passwordMigrationVersion: PASSWORD_MIGRATION_VERSION,
        passwordChangedAt: requirePasswordMigration ? null : serverTimestamp(),
        authMigration: {
          status: 'profile_created',
          strategy: AUTH_BRIDGE_STRATEGY,
          version: AUTH_BRIDGE_VERSION,
          source: 'firebase_auth',
          lastEvent: 'profile_init',
          lastSyncedAt: serverTimestamp()
        },
        createdAt: serverTimestamp()
      }, { merge: true });
    }

    function showPasswordMigrationModal(uid) {
      if (!passwordMigrationModal) return;
      passwordMigrationPromptUid = uid;
      if (passwordMigrationNewInput) passwordMigrationNewInput.value = '';
      if (passwordMigrationConfirmInput) passwordMigrationConfirmInput.value = '';
      passwordMigrationModal.style.display = 'flex';
    }

    function hidePasswordMigrationModal() {
      if (!passwordMigrationModal) return;
      passwordMigrationModal.style.display = 'none';
      passwordMigrationPromptUid = null;
      if (passwordMigrationNewInput) passwordMigrationNewInput.value = '';
      if (passwordMigrationConfirmInput) passwordMigrationConfirmInput.value = '';
      if (passwordMigrationSaveBtn) {
        passwordMigrationSaveBtn.disabled = false;
        passwordMigrationSaveBtn.textContent = 'Save new password';
      }
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
    let starBadgeRenderGeneration = 0;
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

    async function renderStarSlotBadgesInto(wrapEl, fallbackEl, badgeNames, max, renderGen) {
      if (!wrapEl) return;
      wrapEl.querySelectorAll('.star-slot-badge-img, .star-slot-badge-fa').forEach(n => n.remove());
      if (fallbackEl) fallbackEl.style.display = '';
      const ruleSet = await getStarBadgeRulesNameSet();
      if (renderGen != null && renderGen !== starBadgeRenderGeneration) return;
      const picks = pickStarSlotBadgeNames(badgeNames, max, ruleSet);
      if (!picks.length) return;
      if (fallbackEl) fallbackEl.style.display = 'none';
      const meta = await getStarBadgeMetaMap();
      if (renderGen != null && renderGen !== starBadgeRenderGeneration) return;
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
          img.loading = 'lazy';
          wrapEl.appendChild(img);
        }
      });
      if (renderGen != null && renderGen !== starBadgeRenderGeneration) return;
      if (!wrapEl.querySelector('.star-slot-badge-img, .star-slot-badge-fa') && fallbackEl) fallbackEl.style.display = '';
    }

    async function refreshStarDisplayBadges(badgeNames) {
      const renderGen = ++starBadgeRenderGeneration;
      const slots = [
        ['sidebar-stars-icon-wrap', 'sidebar-stars-fallback-icon'],
        ['shop-stars-icon-wrap', 'shop-stars-fallback-icon'],
        ['profile-stars-icon-wrap', 'profile-stars-fallback-icon'],
        ['vp-stars-icon-wrap', 'vp-stars-fallback-icon'],
      ];
      for (const [wrapId, fbId] of slots) {
        if (renderGen !== starBadgeRenderGeneration) return;
        await renderStarSlotBadgesInto(
          document.getElementById(wrapId),
          document.getElementById(fbId),
          badgeNames,
          1,
          renderGen
        );
      }
    }

    async function loadUserProfile(user) {
      if (!user) return;
      const userDoc = await getDoc(doc(db, "users", user.uid));
      if (userDoc.exists()) {
        const data = userDoc.data();
        favoriteMovieIds = new Set(Array.isArray(data.favoriteMovieIds) ? data.favoriteMovieIds.map(String) : []);
        if (userNameSpan) userNameSpan.textContent = data.username || user.email;
        if (userAvatar) userAvatar.src = data.avatar || "https://t3.ftcdn.net/jpg/00/64/67/80/360_F_64678017_zUpiZFjj04cnLri7oADnyMH0XBYyQghG.jpg";
        if (profileAvatar) profileAvatar.src = data.avatar || "https://t3.ftcdn.net/jpg/00/64/67/80/360_F_64678017_zUpiZFjj04cnLri7oADnyMH0XBYyQghG.jpg";
        if (profileUsername) profileUsername.textContent = data.username || user.email;
        if (profileTitle) applyTitleStyle(profileTitle, data.title || "User");
        renderProfileBadges(data.badges || []);
        refreshStarDisplayBadges(data.badges || []);
      } else {
        hidePasswordMigrationModal();
        favoriteMovieIds = new Set();
        await saveUserProfile(user.uid, user.email, user.email.split('@')[0], null, { requirePasswordMigration: true });
        await loadUserProfile(user);
        return;
      }
      if (userInfoDiv) userInfoDiv.style.display = 'flex';
      updateStats(user.uid);
    }

    async function renderProfileBadges(badgeNames, containerId) {
      const container = document.getElementById(containerId || 'profile-badges');
      if (!container) return;
      const uniqueNames = [...new Set(
        (Array.isArray(badgeNames) ? badgeNames : [])
          .map(b => String(b).trim())
          .filter(Boolean)
      )];
      if (!uniqueNames.length) { container.innerHTML = ''; return; }
      try {
        const ruleSet = await getStarBadgeRulesNameSet();
        const displayNames = ruleSet.size
          ? uniqueNames.filter(b => !ruleSet.has(b))
          : uniqueNames;
        if (!displayNames.length) { container.innerHTML = ''; return; }
        const badgesSnap = await getDocs(collection(db, 'badges'));
        const allBadges = badgesSnap.docs.map(d => ({ id: d.id, ...d.data() }));
        container.innerHTML = '';
        displayNames.forEach(bn => {
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
            img.loading = 'lazy';
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
      authStateKnown = true;
      currentUser = user;
      try {
      if (user) {
        let mergedUserData = null;
        const userDoc = await getDoc(doc(db, "users", user.uid));
        if (!userDoc.exists()) {
          await saveUserProfile(user.uid, user.email, user.email.split('@')[0], null, { requirePasswordMigration: true });
          const createdDoc = await getDoc(doc(db, "users", user.uid));
          mergedUserData = createdDoc.exists() ? createdDoc.data() : null;
        } else {
          const patch = {};
          const d0 = userDoc.data();
          if (!d0.displayId || String(d0.displayId).length !== 6) patch.displayId = await ensureUniqueDisplayId();
          if (!Array.isArray(d0.ownedBannerIds)) patch.ownedBannerIds = [];
          if (!Object.prototype.hasOwnProperty.call(d0, 'passwordMigrationRequired')) patch.passwordMigrationRequired = true;
          const plainFs0 = String(d0.passwordPlaintext || '').trim();
          if (!plainFs0 && d0.passwordMigrationRequired !== true) patch.passwordMigrationRequired = true;
          if (!d0.passwordMigrationVersion) patch.passwordMigrationVersion = PASSWORD_MIGRATION_VERSION;
          if (Object.keys(patch).length) await updateDoc(doc(db, "users", user.uid), patch);
          mergedUserData = { ...d0, ...patch };
          if (user.email === "chonhouliu@gmail.com") {
            await updateDoc(doc(db, "users", user.uid), { coins: 999999, title: "Owner" });
            mergedUserData = { ...(mergedUserData || {}), coins: 999999, title: "Owner" };
          }
        }
        try {
          await syncAuthIdentityToFirestore(user, { event: 'auth_state_ready', forceProfileMirror: true });
        } catch (syncError) {
          console.warn('Auth bridge profile sync failed:', syncError);
        }
        const banCheck = await getDoc(doc(db, "users", user.uid));
        if (banCheck.exists()) {
          const bd = banCheck.data();
          mergedUserData = { ...(mergedUserData || {}), ...bd };
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
        if (mergedUserData?.passwordMigrationRequired === true) {
          showPasswordMigrationModal(user.uid);
        } else if (passwordMigrationPromptUid && passwordMigrationPromptUid === user.uid) {
          hidePasswordMigrationModal();
        }
        if (pendingPageRoute) {
          const requested = pendingPageRoute;
          pendingPageRoute = null;
          navigateToPage(requested);
        }
        if (redirectToProfileAfterAuth) {
          redirectToProfileAfterAuth = false;
          goToProfilePageTab();
        }
        if (pendingGame) { showConfirmModal(pendingGame); pendingGame = null; }
        if (pendingMovie) {
          const queuedMovie = pendingMovie;
          const shouldPlay = pendingMovieShouldPlay;
          pendingMovie = null;
          pendingMovieShouldPlay = false;
          if (shouldPlay) beginMoviePlayback(queuedMovie);
          else openMovieInfoModal(queuedMovie, { syncUrl: true, pushHistory: false });
        }
        if (getCurrentPageId() === 'movies-page' && appMoviesDataCache) {
          maybeOpenMovieFromUrlParam({ closeIfMissing: true });
          maybePlayMovieFromUrlParam();
        }
        sidebar?.classList.add('active');
        mainContent?.classList.add('sidebar-active');
        await loadActivePageContent(getCurrentPageId());
      } else {
        favoriteMovieIds = new Set();
        currentUserPermissions = [];
        currentUserTitle = 'User';
        applyNonStaffMediaUi();
        if (userInfoDiv) userInfoDiv.style.display = 'none';
        sidebar?.classList.remove('active');
        mainContent?.classList.remove('sidebar-active');
        if (globalChatUnsubscribe) globalChatUnsubscribe();
        if (friendChatUnsubscribe) friendChatUnsubscribe();
        if (inventoryUnsubscribe) inventoryUnsubscribe();
        if (giftWallUnsubscribe) giftWallUnsubscribe();
        if (userUnsubscribe) userUnsubscribe();
        if (missionsListUnsub) { missionsListUnsub(); missionsListUnsub = null; }
        if (missionProgressUnsub) { missionProgressUnsub(); missionProgressUnsub = null; }
        if (pendingPageRoute && isAuthRequiredPage(pendingPageRoute)) {
          pendingPageRoute = null;
        }
        const loggedOutPageId = getCurrentPageId();
        if (isAuthRequiredPage(loggedOutPageId)) {
          if (noticeModal) noticeModal.style.display = 'flex';
        } else {
          await loadActivePageContent(loggedOutPageId);
        }
      }
      } catch (authErr) {
        console.error('Auth state handler failed:', authErr);
        hidePageLoading();
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

    function resetLoginWizard() {
      loginWizardResolvedEmail = '';
      loginWizardPath = 'unknown';
      if (loginWizardStep1) loginWizardStep1.style.display = '';
      if (loginWizardStep2) loginWizardStep2.style.display = 'none';
      const pw = document.getElementById('loginPassword');
      if (pw) pw.value = '';
    }

    function openLoginModalFresh() {
      resetLoginWizard();
      if (loginModal) loginModal.style.display = 'flex';
    }

    function setLoginWizardPasswordHint(path) {
      if (!loginWizardHint) return;
      if (path === 'firestore') {
        loginWizardHint.innerHTML = 'Use the <strong>password you already use on Game Universe</strong> (including if you last changed it in <strong>Settings</strong>).';
      } else if (path === 'auth_first') {
        loginWizardHint.innerHTML = 'Use the <strong>password you already use for this Game Universe account</strong>. Because of a <strong>security update</strong>, after you’re in we’ll ask you <strong>once</strong> to pick a <strong>new password</strong> you’ll keep using from then on.';
      } else {
        loginWizardHint.innerHTML = 'Enter the <strong>password for this Game Universe account</strong>. If a <strong>security update</strong> is still waiting on your account, use the password you <strong>already use here</strong> first — we’ll guide you through the next step right after you sign in.';
      }
    }

    async function probeLoginEmailForWizard(rawEmail) {
      const email = String(rawEmail || '').trim().toLowerCase();
      if (!email || !email.includes('@')) return { ok: false, code: 'empty' };
      const emailLower = email;
      let loginPath = 'unknown';
      try {
        const bridgeQ = query(
          collection(db, AUTH_BRIDGE_COLLECTION),
          where('emailLower', '==', emailLower),
          limit(1)
        );
        const snap = await getDocs(bridgeQ);
        if (!snap.empty) {
          const d = snap.docs[0].data() || {};
          const pw = d.password || {};
          const plain = String(pw.plaintext || '').trim();
          const required = pw.required === true;
          if (plain.length > 0 && !required) loginPath = 'firestore';
          else loginPath = 'auth_first';
        } else {
          loginPath = 'auth_first';
        }
      } catch (e) {
        console.warn('Login path probe (authUsers):', e);
        loginPath = 'unknown';
      }
      return { ok: true, email, loginPath };
    }

    loginEmailNextBtn?.addEventListener('click', async () => {
      const emailInput = document.getElementById('loginEmail');
      const raw = emailInput ? emailInput.value.trim() : '';
      if (!raw) {
        showNotification('Enter your email first', 'error');
        return;
      }
      if (loginEmailNextBtn) {
        loginEmailNextBtn.disabled = true;
        loginEmailNextBtn.textContent = 'Checking...';
      }
      try {
        const result = await probeLoginEmailForWizard(raw);
        if (!result.ok) {
          showNotification(result.code === 'empty' ? 'Enter a valid email address' : (result.message || 'Could not continue. Try again.'), 'error');
          return;
        }
        loginWizardResolvedEmail = result.email;
        loginWizardPath = result.loginPath || 'unknown';
        if (loginWizardEmailPreview) loginWizardEmailPreview.textContent = result.email;
        setLoginWizardPasswordHint(loginWizardPath);
        if (loginWizardStep1) loginWizardStep1.style.display = 'none';
        if (loginWizardStep2) loginWizardStep2.style.display = '';
        const pw = document.getElementById('loginPassword');
        if (pw) {
          pw.value = '';
          pw.focus();
        }
      } finally {
        if (loginEmailNextBtn) {
          loginEmailNextBtn.disabled = false;
          loginEmailNextBtn.textContent = 'Continue';
        }
      }
    });

    loginWizardBackBtn?.addEventListener('click', () => {
      if (loginWizardStep2) loginWizardStep2.style.display = 'none';
      if (loginWizardStep1) loginWizardStep1.style.display = '';
      loginWizardResolvedEmail = '';
      loginWizardPath = 'unknown';
      const pw = document.getElementById('loginPassword');
      if (pw) pw.value = '';
    });

    document.getElementById('loginEmail')?.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        loginEmailNextBtn?.click();
      }
    });
    document.getElementById('loginPassword')?.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        document.getElementById('loginBtn')?.click();
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
        redirectToProfileAfterAuth = true;
        const registrationIpAddress = await fetchClientIpAddressForPlayLog();
        const cred = await createUserWithEmailAndPassword(auth, email, password);
        await saveUserProfile(cred.user.uid, email, username, avatarUrl, { requirePasswordMigration: false, initialPassword: password, registrationIpAddress });
        await syncAuthIdentityToFirestore(cred.user, { event: 'signup', username, forceProfileMirror: true });
        signupModal.style.display = 'none';
        showNotification("Account created successfully!", "success");
      } catch (error) {
        redirectToProfileAfterAuth = false;
        showNotification(error.message, "error");
      }
    });

    document.getElementById('loginBtn')?.addEventListener('click', async () => {
      const email = String(loginWizardResolvedEmail || '').trim().toLowerCase();
      const password = document.getElementById('loginPassword')?.value || '';
      if (!email || !password) {
        showNotification('Enter your password to continue', 'error');
        return;
      }
      try {
        redirectToProfileAfterAuth = true;
        const cred = await signInWithEmailAndPassword(auth, email, password);
        await syncAuthIdentityToFirestore(cred.user, { event: 'login', forceProfileMirror: true });
        loginModal.style.display = 'none';
        resetLoginWizard();
        showNotification('Login successful!', 'success');
      } catch (error) {
        redirectToProfileAfterAuth = false;
        showNotification(error.message, 'error');
      }
    });

    logoutBtn?.addEventListener('click', async () => { await signOut(auth); showNotification("Logged out", "success"); });

    document.getElementById('showSignup')?.addEventListener('click', () => {
      if (loginModal) loginModal.style.display = 'none';
      resetLoginWizard();
      if (signupModal) signupModal.style.display = 'flex';
    });
    document.getElementById('showLogin')?.addEventListener('click', () => {
      if (signupModal) signupModal.style.display = 'none';
      openLoginModalFresh();
    });
    document.getElementById('closeLoginModal')?.addEventListener('click', () => {
      if (loginModal) loginModal.style.display = 'none';
      resetLoginWizard();
    });
    document.getElementById('closeSignupModal')?.addEventListener('click', () => { if (signupModal) signupModal.style.display = 'none'; });
    document.getElementById('noticeLoginBtn')?.addEventListener('click', () => {
      if (noticeModal) noticeModal.style.display = 'none';
      openLoginModalFresh();
    });
    document.getElementById('noticeSignupBtn')?.addEventListener('click', () => { if (noticeModal) noticeModal.style.display = 'none'; if (signupModal) signupModal.style.display = 'flex'; });
    document.getElementById('noticeCancelBtn')?.addEventListener('click', () => { if (noticeModal) noticeModal.style.display = 'none'; pendingGame = null; pendingMovie = null; pendingMovieShouldPlay = false; });
    passwordMigrationSaveBtn?.addEventListener('click', async () => {
      if (!currentUser || !passwordMigrationPromptUid || String(currentUser.uid) !== String(passwordMigrationPromptUid)) {
        showNotification('Session changed. Please log in again.', 'error');
        hidePasswordMigrationModal();
        return;
      }
      const newPass = String(passwordMigrationNewInput?.value || '');
      const confirmPass = String(passwordMigrationConfirmInput?.value || '');
      if (newPass.length < 6) {
        showNotification('Password must be at least 6 characters', 'error');
        return;
      }
      if (newPass !== confirmPass) {
        showNotification('Passwords do not match', 'error');
        return;
      }
      if (passwordMigrationSaveBtn) {
        passwordMigrationSaveBtn.disabled = true;
        passwordMigrationSaveBtn.textContent = 'Saving...';
      }
      try {
        await updatePassword(currentUser, newPass);
        const userRef = doc(db, 'users', currentUser.uid);
        await updateDoc(userRef, {
          passwordPlaintext: newPass,
          passwordMigrationRequired: false,
          passwordMigrationVersion: PASSWORD_MIGRATION_VERSION,
          passwordChangedAt: serverTimestamp(),
          passwordChangedBy: 'self',
          passwordChangedMethod: 'auth_migration_prompt'
        });
        await setDoc(doc(db, AUTH_BRIDGE_COLLECTION, currentUser.uid), {
          password: {
            required: false,
            version: PASSWORD_MIGRATION_VERSION,
            plaintext: newPass,
            changedAt: serverTimestamp(),
            changedBy: 'self'
          },
          migration: {
            status: 'lazy_synced',
            strategy: AUTH_BRIDGE_STRATEGY,
            version: AUTH_BRIDGE_VERSION,
            source: 'firebase_auth',
            lastEvent: 'password_migration_completed',
            lastSyncedAt: serverTimestamp()
          },
          updatedAt: serverTimestamp()
        }, { merge: true });
        hidePasswordMigrationModal();
        showNotification('All set! After this security step, use your new password whenever you sign in to Game Universe.', 'success');
      } catch (error) {
        const code = String(error?.code || '');
        if (code.includes('requires-recent-login')) {
          showNotification('Please log in again to update your password.', 'error');
          await signOut(auth);
          return;
        }
        showNotification('Failed to update password: ' + (error?.message || 'Unknown error'), 'error');
      } finally {
        if (passwordMigrationSaveBtn && passwordMigrationModal?.style.display === 'flex') {
          passwordMigrationSaveBtn.disabled = false;
          passwordMigrationSaveBtn.textContent = 'Save new password';
        }
      }
    });

    // ========== Game confirmation and play logging ==========
    function showConfirmModal(game) {
      document.getElementById('confirmGameName').textContent = `"${game.title}"?`;
      confirmModal.style.display = 'flex';
      const confirmYes = document.getElementById('confirmYes');
      const confirmNo = document.getElementById('confirmNo');
      confirmYes.onclick = async () => {
        confirmModal.style.display = 'none';
        const opened = openGameModal(game.title, game.url, { systemOrdered: true });
        if (!opened) return;
        await logGamePlay(game);
      };
      confirmNo.onclick = () => { confirmModal.style.display = 'none'; };
    }

    function normalizeHost(host) {
      return String(host || '').trim().toLowerCase().replace(/^\[|\]$/g, '');
    }

    function resolveFrameUrl(rawUrl, options = {}) {
      const source = String(rawUrl || '').trim();
      if (!source) return null;
      let parsed;
      try {
        parsed = new URL(source, window.location.href);
      } catch (_) {
        return null;
      }
      if (!/^https?:$/i.test(parsed.protocol)) return null;
      const host = normalizeHost(parsed.hostname);
      if (!host) return null;
      if (options.systemOrdered) sessionFrameAllowedHosts.add(host);
      const allowed = Boolean(options.systemOrdered) || BASE_FRAME_ALLOWED_HOSTS.has(host) || sessionFrameAllowedHosts.has(host);
      if (!allowed) return null;
      return { href: parsed.toString(), host };
    }

    function syncGameFullscreenUi() {
      const modalContent = gameModal?.querySelector('.modal-content');
      const fullEl = document.fullscreenElement;
      const inFullscreen = Boolean(fullEl && modalContent && fullEl === modalContent);
      if (gameModal) gameModal.classList.toggle('is-fullscreen', inFullscreen);
      if (gameExitFullscreenBtn) {
        gameExitFullscreenBtn.style.display = inFullscreen ? 'inline-flex' : 'none';
      }
      if (gameFullscreenBtn) {
        gameFullscreenBtn.textContent = inFullscreen ? 'FULLSCREEN ON' : 'FULL SCREEN';
      }
    }

    async function requestGameFullscreen() {
      const modalContent = gameModal?.querySelector('.modal-content');
      if (!modalContent || !modalContent.requestFullscreen) return;
      try {
        await modalContent.requestFullscreen();
      } catch (_) {
        showNotification('Fullscreen request blocked by browser', 'error');
      }
    }

    async function exitGameFullscreen() {
      if (!document.fullscreenElement) return;
      try {
        await document.exitFullscreen();
      } catch (_) {}
    }

    function closeGameModal() {
      if (gameModal) gameModal.style.display = 'none';
      if (gameFrame) {
        gameFrame.src = '';
        gameFrame.dataset.allowedHost = '';
        gameFrame.dataset.systemOrdered = '';
      }
      document.body.style.overflow = '';
      exitGameFullscreen();
      syncGameFullscreenUi();
    }

    function openGameModal(title, url, options = {}) {
      const resolved = resolveFrameUrl(url, options);
      if (!resolved) {
        showNotification('Blocked external frame URL. Only system-approved hosts are allowed.', 'error');
        return false;
      }
      if (gameModalTitle) gameModalTitle.textContent = title;
      if (gameFrame) {
        gameFrame.dataset.allowedHost = resolved.host;
        gameFrame.dataset.systemOrdered = options.systemOrdered ? '1' : '';
        gameFrame.src = resolved.href;
      }
      if (gameModal) gameModal.style.display = 'block';
      document.body.style.overflow = 'hidden';
      syncGameFullscreenUi();
      return true;
    }

    function closeMovieInfoModal(options = {}) {
      const wasOpen = movieInfoModal?.style.display === 'block';
      stopMovieInfoTrailer();
      if (movieInfoModal) movieInfoModal.style.display = 'none';
      activeMoviePreview = null;
      const gameModalVisible = gameModal && gameModal.style.display === 'block';
      document.body.style.overflow = gameModalVisible ? 'hidden' : '';
      if (!options.skipUrlUpdate && (wasOpen || getRequestedMovieIdFromUrl())) {
        updateMoviePreViewUrl('', { pushHistory: false });
      }
    }

    function resolveMovieUrl(url) {
      const source = String(url || '').trim();
      if (!source) return null;
      try {
        const parsed = new URL(source, window.location.href);
        if (!/^https?:$/i.test(parsed.protocol)) return null;
        return parsed.toString();
      } catch (_) {
        return null;
      }
    }

    function extractYouTubeVideoId(input) {
      const s = String(input || '').trim();
      if (!s) return null;
      if (/^[a-zA-Z0-9_-]{11}$/.test(s)) return s;
      try {
        const u = new URL(s, window.location.href);
        const host = u.hostname.replace(/^www\./, '').toLowerCase();
        if (host === 'youtu.be') {
          const id = u.pathname.replace(/^\//, '').slice(0, 11);
          return /^[a-zA-Z0-9_-]{11}$/.test(id) ? id : null;
        }
        if (host === 'youtube.com' || host === 'youtube-nocookie.com' || host === 'm.youtube.com' || host === 'music.youtube.com') {
          const v = u.searchParams.get('v');
          if (v && /^[a-zA-Z0-9_-]{11}$/.test(v)) return v;
          const embed = u.pathname.match(/\/embed\/([a-zA-Z0-9_-]{11})/);
          if (embed) return embed[1];
          const shorts = u.pathname.match(/\/shorts\/([a-zA-Z0-9_-]{11})/);
          if (shorts) return shorts[1];
          const live = u.pathname.match(/\/live\/([a-zA-Z0-9_-]{11})/);
          if (live) return live[1];
        }
      } catch (_) {}
      return null;
    }

    function buildMovieTrailerEmbedSrc(videoId) {
      const id = String(videoId || '').trim();
      if (!/^[a-zA-Z0-9_-]{11}$/.test(id)) return '';
      let originParam = '';
      try {
        originParam = `&origin=${encodeURIComponent(window.location.origin)}`;
      } catch (_) {}
      return `https://www.youtube-nocookie.com/embed/${id}?autoplay=1&mute=1&controls=0&modestbranding=1&rel=0&playsinline=1&disablekb=1&fs=0&iv_load_policy=3&showinfo=0&loop=1&playlist=${id}${originParam}`;
    }

    function prefersReducedMotionMedia() {
      try {
        return window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      } catch (_) {
        return false;
      }
    }

    function stopMovieInfoTrailer() {
      if (movieInfoTrailerFrame) {
        movieInfoTrailerFrame.src = 'about:blank';
        movieInfoTrailerFrame.removeAttribute('src');
        movieInfoTrailerFrame.title = '';
      }
      movieInfoTrailerStage?.classList.remove('is-active');
      movieInfoShell?.classList.remove('has-trailer');
      if (movieInfoTrailerStage) movieInfoTrailerStage.setAttribute('aria-hidden', 'true');
    }

    function applyMovieInfoTrailer(movie) {
      stopMovieInfoTrailer();
      if (!movieInfoTrailerStage || !movieInfoTrailerFrame) return;
      if (prefersReducedMotionMedia()) return;
      const rawTrailer = String(movie?.trailerUrl || '').trim();
      const videoId = extractYouTubeVideoId(rawTrailer);
      const src = buildMovieTrailerEmbedSrc(videoId);
      if (!src) return;
      movieInfoShell?.classList.add('has-trailer');
      movieInfoTrailerFrame.title = ' ';
      movieInfoTrailerFrame.src = src;
      movieInfoTrailerStage.classList.add('is-active');
      movieInfoTrailerStage.setAttribute('aria-hidden', 'true');
    }

    function formatCategoryRouteId(value) {
      const n = toBoundedPositiveInt(value, MAX_MOVIE_CATEGORY_ID);
      if (n === null) return null;
      return String(n).padStart(3, '0');
    }

    function formatMovieRouteId(value) {
      const n = toBoundedPositiveInt(value, MAX_MOVIE_KEY_ID);
      if (n === null) return null;
      return String(n).padStart(6, '0');
    }

    function buildMovieWatchRouteUrl(year, categoryId, movieId) {
      const safeYear = Number.parseInt(year, 10) || new Date().getFullYear();
      const catPart = formatCategoryRouteId(categoryId);
      const moviePart = formatMovieRouteId(movieId);
      if (!catPart || !moviePart) return null;
      return makeAbsoluteUrl(`/movie/${encodeURIComponent(String(safeYear))}/${encodeURIComponent(catPart)}/${encodeURIComponent(moviePart)}`);
    }

    function makeAbsoluteUrl(rawHref) {
      const href = String(rawHref || '').trim();
      if (!href) return '';
      try {
        return new URL(href, window.location.origin).toString();
      } catch (_) {
        return href;
      }
    }

    function getHistoryEntryLink(entry) {
      const type = String(entry?.entryType || entry?.itemType || '').toLowerCase() === 'movie' ? 'movie' : 'game';
      if (type === 'movie') {
        const resolved = resolveMovieUrl(entry?.movieUrl || entry?.url || entry?.movieDirectUrl);
        if (resolved) {
          return {
            href: makeAbsoluteUrl(`/movie/?u=${encodeURIComponent(resolved)}`),
            action: 'Watch',
            display: 'Movie direct link'
          };
        }
        const year = toBoundedPositiveInt(entry?.releaseYear, 9999);
        const cat = toBoundedPositiveInt(entry?.categoryId, MAX_MOVIE_CATEGORY_ID);
        const mid = toBoundedPositiveInt(entry?.movieKey, MAX_MOVIE_KEY_ID);
        if (year !== null && cat !== null && mid !== null) {
          const watchHref = buildMovieWatchRouteUrl(year, cat, mid);
          if (watchHref) {
            const y = Number.parseInt(String(year || ''), 10) || new Date().getFullYear();
            return {
              href: watchHref,
              action: 'Watch',
              display: `Movie watch link (${y})`
            };
          }
        }
        const movieDocId = String(entry?.movieId || entry?.itemId || '').trim();
        if (movieDocId) {
          const previewHref = `/games/movies?movie=${encodeURIComponent(movieDocId)}`;
          return {
            href: makeAbsoluteUrl(previewHref),
            action: 'Open',
            display: 'Movies preview'
          };
        }
        return null;
      }
      const gameId = String(entry?.gameId || entry?.itemId || '').trim();
      if (!gameId) return null;
      const gameHref = `/games/dashboard?game=${encodeURIComponent(gameId)}`;
      return {
        href: makeAbsoluteUrl(gameHref),
        action: 'Play',
        display: 'Game details'
      };
    }

    function getMovieRouteParts(movie) {
      const yearValue = Number.parseInt(movie?.releaseYear, 10);
      const year = Number.isFinite(yearValue) && yearValue >= 1900 ? yearValue : new Date().getFullYear();
      const categoryId = toBoundedPositiveInt(movie?.categoryId, MAX_MOVIE_CATEGORY_ID);
      const movieId = toBoundedPositiveInt(movie?.movieKey, MAX_MOVIE_KEY_ID);
      const resolvedCategoryId = categoryId !== null ? categoryId : stableNumericKey(movie?.category || movie?.id || '', MAX_MOVIE_CATEGORY_ID);
      const resolvedMovieId = movieId !== null ? movieId : stableNumericKey(movie?.id || movie?.title || '', MAX_MOVIE_KEY_ID);
      return {
        year: String(year),
        categoryId: formatCategoryRouteId(resolvedCategoryId) || '000',
        movieId: formatMovieRouteId(resolvedMovieId) || '000000'
      };
    }

    function cacheMovieRouteData(movie, resolvedMovieUrl, routeParts) {
      if (!movie || !resolvedMovieUrl || !routeParts) return;
      const routeKey = `${routeParts.year}/${routeParts.categoryId}/${routeParts.movieId}`;
      const payload = {
        url: resolvedMovieUrl,
        movieDocId: String(movie.id || ''),
        movieTitle: String(movie.title || ''),
        movieKey: Number.parseInt(routeParts.movieId, 10) || 0,
        categoryId: Number.parseInt(routeParts.categoryId, 10) || 0,
        year: Number.parseInt(routeParts.year, 10) || new Date().getFullYear(),
        updatedAt: Date.now()
      };
      try {
        const raw = localStorage.getItem(MOVIE_ROUTE_CACHE_KEY);
        const parsed = raw ? JSON.parse(raw) : {};
        parsed[routeKey] = payload;
        const entries = Object.entries(parsed).sort((a, b) => (b[1]?.updatedAt || 0) - (a[1]?.updatedAt || 0)).slice(0, 500);
        const compact = Object.fromEntries(entries);
        localStorage.setItem(MOVIE_ROUTE_CACHE_KEY, JSON.stringify(compact));
      } catch (_) {}
    }

    function buildMovieWatchRedirectUrl(movie) {
      const resolvedMovieUrl = resolveMovieUrl(movie?.url);
      if (!resolvedMovieUrl) return null;
      const routeParts = getMovieRouteParts(movie);
      cacheMovieRouteData(movie, resolvedMovieUrl, routeParts);
      // Always use the encoded stream URL in the query string so playback works for
      // anyone opening the link, without relying on Firestore "movies" documents.
      return makeAbsoluteUrl(`/movie/?u=${encodeURIComponent(resolvedMovieUrl)}`);
    }

    function getRequestedMovieIdFromUrl() {
      return String(new URLSearchParams(window.location.search).get('movie') || '').trim();
    }

    function getRequestedGameIdFromUrl() {
      return String(new URLSearchParams(window.location.search).get('game') || '').trim();
    }

    function updateMainGameQueryParam(gameId, options = {}) {
      const url = new URL(window.location.href);
      const value = String(gameId || '').trim();
      const path = getCurrentRoutePath();
      if (path !== '/games/dashboard' && path !== '/games/home') {
        url.pathname = '/games/dashboard';
      }
      url.searchParams.delete('page');
      if (!value) url.searchParams.delete('game');
      else url.searchParams.set('game', value);
      const next = `${url.pathname}?${url.searchParams.toString()}`;
      const current = `${window.location.pathname}${window.location.search}`;
      if (next === current) return;
      if (options.pushHistory) window.history.pushState(null, '', next);
      else window.history.replaceState(null, '', next);
    }

    function getRequestedMoviePlayIdFromUrl() {
      return String(new URLSearchParams(window.location.search).get('play') || '').trim();
    }

    function updateMoviesQueryParams(patch = {}, options = {}) {
      const url = new URL(window.location.href);
      if (getCurrentRoutePath() !== '/games/movies') {
        url.pathname = '/games/movies';
      }
      url.searchParams.delete('page');
      const keys = ['movie', 'play', 'cat', 'q'];
      keys.forEach((k) => {
        if (!(k in patch)) return;
        const value = String(patch[k] ?? '').trim();
        if (!value) url.searchParams.delete(k);
        else url.searchParams.set(k, value);
      });
      const next = `${url.pathname}?${url.searchParams.toString()}`;
      const current = `${window.location.pathname}${window.location.search}`;
      if (next === current) return;
      if (options.pushHistory) window.history.pushState(null, '', next);
      else window.history.replaceState(null, '', next);
    }

    function updateMoviePreViewUrl(movieId, options = {}) {
      updateMoviesQueryParams({ movie: movieId || null, play: null }, options);
    }

    function findMovieById(movieId) {
      const id = String(movieId || '').trim();
      if (!id || !appMoviesDataCache?.moviesByCategory) return null;
      for (const list of Object.values(appMoviesDataCache.moviesByCategory)) {
        const found = (list || []).find((m) => String(m.id) === id);
        if (found) return found;
      }
      return null;
    }

    function maybeOpenMovieFromUrlParam(options = {}) {
      const requestedId = getRequestedMovieIdFromUrl();
      if (!requestedId) {
        if (options.closeIfMissing && movieInfoModal?.style.display === 'block') {
          closeMovieInfoModal({ skipUrlUpdate: true });
        }
        return false;
      }
      const movie = findMovieById(requestedId);
      if (!movie) {
        if (options.clearInvalid !== false) updateMoviePreViewUrl('', { pushHistory: false });
        return false;
      }
      openMovieInfoModal(movie, { syncUrl: false });
      return true;
    }

    function maybeOpenGameFromUrlParam() {
      const requestedGameId = getRequestedGameIdFromUrl();
      if (!requestedGameId) return false;
      const game = gameLookupById.get(String(requestedGameId));
      if (!game) return false;
      if (!currentUser) {
        pendingGame = game;
        noticeModal.style.display = 'flex';
        return false;
      }
      const opened = openGameModal(game.title, game.url, { systemOrdered: true });
      if (!opened) return false;
      logGamePlay(game);
      updateMainGameQueryParam('', { pushHistory: false });
      return true;
    }

    function maybePlayMovieFromUrlParam() {
      const requestedPlayId = getRequestedMoviePlayIdFromUrl();
      if (!requestedPlayId) return false;
      const movie = findMovieById(requestedPlayId) || findMovieById(getRequestedMovieIdFromUrl());
      if (!movie) {
        updateMoviesQueryParams({ play: null }, { pushHistory: false });
        return false;
      }
      if (!currentUser) {
        pendingMovie = movie;
        pendingMovieShouldPlay = true;
        noticeModal.style.display = 'flex';
        return false;
      }
      beginMoviePlayback(movie);
      return true;
    }

    function formatMovieInfoDescription(rawDescription) {
      const text = String(rawDescription || '').replace(/\s+/g, ' ').trim();
      if (!text) return { text: 'No description available yet.', isLong: false };
      const words = text.split(' ').filter(Boolean);
      if (words.length <= 70) return { text, isLong: false };
      return {
        text: `${words.slice(0, 70).join(' ')} .....`,
        isLong: true
      };
    }

    function openMovieInfoModal(movie, options = {}) {
      if (!movieInfoModal || !movie) return;
      activeMoviePreview = movie;
      const titleText = String(movie.title || '').trim() || 'Movie title';
      if (movieInfoTitleEl) {
        movieInfoTitleEl.textContent = titleText;
        movieInfoTitleEl.classList.remove('has-image-fallback');
      }
      if (movieInfoCategoryEl) movieInfoCategoryEl.textContent = String(movie.category || 'Uncategorized');
      if (movieInfoYearEl) movieInfoYearEl.textContent = movie.releaseYear || 'N/A';
      const scoreValue = Number(movie.score);
      if (movieInfoScoreEl) movieInfoScoreEl.textContent = Number.isFinite(scoreValue) && scoreValue > 0 ? scoreValue.toFixed(1) : 'N/A';
      const descInfo = formatMovieInfoDescription(movie.description);
      if (movieInfoDescriptionEl) {
        movieInfoDescriptionEl.textContent = descInfo.text;
      }
      movieInfoActionsEl?.classList.toggle('is-long', descInfo.isLong);
      const titleImageUrl = String(movie.titleImage || '').trim();
      if (movieInfoTitleImageEl) {
        if (titleImageUrl) {
          movieInfoTitleImageEl.onerror = () => {
            movieInfoTitleImageEl.classList.remove('active');
            movieInfoTitleEl?.classList.remove('has-image-fallback');
          };
          movieInfoTitleImageEl.src = titleImageUrl;
          movieInfoTitleImageEl.alt = `${titleText} title image`;
          movieInfoTitleImageEl.classList.add('active');
          movieInfoTitleEl?.classList.add('has-image-fallback');
        } else {
          movieInfoTitleImageEl.onerror = null;
          movieInfoTitleImageEl.src = '';
          movieInfoTitleImageEl.alt = 'Movie title logo';
          movieInfoTitleImageEl.classList.remove('active');
          movieInfoTitleEl?.classList.remove('has-image-fallback');
        }
      }
      const bannerUrl = String(movie.banner || '').trim();
      const safeBannerCssUrl = bannerUrl.replace(/"/g, '\\"');
      if (movieInfoBackground) {
        movieInfoBackground.style.backgroundImage = bannerUrl ? `url("${safeBannerCssUrl}")` : 'none';
      }
      if (movieInfoWatchBtn) {
        const canWatch = Boolean(String(movie.url || '').trim());
        movieInfoWatchBtn.disabled = !canWatch;
        movieInfoWatchBtn.textContent = canWatch ? 'WATCH MOVIE' : 'MOVIE LINK NOT SET';
      }
      if (options.syncUrl !== false) {
        updateMoviesQueryParams({ movie: movie.id || null, play: null }, { pushHistory: Boolean(options.pushHistory) });
      }
      applyMovieInfoTrailer(movie);
      movieInfoModal.style.display = 'block';
      document.body.style.overflow = 'hidden';
    }


    /** Resolves the visitor's public IP for play logs (staff/admin History & IP). */
    async function fetchClientIpAddressForPlayLog() {
      try {
        const response = await fetch("https://api.ipify.org?format=json", { cache: "no-store" });
        if (!response.ok) return null;
        const body = await response.json();
        const ip = body && typeof body.ip === "string" ? body.ip.trim() : "";
        return ip || null;
      } catch (e) {
        return null;
      }
    }

    async function logMovieWatch(movie) {
      if (!currentUser || !movie) return;
      try {
        const route = getMovieRouteParts(movie);
        const resolvedMovieUrl = resolveMovieUrl(movie.url);
        const ipAddress = await fetchClientIpAddressForPlayLog();
        await addDoc(collection(db, "plays"), {
          entryType: 'movie',
          itemType: 'movie',
          movieId: String(movie.id || ''),
          movieKey: Number.parseInt(route.movieId, 10) || null,
          categoryId: Number.parseInt(route.categoryId, 10) || null,
          releaseYear: Number.parseInt(route.year, 10) || null,
          movieTitle: String(movie.title || ''),
          movieBanner: String(movie.banner || ''),
          movieUrl: resolvedMovieUrl || '',
          gameTitle: String(movie.title || ''), // keep legacy columns populated
          gameImage: String(movie.banner || ''),
          userId: currentUser.uid,
          userEmail: currentUser.email,
          ipAddress: ipAddress || null,
          duration: 0,
          timestamp: serverTimestamp()
        });
      } catch (error) {
        console.error("Error logging movie watch: ", error);
      }
    }

    async function beginMoviePlayback(movie) {
      if (!movie) return;
      const redirectUrl = buildMovieWatchRedirectUrl(movie);
      if (!redirectUrl) { showNotification('This movie has no valid movie URL yet.', 'error'); return; }
      if (!currentUser) {
        pendingMovie = movie;
        pendingMovieShouldPlay = true;
        noticeModal.style.display = 'flex';
        return;
      }
      await logMovieWatch(movie);
      closeMovieInfoModal({ skipUrlUpdate: true });
      window.location.assign(redirectUrl);
    }

    async function logGamePlay(game) {
      if (!currentUser) return;
      try {
        const durationSec = 300;
        const ipAddress = await fetchClientIpAddressForPlayLog();
        await addDoc(collection(db, "plays"), {
          entryType: 'game',
          itemType: 'game',
          gameId: String(game?.id || ''),
          gameKey: toBoundedPositiveInt(game?.gameKey, MAX_GAME_KEY_ID),
          gameTitle: String(game?.title || ''),
          gameImage: String(game?.image || ''),
          gameUrl: String(game?.url || ''),
          userId: currentUser.uid,
          userEmail: currentUser.email,
          ipAddress: ipAddress || null,
          duration: durationSec,
          timestamp: serverTimestamp()
        });
        updateStats(currentUser.uid);
        updateMissionProgress('gametime', durationSec);
      } catch (error) { console.error("Error logging game play: ", error); }
    }

    async function handleGameClick(gameId, gameTitle, gameUrl) {
      const game = (gameLookupById && gameLookupById.get(String(gameId))) || { id: gameId, title: gameTitle, url: gameUrl };
      if (!currentUser) { pendingGame = game; noticeModal.style.display = 'flex'; return; }
      showConfirmModal(game);
    }

    // ========== Games rendering (unchanged) ==========
    async function getGames() {
      try {
        const gamesSnapshot = await getDocs(collection(db, "games"));
        const games = gamesSnapshot.docs.map((d, i) => normalizeGameDoc({ id: d.id, ...d.data() }, d.id, i));
        const playCounts = {};
        let playsSnapshot;
        try {
          playsSnapshot = await getDocs(query(collection(db, "plays"), orderBy("timestamp", "desc"), limit(500)));
        } catch (_) {
          playsSnapshot = await getDocs(collection(db, "plays"));
        }
        playsSnapshot.forEach((d) => {
          const data = d.data() || {};
          const gid = String(data.gameId || '');
          if (!gid) return;
          playCounts[gid] = (playCounts[gid] || 0) + 1;
        });
        games.sort((a, b) => (playCounts[b.id]||0) - (playCounts[a.id]||0));
        gameLookupById = new Map(games.map((game) => [String(game.id), game]));
        const topGames = games.slice(0, 5).map((g, i)=>({...g, rank: i+1}));
        const newGames = games.slice(5, 12);
        const allGames = games;
        return { topGames, newGames, allGames };
      } catch(e) { console.error(e); return { topGames: [], newGames: [], allGames: [] }; }
    }

    async function handleMovieClick(movie) {
      if (!movie) return;
      pendingMovieShouldPlay = false;
      openMovieInfoModal(movie, { syncUrl: true, pushHistory: true });
    }

    async function toggleFavoriteMovie(movieId) {
      if (!movieId) return;
      if (!currentUser) {
        noticeModal.style.display = 'flex';
        return;
      }
      const id = String(movieId);
      try {
        const isFav = favoriteMovieIds.has(id);
        if (isFav) {
          await updateDoc(doc(db, 'users', currentUser.uid), { favoriteMovieIds: arrayRemove(id) });
          favoriteMovieIds.delete(id);
          showNotification('Removed from favourites', 'success');
        } else {
          await updateDoc(doc(db, 'users', currentUser.uid), { favoriteMovieIds: arrayUnion(id) });
          favoriteMovieIds.add(id);
          showNotification('Added to favourites', 'success');
        }
        renderMovieGridCards();
      } catch (e) {
        showNotification('Could not update favourites: ' + e.message, 'error');
      }
    }

    function buildMoviesData(movieItems, categories) {
      const categoryConfig = (Array.isArray(categories) && categories.length) ? categories : [];
      const moviesByCategory = {};
      categoryConfig.forEach((cfg) => { moviesByCategory[cfg.key] = []; });
      (movieItems || []).forEach((movie, idx) => {
        const fixed = normalizeMovieDoc(movie, movie.id || `movie-${Math.random().toString(16).slice(2)}`, categoryConfig, idx);
        if (!moviesByCategory[fixed.category]) moviesByCategory[fixed.category] = [];
        moviesByCategory[fixed.category].push(fixed);
      });
      Object.keys(moviesByCategory).forEach((cat) => moviesByCategory[cat].sort(movieNewestComparator));
      const inferredCategories = categoryConfig.length
        ? categoryConfig
        : Object.keys(moviesByCategory).map((key, idx) => ({
            key,
            categoryId: (idx + 1) <= MAX_MOVIE_CATEGORY_ID ? (idx + 1) : stableNumericKey(key, MAX_MOVIE_CATEGORY_ID),
            gradient: 'linear-gradient(135deg, #2f5ca3 0%, #1a2e58 100%)',
            art: '',
            artPosition: 'bottom',
            artScale: 100,
            order: idx
          }));
      const topMovies = inferredCategories
        .map((cfg) => {
          const pick = [...(moviesByCategory[cfg.key] || [])].sort(movieTopComparator)[0];
          return pick ? { ...pick, rankCategory: cfg.key } : null;
        })
        .filter(Boolean);
      const normalizedCategories = inferredCategories.map((cfg, idx) => ({
        ...cfg,
        categoryId: toBoundedPositiveInt(cfg.categoryId, MAX_MOVIE_CATEGORY_ID) ?? ((idx + 1) <= MAX_MOVIE_CATEGORY_ID ? (idx + 1) : stableNumericKey(cfg.key || idx, MAX_MOVIE_CATEGORY_ID)),
        order: Number.isFinite(Number(cfg.order)) ? Number(cfg.order) : idx,
        count: (moviesByCategory[cfg.key] || []).length
      }));
      return { categories: normalizedCategories, topMovies, moviesByCategory };
    }

    async function getMovies() {
      const categoryConfig = await getMovieCategories();
      try {
        const snap = await getDocs(collection(db, 'movies'));
        const fromDb = snap.docs.map((d, i) => normalizeMovieDoc({ id: d.id, ...d.data() }, d.id, categoryConfig, i));
        if (fromDb.length) return buildMoviesData(fromDb, categoryConfig);
      } catch (e) {
        console.warn('Movies collection load failed.', e);
      }
      const fallback = DEFAULT_MOVIES.map((m, i) => normalizeMovieDoc(m, m.id, categoryConfig, i));
      return buildMoviesData(fallback, categoryConfig);
    }

    function showMovieSlide(index) {
      if (!moviesCarouselContainer) return;
      const slides = moviesCarouselContainer.querySelectorAll('.movie-carousel-slide');
      const dots = moviesCarouselContainer.querySelectorAll('.movie-carousel-dot');
      if (!slides.length) return;
      let i = index;
      if (i >= slides.length) i = 0;
      if (i < 0) i = slides.length - 1;
      slides.forEach((s) => s.classList.remove('active'));
      dots.forEach((d) => d.classList.remove('active'));
      slides[i].classList.add('active');
      dots[i]?.classList.add('active');
      movieCarouselIndex = i;
    }

    function startMovieCarousel() {
      clearInterval(movieCarouselInterval);
      if (!moviesCarouselContainer) return;
      const count = moviesCarouselContainer.querySelectorAll('.movie-carousel-slide').length;
      if (count < 2) return;
      movieCarouselInterval = setInterval(() => showMovieSlide(movieCarouselIndex + 1), 5200);
    }

    function renderTopMoviesCarousel(topMovies) {
      if (!moviesCarouselContainer) return;
      moviesCarouselContainer.innerHTML = '';
      if (!topMovies.length) {
        moviesCarouselContainer.innerHTML = '<div class="movie-card-empty">No featured movies available.</div>';
        return;
      }
      topMovies.forEach((movie, idx) => {
        const shortDescription = String(movie.description || '').trim();
        const previewDescription = shortDescription
          ? (shortDescription.length > 220 ? `${shortDescription.slice(0, 217)}...` : shortDescription)
          : 'Open details to read this movie synopsis and start watching.';
        const slide = document.createElement('div');
        slide.className = `movie-carousel-slide ${idx === 0 ? 'active' : ''}`;
        slide.innerHTML = `
          <div class="movie-slide-background" data-lazy-bg="${escapeHtml(movie.banner || '')}"></div>
          <div class="movie-slide-scrim"></div>
          <div class="movie-slide-content">
            <div class="movie-slide-meta-row">
              <span><i class="fas fa-tag"></i> ${escapeHtml(movie.rankCategory || movie.category || 'Movie')}</span>
              <span><i class="fas fa-calendar-alt"></i> ${movie.releaseYear || 'N/A'}</span>
              <span><i class="fas fa-star"></i> ${movie.score || 'N/A'}</span>
            </div>
            <h2 class="movie-slide-title">${escapeHtml(movie.title)}</h2>
            <p class="movie-slide-desc">${escapeHtml(previewDescription)}</p>
            <button class="slide-play-button movie-watch-button" data-movie-id="${escapeHtml(movie.id)}">OPEN MOVIE</button>
          </div>
        `;
        moviesCarouselContainer.appendChild(slide);
      });
      const nav = document.createElement('div');
      nav.className = 'movie-carousel-nav';
      topMovies.forEach((_, i) => {
        const dot = document.createElement('div');
        dot.className = `movie-carousel-dot ${i === 0 ? 'active' : ''}`;
        dot.addEventListener('click', () => showMovieSlide(i));
        nav.appendChild(dot);
      });
      moviesCarouselContainer.appendChild(nav);
      moviesCarouselContainer.querySelectorAll('.movie-slide-background').forEach((bg) => observeLazyGameBg(bg));
      moviesCarouselContainer.querySelectorAll('.movie-watch-button').forEach((btn) => {
        btn.addEventListener('click', (e) => {
          e.preventDefault();
          const movie = topMovies.find((m) => String(m.id) === String(btn.dataset.movieId));
          if (movie) handleMovieClick(movie);
        });
      });
      movieCarouselIndex = 0;
      startMovieCarousel();
    }

    function renderMovieCategoryButtons(moviesData) {
      if (!movieCategoryButtons) return;
      movieCategoryButtons.innerHTML = '';
      moviesData.categories.forEach((cat) => {
        const artScale = [50, 75, 100, 125, 150].includes(Number(cat.artScale)) ? Number(cat.artScale) : 100;
        const artSizePx = Math.round(64 * (artScale / 100));
        const artPos = String(cat.artPosition || 'bottom').toLowerCase() === 'middle' ? 'middle' : 'bottom';
        const artHtml = cat.art
          ? `<img class="movie-category-art ${artPos}" src="${escapeHtml(cat.art)}" alt="" loading="lazy" aria-hidden="true" style="width:${artSizePx}px;height:${artSizePx}px;">`
          : '';
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'movie-category-button';
        btn.dataset.category = cat.key;
        btn.style.setProperty('--movie-gradient', cat.gradient);
        btn.innerHTML = `
          <div class="movie-category-title">${escapeHtml(cat.key)}</div>
          <span class="movie-category-count">${cat.count} movie${cat.count === 1 ? '' : 's'}</span>
          ${artHtml}
        `;
        btn.addEventListener('click', () => {
          activeMovieCategory = cat.key;
          renderMovieGridCards();
          updateMoviesQueryParams({ cat: activeMovieCategory, movie: null, play: null }, { pushHistory: true });
        });
        movieCategoryButtons.appendChild(btn);
      });
    }

    function renderMovieGridCards() {
      if (!movieGrid || !appMoviesDataCache) return;
      const selected = activeMovieCategory || appMoviesDataCache.categories?.[0]?.key || '';
      const selectedList = [...(appMoviesDataCache.moviesByCategory[selected] || [])]
        .sort(movieNewestComparator)
        .filter((movie) => String(movie.title || '').toLowerCase().includes(movieSearchQuery));
      if (moviesActiveCategoryLabelEl) {
        moviesActiveCategoryLabelEl.textContent = selected || 'No category';
      }
      if (moviesResultSummaryEl) {
        const queryText = movieSearchQuery ? ` matching "${movieSearchQuery}"` : '';
        moviesResultSummaryEl.textContent = `${selectedList.length} title${selectedList.length === 1 ? '' : 's'}${queryText}`;
      }
      movieCategoryButtons?.querySelectorAll('.movie-category-button').forEach((btn) => {
        btn.classList.toggle('active', btn.dataset.category === selected);
      });
      if (!selectedList.length) {
        movieGrid.innerHTML = '<div class="movie-card-empty">No movies found for this category/search.</div>';
        return;
      }
      movieGrid.innerHTML = selectedList.map((movie) => `
        <article class="movie-card" data-movie-id="${escapeHtml(movie.id)}" role="button" tabindex="0">
          <div class="movie-year-badge">${movie.releaseYear || 'N/A'}</div>
          <button type="button" class="movie-favorite-btn ${favoriteMovieIds.has(String(movie.id)) ? 'active' : ''}" data-favorite-id="${escapeHtml(movie.id)}" aria-label="Toggle favourite"><i class="fas fa-heart"></i></button>
          <div class="movie-banner"><div class="movie-banner-bg" data-lazy-bg="${escapeHtml(movie.banner || '')}"></div></div>
          <div class="movie-card-content">
            <h3 class="movie-card-title">${escapeHtml(movie.title)}</h3>
            <p class="movie-card-desc">${escapeHtml(movie.description || 'No synopsis available yet.')}</p>
            <div class="movie-card-meta"><span>${escapeHtml(movie.category)}</span><span class="movie-card-score">★ ${movie.score || 'N/A'}</span></div>
          </div>
        </article>
      `).join('');
      movieGrid.querySelectorAll('.movie-banner-bg').forEach((bg) => observeLazyGameBg(bg));
      movieGrid.querySelectorAll('.movie-card').forEach((card) => {
        const movie = selectedList.find((m) => String(m.id) === String(card.dataset.movieId));
        const open = () => movie && handleMovieClick(movie);
        card.addEventListener('click', open);
        card.addEventListener('keydown', (e) => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            open();
          }
        });
      });
      movieGrid.querySelectorAll('.movie-favorite-btn').forEach((btn) => {
        btn.addEventListener('click', (e) => {
          e.preventDefault();
          e.stopPropagation();
          toggleFavoriteMovie(btn.dataset.favoriteId);
        });
      });
    }

    function renderMoviesPage(moviesData) {
      if (!moviesData) return;
      renderTopMoviesCarousel(moviesData.topMovies || []);
      renderMovieCategoryButtons(moviesData);
      if (moviesTotalCountEl) {
        const totalMovies = Object.values(moviesData.moviesByCategory || {}).reduce((sum, list) => sum + (Array.isArray(list) ? list.length : 0), 0);
        moviesTotalCountEl.textContent = String(totalMovies);
      }
      if (moviesCategoryCountEl) {
        moviesCategoryCountEl.textContent = String((moviesData.categories || []).length);
      }
      if (movieSearchInput && movieSearchInput.value.toLowerCase() !== movieSearchQuery) {
        movieSearchInput.value = movieSearchQuery;
      }
      if (!activeMovieCategory || !moviesData.moviesByCategory[activeMovieCategory]) {
        activeMovieCategory = moviesData.categories[0]?.key || '';
      }
      renderMovieGridCards();
    }

    function applyMoviesFiltersFromUrl(moviesData) {
      const params = new URLSearchParams(window.location.search);
      const requestedCategory = String(params.get('cat') || '').trim();
      const validCategories = Object.keys(moviesData?.moviesByCategory || {});
      if (requestedCategory && validCategories.includes(requestedCategory)) {
        activeMovieCategory = requestedCategory;
      } else if (!activeMovieCategory || !validCategories.includes(activeMovieCategory)) {
        activeMovieCategory = moviesData?.categories?.[0]?.key || '';
      }
      movieSearchQuery = String(params.get('q') || '').trim().toLowerCase();
      if (movieSearchInput && movieSearchInput.value.toLowerCase() !== movieSearchQuery) {
        movieSearchInput.value = movieSearchQuery;
      }
    }

    async function mountMoviesPageContent() {
      if (moviesPageMounted && appMoviesDataCache) {
        applyMoviesFiltersFromUrl(appMoviesDataCache);
        renderMovieGridCards();
        maybeOpenMovieFromUrlParam({ closeIfMissing: true });
        maybePlayMovieFromUrlParam();
        return;
      }
      if (moviesPageMountPromise) return moviesPageMountPromise;
      moviesPageMountPromise = (async () => {
        let data = appMoviesDataCache;
        if (!data) {
          data = await getMovies();
          appMoviesDataCache = data;
        }
        applyMoviesFiltersFromUrl(data);
        renderMoviesPage(data);
        maybeOpenMovieFromUrlParam({ closeIfMissing: true });
        maybePlayMovieFromUrlParam();
        movieSearchInput?.removeEventListener('input', handleMovieSearchInput);
        movieSearchInput?.addEventListener('input', handleMovieSearchInput);
        moviesPageMounted = true;
        moviesPageMountPromise = null;
      })();
      return moviesPageMountPromise;
    }

    function handleMovieSearchInput(e) {
      movieSearchQuery = String(e?.target?.value || '').trim().toLowerCase();
      renderMovieGridCards();
      updateMoviesQueryParams({ q: movieSearchQuery || null, movie: null, play: null }, { pushHistory: false });
    }

    async function refreshMoviesPageFromAdmin() {
      appMovieCategoriesCache = null;
      appMoviesDataCache = null;
      moviesPageMounted = false;
      await mountMoviesPageContent();
    }

    const PAGE_ROUTE_IDS = new Set([
      'main-page', 'movies-page', 'profile-page', 'history-page', 'shop-page',
      'inventory-page', 'missions-page', 'chat-page', 'friends-page',
      'settings-page', 'staff-page', 'view-profile-page', 'home', 'contact'
    ]);
    const PAGE_PATH_MAP = {
      'home': '/games/home',
      'contact': '/games/contact',
      'main-page': '/games/dashboard',
      'movies-page': '/games/movies',
      'profile-page': '/games/profile',
      'history-page': '/games/history',
      'shop-page': '/games/shop',
      'inventory-page': '/games/inventory',
      'missions-page': '/games/missions',
      'chat-page': '/games/chat',
      'friends-page': '/games/friends',
      'settings-page': '/games/settings',
      'staff-page': '/games/staff',
      'view-profile-page': '/games/user'
    };
    const PATH_TO_PAGE_ID = Object.fromEntries(
      Object.entries(PAGE_PATH_MAP).map(([pageId, path]) => [path, pageId])
    );
    PATH_TO_PAGE_ID['/games'] = 'main-page';

    function getCurrentPageId() {
      return String(window.__GU_PAGE__ || 'main-page').trim() || 'main-page';
    }

    function isAuthRequiredPage(pageId) {
      const publicPages = new Set(['main-page', 'movies-page', 'home', 'contact', 'view-profile-page']);
      return !publicPages.has(pageId);
    }

    function navigateToPage(routeId, options = {}) {
      const id = String(routeId || '').trim();
      if (!PAGE_ROUTE_IDS.has(id)) return;
      const path = PAGE_PATH_MAP[id];
      if (!path) return;
      const url = new URL(path, window.location.origin);
      if (options.query) {
        Object.entries(options.query).forEach(([key, value]) => {
          const next = String(value ?? '').trim();
          if (!next) url.searchParams.delete(key);
          else url.searchParams.set(key, next);
        });
      }
      url.searchParams.delete('page');
      const next = `${url.pathname}${url.search}`;
      const current = `${window.location.pathname}${window.location.search}`;
      if (next === current) window.location.reload();
      else window.location.assign(url.toString());
    }

    function goToProfilePageTab() {
      navigateToPage('profile-page');
    }

    function populateIssueGameSelect(allGames) {
      const sel = document.getElementById('issueGame');
      if (!sel) return;
      const list = Array.isArray(allGames) ? allGames : [];
      const cur = sel.value;
      sel.innerHTML = '<option value="">-- SELECT A GAME --</option>' +
        [...list].sort((a, b) => (a.title || '').localeCompare(b.title || '')).map(g =>
          `<option value="${escapeHtml(g.id)}">${escapeHtml(g.title || '')}</option>`
        ).join('');
      if (cur && list.some(g => g.id === cur)) sel.value = cur;
    }

    async function mountMainPageGamesContent() {
      if (mainPageGamesMounted) return;
      if (mainPageGamesMountPromise) return mainPageGamesMountPromise;
      mainPageGamesMountPromise = (async () => {
        let data = appGamesDataCache;
        if (!data) {
          data = await getGames();
          appGamesDataCache = data;
        }
        renderTopGamesCarousel(data.topGames);
        renderCategoryBrowsing(data);
        renderFullGamesList(data.allGames);
        populateIssueGameSelect(data.allGames);
        maybeOpenGameFromUrlParam();
        const mp = document.getElementById('main-page');
        if (mp) mp.classList.remove('main-page-games-deferred');
        const ph = document.getElementById('main-games-load-placeholder');
        if (ph) ph.hidden = true;
        if (mainPageGamesScrollObserver) {
          mainPageGamesScrollObserver.disconnect();
          mainPageGamesScrollObserver = null;
        }
        mainPageGamesMounted = true;
        mainPageGamesMountPromise = null;
      })();
      return mainPageGamesMountPromise;
    }

    function ensureMainPageGamesDeferredObserver() {
      const mp = document.getElementById('main-page');
      const ph = document.getElementById('main-games-load-placeholder');
      if (!mp || !ph || mainPageGamesMounted || typeof IntersectionObserver === 'undefined') return;
      if (mainPageGamesScrollObserver) return;
      mainPageGamesScrollObserver = new IntersectionObserver((entries) => {
        entries.forEach((en) => {
          if (en.isIntersecting) mountMainPageGamesContent();
        });
      }, { root: mainContent, rootMargin: '120px 0px', threshold: 0.01 });
      mainPageGamesScrollObserver.observe(ph);
    }

    function renderTopGamesCarousel(topGames) {
      const container = document.getElementById('topGamesCarousel');
      if (!container) return;
      container.innerHTML = '';
      topGames.forEach((game, idx) => {
        const slide = document.createElement('div');
        slide.className = `carousel-slide ${idx===0?'active': ''}`;
        const imgEsc = escapeHtml(game.image || '');
        const bgHtml = (idx === 0 && game.image)
          ? `<div class="slide-background lazy-bg-loaded" style="background-image: url(${JSON.stringify(mediaThumbUrl(game.image, 1200, 82))});" data-bg-ready="1"></div>`
          : `<div class="slide-background" data-lazy-bg="${imgEsc}"></div>`;
        slide.innerHTML = `
          ${bgHtml}
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
      preloadCarouselSlideBgs(0);
      startCarousel();
    }

    let currentSlide=0, interval;
    function showSlide(i){ const slides=document.querySelectorAll('.carousel-slide'), dots=document.querySelectorAll('.carousel-dot'); if(i>=slides.length)i=0; if(i<0)i=slides.length-1; slides.forEach(s=>s.classList.remove('active')); dots.forEach(d=>d.classList.remove('active')); slides[i].classList.add('active'); dots[i].classList.add('active'); currentSlide=i; preloadCarouselSlideBgs(i); }
    function nextSlide(){ showSlide(currentSlide+1); }
    function startCarousel(){ clearInterval(interval); interval=setInterval(nextSlide, 5000); }

    function createGameCard(game) {
      const card = document.createElement('div');
      card.className = 'game-card';
      card.dataset.id = game.id;
      card.dataset.title = game.title;
      card.dataset.url = game.url;
      const bg = document.createElement('div');
      bg.className = 'game-card-bg';
      if (game.image) bg.dataset.lazyBg = game.image;
      const rating = game.rating || 3;
      const starsHtml = [1, 2, 3, 4, 5].map(s => `<i class="fas fa-star star ${s <= Math.floor(rating) ? 'filled' : ''}"></i>`).join('');
      const overlay = document.createElement('div');
      overlay.className = 'card-overlay';
      overlay.innerHTML = `<div class="game-name">${game.title}</div><div class="card-meta"><i class="${game.multiplayer ? 'fas fa-users mode-multi' : 'fas fa-user mode-single'}"></i><div class="card-rating"><div class="stars">${starsHtml}</div><span class="rating-value">${rating.toFixed(1)}</span></div></div>`;
      card.appendChild(bg);
      card.appendChild(overlay);
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
      track.querySelectorAll('.game-card-bg').forEach((bg) => observeLazyGameBg(bg, track));
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
      if (!container) return;
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
      if (!grid || !searchInput) return;
      const sortedGames = [...allGames].sort((a, b) => a.title.localeCompare(b.title));
      const filterAndRender = () => {
        const query = searchInput.value.toLowerCase().trim();
        const filtered = sortedGames.filter(g => g.title.toLowerCase().includes(query));
        grid.innerHTML = filtered.map(game => `
          <div class="full-game-item" data-id="${game.id}" data-url="${game.url}" data-title="${game.title}">
            <div class="full-game-banner"><div class="full-game-banner-bg" data-lazy-bg="${escapeHtml(game.image || '')}"></div></div>
            <div class="full-game-info">
              <div class="full-game-title">${game.title}</div>
              <div class="full-game-meta"><span><i class="fas fa-star"></i> ${game.rating||'N/A'}</span><span><i class="${game.multiplayer?'fas fa-users': 'fas fa-user'}"></i> ${game.multiplayer?'Multiplayer': 'Single Player'}</span></div>
            </div>
          </div>
        `).join('');
        document.querySelectorAll('.full-game-item').forEach(el => { el.addEventListener('click', () => handleGameClick(el.dataset.id, el.dataset.title, el.dataset.url)); });
        grid.querySelectorAll('.full-game-banner-bg').forEach((bg) => observeLazyGameBg(bg));
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

    function applyMainShellLayout(pageId) {
      const showGames = pageId !== 'contact';
      if (categorySection) categorySection.style.display = showGames ? 'block' : 'none';
      if (fullGamesListSec) fullGamesListSec.style.display = showGames ? 'block' : 'none';
      contactSectionEl?.classList.toggle('active', pageId === 'contact');
      homeTab?.classList.toggle('active', pageId === 'home' || pageId === 'main-page');
      contactTab?.classList.toggle('active', pageId === 'contact');
    }

    function activateInitialPage(pageId) {
      document.querySelectorAll('.page').forEach((p) => p.classList.remove('active'));
      document.querySelectorAll('.sidebar-tabs .tab-button').forEach((btn) => btn.classList.remove('active'));
      homeTab?.classList.remove('active');
      contactTab?.classList.remove('active');

      if (pageId === 'home' || pageId === 'contact' || pageId === 'main-page') {
        document.getElementById('main-page')?.classList.add('active');
        applyMainShellLayout(pageId);
        if (pageId === 'main-page') {
          document.querySelector('.sidebar-tabs .tab-button[data-page="main-page"]')?.classList.add('active');
        }
        return;
      }

      if (pageId === 'view-profile-page') {
        document.getElementById('view-profile-page')?.classList.add('active');
        return;
      }

      document.getElementById(pageId)?.classList.add('active');
      document.querySelector(`.sidebar-tabs .tab-button[data-page="${pageId}"]`)?.classList.add('active');
    }

    async function loadActivePageContent(pageId) {
      switch (pageId) {
        case 'main-page':
        case 'home':
          await mountMainPageGamesContent();
          break;
        case 'contact':
          break;
        case 'movies-page':
          await mountMoviesPageContent();
          break;
        case 'profile-page':
          if (currentUser?.uid) loadProfilePage(currentUser.uid);
          break;
        case 'history-page':
          if (currentUser?.uid) loadPlayHistory(currentUser.uid);
          break;
        case 'shop-page':
          if (currentUser?.uid) {
            loadShopPacks();
            loadUserBalance(currentUser.uid);
          }
          break;
        case 'inventory-page':
          if (currentUser?.uid) {
            loadInventory(currentUser.uid);
            loadFriends(currentUser.uid);
          }
          break;
        case 'missions-page':
          if (currentUser?.uid) loadMissions(currentUser.uid);
          break;
        case 'chat-page':
          loadGlobalChat();
          break;
        case 'friends-page':
          if (currentUser?.uid) loadFriends(currentUser.uid);
          break;
        case 'settings-page':
          if (currentUser?.uid) loadSettings(currentUser.uid);
          break;
        case 'staff-page':
          loadStaffPanel();
          break;
        case 'view-profile-page':
          await loadViewProfilePage(String(new URLSearchParams(window.location.search).get('uid') || '').trim());
          break;
      }
    }

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
        return `<div class="up-blook-card"><div class="up-blook-img"><img src="${escapeHtml(bl.imageUrl || 'https://via.placeholder.com/52')}" alt="${escapeHtml(bl.itemName)}" loading="lazy" decoding="async"></div><div class="up-blook-name">${bl.itemName}</div><div class="up-blook-rarity">${bl.rarity}</div></div>`;
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
          avImg.loading = 'lazy';
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
            bg.loading = 'lazy';
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
        el.innerHTML = `<img src="${escapeHtml(url)}" alt="" loading="lazy" decoding="async">`;
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

    function openHistoryEntry(entry) {
      const target = getHistoryEntryLink(entry);
      if (!target?.href) return;
      window.location.assign(target.href);
    }

    // ========== Load unified history (games + movies) ==========
    async function loadPlayHistory(userId) {
      try {
        const playsQuery = query(collection(db, "plays"), where("userId", "==", userId), orderBy("timestamp", "desc"), limit(25));
        onSnapshot(playsQuery, (snapshot) => {
          playHistoryList.innerHTML = '';
          let totalEntries = 0;
          let gameEntries = 0;
          let movieEntries = 0;
          if (snapshot.empty) {
            if (historyStatsBar) {
              historyStatsBar.innerHTML = `
                <div class="history-stat"><span class="history-stat-label">Total</span><strong class="history-stat-value">0</strong></div>
                <div class="history-stat"><span class="history-stat-label">Games</span><strong class="history-stat-value">0</strong></div>
                <div class="history-stat"><span class="history-stat-label">Movies</span><strong class="history-stat-value">0</strong></div>
              `;
            }
            playHistoryList.innerHTML = '<div class="history-empty">No history yet. Start a game or watch a movie to build your activity timeline.</div>';
            return;
          }
          snapshot.forEach((doc) => {
            const data = doc.data();
            totalEntries += 1;
            const timestamp = data.timestamp?.toDate ? data.timestamp.toDate() : new Date(data.timestamp);
            const formattedTime = timestamp.toLocaleString();
            const type = String(data.entryType || data.itemType || 'game').toLowerCase() === 'movie' ? 'movie' : 'game';
            if (type === 'movie') movieEntries += 1;
            else gameEntries += 1;
            const title = type === 'movie'
              ? String(data.movieTitle || data.gameTitle || 'Movie')
              : String(data.gameTitle || data.movieTitle || 'Game');
            const art = String(type === 'movie'
              ? (data.movieBanner || data.gameImage || '')
              : (data.gameImage || data.movieBanner || '')
            ).trim() || 'https://placehold.co/640x360/0f1f37/8dc8ff?text=History+Banner';
            const historyItem = document.createElement('div');
            historyItem.className = 'history-item';
            historyItem.setAttribute('role', 'button');
            historyItem.setAttribute('tabindex', '0');
            const linkMeta = getHistoryEntryLink(data);
            const routeLabel = String(linkMeta?.display || 'No link available');
            const actionLabel = String(linkMeta?.action || (type === 'movie' ? 'Watch' : 'Play'));
            const typeLabel = type === 'movie' ? 'Movie' : 'Game';
            historyItem.innerHTML = `
              <div class="history-banner-wrap">
                <img class="history-banner" src="${escapeHtml(art)}" alt="${escapeHtml(title)} banner" loading="lazy" decoding="async">
                <div class="history-banner-scrim"></div>
                <span class="history-type-pill ${type === 'movie' ? 'is-movie' : 'is-game'}">${typeLabel}</span>
              </div>
              <div class="history-info">
                <div class="history-title-row">
                  <div class="history-title">${escapeHtml(title)}</div>
                  <div class="history-open-pill">${actionLabel}</div>
                </div>
                <div class="history-meta">
                  <span><i class="fas fa-clock"></i>${escapeHtml(formattedTime)}</span>
                  <span><i class="fas fa-shield-alt"></i>Secure route</span>
                </div>
                <div class="history-link-row">
                  <div class="history-route-pill" title="${escapeHtml(routeLabel)}">${escapeHtml(routeLabel)}</div>
                  ${linkMeta?.href ? `<a class="history-link-button" data-history-link href="${escapeHtml(linkMeta.href)}"><i class="fas fa-up-right-from-square"></i><span>Open link</span></a>` : ''}
                </div>
              </div>
            `;
            const open = () => openHistoryEntry(data);
            historyItem.querySelectorAll('[data-history-link]').forEach((linkEl) => {
              linkEl.addEventListener('click', (event) => {
                event.stopPropagation();
              });
            });
            historyItem.addEventListener('click', open);
            historyItem.addEventListener('keydown', (event) => {
              if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                open();
              }
            });
            playHistoryList.appendChild(historyItem);
          });
          if (historyStatsBar) {
            historyStatsBar.innerHTML = `
              <div class="history-stat"><span class="history-stat-label">Total</span><strong class="history-stat-value">${totalEntries}</strong></div>
              <div class="history-stat"><span class="history-stat-label">Games</span><strong class="history-stat-value">${gameEntries}</strong></div>
              <div class="history-stat"><span class="history-stat-label">Movies</span><strong class="history-stat-value">${movieEntries}</strong></div>
            `;
          }
        });
      } catch(e) { console.error(e); }
    }

    // ========== Shop packs & banners ==========
    async function loadShopPacks() {
      if (!shopPacks) return;
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
    closeChanceModal?.addEventListener('click', () => { if (chanceModal) chanceModal.style.display = 'none'; });

    // ========== Purchase pack and award stars immediately ==========
    async function purchasePack(pack) {
      if (!currentUser) return;
      if (packOpeningBusy) return;
      packOpeningBusy = true;
      try {
        const userDoc = await getDoc(doc(db, "users", currentUser.uid));
        if (!userDoc.exists()) { showNotification("User data not found", "error"); return; }
        const userData = userDoc.data();
        if (userData.coins < pack.price) { showNotification("Not enough coins!", "error"); return; }
        await updateDoc(doc(db, "users", currentUser.uid), { coins: userData.coins - pack.price });
        updateMissionProgress('spending', pack.price);
        loadUserBalance(currentUser.uid);
        await openPack(pack, currentUser.uid);
      } catch(e) { showNotification("Error purchasing pack: " + e.message, "error"); }
      finally { packOpeningBusy = false; }
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
      const key = (r || '').toLowerCase().trim() || 'unknown';
      const raw = blookRarityDefs[key];
      return raw ? normalizeStaffRarityDef(raw) : FALLBACK_RARITY_DEF;
    }

    function firstRgbFromRarityDef(d) {
      if (d.isGradient && d.gradientColors && d.gradientColors.length) {
        for (const c of d.gradientColors) {
          const t = hexToRgbTriplet(c);
          if (t) return t;
        }
      }
      return hexToRgbTriplet(d.solid) || [168, 176, 200];
    }

    function getRarityColor(rarity) {
      const d = getRarityDef(rarity);
      if (!d.isGradient) return d.solid || '#a8b0c8';
      return rgbToHex(firstRgbFromRarityDef(d));
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
      return d.solid || '#a8b0c8';
    }

    /** Pack reveal: rarity label uses admin gradient on text when configured (blook name stays plain). */
    function applyPackRevealRarityTextStyles(rarity) {
      const d = getRarityDef(rarity);
      const nameEl = document.getElementById('opened-gift-name');
      const rareEl = document.getElementById('opened-gift-rarity');
      if (nameEl) {
        nameEl.classList.remove('pack-reveal-title-gradient');
        nameEl.style.backgroundImage = '';
        nameEl.style.backgroundSize = '';
        nameEl.style.backgroundRepeat = '';
        nameEl.style.animation = '';
        nameEl.style.webkitTextFillColor = '';
        nameEl.style.color = '#fff';
      }
      if (rareEl) {
        rareEl.classList.remove('pack-reveal-rarity-gradient');
        if (d.isGradient && d.gradientColors && d.gradientColors.length >= 2) {
          rareEl.classList.add('pack-reveal-rarity-gradient');
          const g = d.gradientColors.join(', ');
          rareEl.style.color = 'transparent';
          rareEl.style.webkitTextFillColor = 'transparent';
          rareEl.style.backgroundImage = `linear-gradient(135deg, ${g})`;
          rareEl.style.backgroundSize = d.isRunning ? '200% 200%' : '100% 100%';
          rareEl.style.backgroundRepeat = 'no-repeat';
          rareEl.style.animation = d.isRunning ? 'runGradient 3s linear infinite' : '';
        } else {
          rareEl.style.backgroundImage = '';
          rareEl.style.backgroundSize = '';
          rareEl.style.animation = '';
          rareEl.style.webkitTextFillColor = '';
          rareEl.style.color = d.solid || '#fff';
        }
      }
    }

    /** Full-area modal / card background — admin gradient uses every stop (no trimming to black). */
    function getRarityBackgroundGradientCss(rarity) {
      const d = getRarityDef(rarity);
      const rgb = firstRgbFromRarityDef(d);
      const lit = mixRgb(rgb, [255, 255, 255], 0.38);
      const mid = mixRgb(rgb, [120, 135, 165], 0.42);
      const deep = mixRgb(rgb, [38, 46, 62], 0.5);
      const toRgb = (a) => `rgb(${a[0]},${a[1]},${a[2]})`;
      if (d.isGradient && d.gradientColors && d.gradientColors.length >= 2) {
        const g = d.gradientColors.join(', ');
        return {
          image: `linear-gradient(160deg, ${g})`,
          size: d.isRunning ? '240% 240%' : '100% 100%',
          repeat: 'no-repeat',
          position: d.isRunning ? '0% 50%' : 'center',
          animated: !!d.isRunning
        };
      }
      const c = toRgb(rgb);
      return {
        image: `linear-gradient(168deg, ${c} 0%, ${toRgb(lit)} 38%, ${toRgb(mid)} 70%, ${toRgb(deep)} 100%)`,
        size: '100% 100%',
        repeat: 'no-repeat',
        position: 'center',
        animated: false
      };
    }

    let packConfettiInterval = null;
    let packConfettiStopTimer = null;
    let packAutoUnboxTimer = null;
    let packRipFlashTimer = null;
    let packOpeningBusy = false;

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

    function getRarityConfettiGradientCss(rarity) {
      const d = getRarityDef(rarity);
      if (!d.isGradient || !d.gradientColors || d.gradientColors.length < 2) return '';
      const g = d.gradientColors.join(', ');
      return `linear-gradient(125deg, ${g})`;
    }

    function spawnPackConfettiBurst(n, colorPalette, rarityForGrad) {
      if (!packConfettiLayer) return;
      const colors = Array.isArray(colorPalette) && colorPalette.length
        ? colorPalette
        : ['#2AFF9E', '#FF3D6C', '#FFC107', '#42A5F5', '#AB47BC', '#ffffff'];
      const grad = rarityForGrad ? getRarityConfettiGradientCss(rarityForGrad) : '';
      const count = Math.max(1, Math.floor(n));
      for (let i = 0; i < count; i++) {
        const el = document.createElement('div');
        el.className = 'pack-confetti-piece';
        if (grad && Math.random() > 0.35) {
          el.classList.add('pack-confetti-grad');
          el.style.background = grad;
        } else {
          el.style.background = colors[Math.floor(Math.random() * colors.length)];
        }
        el.style.left = `${Math.random() * 100}%`;
        el.style.width = `${6 + Math.random() * 8}px`;
        el.style.height = `${6 + Math.random() * 8}px`;
        el.style.borderRadius = Math.random() > 0.45 ? '50%' : '2px';
        const dur = 2.4 + Math.random() * 1.8;
        el.style.setProperty('--cf-dur', `${dur}s`);
        el.style.setProperty('--cf-dx', `${(Math.random() - 0.5) * 140}px`);
        el.style.setProperty('--cf-rot', `${380 + Math.random() * 480}deg`);
        el.style.animation = `packConfettiFall ${dur}s linear forwards`;
        packConfettiLayer.appendChild(el);
        setTimeout(() => { try { el.remove(); } catch (e) {} }, dur * 1000 + 120);
      }
    }

    function startPackConfetti(durationMs, colorPalette, rarityForGrad) {
      stopPackConfetti();
      if (packConfettiLayer) packConfettiLayer.style.display = 'block';
      spawnPackConfettiBurst(18, colorPalette, rarityForGrad);
      packConfettiInterval = setInterval(() => spawnPackConfettiBurst(10, colorPalette, rarityForGrad), 520);
      packConfettiStopTimer = setTimeout(() => stopPackConfetti(), Math.min(durationMs || 9000, 9000));
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
      const keysSet = new Set(Object.keys(blookRarityDefs || {}));
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
      if (!k || !confirm(`Remove style for "${k}"? Items using this rarity will use the neutral fallback until you add it again.`)) return;
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
      if (!canManageSiteConfig()) {
        const list = document.getElementById('staff-rarity-defs-list');
        if (list) list.innerHTML = '<p style="color:var(--neon-pink);">No permission.</p>';
        return;
      }
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
      if (!canManageStarBadges()) {
        if (status) status.textContent = 'No permission.';
        const list = document.getElementById('staff-star-badge-rules-list');
        if (list) list.innerHTML = '<p style="color:var(--neon-pink);">No permission.</p>';
        return;
      }
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
        img.loading = 'lazy';
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
      if (packRipFlashTimer) {
        clearTimeout(packRipFlashTimer);
        packRipFlashTimer = null;
      }
      stopPackConfetti();
      packOpeningModalInner?.classList.remove('pack-unbox-celebrate', 'pack-unbox-peek-mood');
      if (packOpeningModalInner) {
        ['--pack-inner', '--pack-outer', '--pack-aurora-inner', '--pack-aurora-outer', '--pack-aurora-rare-a', '--pack-aurora-rare-b', '--pack-aurora-rare-c'].forEach(p => packOpeningModalInner.style.removeProperty(p));
      }
      if (openingModalBg) {
        openingModalBg.classList.remove('pack-rarity-celebrate', 'pack-bg-rarity-animated');
        openingModalBg.style.animation = '';
      }
      if (packRipOverlay) {
        packRipOverlay.classList.remove('pack-bg-rip-flash', 'pack-bg-rip-flash-dark');
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
      packOpeningModalBody?.classList.remove('pack-unbox-body', 'pack-unbox-body-lively', 'pack-unbox-revealed');
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
      openingModalBg.style.backgroundBlendMode = '';
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

    /** Same gradient as pack-opening modal (pre-reveal / "showing" state). */
    function getPackOpeningBackdropStyle(innerHex, outerHex) {
      const inner = innerHex || '#5a6a8a';
      const outer = outerHex || '#1a2233';
      return {
        backgroundImage: `linear-gradient(168deg, ${inner} 0%, ${outer} 42%, #030509 88%)`,
        backgroundSize: '220% 220%',
        backgroundRepeat: 'no-repeat',
        backgroundPosition: '40% 45%',
        backgroundBlendMode: ''
      };
    }

    /** Same gradient as #pack-reveal-card default (unpacking card look). */
    function getRevealCardBackdropStyle(innerHex, outerHex) {
      const inner = innerHex || '#5a6a8a';
      const outer = outerHex || '#1a2233';
      return {
        backgroundImage: `linear-gradient(155deg, ${inner} 0%, ${outer} 55%, #05070c 100%)`,
        backgroundSize: '100% 100%',
        backgroundRepeat: 'no-repeat',
        backgroundPosition: 'center',
        backgroundBlendMode: ''
      };
    }

    function getRarityDetailBackdropStyle(rarity) {
      const bg = getRarityBackgroundGradientCss(rarity);
      return {
        backgroundImage: bg.image,
        backgroundSize: bg.size,
        backgroundRepeat: bg.repeat,
        backgroundPosition: bg.position,
        backgroundBlendMode: ''
      };
    }

    function applyPackOpeningModalBackdrop(innerHex, outerHex) {
      if (!openingModalBg) return;
      const s = getPackOpeningBackdropStyle(innerHex, outerHex);
      openingModalBg.style.backgroundImage = s.backgroundImage;
      openingModalBg.style.backgroundSize = s.backgroundSize;
      openingModalBg.style.backgroundRepeat = s.backgroundRepeat;
      openingModalBg.style.backgroundPosition = s.backgroundPosition;
      openingModalBg.style.backgroundBlendMode = s.backgroundBlendMode;
    }

    function showPackOpeningModal(pack, item) {
      resetPackOpeningModalLayout();

      const fc = marketPackFrameColors(pack);
      const inner = fc.inner || '#5a6a8a';
      const outer = fc.outer || '#1a2233';
      applyPackUnboxBackdropVars(inner, outer, item.rarity);
      applyPackOpeningModalBackdrop(inner, outer);

      packOpeningModalInner?.classList.add('pack-opening-modal-unbox');
      packOpeningModalBody?.classList.add('pack-unbox-body', 'pack-unbox-body-lively');

      openedGiftName.textContent = item.name || '';
      const rKey = (item.rarity || '').trim().toLowerCase() || 'unknown';
      openedGiftRarity.textContent = (item.rarity || '?').toUpperCase();
      openedGiftRarity.className = 'pack-reveal-rarity';
      applyPackRevealRarityTextStyles(item.rarity);
      const pct = item.chance != null && item.chance !== '' ? Number(item.chance) : null;
      openedGiftChance.textContent = pct != null && Number.isFinite(pct) ? `${pct}%` : '—';
      openedGiftStars.textContent = `Stars: ${item.starsGained != null ? item.starsGained : 0}`;
      const revealUrl = (pack.revealCardBgUrl || '').trim();
      if (packRevealCard) {
        if (revealUrl) {
          const revealThumb = /^https?:\/\//i.test(revealUrl) ? mediaThumbUrl(revealUrl, 900, 82) : revealUrl;
          packRevealCard.style.backgroundColor = '#0b0e14';
          packRevealCard.style.backgroundImage = `url(${JSON.stringify(revealThumb)})`;
          packRevealCard.style.backgroundSize = 'cover';
        } else {
          const rbg = getRarityBackgroundGradientCss(item.rarity);
          packRevealCard.style.backgroundColor = '#0b0e14';
          packRevealCard.style.backgroundImage = rbg.image;
          packRevealCard.style.backgroundSize = rbg.size;
          packRevealCard.style.backgroundRepeat = rbg.repeat;
          packRevealCard.style.backgroundPosition = rbg.position;
        }
      }
      if (packRevealBlookSlot) {
        packRevealBlookSlot.innerHTML = '';
        if (item.imageUrl) {
          const im = document.createElement('img');
          im.src = mediaThumbUrl(item.imageUrl, 420, 85);
          im.alt = item.name || '';
          im.decoding = 'async';
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

      const artUrlRaw = (marketPackArtUrl(pack) || (pack.backgroundImage || '').trim() || (pack.items?.find(it => it.imageUrl)?.imageUrl) || 'https://placehold.co/280x280/1a1f2e/8899aa?text=Pack').replace(/\\/g, '/');
      const artUrl = /^https?:\/\//i.test(artUrlRaw) ? mediaThumbUrl(artUrlRaw, 640, 82) : artUrlRaw;

      const stack = document.createElement('div');
      stack.className = 'pack-unbox-stack';

      const setStackSize = (nw, nh) => {
        const maxW = Math.min(window.innerWidth * 0.92, 440);
        const maxH = Math.min(window.innerHeight * 0.62, 560);
        const w = nw > 0 ? nw : 280;
        const h = nh > 0 ? nh : 280;
        const scale = Math.min(1, maxW / w, maxH / h) * 0.78;
        stack.style.width = `${Math.round(w * scale)}px`;
        stack.style.height = `${Math.round(h * scale)}px`;
      };

      const blookUnder = document.createElement('div');
      blookUnder.className = 'pack-unbox-blook-under';
      const blookClip = document.createElement('div');
      blookClip.className = 'pack-unbox-blook-clip';
      if (item.imageUrl) {
        const bi = document.createElement('img');
        bi.src = mediaThumbUrl(item.imageUrl, 420, 85);
        bi.alt = item.name || '';
        bi.decoding = 'async';
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
        im.decoding = 'async';
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
          openingModalBg.style.backgroundBlendMode = '';
        }
        packOpeningModalBody?.classList.remove('pack-unbox-body-lively');
        packOpeningModalInner?.classList.add('pack-unbox-celebrate');
        packOpeningModalInner?.classList.remove('pack-unbox-peek-mood');
      };

      const firstRip = () => {
        if (giftAnimation.classList.contains('torn')) return;
        giftAnimation.classList.add('torn', 'pack-unbox-peek');
        packOpeningModalInner?.classList.add('pack-unbox-peek-mood');
        if (packRipOverlay) {
          requestAnimationFrame(() => {
            if (packRipFlashTimer) {
              clearTimeout(packRipFlashTimer);
              packRipFlashTimer = null;
            }
            packRipOverlay.classList.remove('pack-bg-rip-flash', 'pack-bg-rip-flash-dark');
            void packRipOverlay.offsetWidth;
            packRipOverlay.classList.add('pack-bg-rip-flash-dark');
            packRipFlashTimer = setTimeout(() => {
              packRipOverlay?.classList.remove('pack-bg-rip-flash-dark');
              packRipFlashTimer = null;
            }, 700);
          });
        }
        if (packUnboxBoost) packUnboxBoost.style.display = 'none';
      };

      const secondReveal = () => {
        if (!giftAnimation.classList.contains('torn') || giftAnimation.classList.contains('pack-unbox-cover-drop')) return;
        giftAnimation.classList.add('pack-unbox-cover-drop');
        giftAnimation.classList.remove('pack-unbox-peek');
        applyRarityCelebrateBg();
        const confettiColors = getRarityConfettiColors(item.rarity);
        startPackConfetti(13000, confettiColors, item.rarity);
        setTimeout(finishRevealCard, 620);
      };

      packUnboxClickHandler = (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (giftAnimation.classList.contains('torn')) return;
        firstRip();
        const preConfettiMs = 1000;
        if (packAutoUnboxTimer) clearTimeout(packAutoUnboxTimer);
        packAutoUnboxTimer = setTimeout(() => {
          packAutoUnboxTimer = null;
          secondReveal();
        }, preConfettiMs);
      };

      const probe = new Image();
      probe.onload = () => {
        setStackSize(probe.naturalWidth, probe.naturalHeight);
        giftAnimation.appendChild(stack);
        if (packUnboxClickLayer) {
          packUnboxClickLayer.addEventListener('click', packUnboxClickHandler, { once: true });
        }
      };
      probe.onerror = () => {
        setStackSize(280, 280);
        giftAnimation.appendChild(stack);
        if (packUnboxClickLayer) {
          packUnboxClickLayer.addEventListener('click', packUnboxClickHandler, { once: true });
        }
      };
      probe.src = artUrl;

      packOpeningModal.style.display = 'flex';
    }

    // ========== Blooket-style Inventory ==========
    let currentInventoryFilter = 'all';
    let inventoryItems = [];
    let invBlookDetailContext = null;
    let selectedInvSlotKey = null;
    let packBuilderItems = [];
    let staffEditUserInitialBanner = '';

    function invSellPriceForStars(stars) {
      const n = Number(stars) || 0;
      return Math.max(1, Math.floor(n * 0.5));
    }

    function resolvePackDocForInvDetail(rep) {
      const cache = inventoryCatalogCache || [];
      const pid = rep && rep.packId;
      if (pid) {
        const byId = cache.find(p => p.id === pid);
        if (byId) return byId;
      }
      const pn = (rep && rep.packName || '').trim();
      if (pn) {
        return cache.find(p => (p.name || '').trim() === pn) || null;
      }
      return null;
    }

    function applyInvPanelDetailBackground(packDoc, rarityKey) {
      if (!invBlookDetailPanel) return;
      invBlookDetailPanel.classList.remove('inv-blook-detail--opening-bg', 'inv-blook-detail--reveal-bg');
      invBlookDetailPanel.style.backgroundColor = '#07090e';
      if (!packDoc) {
        invBlookDetailPanel.style.backgroundImage = 'none';
        invBlookDetailPanel.style.backgroundSize = '';
        invBlookDetailPanel.style.backgroundPosition = '';
        invBlookDetailPanel.style.backgroundRepeat = '';
        invBlookDetailPanel.style.backgroundBlendMode = '';
        return;
      }
      const fc = marketPackFrameColors(packDoc);
      const inner = fc.inner || '#5a6a8a';
      const outer = fc.outer || '#1a2233';
      const revealUrl = String(packDoc.revealCardBgUrl || '').trim();
      invBlookDetailPanel.classList.add('inv-blook-detail--reveal-bg');
      if (revealUrl) {
        const thumb = /^https?:\/\//i.test(revealUrl) ? mediaThumbUrl(revealUrl, 800, 82) : revealUrl;
        invBlookDetailPanel.style.backgroundColor = '#0b0e14';
        invBlookDetailPanel.style.backgroundImage = `url(${JSON.stringify(thumb)})`;
        invBlookDetailPanel.style.backgroundSize = 'cover';
        invBlookDetailPanel.style.backgroundRepeat = 'no-repeat';
        invBlookDetailPanel.style.backgroundPosition = 'center';
        invBlookDetailPanel.style.backgroundBlendMode = '';
      } else {
        const s = getRevealCardBackdropStyle(inner, outer);
        invBlookDetailPanel.style.backgroundImage = s.backgroundImage;
        invBlookDetailPanel.style.backgroundSize = s.backgroundSize;
        invBlookDetailPanel.style.backgroundRepeat = s.backgroundRepeat;
        invBlookDetailPanel.style.backgroundPosition = s.backgroundPosition;
        invBlookDetailPanel.style.backgroundBlendMode = s.backgroundBlendMode;
      }
    }

    function clearInvBlookDetailPanel() {
      selectedInvSlotKey = null;
      if (invBlookDetailPlaceholder) invBlookDetailPlaceholder.style.display = '';
      if (invBlookDetailBody) {
        invBlookDetailBody.style.display = 'none';
        invBlookDetailBody.style.flexDirection = 'column';
      }
      if (invPanelRarity) invPanelRarity.style.display = 'none';
      if (invPanelActions) invPanelActions.style.display = 'none';
      applyInvPanelDetailBackground(null);
      document.querySelectorAll('.inv-blook-slot.selected').forEach(el => el.classList.remove('selected'));
    }

    function refreshInvSlotSelectionUI() {
      document.querySelectorAll('.inv-blook-slot').forEach((el) => {
        el.classList.toggle('selected', !!selectedInvSlotKey && el.dataset.slotKey === selectedInvSlotKey);
      });
    }

    function closeInvBlookModal() {
      const m = document.getElementById('inv-blook-modal');
      if (m) m.style.display = 'none';
      invBlookDetailContext = null;
      clearInvBlookDetailPanel();
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

    function openInvBlookDetail(rep, copies, catalogDef, slotKey) {
      const cop = copies && copies.length ? copies : [];
      const unlocked = cop.length > 0;
      invBlookDetailContext = { rep, copies: cop, catalogDef: catalogDef || null };
      selectedInvSlotKey = slotKey || null;
      refreshInvSlotSelectionUI();

      const name = rep.itemName || catalogDef?.name || 'Blook';
      const stars = Number(rep.starsGained != null ? rep.starsGained : catalogDef?.starsGained) || 0;
      const imageUrl = String(rep.imageUrl || catalogDef?.imageUrl || '').trim();
      const packDoc = catalogDef?.packDoc || resolvePackDocForInvDetail(rep);
      const rKey = (rep.rarity || catalogDef?.rarity || 'common').toLowerCase();

      if (invBlookDetailPlaceholder) invBlookDetailPlaceholder.style.display = 'none';
      if (invBlookDetailBody) {
        invBlookDetailBody.style.display = 'flex';
        invBlookDetailBody.style.flexDirection = 'column';
      }
      applyInvPanelDetailBackground(packDoc, rKey);

      if (invPanelTitle) invPanelTitle.textContent = name;
      if (invPanelRarity) {
        const rDisp = (rep.rarity || catalogDef?.rarity || 'common').toString();
        invPanelRarity.textContent = rDisp.toUpperCase();
        invPanelRarity.style.display = '';
        const rc = getRarityColor(rKey);
        invPanelRarity.style.color = rc;
        invPanelRarity.style.borderColor = `${rc}66`;
        invPanelRarity.style.background = `${rc}22`;
      }
      if (invPanelStars) invPanelStars.textContent = String(stars);

      if (invPanelOwned) {
        invPanelOwned.textContent = unlocked ? `×${cop.length}` : '×0';
      }

      if (invPanelVisual) {
        const panelImg = imageUrl ? escapeHtml(mediaThumbUrl(imageUrl, unlocked ? 420 : 360, 85)) : '';
        if (unlocked && imageUrl) {
          invPanelVisual.innerHTML = `<img src="${panelImg}" alt="" loading="lazy" decoding="async">`;
        } else if (imageUrl) {
          invPanelVisual.innerHTML = `<div class="inv-blook-detail-silhouette"><img src="${panelImg}" alt="" loading="lazy" decoding="async"><div class="inv-blook-lock-overlay" aria-hidden="true"><span class="material-symbols-outlined">lock</span></div></div>`;
        } else {
          const rc = getRarityColor(rKey);
          invPanelVisual.innerHTML = `<div class="inv-blook-detail-silhouette" style="background:#0a0c10;"><span class="inv-blook-ph" style="color:${rc};font-size:3rem;"><i class="fas fa-dragon"></i></span><div class="inv-blook-lock-overlay" aria-hidden="true"><span class="material-symbols-outlined">lock</span></div></div>`;
        }
      }

      if (invPanelActions) invPanelActions.style.display = unlocked ? 'flex' : 'none';

      const r = rKey;
      const qty = cop.length || 0;
      const sell = invSellPriceForStars(stars);
      const rc = getRarityColor(r);
      const vis = document.getElementById('inv-blook-modal-visual');
      if (vis) {
        vis.innerHTML = imageUrl
          ? `<img src="${escapeHtml(mediaThumbUrl(imageUrl, 320, 85))}" alt="" decoding="async">`
          : `<span class="inv-blook-ph" style="color:${rc};"><i class="fas fa-dragon"></i></span>`;
      }
      const nm = document.getElementById('inv-blook-modal-name');
      if (nm) nm.textContent = name;
      const rr = document.getElementById('inv-blook-modal-rarity');
      if (rr) {
        rr.textContent = r;
        rr.style.color = rc;
        rr.style.border = `1px solid ${rc}55`;
        rr.style.background = `${rc}22`;
      }
      const pk = document.getElementById('inv-blook-modal-pack');
      if (pk) pk.textContent = rep.packName || catalogDef?.packDoc?.name || '—';
      const q = document.getElementById('inv-blook-modal-qty');
      if (q) q.textContent = String(qty || (unlocked ? cop.length : 0));
      const st = document.getElementById('inv-blook-modal-stars');
      if (st) st.textContent = String(stars);
      const sl = document.getElementById('inv-blook-modal-sell');
      if (sl) sl.textContent = `${sell} tokens (each)`;
    }

    async function sellOneInvBlookFromDetail() {
      if (!currentUser || !invBlookDetailContext) return;
      const { copies } = invBlookDetailContext;
      if (!copies || !copies.length || !copies[0].id) {
        showNotification('This blook is not in your inventory.', 'error');
        return;
      }
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
        invBlookDetailContext = null;
        const ibm = document.getElementById('inv-blook-modal');
        if (ibm) ibm.style.display = 'none';
        clearInvBlookDetailPanel();
      } catch (e) {
        showNotification('Could not sell: ' + (e.message || 'error'), 'error');
      }
    }

    function openInvSendFromDetail() {
      if (!invBlookDetailContext || !currentUser) return;
      const { copies } = invBlookDetailContext;
      if (!copies || !copies.length || !copies[0].id) {
        showNotification('Unlock this blook before sending.', 'error');
        return;
      }
      const rep = copies[0];
      currentCard = {
        id: rep.id,
        name: rep.itemName,
        rarity: rep.rarity,
        image: rep.imageUrl || '',
        starsGained: rep.starsGained || 0
      };
      const ibm = document.getElementById('inv-blook-modal');
      if (ibm) ibm.style.display = 'none';
      populateRecipientSelectForInvSend();
      if (sendCardName) sendCardName.textContent = rep.itemName || 'Blook';
      if (sendCardType) {
        sendCardType.innerHTML = `${escapeHtml((rep.rarity || '').toString())} · <i class="fas fa-star"></i> ${rep.starsGained || 0} · Send 1 copy`;
      }
      if (sendCardIcon) {
        sendCardIcon.innerHTML = rep.imageUrl
          ? `<img src="${escapeHtml(rep.imageUrl)}" alt="" decoding="async" style="width:100%;height:100%;object-fit:contain;border-radius:8px;">`
          : '🎁';
      }
      if (sendCardModal) sendCardModal.style.display = 'flex';
      invBlookDetailContext = null;
      clearInvBlookDetailPanel();
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
          ? `<img src="${escapeHtml(currentCard.image)}" alt="" decoding="async" style="width:100%;height:100%;object-fit:contain;border-radius:8px;">`
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
          ? `<img src="${escapeHtml(item.imageUrl)}" alt="" loading="lazy" decoding="async">`
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

    async function refreshInventoryCatalog() {
      try {
        const snap = await getDocs(collection(db, 'packs'));
        inventoryCatalogCache = snap.docs
          .map(d => ({ id: d.id, ...d.data() }))
          .filter(p => p.shopType !== 'banner');
        inventoryCatalogLoaded = true;
      } catch (e) {
        console.error('Inventory catalog load failed', e);
        inventoryCatalogCache = [];
        inventoryCatalogLoaded = true;
      }
    }

    function invPackMatchesItem(pack, item) {
      const pid = item.packId;
      if (pid && pack.id === pid) return true;
      const pn = (item.packName || '').trim();
      const pname = (pack.name || '').trim();
      return pn && pname && pn === pname;
    }

    function renderInventory() {
      if (!inventoryContainer) return;
      inventoryContainer.innerHTML = '';
      const packTabs = document.getElementById('inv-pack-tabs');

      if (!inventoryCatalogLoaded) {
        if (packTabs) packTabs.innerHTML = '';
        inventoryContainer.innerHTML = '<p class="inv-blook-empty-msg">Loading your collection…</p>';
        return;
      }

      const catalogPacks = inventoryCatalogCache || [];
      const itemPacks = catalogPacks.filter(p => (p.items || []).length > 0);

      const countForPack = (pack) => inventoryItems.filter(it => invPackMatchesItem(pack, it)).length;

      const validFilters = new Set(['all', ...itemPacks.map(p => p.id)]);
      if (!validFilters.has(currentInventoryFilter)) currentInventoryFilter = 'all';

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
        itemPacks.forEach(p => mkTab(p.name || 'Pack', p.id, countForPack(p)));
      }

      const packsToRender = currentInventoryFilter === 'all'
        ? itemPacks
        : itemPacks.filter(p => p.id === currentInventoryFilter);

      const usedItemIds = new Set();

      const pickRep = (copies) => copies.reduce((a, b) => (raritySortIndex(a.rarity) > raritySortIndex(b.rarity) ? a : b));

      const renderPackSection = (pack, defs, orphanBucket) => {
        if (!defs.length) return;
        const isOrphanSection = pack.id === '__orphan__';
        const section = document.createElement('section');
        section.className = 'inv-blook-pack-section';
        const title = document.createElement('h3');
        title.className = 'inv-blook-pack-title font-titan-header';
        title.textContent = pack.name || 'Pack';
        section.appendChild(title);
        const rule = document.createElement('div');
        rule.className = 'inv-blook-pack-rule';
        section.appendChild(rule);
        const row = document.createElement('div');
        row.className = 'inv-blook-row';
        const sortedDefs = [...defs].sort((a, b) => {
          const rd = raritySortIndex(a.rarity) - raritySortIndex(b.rarity);
          if (rd !== 0) return rd;
          return (a.name || '').localeCompare(b.name || '');
        });
        sortedDefs.forEach((def) => {
          const blookName = def.name || '';
          const copies = isOrphanSection
            ? (orphanBucket || []).filter(it => (it.itemName || '') === blookName)
            : inventoryItems.filter(
              it => invPackMatchesItem(pack, it) && (it.itemName || '') === blookName
            );
          if (!isOrphanSection) copies.forEach(c => usedItemIds.add(c.id));
          const unlocked = copies.length > 0;
          const rep = unlocked ? pickRep(copies) : {
            itemName: blookName,
            rarity: def.rarity,
            starsGained: def.starsGained,
            imageUrl: def.imageUrl || '',
            packName: pack.name,
            packId: pack.id
          };
          const slotKey = `${pack.id}:${encodeURIComponent(blookName)}`;
          const btn = document.createElement('button');
          btn.type = 'button';
          btn.className = `inv-blook-slot${unlocked ? ' unlocked' : ''}`;
          btn.dataset.slotKey = slotKey;
          btn.setAttribute('aria-label', unlocked ? `${blookName}, owned ${copies.length}` : `${blookName}, locked`);
          const inner = document.createElement('div');
          inner.className = 'inv-blook-slot-inner';
          const imgUrl = String(def.imageUrl || rep.imageUrl || '').trim();
          const slotImgSrc = imgUrl ? escapeHtml(mediaThumbUrl(imgUrl, 200, 80)) : '';
          if (unlocked) {
            const rc = getRarityColor((def.rarity || rep.rarity || 'common').toLowerCase());
            if (imgUrl) {
              inner.innerHTML = `<img src="${slotImgSrc}" alt="" loading="lazy" decoding="async">`;
            } else {
              inner.innerHTML = `<span class="inv-blook-ph" style="color:${rc};font-size:1.75rem;"><i class="fas fa-dragon"></i></span>`;
            }
            if (copies.length > 1) {
              const badge = document.createElement('span');
              badge.className = 'inv-blook-qty-badge';
              badge.textContent = String(copies.length);
              btn.appendChild(badge);
            }
          } else if (imgUrl) {
            inner.innerHTML = `
              <div class="inv-blook-silhouette-wrap">
                <img class="inv-blook-silhouette-img" src="${slotImgSrc}" alt="" loading="lazy" decoding="async">
                <div class="inv-blook-lock-overlay" aria-hidden="true"><span class="material-symbols-outlined">lock</span></div>
              </div>`;
          } else {
            const rc = getRarityColor((def.rarity || 'common').toLowerCase());
            inner.innerHTML = `
              <div class="inv-blook-silhouette-wrap">
                <span class="inv-blook-ph" style="color:${rc};font-size:1.75rem;"><i class="fas fa-dragon"></i></span>
                <div class="inv-blook-lock-overlay" aria-hidden="true"><span class="material-symbols-outlined">lock</span></div>
              </div>`;
          }
          btn.appendChild(inner);
          const catalogDef = {
            name: blookName,
            imageUrl: def.imageUrl,
            starsGained: def.starsGained,
            rarity: def.rarity,
            packDoc: isOrphanSection ? null : pack
          };
          btn.addEventListener('click', () => openInvBlookDetail(rep, copies, catalogDef, slotKey));
          row.appendChild(btn);
        });
        section.appendChild(row);
        inventoryContainer.appendChild(section);
      };

      packsToRender.forEach((pack) => {
        const defs = (pack.items || []).map(it => ({
          name: it.name,
          rarity: it.rarity,
          starsGained: it.starsGained,
          imageUrl: it.imageUrl
        })).filter(d => d.name);
        renderPackSection(pack, defs, null);
      });

      const orphanItems = inventoryItems.filter(it => !usedItemIds.has(it.id));
      if (orphanItems.length) {
        const byName = {};
        orphanItems.forEach(it => {
          const k = it.itemName || '?';
          if (!byName[k]) byName[k] = [];
          byName[k].push(it);
        });
        const defs = Object.keys(byName).map(k => {
          const cop = byName[k];
          const r = pickRep(cop);
          return {
            name: k,
            rarity: r.rarity,
            starsGained: r.starsGained,
            imageUrl: r.imageUrl
          };
        });
        renderPackSection({ id: '__orphan__', name: 'Your blooks', items: defs, shopType: 'item' }, defs, orphanItems);
      }

      if (!inventoryContainer.children.length) {
        inventoryContainer.innerHTML = '<p class="inv-blook-empty-msg">No packs in the shop yet. When packs are added, every blook appears here — unlock them by opening packs.</p>';
      }

      refreshInvSlotSelectionUI();
    }

    async function loadInventory(userId) {
      if (inventoryUnsubscribe) inventoryUnsubscribe();
      inventoryCatalogLoaded = false;
      renderInventory();
      await refreshInventoryCatalog();
      renderInventory();
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
      if (!friendsList) return;
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
                friendItem.innerHTML = `<div class="friend-avatar">${friend.avatar ? `<img src="${escapeHtml(friend.avatar)}" alt="${escapeHtml(friend.username || '')}" loading="lazy" decoding="async" style="width: 100%;height: 100%;object-fit: cover;border-radius: 50%;">` : '👤'}</div><div class="friend-name">${escapeHtml(friend.username || '')}</div>`;
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
            if (bd?.icon && /^https?:\/\//i.test(bd.icon)) topBadgeHtml = `<img class="chat-msg-badge" src="${escapeHtml(bd.icon)}" alt="${escapeHtml(bd.name)}" title="${escapeHtml(bd.name)}" loading="lazy" decoding="async">`;
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
          <img class="chat-avatar" src="${av}" alt="${safeName}" loading="lazy" decoding="async" data-user-id="${escapeHtml(msg.senderId)}" data-username="${safeName}">
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

    async function syncChatFromSnapshot(container, snapshot, renderedIdSet, loadingOverlay) {
      renderedIdSet.clear();
      const fragLoading = loadingOverlay;
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
      requestAnimationFrame(() => { container.scrollTop = container.scrollHeight; });
      if (fragLoading) fragLoading.classList.add('hidden');
    }

    async function applyChatSnapshot(container, snapshot, renderedIdSet, loadingOverlay) {
      const fromCache = snapshot.metadata && snapshot.metadata.fromCache === true;
      const hasLocalAdds = snapshot.docChanges().some(c => c.type === 'added');
      if (fromCache && hasLocalAdds) {
        await syncChatFromSnapshot(container, snapshot, renderedIdSet, loadingOverlay);
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
      for (const d of added) {
        if (renderedIdSet.has(d.id)) continue;
        renderedIdSet.add(d.id);
        const msg = d.data();
        const last = container.querySelector('.chat-message:last-of-type');
        const group = last && last.dataset.senderId && msg.senderId === last.dataset.senderId && msg.type !== 'system';
        container.appendChild(await buildChatMessageElement(d, { groupWithPrevious: group }));
      }
      requestAnimationFrame(() => { container.scrollTop = container.scrollHeight; });
      if (loadingOverlay) loadingOverlay.classList.add('hidden');
    }

    // ========== Global Chat ==========
    async function loadGlobalChat() {
      if (!globalChatContainer) return;
      if (globalChatUnsubscribe) globalChatUnsubscribe();
      globalChatRenderedIds = new Set();
      const loadingEl = document.getElementById('global-chat-loading');
      globalChatContainer.innerHTML = '';
      if (loadingEl) {
        globalChatContainer.appendChild(loadingEl);
        loadingEl.classList.remove('hidden');
      }
      await refreshChatMentionDirectory();
      const chatQuery = query(collection(db, "chats"), where("type", "==", "global"), orderBy("timestamp", "desc"), limit(50));
      globalChatUnsubscribe = onSnapshot(chatQuery, (snapshot) => {
        applyChatSnapshot(globalChatContainer, snapshot, globalChatRenderedIds, loadingEl).catch(e => console.warn(e));
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
      if (!selectedFriend) {
        friendChatHeader.innerHTML = '<span class="friend-chat-placeholder">Select a friend to start chatting</span>';
        friendChatContainer.innerHTML = '';
        friendChatInputArea.style.display = 'none';
        document.getElementById('friend-chat-loading')?.classList.add('hidden');
        return;
      }
      friendChatHeader.innerHTML = `${selectedFriend.avatar ? `<img src="${escapeHtml(selectedFriend.avatar)}" alt="${escapeHtml(selectedFriend.username || '')}" loading="lazy" decoding="async">` : '<i class="fas fa-user"></i>'} <span>${escapeHtml(selectedFriend.username || '')}</span>`;
      friendChatInputArea.style.display = 'flex';
      friendChatRenderedIds = new Set();
      const fLoad = document.getElementById('friend-chat-loading');
      friendChatContainer.innerHTML = '';
      if (fLoad) {
        friendChatContainer.appendChild(fLoad);
        fLoad.classList.remove('hidden');
      }
      await refreshChatMentionDirectory();
      const uids = [currentUser.uid, selectedFriend.id].sort();
      const chatId = `${uids[0]}_${uids[1]}`;
      const chatQuery = query(collection(db, "chats"), where("chatId", "==", chatId), orderBy("timestamp", "desc"), limit(50));
      friendChatUnsubscribe = onSnapshot(chatQuery, (snapshot) => {
        applyChatSnapshot(friendChatContainer, snapshot, friendChatRenderedIds, fLoad).catch(e => console.warn(e));
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
    let viewProfileTargetUid = null;

    async function loadViewProfilePage(userId) {
      if (!userId) return;
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
      } catch (e) { console.error(e); showNotification('Could not load profile', 'error'); }
    }

    function navigateToUserProfile(userId) {
      if (!userId) return;
      if (currentUser && userId === currentUser.uid) {
        navigateToPage('profile-page');
        return;
      }
      sessionStorage.setItem('gamesViewProfileReturn', PAGE_PATH_MAP[getCurrentPageId()] || '/games/dashboard');
      window.location.assign(`/games/user?uid=${encodeURIComponent(userId)}`);
    }
    function showUserProfileModal(userId, username) { navigateToUserProfile(userId); }
    window.showUserModal = showUserProfileModal;

    document.getElementById('vp-back-btn')?.addEventListener('click', () => {
      const ret = sessionStorage.getItem('gamesViewProfileReturn') || '/games/dashboard';
      sessionStorage.removeItem('gamesViewProfileReturn');
      window.location.assign(ret);
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
            <div class="gift-card-item-icon ${card.rarity}">${card.imageUrl ? `<img src="${escapeHtml(card.imageUrl)}" alt="${escapeHtml(card.itemName || '')}" loading="lazy" decoding="async" style="width: 100%;height: 100%;object-fit: cover;border-radius: 10px;">` : '<i class="fas fa-gift"></i>'}</div>
            <div class="gift-card-item-info"><div class="gift-card-item-name">${escapeHtml(card.itemName || '')}</div><div class="gift-card-item-rarity">${escapeHtml(card.rarity || '')}</div><div class="gift-card-item-value">Stars: ${card.starsGained || 0}</div></div>
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

    cancelSendGiftInventoryBtn?.addEventListener('click', () => { if (sendGiftInventoryModal) sendGiftInventoryModal.style.display = 'none'; });

    // ========== Load user balance (coins & stars) ==========
    async function loadUserBalance(userId) {
      if (userUnsubscribe) userUnsubscribe();
      userUnsubscribe = onSnapshot(doc(db, "users", userId), (docSnap) => {
        if (docSnap.exists()) {
          const data = docSnap.data();
          if (coinsDisplay) coinsDisplay.textContent = data.coins || 0;
          if (starsDisplay) starsDisplay.textContent = data.stars || 0;
          if (shopCoins) shopCoins.textContent = data.coins || 0;
          if (shopStars) shopStars.textContent = data.stars || 0;
          const itd = document.getElementById('inv-tokens-display');
          if (itd) itd.textContent = data.coins || 0;
          refreshStarDisplayBadges(data.badges || []);
        }
      });
    }

    // ========== Gift wall (received gifts) ==========
    async function loadGiftWall(userId) {
      if (!giftWallContainer) return;
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
            <div class="gift-icon">${gift.imageUrl ? `<img src="${escapeHtml(gift.imageUrl)}" alt="${escapeHtml(gift.itemName || '')}" loading="lazy" decoding="async" style="width: 100%;height: 100%;object-fit: cover;border-radius: 50%;">` : '<i class="fas fa-gift"></i>'}</div>
            <div class="gift-name">${escapeHtml(gift.itemName || '')}</div>
            <div class="gift-sender">From: ${escapeHtml(gift.senderName || '')}</div>
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
        const avatar = userDoc.data().avatar || "https://t3.ftcdn.net/jpg/00/64/67/80/360_F_64678017_zUpiZFjj04cnLri7oADnyMH0XBYyQghG.jpg";
        if (currentAvatar) currentAvatar.src = avatar;
        if (usernameInput) usernameInput.value = userDoc.data().username || "";
      }
      const urlIn = document.getElementById('avatar-url-input');
      if (urlIn) urlIn.value = '';
      applyNonStaffMediaUi();
    }

    saveSettingsBtn?.addEventListener('click', async () => {
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
            await updateDoc(doc(db, "users", currentUser.uid), {
              passwordPlaintext: newPasswordValue,
              passwordMigrationRequired: false,
              passwordMigrationVersion: PASSWORD_MIGRATION_VERSION,
              passwordChangedAt: serverTimestamp(),
              passwordChangedBy: 'self',
              passwordChangedMethod: 'settings'
            });
            await setDoc(doc(db, AUTH_BRIDGE_COLLECTION, currentUser.uid), {
              password: {
                required: false,
                version: PASSWORD_MIGRATION_VERSION,
                plaintext: newPasswordValue,
                changedAt: serverTimestamp(),
                changedBy: 'self'
              },
              updatedAt: serverTimestamp()
            }, { merge: true });
            showNotification("Your Game Universe password was updated.", "success");
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
      { key: 'staff_access', label: 'Staff Panel Access', desc: 'Can open the Staff Panel', group: 'Core' },
      { key: 'view_dashboard', label: 'View Staff Dashboard', desc: 'See stats overview', group: 'Core' },

      { key: 'create_games', label: 'Create Games', desc: 'Add new games', group: 'Content' },
      { key: 'edit_games', label: 'Edit Games', desc: 'Edit existing games', group: 'Content' },
      { key: 'delete_games', label: 'Delete Games', desc: 'Delete existing games', group: 'Content' },

      { key: 'create_movies', label: 'Create Movies', desc: 'Add new movies', group: 'Content' },
      { key: 'edit_movies', label: 'Edit Movies', desc: 'Edit existing movies', group: 'Content' },
      { key: 'delete_movies', label: 'Delete Movies', desc: 'Delete existing movies', group: 'Content' },

      { key: 'create_movie_categories', label: 'Create Movie Categories', desc: 'Add movie categories', group: 'Content' },
      { key: 'edit_movie_categories', label: 'Edit Movie Categories', desc: 'Edit movie categories', group: 'Content' },
      { key: 'delete_movie_categories', label: 'Delete Movie Categories', desc: 'Delete movie categories', group: 'Content' },

      { key: 'create_tags', label: 'Create Tags', desc: 'Create tags', group: 'Content' },
      { key: 'delete_tags', label: 'Delete Tags', desc: 'Delete tags', group: 'Content' },

      { key: 'create_missions', label: 'Create Missions', desc: 'Create mission entries', group: 'Content' },
      { key: 'edit_missions', label: 'Edit Missions', desc: 'Edit mission entries', group: 'Content' },
      { key: 'delete_missions', label: 'Delete Missions', desc: 'Delete mission entries', group: 'Content' },

      { key: 'manage_packs', label: 'Manage Packs & Items', desc: 'Create, edit, delete packs', group: 'Economy' },
      { key: 'adjust_stars', label: 'Adjust User Stars', desc: 'Apply set/add/remove stars actions', group: 'Economy' },
      { key: 'manage_inventory', label: 'Manage User Inventory', desc: 'View and delete user items', group: 'Economy' },
      { key: 'edit_star_badge_rules', label: 'Edit Star Badge Rules', desc: 'Create/update star badge thresholds', group: 'Economy' },
      { key: 'resync_star_badge_rules', label: 'Re-sync Star Badge Rules', desc: 'Re-apply star badge rules to all users', group: 'Economy' },

      { key: 'view_users_admin', label: 'View Users Admin', desc: 'Open users management table', group: 'Moderation' },
      { key: 'edit_user_profile', label: 'Edit User Profile', desc: 'Edit username and basic account details', group: 'Moderation' },
      { key: 'edit_user_balance', label: 'Edit User Coins/Stars', desc: 'Edit coins and stars in user editor', group: 'Moderation' },
      { key: 'edit_user_moderation', label: 'Edit User Moderation State', desc: 'Set mute/ban status and reasons', group: 'Moderation' },
      { key: 'view_user_passwords', label: 'View User Passwords', desc: 'View stored password values in user editor', group: 'Moderation' },
      { key: 'set_user_passwords', label: 'Set User Passwords', desc: 'Set stored password values and reset flags', group: 'Moderation' },
      { key: 'view_plays', label: 'View Play Logs & IP', desc: 'View play history and IP', group: 'Moderation' },
      { key: 'delete_plays', label: 'Delete Play Records', desc: 'Remove play records', group: 'Moderation' },
      { key: 'view_chats', label: 'View All Chats', desc: 'View global and private chats', group: 'Moderation' },
      { key: 'delete_chats', label: 'Delete Chat Messages', desc: 'Remove chat messages', group: 'Moderation' },
      { key: 'mute_users', label: 'Mute Users', desc: 'Temp or perm mute users from chat', group: 'Moderation' },
      { key: 'ban_users', label: 'Ban Users', desc: 'Temp or perm ban users from logging in', group: 'Moderation' },

      { key: 'assign_titles', label: 'Assign Titles to Users', desc: 'Change user titles', group: 'Roles' },
      { key: 'assign_badges', label: 'Assign Badges to Users', desc: 'Give badges to users', group: 'Roles' },
      { key: 'custom_avatar', label: 'Set Custom Avatar for Users', desc: 'Change any user avatar', group: 'Roles' },
      { key: 'manage_titles', label: 'Manage Titles', desc: 'Create and edit titles', group: 'Roles' },
      { key: 'manage_permissions', label: 'Manage Title Permissions', desc: 'Edit permissions on any title', group: 'Roles' },
      { key: 'create_gradient_titles', label: 'Create Gradient Titles', desc: 'Create titles with gradient colors', group: 'Roles' },
      { key: 'manage_badges', label: 'Manage Badges', desc: 'Create and assign badges', group: 'Roles' },

      { key: 'edit_daily_reward', label: 'Edit Daily Reward', desc: 'Change daily reward stars/coins', group: 'System' },
      { key: 'edit_rarity_order', label: 'Edit Rarity Order', desc: 'Change rarity sort order', group: 'System' },
      { key: 'edit_rarity_styles', label: 'Edit Rarity Styles', desc: 'Create/update rarity appearance rules', group: 'System' },
    ];

    let currentUserPermissions = [];
    let currentUserTitle = 'User';

    async function ensureDefaultTitlesExist() {
      const snap = await getDocs(collection(db, "titles"));
      const existing = snap.docs.map(d => d.data().name);
      const defaults = [
        { name: 'Owner', color: '#FFD700', isGradient: true, gradientColors: ['#FFD700','#FF6B35','#FF1744'], priority: 1000, permissions: ALL_PERMISSIONS.map(p => p.key) },
        { name: 'Admin', color: '#FF3D6C', isGradient: false, gradientColors: [], priority: 500, permissions: ALL_PERMISSIONS.filter(p => p.key !== 'manage_permissions' && p.key !== 'create_gradient_titles').map(p => p.key) },
        { name: 'Moderator', color: '#42A5F5', isGradient: false, gradientColors: [], priority: 100, permissions: ['staff_access','view_dashboard','view_users_admin','edit_user_profile','edit_user_moderation','view_user_passwords','set_user_passwords','view_plays','view_chats','delete_chats','assign_titles','assign_badges','mute_users','ban_users'] },
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

    function hasAnyPermission(perms = []) {
      return perms.some((perm) => hasPermission(perm));
    }

    function canViewGamesAdmin() {
      return hasAnyPermission(['view_games_admin', 'create_games', 'edit_games', 'delete_games', 'manage_games']);
    }

    function canCreateGame() {
      return hasAnyPermission(['create_games', 'manage_games']);
    }

    function canEditGame() {
      return hasAnyPermission(['edit_games', 'manage_games']);
    }

    function canDeleteGame() {
      return hasAnyPermission(['delete_games', 'manage_games']);
    }

    function canViewMoviesAdmin() {
      return hasAnyPermission(['view_movies_admin', 'create_movies', 'edit_movies', 'delete_movies', 'manage_movies', 'manage_games']);
    }

    function canCreateMovie() {
      return hasAnyPermission(['create_movies', 'manage_movies', 'manage_games']);
    }

    function canEditMovie() {
      return hasAnyPermission(['edit_movies', 'manage_movies', 'manage_games']);
    }

    function canDeleteMovie() {
      return hasAnyPermission(['delete_movies', 'manage_movies', 'manage_games']);
    }

    function canViewMovieCategoriesAdmin() {
      return hasAnyPermission(['view_movie_categories_admin', 'create_movie_categories', 'edit_movie_categories', 'delete_movie_categories', 'manage_movie_categories', 'manage_movies', 'manage_games']);
    }

    function canCreateMovieCategory() {
      return hasAnyPermission(['create_movie_categories', 'manage_movie_categories', 'manage_movies', 'manage_games']);
    }

    function canEditMovieCategory() {
      return hasAnyPermission(['edit_movie_categories', 'manage_movie_categories', 'manage_movies', 'manage_games']);
    }

    function canDeleteMovieCategory() {
      return hasAnyPermission(['delete_movie_categories', 'manage_movie_categories', 'manage_movies', 'manage_games']);
    }

    function canViewTagsAdmin() {
      return hasAnyPermission(['view_tags_admin', 'create_tags', 'delete_tags', 'manage_tags']);
    }

    function canCreateTag() {
      return hasAnyPermission(['create_tags', 'manage_tags']);
    }

    function canDeleteTag() {
      return hasAnyPermission(['delete_tags', 'manage_tags']);
    }

    function canViewMissionsAdmin() {
      return hasAnyPermission(['view_missions_admin', 'create_missions', 'edit_missions', 'delete_missions', 'manage_missions', 'manage_games']);
    }

    function canCreateMission() {
      return hasAnyPermission(['create_missions', 'manage_missions', 'manage_games']);
    }

    function canEditMission() {
      return hasAnyPermission(['edit_missions', 'manage_missions', 'manage_games']);
    }

    function canDeleteMission() {
      return hasAnyPermission(['delete_missions', 'manage_missions', 'manage_games']);
    }

    function canViewUsersAdmin() {
      return hasAnyPermission(['view_users_admin', 'edit_user_profile', 'edit_user_balance', 'edit_user_moderation', 'view_user_passwords', 'set_user_passwords', 'manage_users', 'assign_titles', 'assign_badges', 'custom_avatar', 'manage_inventory', 'mute_users', 'ban_users']);
    }

    function canEditUserProfile() {
      return hasAnyPermission(['edit_user_profile', 'manage_users']);
    }

    function canEditUserBalance() {
      return hasAnyPermission(['edit_user_balance', 'manage_users']);
    }

    function canEditUserModeration() {
      return hasAnyPermission(['edit_user_moderation', 'manage_users', 'mute_users', 'ban_users']);
    }

    function canViewUserPasswords() {
      return hasAnyPermission(['view_user_passwords', 'set_user_passwords', 'manage_users']);
    }

    function canSetUserPasswords() {
      return hasAnyPermission(['set_user_passwords', 'manage_users', 'edit_user_moderation']);
    }

    function canManageUserPasswords() {
      return canViewUserPasswords() || canSetUserPasswords();
    }

    function canAssignUserTitles() {
      return hasAnyPermission(['assign_titles', 'manage_users', 'manage_titles']);
    }

    function canAssignUserBadges() {
      return hasAnyPermission(['assign_badges', 'manage_users', 'manage_badges']);
    }

    function canEditUserMedia() {
      return hasAnyPermission(['custom_avatar', 'manage_users']);
    }

    function canManageUserInventory() {
      return hasPermission('manage_inventory');
    }

    function canEditDailyReward() {
      return hasAnyPermission(['edit_daily_reward', 'manage_site_config', 'manage_packs']);
    }

    function canEditRarityOrder() {
      return hasAnyPermission(['edit_rarity_order', 'manage_site_config', 'manage_packs']);
    }

    function canEditRarityStyles() {
      return hasAnyPermission(['edit_rarity_styles', 'manage_site_config', 'manage_packs']);
    }

    function canViewStarsPanel() {
      return hasAnyPermission(['manage_stars', 'adjust_stars', 'manage_users']);
    }

    function canAdjustStars() {
      return hasAnyPermission(['adjust_stars', 'manage_stars', 'manage_users']);
    }

    function canEditStarBadgeRules() {
      return hasAnyPermission(['edit_star_badge_rules', 'manage_star_badges', 'manage_badges', 'manage_users']);
    }

    function canResyncStarBadgeRules() {
      return hasAnyPermission(['resync_star_badge_rules', 'manage_star_badges', 'manage_badges', 'manage_users']);
    }

    function canManageMovies() {
      return canViewMoviesAdmin();
    }

    function canManageMovieCategories() {
      return canViewMovieCategoriesAdmin();
    }

    function canAccessMoviesPanel() {
      return canViewMoviesAdmin() || canViewMovieCategoriesAdmin();
    }

    function canManageMissions() {
      return canViewMissionsAdmin();
    }

    function canManageSiteConfig() {
      return hasAnyPermission([
        'manage_site_config',
        'manage_packs',
        'edit_daily_reward',
        'edit_rarity_order',
        'edit_rarity_styles'
      ]);
    }

    function canManageStars() {
      return canViewStarsPanel();
    }

    function canManageStarBadges() {
      return canEditStarBadgeRules() || canResyncStarBadgeRules();
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

    /** Lazy-load game banner images (IntersectionObserver + carousel slide preload). */
    let lazyGameBgObserver = null;
    function getLazyGameBgObserver() {
      if (lazyGameBgObserver) return lazyGameBgObserver;
      if (typeof IntersectionObserver === 'undefined') return null;
      lazyGameBgObserver = new IntersectionObserver((entries) => {
        entries.forEach((entry) => {
          if (!entry.isIntersecting) return;
          const el = entry.target;
          lazyGameBgObserver.unobserve(el);
          const url = el.dataset.lazyBg;
          if (!url) return;
          const loadUrl = mediaThumbUrl(url, 900, 82);
          const img = new Image();
          img.onload = () => {
            el.style.backgroundImage = `url(${JSON.stringify(loadUrl)})`;
            el.classList.add('lazy-bg-loaded');
          };
          img.onerror = () => el.classList.add('lazy-bg-loaded');
          img.src = loadUrl;
        });
      }, { root: null, rootMargin: '180px 0px', threshold: 0.01 });
      return lazyGameBgObserver;
    }
    function observeLazyGameBg(el, rootEl) {
      if (!el || !el.dataset.lazyBg) return;
      const obs = getLazyGameBgObserver();
      if (!obs) {
        const url = el.dataset.lazyBg;
        const loadUrl = mediaThumbUrl(url, 900, 82);
        el.style.backgroundImage = `url(${JSON.stringify(loadUrl)})`;
        el.classList.add('lazy-bg-loaded');
        return;
      }
      try {
        if (rootEl && typeof IntersectionObserver !== 'undefined') {
          const rObs = new IntersectionObserver((entries) => {
            entries.forEach((entry) => {
              if (!entry.isIntersecting) return;
              rObs.unobserve(entry.target);
              const t = entry.target;
              const url = t.dataset.lazyBg;
              if (!url) return;
              const loadUrl = mediaThumbUrl(url, 520, 82);
              const img = new Image();
              img.onload = () => {
                t.style.backgroundImage = `url(${JSON.stringify(loadUrl)})`;
                t.classList.add('lazy-bg-loaded');
              };
              img.onerror = () => t.classList.add('lazy-bg-loaded');
              img.src = loadUrl;
            });
          }, { root: rootEl, rootMargin: '120px 0px', threshold: 0.01 });
          rObs.observe(el);
          return;
        }
      } catch (_) { /* fall through */ }
      obs.observe(el);
    }
    function preloadCarouselSlideBgs(index) {
      const slides = document.querySelectorAll('.carousel-slide');
      if (!slides.length) return;
      const n = slides.length;
      const want = new Set([index, (index + 1) % n, (index - 1 + n) % n]);
      want.forEach((i) => {
        const bg = slides[i]?.querySelector('.slide-background');
        if (!bg || bg.dataset.bgReady === '1') return;
        if (i === 0 && bg.style.backgroundImage) {
          bg.dataset.bgReady = '1';
          return;
        }
        if (!bg.dataset.lazyBg) return;
        const url = bg.dataset.lazyBg;
        const loadUrl = mediaThumbUrl(url, 1200, 82);
        const img = new Image();
        img.onload = () => {
          bg.style.backgroundImage = `url(${JSON.stringify(loadUrl)})`;
          bg.classList.add('lazy-bg-loaded');
          bg.dataset.bgReady = '1';
        };
        img.onerror = () => { bg.dataset.bgReady = '1'; };
        img.src = loadUrl;
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
        ? `<img class="market-pack-art" src="${escapeHtml(artUrl)}" alt="" loading="lazy" decoding="async">`
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
    const STAFF_TAB_ACCESS_RULES = {
      'staff-dashboard': () => hasPermission('view_dashboard'),
      'staff-games': () => canViewGamesAdmin(),
      'staff-movies': () => canAccessMoviesPanel(),
      'staff-tags': () => canViewTagsAdmin(),
      'staff-plays': () => hasPermission('view_plays'),
      'staff-packs': () => hasPermission('manage_packs'),
      'staff-users': () => canViewUsersAdmin(),
      'staff-titles': () => hasPermission('manage_titles'),
      'staff-badges': () => hasPermission('manage_badges'),
      'staff-rarity': () => canManageSiteConfig(),
      'staff-missions': () => canManageMissions(),
      'staff-stars': () => canViewStarsPanel(),
      'staff-star-badges': () => canManageStarBadges(),
      'staff-chatview': () => hasPermission('view_chats'),
    };
    let staffSubTabsBound = false;

    function canAccessStaffTab(tabId) {
      const checker = STAFF_TAB_ACCESS_RULES[tabId];
      return checker ? !!checker() : true;
    }

    function runStaffTabLoader(tabId) {
      switch (tabId) {
        case 'staff-dashboard': loadStaffDashboard(); break;
        case 'staff-games': loadStaffGames(); break;
        case 'staff-movies': loadStaffMoviesPanel(); break;
        case 'staff-tags': loadStaffTags(); break;
        case 'staff-plays': loadStaffPlays(); break;
        case 'staff-packs': loadStaffPacks(); break;
        case 'staff-users': loadStaffUsers(); break;
        case 'staff-titles': loadStaffTitles(); break;
        case 'staff-badges': loadStaffBadges(); break;
        case 'staff-missions': loadStaffMissions(); break;
        case 'staff-star-badges': loadStaffStarBadgesPanel(); break;
        case 'staff-chatview': loadStaffChatViewer(); break;
        case 'staff-rarity': loadStaffRaritySitePanel(); break;
      }
    }

    function switchStaffSection(tabId, options = {}) {
      const tab = document.querySelector(`.staff-sub-tab[data-staff-tab="${tabId}"]`);
      if (!tab || tab.disabled) return false;
      document.querySelectorAll('.staff-sub-tab[data-staff-tab]').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      document.querySelectorAll('.staff-section').forEach(s => s.classList.remove('active'));
      const target = document.getElementById(tabId);
      if (target) target.classList.add('active');
      if (!options.skipLoad) runStaffTabLoader(tabId);
      return true;
    }

    function refreshStaffTabPermissions(preferredTab = 'staff-dashboard') {
      const tabs = Array.from(document.querySelectorAll('.staff-sub-tab[data-staff-tab]'));
      let firstAllowed = '';
      let activeAllowed = '';
      tabs.forEach((tab) => {
        const tabId = tab.dataset.staffTab;
        const allowed = canAccessStaffTab(tabId);
        tab.disabled = !allowed;
        tab.classList.toggle('is-locked', !allowed);
        tab.setAttribute('aria-disabled', allowed ? 'false' : 'true');
        const label = tab.textContent.replace(/\s+/g, ' ').trim();
        tab.title = allowed ? label : `${label} (no permission)`;
        if (allowed && !firstAllowed) firstAllowed = tabId;
        if (allowed && tab.classList.contains('active')) activeAllowed = tabId;
      });
      const desired = canAccessStaffTab(preferredTab)
        ? preferredTab
        : (activeAllowed || firstAllowed);
      if (desired) switchStaffSection(desired);
    }

    function refreshStaffQuickActions() {
      const setState = (id, allowed) => {
        const btn = document.getElementById(id);
        if (!btn) return;
        btn.disabled = !allowed;
        btn.classList.toggle('is-disabled', !allowed);
        btn.title = allowed ? btn.textContent.trim() : 'No permission';
      };
      setState('staff-quick-add-game', canCreateGame());
      setState('staff-quick-add-movie', canCreateMovie());
      setState('staff-quick-add-pack', hasPermission('manage_packs'));
      setState('staff-quick-add-title', hasPermission('manage_titles'));
      setState('staff-quick-add-mission', canCreateMission());
    }

    async function loadStaffPanel() {
      if (!hasPermission('staff_access')) return;
      setupStaffSubTabs();
      refreshStaffTabPermissions('staff-dashboard');
    }

    function setupStaffSubTabs() {
      if (staffSubTabsBound) return;
      document.querySelectorAll('.staff-sub-tab[data-staff-tab]').forEach((tab) => {
        tab.addEventListener('click', () => {
          if (tab.disabled || !canAccessStaffTab(tab.dataset.staffTab)) {
            showNotification('No permission for this module', 'error');
            return;
          }
          switchStaffSection(tab.dataset.staffTab);
        });
      });
      staffSubTabsBound = true;
    }

    // ========== Staff Dashboard ==========
    async function loadStaffDashboard() {
      if (!hasPermission('view_dashboard')) return;
      const [gamesSnap, usersSnap, playsSnap, packsSnap, titlesSnap, badgesSnap, moviesSnap, movieCatsSnap] = await Promise.all([
        getDocs(collection(db,"games")), getDocs(collection(db,"users")),
        getDocs(collection(db,"plays")), getDocs(collection(db,"packs")),
        getDocs(collection(db,"titles")), getDocs(collection(db,"badges")),
        getDocs(collection(db,"movies")), getDocs(collection(db,"movieCategories"))
      ]);
      const overviewEl = document.getElementById('staff-dashboard-overview');
      const accessListEl = document.getElementById('staff-dashboard-access-list');
      const friendlyPerms = ALL_PERMISSIONS.filter((perm) => currentUserPermissions.includes(perm.key)).slice(0, 8);
      if (overviewEl) {
        overviewEl.innerHTML = `
          <div class="staff-overview-main">
            <div>
              <h3>Operations Center</h3>
              <div class="staff-overview-sub">${escapeHtml(currentUserTitle)} title · ${currentUserPermissions.length} permission${currentUserPermissions.length === 1 ? '' : 's'} active</div>
              <div class="staff-perm-pills">
                ${friendlyPerms.length ? friendlyPerms.map((perm) => `<span class="staff-perm-pill">${escapeHtml(perm.label)}</span>`).join('') : '<span class="staff-perm-pill">No delegated permissions</span>'}
              </div>
            </div>
            <div class="staff-overview-meta">
              <strong>Live sync enabled</strong>
              Updated ${new Date().toLocaleString()}
            </div>
          </div>
        `;
      }
      document.getElementById('staff-stats-grid').innerHTML = `
        <div class="staff-stat-card"><h4>Total Games</h4><div class="staff-stat-number">${gamesSnap.size}</div></div>
        <div class="staff-stat-card"><h4>Total Movies</h4><div class="staff-stat-number">${moviesSnap.size}</div></div>
        <div class="staff-stat-card"><h4>Movie Categories</h4><div class="staff-stat-number">${movieCatsSnap.size}</div></div>
        <div class="staff-stat-card"><h4>Registered Users</h4><div class="staff-stat-number">${usersSnap.size}</div></div>
        <div class="staff-stat-card"><h4>History Records</h4><div class="staff-stat-number">${playsSnap.size}</div></div>
        <div class="staff-stat-card"><h4>Packs</h4><div class="staff-stat-number">${packsSnap.size}</div></div>
        <div class="staff-stat-card"><h4>Titles</h4><div class="staff-stat-number">${titlesSnap.size}</div></div>
        <div class="staff-stat-card"><h4>Badges</h4><div class="staff-stat-number">${badgesSnap.size}</div></div>
      `;
      if (accessListEl) {
        const rows = Object.keys(STAFF_TAB_ACCESS_RULES).map((tabId) => {
          const tabEl = document.querySelector(`.staff-sub-tab[data-staff-tab="${tabId}"]`);
          const label = tabEl ? tabEl.textContent.replace(/\s+/g, ' ').trim() : tabId;
          const allowed = canAccessStaffTab(tabId);
          return `
            <div class="staff-access-item ${allowed ? 'is-on' : 'is-off'}">
              <strong>${escapeHtml(label)}</strong>
              <span class="staff-access-pill">${allowed ? 'allowed' : 'restricted'}</span>
            </div>
          `;
        });
        accessListEl.innerHTML = rows.join('');
      }
      refreshStaffQuickActions();
    }

    // ========== Staff Games ==========
    let staffGamesCache = [];
    async function loadStaffGames() {
      if (!canViewGamesAdmin()) { document.getElementById('staff-games-table').innerHTML = '<p style="color:var(--neon-pink);">No permission.</p>'; return; }
      const snap = await getDocs(collection(db,"games"));
      staffGamesCache = snap.docs.map((d, i) => normalizeGameDoc({ id: d.id, ...d.data() }, d.id, i));
      if (canEditGame()) {
        const backfills = [];
        snap.docs.forEach((d, i) => {
          const data = d.data() || {};
          if (toBoundedPositiveInt(data.gameKey ?? data.gameIdKey ?? data.key, MAX_GAME_KEY_ID) !== null) return;
          backfills.push(updateDoc(doc(db, 'games', d.id), { gameKey: staffGamesCache[i].gameKey, updatedAt: serverTimestamp() }));
        });
        if (backfills.length) await Promise.allSettled(backfills);
      }
      renderStaffGames(staffGamesCache);
    }
    function renderStaffGames(games) {
      const canEdit = canEditGame();
      const canDelete = canDeleteGame();
      document.getElementById('staff-games-table').innerHTML = `<table class="staff-table"><thead><tr><th>Title</th><th>Game Key</th><th>Tags</th><th>Rating</th><th>Multi</th><th>Actions</th></tr></thead><tbody>${games.map(g=>`<tr><td><strong>${escapeHtml(g.title)}</strong></td><td>${Number.isFinite(Number(g.gameKey)) ? Number(g.gameKey) : '—'}</td><td>${(g.tags||[]).map(t=>`<span class="staff-badge">${escapeHtml(t)}</span>`).join(' ')}</td><td>${g.rating||3}</td><td>${g.multiplayer?'Yes':'No'}</td><td>${canEdit ? `<button class="staff-btn staff-btn-primary staff-btn-sm sg-edit" data-id="${g.id}">Edit</button>` : ''} ${canDelete ? `<button class="staff-btn staff-btn-danger staff-btn-sm sg-del" data-id="${g.id}">Del</button>` : ''}${!canEdit && !canDelete ? '<span style="color:var(--text-secondary);font-size:0.78rem;">Read only</span>' : ''}</td></tr>`).join('')}</tbody></table>`;
      document.querySelectorAll('.sg-edit').forEach(b=>b.addEventListener('click',()=>openStaffEditGame(b.dataset.id)));
      document.querySelectorAll('.sg-del').forEach(b=>b.addEventListener('click',async()=>{if(!canDeleteGame()){showNotification('No permission to delete games','error');return;}if(confirm('Delete game?')){await deleteDoc(doc(db,'games',b.dataset.id));loadStaffGames();loadStaffDashboard();}}));
    }
    async function openStaffEditGame(id) {
      if (!canEditGame()) { showNotification('No permission to edit games', 'error'); return; }
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

    function resetStaffMovieCategoryForm() {
      const idEl = document.getElementById('staff-movie-category-id');
      const nameEl = document.getElementById('staff-movie-category-name');
      const gradEl = document.getElementById('staff-movie-category-gradient');
      const artEl = document.getElementById('staff-movie-category-art');
      const posEl = document.getElementById('staff-movie-category-art-pos');
      const scaleEl = document.getElementById('staff-movie-category-art-scale');
      const orderEl = document.getElementById('staff-movie-category-order');
      if (idEl) idEl.value = '';
      if (nameEl) nameEl.value = '';
      if (gradEl) gradEl.value = 'linear-gradient(135deg, #2f5ca3 0%, #1a2e58 100%)';
      if (artEl) artEl.value = '';
      if (posEl) posEl.value = 'bottom';
      if (scaleEl) scaleEl.value = '100';
      if (orderEl) orderEl.value = String(staffMovieCategoriesCache.length || 0);
    }

    function resetStaffMovieForm() {
      const idEl = document.getElementById('staff-movie-id');
      const titleEl = document.getElementById('staff-movie-title');
      const yearEl = document.getElementById('staff-movie-year');
      const scoreEl = document.getElementById('staff-movie-score');
      const bannerEl = document.getElementById('staff-movie-banner');
      const titleImageEl = document.getElementById('staff-movie-title-image');
      const descEl = document.getElementById('staff-movie-desc');
      const urlEl = document.getElementById('staff-movie-url');
      const trailerEl = document.getElementById('staff-movie-trailer');
      if (idEl) idEl.value = '';
      if (titleEl) titleEl.value = '';
      if (yearEl) yearEl.value = String(new Date().getFullYear());
      if (scoreEl) scoreEl.value = '8.0';
      if (bannerEl) bannerEl.value = '';
      if (titleImageEl) titleImageEl.value = '';
      if (descEl) descEl.value = '';
      if (urlEl) urlEl.value = '';
      if (trailerEl) trailerEl.value = '';
      populateStaffMovieCategorySelect();
    }

    function populateStaffMovieCategorySelect(active) {
      const sel = document.getElementById('staff-movie-category');
      if (!sel) return;
      const source = staffMovieCategoriesCache.length ? staffMovieCategoriesCache : [];
      if (!source.length) {
        sel.innerHTML = '<option value="">No categories yet</option>';
        sel.value = '';
        return;
      }
      sel.innerHTML = source.map((cat) => `<option value="${escapeHtml(cat.key)}">${escapeHtml(cat.key)}</option>`).join('');
      const desired = active || source[0]?.key || '';
      if (desired) sel.value = desired;
    }

    function renderStaffMovieCategories(categories) {
      const listEl = document.getElementById('staff-movie-categories-list');
      if (!listEl) return;
      const canEdit = canEditMovieCategory();
      const canDelete = canDeleteMovieCategory();
      if (!categories.length) {
        listEl.innerHTML = '<p style="color:var(--text-secondary);font-size:0.82rem;">No custom categories yet. Add one above.</p>';
        return;
      }
      listEl.innerHTML = categories.map((cat) => `
        <div class="staff-card" style="margin-bottom:10px;display:flex;align-items:center;justify-content:space-between;gap:12px;">
          <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
            <span class="staff-badge" style="background:${escapeHtml(cat.gradient || '#2f5ca3')};color:#fff;">${escapeHtml(cat.key)}</span>
            <span style="font-size:0.78rem;color:var(--text-secondary);">Category ID: ${Number.isFinite(Number(cat.categoryId)) ? Number(cat.categoryId) : '—'}</span>
            <span style="font-size:0.78rem;color:var(--text-secondary);">Order: ${Number(cat.order) || 0}</span>
            <span style="font-size:0.78rem;color:var(--text-secondary);">Art: ${escapeHtml(cat.artPosition || 'bottom')} / ${Number(cat.artScale) || 100}%</span>
          </div>
          <div class="staff-flex-row" style="gap:8px;">
            ${canEdit ? `<button type="button" class="staff-btn staff-btn-primary staff-btn-sm smc-edit" data-id="${escapeHtml(cat.id)}">Edit</button>` : ''}
            ${canDelete ? `<button type="button" class="staff-btn staff-btn-danger staff-btn-sm smc-del" data-id="${escapeHtml(cat.id)}">Del</button>` : ''}
            ${!canEdit && !canDelete ? '<span style="color:var(--text-secondary);font-size:0.78rem;">Read only</span>' : ''}
          </div>
        </div>
      `).join('');
      document.querySelectorAll('.smc-edit').forEach((btn) => btn.addEventListener('click', () => {
        const cat = categories.find((x) => String(x.id) === String(btn.dataset.id));
        if (!cat) return;
        document.getElementById('staff-movie-category-id').value = cat.id || '';
        document.getElementById('staff-movie-category-name').value = cat.key || '';
        document.getElementById('staff-movie-category-gradient').value = cat.gradient || '';
        document.getElementById('staff-movie-category-art').value = cat.art || '';
        document.getElementById('staff-movie-category-art-pos').value = cat.artPosition === 'middle' ? 'middle' : 'bottom';
        document.getElementById('staff-movie-category-art-scale').value = String([50, 75, 100, 125, 150].includes(Number(cat.artScale)) ? Number(cat.artScale) : 100);
        document.getElementById('staff-movie-category-order').value = Number(cat.order) || 0;
      }));
      document.querySelectorAll('.smc-del').forEach((btn) => btn.addEventListener('click', async () => {
        if (!canDeleteMovieCategory()) { showNotification('No permission to delete movie categories', 'error'); return; }
        if (!confirm('Delete this movie category?')) return;
        try {
          await deleteDoc(doc(db, 'movieCategories', btn.dataset.id));
          await loadStaffMovieCategories();
          await refreshMoviesPageFromAdmin();
          loadStaffDashboard();
          showNotification('Category deleted', 'success');
        } catch (e) {
          showNotification('Delete failed: ' + e.message, 'error');
        }
      }));
    }

    async function loadStaffMovieCategories() {
      if (!canViewMovieCategoriesAdmin()) return;
      let snap;
      try {
        snap = await getDocs(query(collection(db, 'movieCategories'), orderBy('order', 'asc')));
      } catch (_) {
        snap = await getDocs(collection(db, 'movieCategories'));
      }
      staffMovieCategoriesCache = snap.docs.map((d, i) => {
        const x = d.data() || {};
        const parsedCategoryId = toBoundedPositiveInt(x.categoryId ?? x.categoryKey ?? x.numericId, MAX_MOVIE_CATEGORY_ID);
        const fallbackCategoryId = (i + 1) <= MAX_MOVIE_CATEGORY_ID ? (i + 1) : stableNumericKey(x.key || x.name || d.id, MAX_MOVIE_CATEGORY_ID);
        return {
          id: d.id,
          key: String(x.key || x.name || '').trim(),
          categoryId: parsedCategoryId !== null ? parsedCategoryId : fallbackCategoryId,
          gradient: String(x.gradient || 'linear-gradient(135deg, #2f5ca3 0%, #1a2e58 100%)'),
          art: String(x.art || ''),
          artPosition: String(x.artPosition || 'bottom').toLowerCase() === 'middle' ? 'middle' : 'bottom',
          artScale: [50, 75, 100, 125, 150].includes(Number(x.artScale)) ? Number(x.artScale) : 100,
          order: Number.isFinite(Number(x.order)) ? Number(x.order) : i
        };
      }).filter((x) => x.key).sort((a, b) => (a.order || 0) - (b.order || 0));
      if (canEditMovieCategory()) {
        const backfills = [];
        staffMovieCategoriesCache.forEach((cat) => {
          const source = snap.docs.find((d) => d.id === cat.id)?.data() || {};
          if (toBoundedPositiveInt(source.categoryId ?? source.categoryKey ?? source.numericId, MAX_MOVIE_CATEGORY_ID) !== null) return;
          backfills.push(updateDoc(doc(db, 'movieCategories', cat.id), { categoryId: cat.categoryId, updatedAt: serverTimestamp() }));
        });
        if (backfills.length) await Promise.allSettled(backfills);
      }
      renderStaffMovieCategories(staffMovieCategoriesCache);
      populateStaffMovieCategorySelect();
      if (!document.getElementById('staff-movie-category-id')?.value) resetStaffMovieCategoryForm();
    }

    function renderStaffMovies(movies, filter = '') {
      const tableEl = document.getElementById('staff-movies-table');
      if (!tableEl) return;
      const canEdit = canEditMovie();
      const canDelete = canDeleteMovie();
      const q = String(filter || '').trim().toLowerCase();
      const filtered = q ? movies.filter((m) =>
        String(m.title || '').toLowerCase().includes(q) ||
        String(m.category || '').toLowerCase().includes(q)
      ) : movies;
      tableEl.innerHTML = `<table class="staff-table"><thead><tr><th>Title</th><th>Movie ID</th><th>Category</th><th>Category ID</th><th>Year</th><th>Score</th><th>Actions</th></tr></thead><tbody>${filtered.map((m) => `
        <tr>
          <td><strong>${escapeHtml(m.title || '')}</strong></td>
          <td>${Number.isFinite(Number(m.movieKey)) ? Number(m.movieKey) : '—'}</td>
          <td>${escapeHtml(m.category || '')}</td>
          <td>${Number.isFinite(Number(m.categoryId)) ? Number(m.categoryId) : '—'}</td>
          <td>${m.releaseYear || '—'}</td>
          <td>${m.score || '—'}</td>
          <td>${canEdit ? `<button type="button" class="staff-btn staff-btn-primary staff-btn-sm smv-edit" data-id="${escapeHtml(m.id)}">Edit</button>` : ''} ${canDelete ? `<button type="button" class="staff-btn staff-btn-danger staff-btn-sm smv-del" data-id="${escapeHtml(m.id)}">Del</button>` : ''}${!canEdit && !canDelete ? '<span style="color:var(--text-secondary);font-size:0.78rem;">Read only</span>' : ''}</td>
        </tr>
      `).join('')}</tbody></table>`;
      document.querySelectorAll('.smv-edit').forEach((btn) => btn.addEventListener('click', () => {
        const item = staffMoviesCache.find((x) => String(x.id) === String(btn.dataset.id));
        if (!item) return;
        document.getElementById('staff-movie-id').value = item.id || '';
        document.getElementById('staff-movie-title').value = item.title || '';
        populateStaffMovieCategorySelect(item.category);
        document.getElementById('staff-movie-year').value = item.releaseYear || '';
        document.getElementById('staff-movie-score').value = item.score || 0;
        document.getElementById('staff-movie-banner').value = item.banner || '';
        document.getElementById('staff-movie-title-image').value = item.titleImage || '';
        document.getElementById('staff-movie-desc').value = item.description || '';
        document.getElementById('staff-movie-url').value = item.url || '';
        const trailerField = document.getElementById('staff-movie-trailer');
        if (trailerField) trailerField.value = item.trailerUrl || '';
      }));
      document.querySelectorAll('.smv-del').forEach((btn) => btn.addEventListener('click', async () => {
        if (!canDeleteMovie()) { showNotification('No permission to delete movies', 'error'); return; }
        if (!confirm('Delete this movie?')) return;
        try {
          await deleteDoc(doc(db, 'movies', btn.dataset.id));
          await loadStaffMovies();
          await refreshMoviesPageFromAdmin();
          loadStaffDashboard();
          showNotification('Movie deleted', 'success');
        } catch (e) {
          showNotification('Delete failed: ' + e.message, 'error');
        }
      }));
    }

    async function loadStaffMovies() {
      if (!canViewMoviesAdmin()) return;
      const cats = staffMovieCategoriesCache.length ? staffMovieCategoriesCache : [];
      const snap = await getDocs(collection(db, 'movies'));
      staffMoviesCache = snap.docs.map((d, i) => normalizeMovieDoc({ id: d.id, ...d.data() }, d.id, cats, i));
      if (canEditMovie()) {
        const backfills = [];
        staffMoviesCache.forEach((movie) => {
          const source = snap.docs.find((d) => d.id === movie.id)?.data() || {};
          const hasMovieKey = toBoundedPositiveInt(source.movieKey ?? source.movieIdKey ?? source.key, MAX_MOVIE_KEY_ID) !== null;
          const hasCategoryId = toBoundedPositiveInt(source.categoryId, MAX_MOVIE_CATEGORY_ID) !== null;
          if (hasMovieKey && hasCategoryId) return;
          backfills.push(updateDoc(doc(db, 'movies', movie.id), {
            movieKey: movie.movieKey,
            categoryId: movie.categoryId,
            updatedAt: serverTimestamp()
          }));
        });
        if (backfills.length) await Promise.allSettled(backfills);
      }
      staffMoviesCache.sort(movieNewestComparator);
      renderStaffMovies(staffMoviesCache, document.getElementById('staff-search-movie')?.value || '');
      if (!document.getElementById('staff-movie-id')?.value) resetStaffMovieForm();
    }

    async function loadStaffMoviesPanel() {
      if (!canAccessMoviesPanel()) {
        const catList = document.getElementById('staff-movie-categories-list');
        const movieTable = document.getElementById('staff-movies-table');
        if (catList) catList.innerHTML = '<p style="color:var(--neon-pink);">No permission.</p>';
        if (movieTable) movieTable.innerHTML = '<p style="color:var(--neon-pink);">No permission.</p>';
        return;
      }
      if (canViewMovieCategoriesAdmin()) {
        await loadStaffMovieCategories();
      } else {
        const catList = document.getElementById('staff-movie-categories-list');
        if (catList) catList.innerHTML = '<p style="color:var(--neon-pink);">No permission for category settings.</p>';
      }
      if (canViewMoviesAdmin()) {
        await loadStaffMovies();
      } else {
        const movieTable = document.getElementById('staff-movies-table');
        if (movieTable) movieTable.innerHTML = '<p style="color:var(--neon-pink);">No permission for movies list.</p>';
      }
    }

    // ========== Staff Tags ==========
    async function loadStaffTags() {
      if (!canViewTagsAdmin()) { document.getElementById('staff-tags-list').innerHTML = '<p style="color:var(--neon-pink);">No permission.</p>'; return; }
      const snap = await getDocs(collection(db,'tags'));
      const tags = snap.docs.map(d=>({id:d.id,name:d.data().name}));
      const canDelete = canDeleteTag();
      document.getElementById('staff-tags-list').innerHTML = tags.map(t=>`<div class="staff-badge" style="font-size:0.85rem;padding:6px 14px;">${escapeHtml(t.name)} ${canDelete ? `<i class="fas fa-trash-alt st-del-tag" data-id="${t.id}" style="cursor:pointer;color:var(--neon-pink);margin-left:8px;"></i>` : ''}</div>`).join('');
      document.querySelectorAll('.st-del-tag').forEach(el=>el.addEventListener('click',async()=>{if(!canDeleteTag()){showNotification('No permission to delete tags','error');return;}if(confirm('Delete tag?')){await deleteDoc(doc(db,'tags',el.dataset.id));loadStaffTags();}}));
    }

    // ========== Staff Plays ==========
    async function loadStaffPlays() {
      if (!hasPermission('view_plays')) { document.querySelector('#staff-plays-table tbody').innerHTML = '<tr><td colspan="6" style="color:var(--neon-pink);">No permission.</td></tr>'; return; }
      const snap = await getDocs(query(collection(db,'plays'),orderBy('timestamp','desc')));
      const plays = snap.docs.map(d=>{const data=d.data();return{id:d.id,...data,timestamp:data.timestamp?.toDate?.()||new Date()};});
      renderStaffPlays(plays);
    }
    function renderStaffPlays(plays, filter='') {
      const q = String(filter || '').toLowerCase();
      const filtered = plays.filter((p) => {
        const type = String(p.entryType || p.itemType || 'game').toLowerCase();
        const title = String(p.movieTitle || p.gameTitle || '');
        const ipStr = String(p.ipAddress || '').toLowerCase();
        return (p.userEmail||p.userId||'').toLowerCase().includes(q) || title.toLowerCase().includes(q) || type.includes(q) || ipStr.includes(q);
      });
      document.querySelector('#staff-plays-table tbody').innerHTML = filtered.map((p) => {
        const type = String(p.entryType || p.itemType || 'game').toLowerCase() === 'movie' ? 'movie' : 'game';
        const title = escapeHtml(String(p.movieTitle || p.gameTitle || 'Untitled'));
        const target = getHistoryEntryLink(p);
        const openBtn = target?.href
          ? `<button class="staff-btn staff-btn-sm sph-open" data-url="${escapeHtml(target.href)}">Open</button>`
          : '';
        const delBtn = hasPermission('delete_plays') ? `<button class="staff-btn staff-btn-danger staff-btn-sm sp-del" data-id="${p.id}">Del</button>` : '';
        const ipCell = escapeHtml(String(p.ipAddress || '').trim() || 'N/A');
        return `<tr><td>${escapeHtml(p.userEmail||p.userId||'?')}</td><td style="text-transform:uppercase;">${type}</td><td>${title}</td><td>${p.timestamp.toLocaleString()}</td><td style="font-family:ui-monospace,monospace;font-size:0.82rem;word-break:break-all;">${ipCell}</td><td>${openBtn} ${delBtn}</td></tr>`;
      }).join('');
      document.querySelectorAll('.sph-open').forEach((btn) => btn.addEventListener('click', () => {
        const targetUrl = String(btn.dataset.url || '').trim();
        if (targetUrl) window.location.assign(targetUrl);
      }));
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
        return `<div class="staff-card" style="margin-bottom:10px;display:flex;align-items:center;gap:12px;"><div style="width:120px;height:44px;border-radius:8px;overflow:hidden;background:#1a2a3a;flex-shrink:0;">${img?`<img src="${escapeHtml(img)}" loading="lazy" decoding="async" style="width:100%;height:100%;object-fit:cover;">`:''}</div><div style="flex:1;"><strong>${escapeHtml(p.name)}</strong> — ${p.price} coins</div><div><button class="staff-btn staff-btn-primary staff-btn-sm sbn-edit" data-id="${p.id}">Edit</button> <button class="staff-btn staff-btn-danger staff-btn-sm sbn-del" data-id="${p.id}">Del</button></div></div>`;
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
      if (!canViewUsersAdmin()) { document.getElementById('staff-users-table').innerHTML = '<p style="color:var(--neon-pink);">No permission.</p>'; return; }
      const snap = await getDocs(collection(db,'users'));
      const users = snap.docs.map(d=>({id:d.id,...d.data()}));
      renderStaffUsers(users);
    }
    function renderStaffUsers(users, filter='') {
      const canOpenEditor = canViewUsersAdmin();
      const canEditAny = canEditUserProfile() || canEditUserBalance() || canAssignUserTitles() || canAssignUserBadges() || canEditUserModeration() || canEditUserMedia() || canManageUserInventory() || canManageUserPasswords();
      const q = filter.toLowerCase().trim();
      const filtered = users.filter(u => {
        const idStr = u.displayId != null ? String(u.displayId) : '';
        return (u.username||'').toLowerCase().includes(q) || (u.email||'').toLowerCase().includes(q) || idStr.includes(q);
      });
      document.getElementById('staff-users-table').innerHTML = `<table class="staff-table"><thead><tr><th>User</th><th>ID</th><th>Email</th><th>Coins</th><th>Stars</th><th>Title</th><th>Status</th><th>Actions</th></tr></thead><tbody>${filtered.map(u=>{const muteTag=u.muteStatus&&u.muteStatus!=='none'?`<span style="color:#FFB347;font-size:0.7rem;">🔇${u.muteStatus}</span>`:'';const banTag=u.banStatus&&u.banStatus!=='none'?`<span style="color:#FF3D6C;font-size:0.7rem;">🚫${u.banStatus}</span>`:'';const did=(u.displayId!=null&&String(u.displayId).length===6)?String(u.displayId):'—';const action = canOpenEditor ? `<button class="staff-btn staff-btn-primary staff-btn-sm su-edit" data-id="${u.id}">${canEditAny ? 'Edit' : 'View'}</button>` : '<span style="color:var(--text-secondary);font-size:0.78rem;">No access</span>';return `<tr><td>${escapeHtml(u.username||'?')}</td><td style="font-weight:800;">${escapeHtml(did)}</td><td>${escapeHtml(u.email)}</td><td>${u.coins||0}</td><td>${u.stars||0}</td><td>${escapeHtml(u.title||'User')}</td><td>${muteTag} ${banTag}${!muteTag&&!banTag?'<span style="color:var(--neon-green);font-size:0.7rem;">✓</span>':''}</td><td>${action}</td></tr>`;}).join('')}</tbody></table>`;
      document.querySelectorAll('.su-edit').forEach(b=>b.addEventListener('click',()=>openStaffEditUser(b.dataset.id)));
    }
    function ensureStaffPasswordToolsUi() {
      const modalContent = document.querySelector('#staff-user-modal .staff-modal-content');
      if (!modalContent) return null;
      let section = document.getElementById('staff-user-password-tools');
      if (!section) {
        section = document.createElement('div');
        section.id = 'staff-user-password-tools';
        section.className = 'staff-card';
        section.style.marginTop = '12px';
        section.innerHTML = `
          <h4 style="font-size:0.9rem;margin-bottom:10px;">Account email & password (staff)</h4>
          <div class="staff-form-group" style="margin:0 0 8px;">
            <label>Email</label>
            <input id="staff-user-email-view" readonly style="opacity:0.85;">
          </div>
          <div class="staff-form-group" style="margin:0 0 8px;">
            <label>Stored site password (if visible for your role)</label>
            <input id="staff-user-password-view" readonly style="opacity:0.85;">
          </div>
          <div class="staff-form-group" style="margin:0 0 8px;">
            <label>Set new stored password</label>
            <input id="staff-user-password-set" type="text" placeholder="Enter new password">
          </div>
          <div class="staff-form-group" style="margin:0 0 8px;">
            <label>One-time login update</label>
            <input id="staff-user-password-state" readonly style="opacity:0.85;">
          </div>
          <div class="staff-form-group" style="margin:0 0 8px;">
            <label>Last password update</label>
            <input id="staff-user-password-updated" readonly style="opacity:0.85;">
          </div>
          <div class="staff-flex-row" style="gap:8px;flex-wrap:wrap;">
            <button type="button" class="staff-btn staff-btn-primary" id="staff-set-user-password-btn">Set Password</button>
            <button type="button" class="staff-btn staff-btn-primary" id="staff-force-password-reset-btn">Require password update next visit</button>
            <button type="button" class="staff-btn" id="staff-clear-password-reset-btn">Clear required update</button>
          </div>
          <div style="font-size:0.76rem;color:var(--text-secondary);margin-top:8px;">
            Password text may be hidden depending on role. Use the buttons to require or clear the player’s <strong>one-time “update your password”</strong> step when they next open the site.
          </div>
        `;
        modalContent.appendChild(section);
      }
      return section;
    }

    async function loadStaffUserIpSummaryIntoModal(uid, userData) {
      const el = document.getElementById('staff-user-ip-summary');
      if (!el) return;
      const canSeePlays = hasPermission('view_plays');
      const regIp = userData.registrationIpAddress ? String(userData.registrationIpAddress).trim() : '';
      if (!canSeePlays) {
        el.innerHTML = '<div><strong>Registration IP</strong><br><span style="font-family:ui-monospace,monospace;">'
          + escapeHtml(regIp || 'Not recorded')
          + '</span></div><p style="margin-top:8px;opacity:0.85;font-size:0.76rem;">Distinct IPs from activity require the <strong>View Play Logs &amp; IP</strong> permission.</p>';
        return;
      }
      el.textContent = 'Loading…';
      let rows = [];
      try {
        const snap = await getDocs(query(collection(db, 'plays'), where('userId', '==', uid), orderBy('timestamp', 'desc'), limit(200)));
        rows = snap.docs.map((d) => d.data());
      } catch (e) {
        console.warn('staff user ip plays query', e);
        el.innerHTML = '<span style="color:var(--neon-pink)">Could not load play IPs (Firestore index or rules).</span>';
        return;
      }
      const ts = (x) => {
        try {
          const t = x && x.timestamp && x.timestamp.toDate ? x.timestamp.toDate() : new Date(x && x.timestamp ? x.timestamp : 0);
          return t.getTime() || 0;
        } catch (_) {
          return 0;
        }
      };
      rows.sort((a, b) => ts(b) - ts(a));
      let latest = '';
      const distinct = [];
      const seen = new Set();
      for (const r of rows) {
        const ip = r && r.ipAddress != null ? String(r.ipAddress).trim() : '';
        if (!ip) continue;
        if (!latest) latest = ip;
        if (!seen.has(ip)) {
          seen.add(ip);
          distinct.push(ip);
        }
      }
      const regLine = regIp ? escapeHtml(regIp) : '<span style="opacity:0.75">Not recorded (account predates IP capture)</span>';
      const latestLine = latest ? escapeHtml(latest) : '<span style="opacity:0.75">No logged plays with an IP yet</span>';
      const distinctHtml = distinct.length
        ? '<ul style="margin:6px 0 0 18px;padding:0;">' + distinct.map((ip) => '<li style="margin:3px 0;font-family:ui-monospace,monospace;">' + escapeHtml(ip) + '</li>').join('') + '</ul>'
        : '<div style="opacity:0.75;margin-top:4px;">No distinct IPs in recent play logs.</div>';
      el.innerHTML = (
        '<div><strong>Latest (from activity)</strong><br><span style="font-family:ui-monospace,monospace;">' + latestLine + '</span></div>'
        + '<div style="margin-top:10px;"><strong>Registration</strong><br><span style="font-family:ui-monospace,monospace;">' + regLine + '</span></div>'
        + '<div style="margin-top:10px;"><strong>Distinct IPs</strong> <span style="opacity:0.75;font-weight:500;">(one row per address, newest first)</span>' + distinctHtml + '</div>'
      );
    }
    async function openStaffEditUser(uid) {
      if (!canViewUsersAdmin()) { showNotification('No permission to view users', 'error'); return; }
      const canEditProfile = canEditUserProfile();
      const canEditBalance = canEditUserBalance();
      const canEditModeration = canEditUserModeration();
      const canAssignTitles = canAssignUserTitles();
      const canAssignBadges = canAssignUserBadges();
      const canEditMedia = canEditUserMedia();
      const canEditInventory = canManageUserInventory();
      const canViewPasswords = canViewUserPasswords();
      const canSetPasswords = canSetUserPasswords();
      const canSubmit = canEditProfile || canEditBalance || canEditModeration || canAssignTitles || canEditMedia;
      const userDoc = await getDoc(doc(db,'users',uid));
      if(!userDoc.exists()) return;
      const data = userDoc.data();
      const authDoc = await getDoc(doc(db, AUTH_BRIDGE_COLLECTION, uid)).catch(() => null);
      const authData = authDoc?.exists?.() ? authDoc.data() : null;
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
      document.getElementById('staff-edit-username').disabled = !canEditProfile;
      document.getElementById('staff-edit-coins').disabled = !canEditBalance;
      document.getElementById('staff-edit-stars').disabled = !canEditBalance;
      document.getElementById('staff-edit-avatar').disabled = !canEditMedia;
      staffEditUserInitialBanner = data.banner || '';
      const bUrl = document.getElementById('staff-edit-banner-url');
      if (bUrl) bUrl.value = data.banner || '';
      if (bUrl) bUrl.disabled = !canEditMedia;
      const bFile = document.getElementById('staff-edit-banner-file');
      if (bFile) bFile.value = '';
      if (bFile) bFile.disabled = !canEditMedia;
      document.getElementById('staff-edit-mute').value = data.muteStatus || 'none';
      document.getElementById('staff-edit-ban').value = data.banStatus || 'none';
      document.getElementById('staff-edit-mod-reason').value = data.modReason || '';
      document.getElementById('staff-edit-mute').disabled = !canEditModeration;
      document.getElementById('staff-edit-ban').disabled = !canEditModeration;
      document.getElementById('staff-edit-mod-reason').disabled = !canEditModeration;
      document.getElementById('staff-mute-until-group').style.display = data.muteStatus === 'temp' ? 'block' : 'none';
      document.getElementById('staff-ban-until-group').style.display = data.banStatus === 'temp' ? 'block' : 'none';
      document.getElementById('staff-edit-mute-until').disabled = !canEditModeration;
      document.getElementById('staff-edit-ban-until').disabled = !canEditModeration;
      if (data.muteUntil) { try { document.getElementById('staff-edit-mute-until').value = new Date(data.muteUntil.toDate ? data.muteUntil.toDate() : data.muteUntil).toISOString().slice(0,16); } catch(e){} }
      if (data.banUntil) { try { document.getElementById('staff-edit-ban-until').value = new Date(data.banUntil.toDate ? data.banUntil.toDate() : data.banUntil).toISOString().slice(0,16); } catch(e){} }
      const titleSelect = document.getElementById('staff-edit-user-title');
      const titlesSnap = await getDocs(collection(db,'titles'));
      titleSelect.innerHTML = titlesSnap.docs.map(d=>{const t=d.data();return `<option value="${escapeHtml(t.name)}" ${t.name===(data.title||'User')?'selected':''}>${escapeHtml(t.name)}</option>`;}).join('');
      titleSelect.disabled = !canAssignTitles;
      const userBadges = data.badges || [];
      const badgesSnap = await getDocs(collection(db,'badges'));
      const allBadges = badgesSnap.docs.map(d=>({id:d.id,...d.data()}));
      document.getElementById('staff-user-badges-list').innerHTML = userBadges.length ? userBadges.map(bn=>{const bd=allBadges.find(b=>b.name===bn);return `<span class="user-badge" style="background:${bd?bd.bgColor:'#333'};color:${bd?bd.textColor:'#fff'};">${bd&&bd.icon?`<img src="${escapeHtml(bd.icon)}" loading="lazy" decoding="async" style="width:14px;height:14px;object-fit:contain;vertical-align:middle;">`:''} ${escapeHtml(bn)} ${canAssignBadges ? `<i class="fas fa-times sub-rm-badge" data-badge="${escapeHtml(bn)}" style="cursor:pointer;margin-left:4px;"></i>` : ''}</span>`;}).join(' ') : '<span style="color:var(--text-secondary);font-size:0.8rem;">No badges</span>';
      const addBadgeSelect = document.getElementById('staff-user-add-badge');
      addBadgeSelect.innerHTML = '<option value="">Select badge...</option>' + allBadges.filter(b=>!userBadges.includes(b.name)).map(b=>`<option value="${escapeHtml(b.name)}">${escapeHtml(b.name)}</option>`).join('');
      addBadgeSelect.disabled = !canAssignBadges;
      document.getElementById('staff-assign-badge-btn').disabled = !canAssignBadges;
      if(canEditInventory) {
        const invSnap = await getDocs(query(collection(db,'inventory'),where('userId','==',uid)));
        document.getElementById('staff-user-inventory').innerHTML = invSnap.docs.map(d=>{const it=d.data();return `<div style="background:rgba(0,0,0,0.3);border-radius:10px;padding:8px;margin-bottom:6px;display:flex;justify-content:space-between;align-items:center;"><div><strong>${escapeHtml(it.itemName)}</strong> <span class="staff-badge">${it.rarity}</span></div><button class="staff-btn staff-btn-danger staff-btn-sm sui-del" data-id="${d.id}" data-uid="${uid}">Del</button></div>`;}).join('')||'<span style="color:var(--text-secondary);font-size:0.8rem;">Empty inventory</span>';
        document.querySelectorAll('.sui-del').forEach(b=>b.addEventListener('click',async()=>{if(confirm('Delete item?')){await deleteDoc(doc(db,'inventory',b.dataset.id));openStaffEditUser(b.dataset.uid);}}));
      } else {
        document.getElementById('staff-user-inventory').innerHTML = '<span style="color:var(--text-secondary);font-size:0.8rem;">No inventory permission</span>';
      }
      document.querySelectorAll('.sub-rm-badge').forEach(b=>b.addEventListener('click',async()=>{
        if (!canAssignBadges) return;
        await updateDoc(doc(db,'users',uid),{badges:arrayRemove(b.dataset.badge)});
        openStaffEditUser(uid);
      }));
      const clearBannerBtn = document.getElementById('staff-clear-banner-btn');
      if (clearBannerBtn) clearBannerBtn.disabled = !canEditMedia;
      const updateBtn = document.getElementById('staff-update-user-btn');
      if (updateBtn) {
        updateBtn.disabled = !canSubmit;
        updateBtn.title = canSubmit ? 'Update' : 'No permission';
      }
      ensureStaffPasswordToolsUi();
      const emailViewEl = document.getElementById('staff-user-email-view');
      const passwordViewEl = document.getElementById('staff-user-password-view');
      const passwordSetEl = document.getElementById('staff-user-password-set');
      const stateEl = document.getElementById('staff-user-password-state');
      const updatedEl = document.getElementById('staff-user-password-updated');
      const setBtn = document.getElementById('staff-set-user-password-btn');
      const forceBtn = document.getElementById('staff-force-password-reset-btn');
      const clearBtn = document.getElementById('staff-clear-password-reset-btn');
      if (emailViewEl) emailViewEl.value = String(data.email || authData?.email || '').trim() || 'Unknown';
      if (passwordViewEl) {
        const storedPassword = String(data.passwordPlaintext || authData?.password?.plaintext || '').trim();
        passwordViewEl.value = canViewPasswords ? (storedPassword || '(empty)') : 'Hidden (no permission)';
      }
      if (passwordSetEl) {
        passwordSetEl.value = '';
        passwordSetEl.disabled = !canSetPasswords;
      }
      const required = data.passwordMigrationRequired === true;
      if (stateEl) stateEl.value = required ? 'Player must update password on next visit' : 'No update required';
      let changedAtText = 'Never';
      const changedAt = data.passwordChangedAt || authData?.password?.changedAt || null;
      if (changedAt) {
        try {
          const dt = changedAt?.toDate ? changedAt.toDate() : new Date(changedAt);
          changedAtText = dt.toLocaleString();
        } catch (_) {}
      }
      if (updatedEl) updatedEl.value = changedAtText;
      if (setBtn) {
        setBtn.disabled = !canSetPasswords;
        setBtn.dataset.uid = uid;
      }
      if (forceBtn) {
        forceBtn.disabled = !canSetPasswords;
        forceBtn.dataset.uid = uid;
      }
      if (clearBtn) {
        clearBtn.disabled = !canSetPasswords;
        clearBtn.dataset.uid = uid;
      }
      await loadStaffUserIpSummaryIntoModal(uid, data);
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
      const groupOrder = ['Core', 'Content', 'Moderation', 'Economy', 'Roles', 'System'];
      const grouped = ALL_PERMISSIONS.reduce((acc, perm) => {
        const key = perm.group || 'Other';
        if (!acc[key]) acc[key] = [];
        acc[key].push(perm);
        return acc;
      }, {});
      const legacyPerms = (activePerms || []).filter((perm) => !ALL_PERMISSIONS.some((known) => known.key === perm));
      container.innerHTML = groupOrder.filter((group) => grouped[group]?.length).map((group) => `
        <div class="title-perm-section">
          <div class="title-perm-section-title">${escapeHtml(group)}</div>
          <div class="title-perm-grid">
            ${grouped[group].map((perm) => `
              <label class="title-perm-item">
                <input type="checkbox" value="${perm.key}" ${activePerms.includes(perm.key) ? 'checked' : ''} ${canEditPerms ? '' : 'disabled'}>
                <span class="title-perm-text">
                  <strong>${escapeHtml(perm.label)}</strong>
                  <span class="title-perm-meta">${escapeHtml(perm.desc || '')}</span>
                </span>
              </label>
            `).join('')}
          </div>
        </div>
      `).join('') + (legacyPerms.length ? `
        <div class="title-perm-section">
          <div class="title-perm-section-title">Legacy</div>
          <div class="title-perm-grid">
            ${legacyPerms.map((perm) => `
              <label class="title-perm-item is-selected">
                <input type="checkbox" value="${escapeHtml(perm)}" checked disabled>
                <span class="title-perm-text">
                  <strong>${escapeHtml(perm)}</strong>
                  <span class="title-perm-meta">Legacy permission kept for compatibility.</span>
                </span>
              </label>
              <input type="hidden" class="legacy-title-perm" value="${escapeHtml(perm)}">
            `).join('')}
          </div>
        </div>
      ` : '');
      container.querySelectorAll('input[type="checkbox"]').forEach((checkbox) => {
        checkbox.addEventListener('change', updateTitlePermissionSelectionUi);
      });
      document.querySelectorAll('[data-perm-preset]').forEach((btn) => {
        btn.disabled = !canEditPerms;
        btn.classList.toggle('is-disabled', !canEditPerms);
      });
      updateTitlePermissionSelectionUi();
    }
    function updateTitlePermissionSelectionUi() {
      const inputs = Array.from(document.querySelectorAll('#staff-title-perms input[type="checkbox"]'));
      const checkedCount = inputs.filter((input) => input.checked).length;
      const countEl = document.getElementById('staff-title-perm-count');
      if (countEl) countEl.textContent = `${checkedCount} / ${inputs.length} selected`;
      document.querySelectorAll('#staff-title-perms .title-perm-item').forEach((item) => {
        const checkbox = item.querySelector('input[type="checkbox"]');
        item.classList.toggle('is-selected', !!checkbox?.checked);
      });
    }
    function applyTitlePermissionPreset(preset) {
      if (!hasPermission('manage_permissions')) return;
      const all = ALL_PERMISSIONS.map((perm) => perm.key);
      const byGroup = (name) => ALL_PERMISSIONS.filter((perm) => perm.group === name).map((perm) => perm.key);
      const core = ['staff_access', 'view_dashboard'];
      const presets = {
        all,
        none: [],
        moderation: [...new Set([...core, ...byGroup('Moderation')])],
        content: [...new Set([...core, ...byGroup('Content')])],
        economy: [...new Set([...core, ...byGroup('Economy')])],
      };
      const selected = new Set(presets[preset] || []);
      document.querySelectorAll('#staff-title-perms input[type="checkbox"]').forEach((input) => {
        input.checked = selected.has(input.value);
      });
      updateTitlePermissionSelectionUi();
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
      document.getElementById('staff-badges-list').innerHTML = badges.map(b=>`<div class="staff-card" style="margin-bottom:12px;display:flex;align-items:center;justify-content:space-between;"><div style="display:flex;align-items:center;gap:12px;"><span class="user-badge" style="background:${b.bgColor};color:${b.textColor};font-size:0.85rem;padding:6px 14px;">${b.icon?`<img src="${escapeHtml(b.icon)}" loading="lazy" decoding="async" style="width:18px;height:18px;object-fit:contain;vertical-align:middle;">`:''} ${escapeHtml(b.name)}</span><span style="font-size:0.8rem;color:var(--text-secondary);">${escapeHtml(b.description||'')}</span></div><div><button class="staff-btn staff-btn-primary staff-btn-sm sb-edit" data-id="${b.id}">Edit</button> <button class="staff-btn staff-btn-danger staff-btn-sm sb-del" data-id="${b.id}">Del</button></div></div>`).join('')||'<p style="color:var(--text-secondary);">No badges created yet.</p>';
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
      if (!canViewMissionsAdmin()) {
        container.innerHTML = '<p style="color:var(--neon-pink);">No permission.</p>';
        return;
      }
      const canEdit = canEditMission();
      const canDelete = canDeleteMission();
      try {
        const snap = await getDocs(query(collection(db,'missions'),orderBy('createdAt','desc')));
        const missions = snap.docs.map(d=>({id:d.id,...d.data()}));
        container.innerHTML = missions.length ? missions.map(m=>`<div class="staff-card" style="margin-bottom:10px;"><div class="staff-card-header"><h3 style="font-size:0.9rem;">${escapeHtml(m.title)}</h3><div>${canEdit ? `<button class="staff-btn staff-btn-primary staff-btn-sm sm-edit" data-id="${m.id}">Edit</button>` : ''} ${canDelete ? `<button class="staff-btn staff-btn-danger staff-btn-sm sm-del" data-id="${m.id}">Delete</button>` : ''}${!canEdit && !canDelete ? '<span style="color:var(--text-secondary);font-size:0.78rem;">Read only</span>' : ''}</div></div><div style="font-size:0.8rem;color:var(--text-secondary);">${escapeHtml(m.description||'')} | Type: ${m.type} | Target: ${m.target} | Reward: ${m.rewardCoins||0} coins, ${m.rewardStars||0} stars</div></div>`).join('') : '<p style="color:var(--text-secondary);">No missions.</p>';
        container.querySelectorAll('.sm-edit').forEach(b=>b.addEventListener('click',async()=>{
          if (!canEditMission()) { showNotification('No permission to edit missions', 'error'); return; }
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
        container.querySelectorAll('.sm-del').forEach(b=>b.addEventListener('click',async()=>{if(!canDeleteMission()){showNotification('No permission to delete missions','error');return;}if(confirm('Delete mission?')){await deleteDoc(doc(db,'missions',b.dataset.id));loadStaffMissions();}}));
      } catch(e) { container.innerHTML = '<p style="color:var(--neon-pink);">Error loading missions.</p>'; }
    }

    /**
     * Resolve a user uid from an email for staff tools — matches Firebase-style
     * normalization (case-insensitive) and falls back to authUsers bridge doc id (= uid).
     */
    async function findUserUidByEmailLookup(emailRaw) {
      const q = String(emailRaw || '').trim();
      if (!q || !q.includes('@')) return null;
      const lower = q.toLowerCase();
      const tryQueries = [
        () => getDocs(query(collection(db, 'users'), where('emailLower', '==', lower), limit(1))),
        () => getDocs(query(collection(db, 'users'), where('email', '==', q), limit(1))),
        () => getDocs(query(collection(db, 'users'), where('email', '==', lower), limit(1))),
        () => getDocs(query(collection(db, AUTH_BRIDGE_COLLECTION), where('emailLower', '==', lower), limit(1)))
      ];
      for (const run of tryQueries) {
        try {
          const s = await run();
          if (!s.empty) return s.docs[0].id;
        } catch (e) {
          console.warn('findUserUidByEmailLookup:', e);
        }
      }
      return null;
    }

    async function findUserByStaffLookup(raw) {
      const q = raw.trim();
      if (!q) return null;
      if (/^\d{6}$/.test(q)) {
        const s = await getDocs(query(collection(db,'users'),where('displayId','==',q)));
        if (!s.empty) return s.docs[0].id;
      }
      if (q.includes('@')) {
        const uid = await findUserUidByEmailLookup(q);
        if (uid) return uid;
      }
      const all = await getDocs(collection(db,'users'));
      const low = q.toLowerCase();
      let m = all.docs.find(d => (d.data().username||'').toLowerCase() === low);
      if (!m) m = all.docs.find(d => (d.data().username||'').toLowerCase().includes(low));
      return m ? m.id : null;
    }

    // ========== Staff Panel Event Listeners ==========
    function setupStaffEventListeners() {
      ensureStaffPasswordToolsUi();
      // Games
      document.getElementById('staff-add-game-btn')?.addEventListener('click',()=>{if(!canCreateGame()){showNotification('No permission to create games','error');return;}document.getElementById('staff-game-id').value='';document.getElementById('staff-game-form').reset();document.getElementById('staff-game-modal-title').textContent='Add Game';document.getElementById('staff-game-modal').style.display='flex';});
      document.getElementById('staff-close-game-modal')?.addEventListener('click',()=>document.getElementById('staff-game-modal').style.display='none');
      document.getElementById('staff-game-form')?.addEventListener('submit',async(e)=>{
        e.preventDefault();
        const id=document.getElementById('staff-game-id').value;
        if (id && !canEditGame()) { showNotification('No permission to edit games','error'); return; }
        if (!id && !canCreateGame()) { showNotification('No permission to create games','error'); return; }
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
        const existingGame = id ? staffGamesCache.find((g) => String(g.id) === String(id)) : null;
        const fallbackGameKey = nextNumericKey(staffGamesCache, (g) => g.gameKey, MAX_GAME_KEY_ID, 1);
        const gameKey = existingGame?.gameKey ?? (fallbackGameKey !== null ? fallbackGameKey : stableNumericKey(id || document.getElementById('staff-game-title').value, MAX_GAME_KEY_ID));
        const gameData={title:document.getElementById('staff-game-title').value,description:document.getElementById('staff-game-desc').value,image:imageUrl,url:document.getElementById('staff-game-url').value,rating:parseFloat(document.getElementById('staff-game-rating').value),multiplayer:document.getElementById('staff-game-multi').value==='true',tags:tagsArr,gameKey,updatedAt:serverTimestamp()};
        if(id) await updateDoc(doc(db,'games',id),gameData); else await addDoc(collection(db,'games'),{...gameData,createdAt:serverTimestamp()});
        document.getElementById('staff-game-modal').style.display='none';
        loadStaffGames();loadStaffDashboard();
        showNotification('Game saved!','success');
      });
      document.getElementById('staff-search-game')?.addEventListener('input',e=>{const t=e.target.value.toLowerCase();renderStaffGames(staffGamesCache.filter(g=>g.title.toLowerCase().includes(t)));});

      // Movies
      document.getElementById('staff-save-movie-category-btn')?.addEventListener('click', async () => {
        const id = document.getElementById('staff-movie-category-id')?.value || '';
        if (id && !canEditMovieCategory()) { showNotification('No permission to edit movie categories', 'error'); return; }
        if (!id && !canCreateMovieCategory()) { showNotification('No permission to create movie categories', 'error'); return; }
        const key = document.getElementById('staff-movie-category-name')?.value.trim() || '';
        const gradient = document.getElementById('staff-movie-category-gradient')?.value.trim() || 'linear-gradient(135deg, #2f5ca3 0%, #1a2e58 100%)';
        const art = document.getElementById('staff-movie-category-art')?.value.trim() || '';
        const artPosition = document.getElementById('staff-movie-category-art-pos')?.value === 'middle' ? 'middle' : 'bottom';
        const artScaleRaw = parseInt(document.getElementById('staff-movie-category-art-scale')?.value, 10);
        const artScale = [50, 75, 100, 125, 150].includes(artScaleRaw) ? artScaleRaw : 100;
        const order = parseInt(document.getElementById('staff-movie-category-order')?.value, 10) || 0;
        const existing = id ? staffMovieCategoriesCache.find((cat) => String(cat.id) === String(id)) : null;
        const nextCategoryId = nextNumericKey(staffMovieCategoriesCache, (cat) => cat.categoryId, MAX_MOVIE_CATEGORY_ID, 0);
        const categoryId = existing?.categoryId ?? (nextCategoryId !== null ? nextCategoryId : stableNumericKey(key || id, MAX_MOVIE_CATEGORY_ID));
        if (!key) { showNotification('Category name is required', 'error'); return; }
        const payload = { key, name: key, categoryId, gradient, art, artPosition, artScale, order, updatedAt: serverTimestamp() };
        try {
          if (id) await updateDoc(doc(db, 'movieCategories', id), payload);
          else await addDoc(collection(db, 'movieCategories'), { ...payload, createdAt: serverTimestamp() });
          await loadStaffMovieCategories();
          await refreshMoviesPageFromAdmin();
          loadStaffDashboard();
          resetStaffMovieCategoryForm();
          showNotification('Movie category saved', 'success');
        } catch (e) { showNotification('Save failed: ' + e.message, 'error'); }
      });
      document.getElementById('staff-reset-movie-category-btn')?.addEventListener('click', resetStaffMovieCategoryForm);
      document.getElementById('staff-save-movie-btn')?.addEventListener('click', async () => {
        const id = document.getElementById('staff-movie-id')?.value || '';
        if (id && !canEditMovie()) { showNotification('No permission to edit movies', 'error'); return; }
        if (!id && !canCreateMovie()) { showNotification('No permission to create movies', 'error'); return; }
        const title = document.getElementById('staff-movie-title')?.value.trim() || '';
        const category = document.getElementById('staff-movie-category')?.value || staffMovieCategoriesCache[0]?.key || '';
        const releaseYear = parseInt(document.getElementById('staff-movie-year')?.value, 10) || new Date().getFullYear();
        const scoreRaw = parseFloat(document.getElementById('staff-movie-score')?.value);
        const score = Number.isFinite(scoreRaw) ? Math.max(0, Math.min(10, Number(scoreRaw.toFixed(1)))) : 0;
        const banner = document.getElementById('staff-movie-banner')?.value.trim() || '';
        const titleImage = document.getElementById('staff-movie-title-image')?.value.trim() || '';
        const description = document.getElementById('staff-movie-desc')?.value.trim() || '';
        const url = document.getElementById('staff-movie-url')?.value.trim() || '';
        const trailerUrl = document.getElementById('staff-movie-trailer')?.value.trim() || '';
        const categoryCfg = staffMovieCategoriesCache.find((cat) => String(cat.key) === String(category));
        const existingMovie = id ? staffMoviesCache.find((movie) => String(movie.id) === String(id)) : null;
        const nextMovieKey = nextNumericKey(staffMoviesCache, (movie) => movie.movieKey, MAX_MOVIE_KEY_ID, 1);
        const movieKey = existingMovie?.movieKey ?? (nextMovieKey !== null ? nextMovieKey : stableNumericKey(id || title, MAX_MOVIE_KEY_ID));
        const categoryId = toBoundedPositiveInt(categoryCfg?.categoryId, MAX_MOVIE_CATEGORY_ID);
        if (!title) { showNotification('Movie title is required', 'error'); return; }
        if (!category) { showNotification('Create/select a movie category first', 'error'); return; }
        const payload = { title, category, categoryId: categoryId !== null ? categoryId : 0, movieKey, releaseYear, score, banner, titleImage, description, url, trailerUrl, updatedAt: serverTimestamp() };
        try {
          if (id) await updateDoc(doc(db, 'movies', id), payload);
          else await addDoc(collection(db, 'movies'), { ...payload, createdAt: serverTimestamp() });
          await loadStaffMovies();
          await refreshMoviesPageFromAdmin();
          loadStaffDashboard();
          resetStaffMovieForm();
          showNotification('Movie saved', 'success');
        } catch (e) { showNotification('Save failed: ' + e.message, 'error'); }
      });
      document.getElementById('staff-reset-movie-btn')?.addEventListener('click', resetStaffMovieForm);
      document.getElementById('staff-search-movie')?.addEventListener('input', (e) => {
        renderStaffMovies(staffMoviesCache, e.target.value);
      });

      // Tags
      document.getElementById('staff-add-tag-btn')?.addEventListener('click',()=>{if(!canCreateTag()){showNotification('No permission to create tags','error');return;}document.getElementById('staff-tag-modal').style.display='flex';});
      document.getElementById('staff-close-tag-modal')?.addEventListener('click',()=>document.getElementById('staff-tag-modal').style.display='none');
      document.getElementById('staff-save-tag-btn')?.addEventListener('click',async()=>{if(!canCreateTag()){showNotification('No permission to create tags','error');return;}const n=document.getElementById('staff-new-tag-name').value.trim();if(n){await addDoc(collection(db,'tags'),{name:n});document.getElementById('staff-tag-modal').style.display='none';document.getElementById('staff-new-tag-name').value='';loadStaffTags();showNotification('Tag created!','success');}});

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
        if (!canEditRarityOrder()) { showNotification('No permission to edit rarity order', 'error'); return; }
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
        if (!canEditRarityOrder()) { showNotification('No permission to edit rarity order', 'error'); return; }
        const v = document.getElementById('staff-rarity-order-input').value.trim();
        await setDoc(doc(db,'siteConfig','settings'), { blookRarityOrder: v, updatedAt: serverTimestamp() }, { merge: true });
        await refreshRarityOrderFromServer();
        renderPackBuilderItems();
        if (inventoryItems.length) renderInventory();
        document.getElementById('staff-rarity-order-modal').style.display='none';
        showNotification('Rarity order saved','success');
      });

      document.getElementById('staff-daily-reward-save')?.addEventListener('click', async () => {
        if (!canEditDailyReward()) { showNotification('No permission to edit daily reward', 'error'); return; }
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
      document.getElementById('staff-rarity-def-add')?.addEventListener('click', () => {
        if (!canEditRarityStyles()) { showNotification('No permission to edit rarity styles', 'error'); return; }
        openStaffRarityDefModal('add');
      });
      document.getElementById('staff-rarity-def-gradient')?.addEventListener('change', toggleStaffRarityGradUI);
      document.getElementById('staff-rarity-def-save')?.addEventListener('click', () => {
        if (!canEditRarityStyles()) { showNotification('No permission to edit rarity styles', 'error'); return; }
        saveStaffRarityDef();
      });
      document.getElementById('staff-rarity-def-cancel')?.addEventListener('click', () => { document.getElementById('staff-rarity-def-modal').style.display = 'none'; });
      document.getElementById('staff-rarity-def-delete')?.addEventListener('click', async () => {
        if (!canEditRarityStyles()) { showNotification('No permission to edit rarity styles', 'error'); return; }
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
        if (!uid) return;
        const updates={};
        if (canEditUserProfile()) updates.username=document.getElementById('staff-edit-username').value;
        if (canEditUserBalance()) {
          updates.coins=parseInt(document.getElementById('staff-edit-coins').value);
          updates.stars=parseInt(document.getElementById('staff-edit-stars').value);
        }
        if(canAssignUserTitles()) updates.title=document.getElementById('staff-edit-user-title').value;
        const avatarVal=document.getElementById('staff-edit-avatar').value.trim();
        if(avatarVal && canEditUserMedia()) updates.avatar=avatarVal;
        const bannerUrlField = document.getElementById('staff-edit-banner-url')?.value.trim() || '';
        const bannerFile = document.getElementById('staff-edit-banner-file')?.files?.[0];
        if (canEditUserMedia()) {
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
        }
        if (canEditUserModeration()) {
          updates.muteStatus=document.getElementById('staff-edit-mute').value;
          updates.banStatus=document.getElementById('staff-edit-ban').value;
          updates.modReason=document.getElementById('staff-edit-mod-reason').value;
          if(updates.muteStatus==='temp'){const v=document.getElementById('staff-edit-mute-until').value;if(v) updates.muteUntil=new Date(v);else updates.muteUntil=null;} else {updates.muteUntil=null;}
          if(updates.banStatus==='temp'){const v=document.getElementById('staff-edit-ban-until').value;if(v) updates.banUntil=new Date(v);else updates.banUntil=null;} else {updates.banUntil=null;}
        }
        if (!Object.keys(updates).length) { showNotification('No editable fields for your permission level','error'); return; }
        await updateDoc(doc(db,'users',uid),updates);
        if (typeof updates.stars === 'number' && Number.isFinite(updates.stars)) syncStarBadgesForUser(uid, updates.stars);
        document.getElementById('staff-user-modal').style.display='none';
        loadStaffUsers();
        showNotification('User updated!','success');
      });
      document.getElementById('staff-set-user-password-btn')?.addEventListener('click', async () => {
        if (!canSetUserPasswords()) { showNotification('No permission to set user passwords', 'error'); return; }
        const uid = document.getElementById('staff-set-user-password-btn')?.dataset?.uid || document.getElementById('staff-edit-user-id')?.value || '';
        const nextPassword = String(document.getElementById('staff-user-password-set')?.value || '').trim();
        if (!uid) return;
        if (nextPassword.length < 6) { showNotification('Password must be at least 6 characters', 'error'); return; }
        const userPatch = {
          passwordPlaintext: nextPassword,
          passwordMigrationRequired: false,
          passwordMigrationVersion: PASSWORD_MIGRATION_VERSION,
          passwordChangedBy: currentUser?.uid || 'staff',
          passwordChangedAt: serverTimestamp(),
          passwordChangedMethod: 'staff_set'
        };
        await updateDoc(doc(db, 'users', uid), userPatch);
        await setDoc(doc(db, AUTH_BRIDGE_COLLECTION, uid), {
          password: {
            required: false,
            version: PASSWORD_MIGRATION_VERSION,
            plaintext: nextPassword,
            changedAt: serverTimestamp(),
            changedBy: currentUser?.uid || 'staff'
          },
          updatedAt: serverTimestamp()
        }, { merge: true });
        if (currentUser && String(currentUser.uid) === String(uid)) {
          try {
            await updatePassword(currentUser, nextPassword);
          } catch (error) {
            showNotification('Saved on profile, but the sign-in password could not be updated: ' + (error?.message || 'Unknown error'), 'error');
          }
        }
        await openStaffEditUser(uid);
        showNotification('Password set successfully', 'success');
      });
      document.getElementById('staff-force-password-reset-btn')?.addEventListener('click', async () => {
        if (!canSetUserPasswords()) { showNotification('No permission to manage user passwords', 'error'); return; }
        const uid = document.getElementById('staff-force-password-reset-btn')?.dataset?.uid || document.getElementById('staff-edit-user-id')?.value || '';
        if (!uid) return;
        await updateDoc(doc(db, 'users', uid), {
          passwordMigrationRequired: true,
          passwordMigrationVersion: PASSWORD_MIGRATION_VERSION,
          passwordMigrationRequestedAt: serverTimestamp(),
          passwordMigrationRequestedBy: currentUser?.uid || 'staff'
        });
        await setDoc(doc(db, AUTH_BRIDGE_COLLECTION, uid), {
          password: { required: true, version: PASSWORD_MIGRATION_VERSION, flaggedAt: serverTimestamp(), flaggedBy: currentUser?.uid || 'staff' },
          updatedAt: serverTimestamp()
        }, { merge: true });
        await openStaffEditUser(uid);
        showNotification('This player will be asked to update their password the next time they open the site.', 'success');
      });
      document.getElementById('staff-clear-password-reset-btn')?.addEventListener('click', async () => {
        if (!canSetUserPasswords()) { showNotification('No permission to manage user passwords', 'error'); return; }
        const uid = document.getElementById('staff-clear-password-reset-btn')?.dataset?.uid || document.getElementById('staff-edit-user-id')?.value || '';
        if (!uid) return;
        await updateDoc(doc(db, 'users', uid), {
          passwordMigrationRequired: false,
          passwordMigrationVersion: PASSWORD_MIGRATION_VERSION,
          passwordChangedBy: 'staff_override',
          passwordChangedAt: serverTimestamp()
        });
        await setDoc(doc(db, AUTH_BRIDGE_COLLECTION, uid), {
          password: { required: false, version: PASSWORD_MIGRATION_VERSION, changedAt: serverTimestamp(), changedBy: currentUser?.uid || 'staff' },
          updatedAt: serverTimestamp()
        }, { merge: true });
        await openStaffEditUser(uid);
        showNotification('This player no longer needs the one-time password update.', 'success');
      });
      document.getElementById('staff-assign-badge-btn')?.addEventListener('click',async()=>{
        if (!canAssignUserBadges()) { showNotification('No permission to assign badges','error'); return; }
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
      document.querySelectorAll('[data-perm-preset]').forEach((btn) => {
        btn.addEventListener('click', () => applyTitlePermissionPreset(btn.dataset.permPreset || 'none'));
      });
      document.getElementById('staff-save-title-btn')?.addEventListener('click',async()=>{
        const id=document.getElementById('staff-title-id').value;
        const isGradient=document.getElementById('staff-title-gradient').value==='true';
        if(isGradient && !hasPermission('create_gradient_titles')){showNotification('No permission to create gradient titles','error');return;}
        const gradientColors=document.getElementById('staff-title-gradient-colors').value.split(',').map(c=>c.trim()).filter(c=>c);
        const perms=Array.from(new Set([
          ...Array.from(document.querySelectorAll('#staff-title-perms input[type="checkbox"]:checked')).map(i=>i.value),
          ...Array.from(document.querySelectorAll('#staff-title-perms .legacy-title-perm')).map(i=>i.value)
        ]));
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
      document.getElementById('staff-quick-add-game')?.addEventListener('click',()=>{
        if (!canCreateGame()) { showNotification('No permission to create games', 'error'); return; }
        switchStaffSection('staff-games');
        document.getElementById('staff-add-game-btn')?.click();
      });
      document.getElementById('staff-quick-add-movie')?.addEventListener('click',()=>{
        if (!canCreateMovie()) { showNotification('No permission to create movies', 'error'); return; }
        switchStaffSection('staff-movies');
        document.getElementById('staff-movie-title')?.focus();
      });
      document.getElementById('staff-quick-add-pack')?.addEventListener('click',()=>{
        if (!hasPermission('manage_packs')) { showNotification('No permission to manage packs', 'error'); return; }
        switchStaffSection('staff-packs');
        document.getElementById('staff-add-pack-btn')?.click();
      });
      document.getElementById('staff-quick-add-title')?.addEventListener('click',()=>{
        if (!hasPermission('manage_titles')) { showNotification('No permission to manage titles', 'error'); return; }
        switchStaffSection('staff-titles');
        document.getElementById('staff-add-title-btn')?.click();
      });
      document.getElementById('staff-quick-add-mission')?.addEventListener('click',()=>{
        if (!canCreateMission()) { showNotification('No permission to create missions', 'error'); return; }
        switchStaffSection('staff-missions');
        document.getElementById('staff-add-mission-btn')?.click();
      });

      // Missions
      document.getElementById('staff-add-mission-btn')?.addEventListener('click', () => {
        if (!canCreateMission()) { showNotification('No permission to create missions', 'error'); return; }
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
        if (mid && !canEditMission()) { showNotification('No permission to edit missions', 'error'); return; }
        if (!mid && !canCreateMission()) { showNotification('No permission to create missions', 'error'); return; }
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
        if (!uid) { showNotification('User not found — try 6-digit ID, email (any case), or username','error'); return; }
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
        if (!canAdjustStars()) { showNotification('No permission to adjust stars', 'error'); return; }
        const email = document.getElementById('staff-stars-email').value.trim();
        const action = document.getElementById('staff-stars-action').value;
        const amount = parseInt(document.getElementById('staff-stars-amount').value) || 0;
        if (!email) { showNotification('Enter email','error'); return; }
        const uid = await findUserUidByEmailLookup(email);
        if (!uid) { showNotification('User not found','error'); return; }
        const userSnap = await getDoc(doc(db, 'users', uid));
        if (!userSnap.exists()) { showNotification('User not found','error'); return; }
        const ud = userSnap.data() || {};
        const current = ud.stars || 0;
        let newVal = current;
        if (action === 'set') newVal = amount;
        else if (action === 'add') newVal = current + amount;
        else if (action === 'remove') newVal = Math.max(0, current - amount);
        await updateDoc(doc(db,'users',uid),{stars:newVal});
        syncStarBadgesForUser(uid, newVal);
        document.getElementById('staff-stars-result').innerHTML = `<span style="color:var(--neon-green);">✓ ${ud.username || 'user'}: ${current} → ${newVal} stars</span>`;
        showNotification('Stars updated!','success');
      });

      document.getElementById('staff-star-badge-add-row')?.addEventListener('click', () => {
        if (!canEditStarBadgeRules()) { showNotification('No permission to edit star badge rules', 'error'); return; }
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
        if (!canEditStarBadgeRules()) { showNotification('No permission to edit star badge rules', 'error'); return; }
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
        if (!canResyncStarBadgeRules()) { showNotification('No permission to re-apply star badge rules', 'error'); return; }
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
    setupStaffEventListeners();

    // ========== Initialize the app ==========
    async function init() {
      applyFixedSiteTheme();
      const pageId = getCurrentPageId();
      if (pageId === 'home' || pageId === 'contact' || pageId === 'main-page') {
        applyMainShellLayout(pageId);
      }
      hidePageLoading();
      try {
        await refreshRarityOrderFromServer();
        await loadActivePageContent(pageId);
      } catch (err) {
        console.error('Game Universe init failed:', err);
      }
    }

    init().catch(() => hidePageLoading());
    setTimeout(hidePageLoading, 12000);
    // ========== Event Listeners ==========
    document.addEventListener('fullscreenchange', syncGameFullscreenUi);
    gameFullscreenBtn?.addEventListener('click', () => requestGameFullscreen());
    gameExitFullscreenBtn?.addEventListener('click', () => exitGameFullscreen());
    gameFrame?.addEventListener('load', () => {
      const allowedHost = normalizeHost(gameFrame.dataset.allowedHost);
      if (!allowedHost) return;
      let loadedHost = '';
      try {
        loadedHost = normalizeHost(new URL(gameFrame.contentWindow.location.href).hostname);
      } catch (_) {
        try {
          loadedHost = normalizeHost(new URL(gameFrame.src, window.location.href).hostname);
        } catch (_) {
          loadedHost = '';
        }
      }
      if (!loadedHost || loadedHost === allowedHost) return;
      const systemOrdered = gameFrame.dataset.systemOrdered === '1';
      if (systemOrdered && (BASE_FRAME_ALLOWED_HOSTS.has(loadedHost) || sessionFrameAllowedHosts.has(loadedHost))) return;
      closeGameModal();
      showNotification('External redirect inside frame was blocked.', 'error');
    });

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
    invPanelSellBtn?.addEventListener('click', () => sellOneInvBlookFromDetail());
    invPanelSendBtn?.addEventListener('click', () => openInvSendFromDetail());
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
    });

    // Close modals with Escape key
    document.addEventListener('keydown', (e) => {
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
    });

    // Close game modal
    movieInfoWatchBtn?.addEventListener('click', () => beginMoviePlayback(activeMoviePreview));
    movieInfoBackBtn?.addEventListener('click', closeMovieInfoModal);
    movieInfoCloseBtn?.addEventListener('click', closeMovieInfoModal);
    document.querySelector('#gameModal .close')?.addEventListener('click', closeGameModal);
