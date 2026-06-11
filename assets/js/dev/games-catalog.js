import { getFirestoreDb } from './firebase-config.js';
import { bindFormToggle } from './access-gate.js';
import { nextNumericKey, showDevToast } from './catalog-utils.js';

const MAX_GAME_KEY_ID = 999999;

export function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function toBoundedPositiveInt(raw, max) {
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n) || n < 0 || n > max) return null;
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

function firstNonEmptyString(...values) {
  for (const value of values) {
    const text = String(value ?? '').trim();
    if (text) return text;
  }
  return '';
}

function toNonNegativeNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : 0;
}

export function normalizeGameDoc(raw, fallbackId, indexHint = 0) {
  const fallbackKey = (indexHint + 1) <= MAX_GAME_KEY_ID
    ? (indexHint + 1)
    : stableNumericKey(fallbackId || raw?.title || '', MAX_GAME_KEY_ID);
  const gameKey = toBoundedPositiveInt(raw.gameKey ?? raw.gameIdKey ?? raw.key, MAX_GAME_KEY_ID);
  return {
    id: raw.id || fallbackId,
    gameKey: gameKey !== null ? gameKey : fallbackKey,
    title: String(raw.title || raw.name || 'Untitled Game'),
    description: String(raw.description || raw.desc || ''),
    image: firstNonEmptyString(
      raw.image,
      raw.banner,
      raw.gameBanner,
      raw.imageUrl,
      raw.bannerUrl,
      raw.coverImage,
      raw.cover,
      raw.thumbnail,
      raw.thumb,
      raw.poster
    ),
    url: String(raw.url || raw.gameUrl || ''),
    rating: Number.parseFloat(raw.rating ?? 3) || 3,
    playCount: toNonNegativeNumber(raw.playCount ?? raw.totalPlays ?? raw.plays ?? raw.timesPlayed ?? raw.launchCount),
    multiplayer: Boolean(raw.multiplayer),
    tags: (() => {
      if (Array.isArray(raw.tags)) return raw.tags.map((t) => String(t).trim()).filter(Boolean);
      if (typeof raw.tags === 'string') return raw.tags.split(',').map((t) => t.trim()).filter(Boolean);
      return [];
    })()
  };
}

async function loadAllTimePlayCounts(fs, db) {
  const countsById = new Map();
  const countsByKey = new Map();

  try {
    const playsSnap = await fs.getDocs(fs.collection(db, 'plays'));
    playsSnap.forEach((d) => {
      const data = d.data() || {};
      const entryType = String(data.entryType || data.itemType || 'game').toLowerCase();
      if (entryType && entryType !== 'game') return;

      const gameId = String(data.gameId || '').trim();
      if (gameId) countsById.set(gameId, (countsById.get(gameId) || 0) + 1);

      const gameKey = toBoundedPositiveInt(data.gameKey, MAX_GAME_KEY_ID);
      if (gameKey !== null) countsByKey.set(gameKey, (countsByKey.get(gameKey) || 0) + 1);
    });
  } catch (e) {
    console.warn('Could not load play counts:', e);
  }

  return { countsById, countsByKey };
}

function resolveTagLabel(raw, tagById, tagByNameLower) {
  const token = String(raw || '').trim();
  if (!token) return null;
  if (tagById.has(token)) return tagById.get(token);
  return tagByNameLower.get(token.toLowerCase()) || token;
}

export async function loadGamesCatalog() {
  const { db, fs } = await getFirestoreDb();
  const tagById = new Map();
  const tagByNameLower = new Map();
  const tagGroups = new Map();
  const tagOptions = [];

  try {
    const tagSnap = await fs.getDocs(fs.collection(db, 'tags'));
    tagSnap.docs.forEach((d) => {
      const name = String(d.data().name || '').trim();
      if (!name) return;
      tagById.set(d.id, name);
      tagByNameLower.set(name.toLowerCase(), name);
      tagGroups.set(name.toLowerCase(), { key: name, games: [] });
      tagOptions.push(name);
    });
  } catch (e) {
    console.warn('Could not load tags:', e);
  }

  const gamesSnap = await fs.getDocs(fs.collection(db, 'games'));
  const playCounts = await loadAllTimePlayCounts(fs, db);
  const games = gamesSnap.docs
    .map((d, i) => {
      const game = normalizeGameDoc({ id: d.id, ...d.data() }, d.id, i);
      game.playCount = Math.max(
        game.playCount,
        playCounts.countsById.get(String(game.id)) || 0,
        playCounts.countsByKey.get(game.gameKey) || 0
      );
      return game;
    })
    .sort((a, b) => a.title.localeCompare(b.title));

  games.forEach((game) => {
    (game.tags || []).forEach((rawTag) => {
      const label = resolveTagLabel(rawTag, tagById, tagByNameLower);
      if (!label) return;
      const key = label.toLowerCase();
      let group = tagGroups.get(key);
      if (!group) {
        group = { key: label, games: [] };
        tagGroups.set(key, group);
      }
      if (!group.games.some((g) => g.id === game.id)) group.games.push(game);
    });
  });

  const categories = [{ key: '__all__', label: 'All games', count: games.length }]
    .concat(
      [...tagGroups.values()]
        .sort((a, b) => a.key.localeCompare(b.key))
        .map(({ key, games: groupGames }) => ({
          key: key.toLowerCase(),
          label: key,
          count: groupGames.length
        }))
    );

  const gamesByCategory = { __all__: games };
  tagGroups.forEach((group, key) => {
    gamesByCategory[key] = group.games.sort(popularGameComparator);
  });

  return { categories, gamesByCategory, games, tagOptions: tagOptions.sort((a, b) => a.localeCompare(b)) };
}

function popularGameComparator(a, b) {
  return (b.playCount || 0) - (a.playCount || 0) || a.title.localeCompare(b.title);
}

function domIdFromCategoryKey(key) {
  return String(key || 'category').toLowerCase().replace(/[^a-z0-9_-]+/g, '-').replace(/^-+|-+$/g, '') || 'category';
}

function largeTileIndexForCategory(catKey, gameCount) {
  if (gameCount <= 1) return 0;
  if (gameCount === 2) return 1;
  const hash = stableNumericKey(catKey, MAX_GAME_KEY_ID);
  const index = hash % gameCount;
  return index === 0 ? Math.min(gameCount - 1, Math.max(1, Math.floor(gameCount / 2))) : index;
}

function arrangeCategoryGames(catKey, games) {
  const sorted = [...games].sort(popularGameComparator);
  if (sorted.length <= 1) return sorted.map((game) => ({ game, large: true }));

  const topGame = sorted.shift();
  const insertAt = largeTileIndexForCategory(catKey, sorted.length + 1);
  sorted.splice(insertAt, 0, topGame);
  return sorted.map((game) => ({ game, large: game.id === topGame.id }));
}

async function addGameToFirestore(formData, existingGames) {
  const { db, fs } = await getFirestoreDb();
  const title = String(formData.get('title') || '').trim();
  const image = String(formData.get('image') || '').trim();
  const url = String(formData.get('url') || '').trim();
  const rating = Number.parseFloat(formData.get('rating'));
  const multiplayer = formData.get('multiplayer') === 'true';
  const tags = String(formData.get('tags') || '')
    .split(',')
    .map((t) => t.trim())
    .filter(Boolean);

  if (!title) throw new Error('Title is required.');
  if (!image) throw new Error('Cover image URL is required.');
  if (!url) throw new Error('Game URL is required.');

  const fallbackGameKey = nextNumericKey(existingGames, (g) => g.gameKey, MAX_GAME_KEY_ID, 1);
  const gameKey = fallbackGameKey !== null
    ? fallbackGameKey
    : stableNumericKey(title, MAX_GAME_KEY_ID);

  await fs.addDoc(fs.collection(db, 'games'), {
    title,
    description: '',
    image,
    url,
    rating: Number.isFinite(rating) ? rating : 3,
    multiplayer,
    tags,
    gameKey,
    createdAt: fs.serverTimestamp(),
    updatedAt: fs.serverTimestamp()
  });
}

function updateHeroStats(root, data) {
  const totalEl = root.querySelector('#devTotalCount');
  const catEl = root.querySelector('#devCategoryCount');
  if (totalEl) totalEl.textContent = String(data.games.length);
  if (catEl) catEl.textContent = String(Math.max(0, data.categories.length - 1));
}

function populateTagDatalist(root, tagOptions) {
  const list = root.querySelector('#devTagSuggestions');
  if (!list) return;
  list.innerHTML = tagOptions.map((tag) => `<option value="${escapeHtml(tag)}"></option>`).join('');
}

function renderFeaturedRow(root, games, openGame) {
  const track = root.querySelector('#gamesFeaturedTrack');
  if (!track) return;
  const featured = [...games]
    .sort(popularGameComparator)
    .slice(0, 10);
  if (!featured.length) {
    track.innerHTML = '<p class="dev-status" style="padding:12px 0;">No featured games yet.</p>';
    return;
  }
  track.innerHTML = featured.map((game) => {
    const bannerImage = game.image
      ? `<img src="${escapeHtml(game.image)}" alt="" loading="lazy">`
      : '';
    return `
      <article class="games-featured-card" data-featured-id="${escapeHtml(game.id)}" tabindex="0">
        <div class="games-featured-banner">${bannerImage}</div>
        <div class="games-featured-body">
          <h3 class="games-featured-title">${escapeHtml(game.title)}</h3>
        </div>
      </article>
    `;
  }).join('');
  track.querySelectorAll('[data-featured-id]').forEach((card) => {
    const play = () => {
      const game = featured.find((g) => String(g.id) === String(card.dataset.featuredId));
      if (game) openGame(game);
    };
    card.addEventListener('click', play);
    card.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        play();
      }
    });
  });
}

export function mountGamesCatalog(root) {
  if (root.__devGamesCatalogMounted) return;
  root.__devGamesCatalogMounted = true;

  const statusEl = root.querySelector('#devStatus');
  const catsEl = root.querySelector('#devCats');
  const gridEl = root.querySelector('#devGrid');
  const qEl = root.querySelector('#devSearch');
  const modalEl = root.querySelector('#devGameModal');
  const modalTitleEl = root.querySelector('#devGameModalTitle');
  const modalFrameEl = root.querySelector('#devGameModalFrame');
  const modalCloseEl = root.querySelector('#devGameModalClose');
  const addForm = root.querySelector('#devAddGameForm');
  const formMsg = root.querySelector('#devFormMsg');
  const catalogueTitleEl = root.querySelector('#allGamesTitle');

  let data = null;
  let activeCat = '__all__';
  let query = '';

  const formToggle = bindFormToggle(root, { backdropSelector: '[data-dev-form-backdrop]' });

  function visibleGames() {
    const list = [...(data?.gamesByCategory?.[activeCat] || [])];
    const needle = query.trim().toLowerCase();
    if (!needle) return list;
    return list.filter((g) => String(g.title || '').toLowerCase().includes(needle));
  }

  function categoryGames(catKey) {
    if (catKey === '__uncategorized__') {
      const categorizedIds = new Set();
      data.categories
        .filter((cat) => cat.key !== '__all__')
        .forEach((cat) => {
          (data.gamesByCategory?.[cat.key] || []).forEach((game) => categorizedIds.add(String(game.id)));
        });
      const untaggedGames = data.games.filter((game) => !categorizedIds.has(String(game.id)));
      const needle = query.trim().toLowerCase();
      const filtered = needle
        ? untaggedGames.filter((g) => String(g.title || '').toLowerCase().includes(needle))
        : untaggedGames;
      return filtered.sort(popularGameComparator);
    }

    const list = [...(data?.gamesByCategory?.[catKey] || [])].sort(popularGameComparator);
    const needle = query.trim().toLowerCase();
    if (!needle) return list;
    return list.filter((g) => String(g.title || '').toLowerCase().includes(needle));
  }

  function openGame(game) {
    const url = String(game?.url || '').trim();
    if (!url) return;
    if (modalTitleEl) modalTitleEl.textContent = game.title || 'Game';
    if (modalFrameEl) modalFrameEl.src = url;
    modalEl?.classList.add('open');
    document.body.style.overflow = 'hidden';
  }

  function closeGameModal() {
    modalEl?.classList.remove('open');
    if (modalFrameEl) modalFrameEl.src = '';
    document.body.style.overflow = '';
  }

  modalCloseEl?.addEventListener('click', closeGameModal);
  modalEl?.addEventListener('click', (e) => {
    if (e.target === modalEl) closeGameModal();
  });

  function renderGameCard(game, options = {}) {
    const banner = game.image
      ? `<img class="dev-card-banner" src="${escapeHtml(game.image)}" alt="" loading="lazy">`
      : '<div class="dev-card-media-fallback" aria-hidden="true"></div>';
    const canPlay = Boolean(String(game.url || '').trim());
    const classes = ['dev-card'];
    if (options.large) classes.push('dev-card-large');
    if (!canPlay) classes.push('dev-card-disabled');
    return `<article class="${classes.join(' ')}" data-game-id="${escapeHtml(game.id)}" tabindex="${canPlay ? '0' : '-1'}" aria-label="${escapeHtml(game.title)}">
      <div class="dev-card-media">
        ${banner}
        <div class="dev-card-play-overlay" aria-hidden="true"></div>
      </div>
      <div class="dev-card-body">
        <h2 class="dev-card-title">${escapeHtml(game.title)}</h2>
      </div>
    </article>`;
  }

  function renderCategorySection(cat, games) {
    if (!games.length) return '';
    const domId = domIdFromCategoryKey(cat.key);
    const sizeClass = games.length <= 4
      ? ' dev-category-section-small'
      : games.length >= 10
        ? ' dev-category-section-large'
        : ' dev-category-section-medium';
    const arrangedGames = arrangeCategoryGames(cat.key, games);
    return `<section class="dev-category-section${sizeClass}" id="dev-category-${escapeHtml(domId)}" aria-labelledby="dev-category-title-${escapeHtml(domId)}">
      <div class="dev-category-head">
        <h3 id="dev-category-title-${escapeHtml(domId)}">${escapeHtml(cat.label)}</h3>
        <span>${games.length} ${games.length === 1 ? 'game' : 'games'}</span>
      </div>
      <div class="dev-category-grid">
        ${arrangedGames.map(({ game, large }) => renderGameCard(game, { large })).join('')}
      </div>
    </section>`;
  }

  function categoriesToRender() {
    if (!data) return [];
    if (activeCat !== '__all__') {
      const category = data.categories.find((cat) => cat.key === activeCat);
      return category ? [category] : [];
    }

    const taggedCategories = data.categories.filter((cat) => cat.key !== '__all__' && cat.count > 0);
    const categorizedIds = new Set();
    taggedCategories.forEach((cat) => {
      (data.gamesByCategory?.[cat.key] || []).forEach((game) => categorizedIds.add(String(game.id)));
    });
    const hasUncategorizedGames = data.games.some((game) => !categorizedIds.has(String(game.id)));
    const categorySections = hasUncategorizedGames
      ? taggedCategories.concat({ key: '__uncategorized__', label: 'More games', count: data.games.length - categorizedIds.size })
      : taggedCategories;

    return categorySections.length
      ? categorySections
      : [{ key: '__all__', label: 'All games', count: data.games.length }];
  }

  function bindGameCards() {
    gridEl.querySelectorAll('[data-game-id]').forEach((card) => {
      const game = data.games.find((g) => String(g.id) === String(card.dataset.gameId));
      if (!game || !String(game.url || '').trim()) return;
      card.style.cursor = 'pointer';
      card.addEventListener('click', () => openGame(game));
      card.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          openGame(game);
        }
      });
    });
  }

  function renderGrid() {
    const sections = categoriesToRender()
      .map((cat) => ({ cat, games: categoryGames(cat.key) }))
      .filter(({ games }) => games.length);

    if (catalogueTitleEl) {
      catalogueTitleEl.textContent = activeCat === '__all__'
        ? 'Game categories'
        : (data.categories.find((cat) => cat.key === activeCat)?.label || 'Games');
    }

    gridEl.innerHTML = sections.length
      ? sections.map(({ cat, games }) => renderCategorySection(cat, games)).join('')
      : '<div class="dev-status" style="grid-column:1/-1;">No games in this view.</div>';

    bindGameCards();
  }

  function renderCats() {
    catsEl.innerHTML = '';
    data.categories.forEach((cat) => {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'dev-cat-btn' + (cat.key === '__all__' ? ' dev-cat-all' : '') + (cat.key === activeCat ? ' active' : '');
      btn.textContent = `${cat.label} (${cat.count})`;
      btn.addEventListener('click', () => {
        activeCat = cat.key;
        catsEl.querySelectorAll('.dev-cat-btn').forEach((el) => el.classList.remove('active'));
        btn.classList.add('active');
        renderGrid();
        root.querySelector('#allGames')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      });
      catsEl.appendChild(btn);
    });
  }

  async function refreshCatalog() {
    data = await loadGamesCatalog();
    updateHeroStats(root, data);
    populateTagDatalist(root, data.tagOptions);
    renderFeaturedRow(root, data.games, openGame);
    renderCats();
    renderGrid();
  }

  qEl?.addEventListener('input', () => {
    query = qEl.value || '';
    renderGrid();
  });

  addForm?.addEventListener('submit', async (e) => {
    e.preventDefault();
    if (formMsg) {
      formMsg.textContent = 'Saving…';
      formMsg.classList.remove('err');
    }
    const submitBtn = addForm.querySelector('[type="submit"]');
    if (submitBtn) submitBtn.disabled = true;
    try {
      await addGameToFirestore(new FormData(addForm), data?.games || []);
      addForm.reset();
      await refreshCatalog();
      formToggle?.close?.();
      if (formMsg) formMsg.textContent = '';
      showDevToast('Game added to catalogue', 'success');
    } catch (err) {
      const message = err?.message || 'Could not save game.';
      if (formMsg) {
        formMsg.textContent = message;
        formMsg.classList.add('err');
      }
      showDevToast(message, 'error');
    } finally {
      if (submitBtn) submitBtn.disabled = false;
    }
  });

  loadGamesCatalog()
    .then((catalog) => {
      data = catalog;
      if (!data.categories.length) throw new Error('No categories loaded.');
      statusEl?.remove();
      catsEl.hidden = false;
      gridEl.hidden = false;
      updateHeroStats(root, data);
      populateTagDatalist(root, data.tagOptions);
      renderFeaturedRow(root, data.games, openGame);
      renderCats();
      renderGrid();
    })
    .catch((err) => {
      if (statusEl) {
        statusEl.textContent = 'Could not load games: ' + (err?.message || String(err));
        statusEl.classList.add('err');
      }
    });
}
