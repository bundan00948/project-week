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
    multiplayer: Boolean(raw.multiplayer),
    tags: (() => {
      if (Array.isArray(raw.tags)) return raw.tags.map((t) => String(t).trim()).filter(Boolean);
      if (typeof raw.tags === 'string') return raw.tags.split(',').map((t) => t.trim()).filter(Boolean);
      return [];
    })()
  };
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
  const games = gamesSnap.docs
    .map((d, i) => normalizeGameDoc({ id: d.id, ...d.data() }, d.id, i))
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
    gamesByCategory[key] = group.games.sort((a, b) => a.title.localeCompare(b.title));
  });

  return { categories, gamesByCategory, games, tagOptions: tagOptions.sort((a, b) => a.localeCompare(b)) };
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

function getFeaturedGames(games, limit = 10) {
  return [...(games || [])]
    .sort((a, b) =>
      Number(Boolean(b.image)) - Number(Boolean(a.image))
      || (b.rating || 0) - (a.rating || 0)
      || a.title.localeCompare(b.title)
    )
    .slice(0, limit);
}

function gameSummary(game) {
  const description = String(game?.description || '').trim();
  if (description) return description;
  const tags = Array.isArray(game?.tags) ? game.tags.filter(Boolean) : [];
  if (tags.length) return `${tags.slice(0, 3).join(' · ')}.`;
  return game?.multiplayer
    ? 'Multiplayer playtest ready.'
    : 'Instant browser game.';
}

function renderSpotlight(root, games, openGame) {
  const spotlight = root.querySelector('#gamesSpotlightCard');
  if (!spotlight) return;
  const game = getFeaturedGames(games, 1)[0];
  if (!game) {
    spotlight.innerHTML = '<div class="games-empty-card">No spotlight game available yet.</div>';
    return;
  }
  const art = game.image
    ? `<img class="games-spotlight-art" src="${escapeHtml(game.image)}" alt="" loading="eager">`
    : '<div class="games-spotlight-fallback" aria-hidden="true"></div>';
  spotlight.innerHTML = `
    ${art}
    <div class="games-spotlight-body">
      <div class="games-spotlight-badges">
        <span>Top pick</span>
        <span>★ ${Number(game.rating || 0).toFixed(1)}</span>
        <span>${game.multiplayer ? 'Multiplayer' : 'Single player'}</span>
      </div>
      <h2 class="games-spotlight-title">${escapeHtml(game.title)}</h2>
      <p class="games-spotlight-desc">${escapeHtml(gameSummary(game))}</p>
      <button type="button" class="games-play-btn" data-spotlight-id="${escapeHtml(game.id)}">
        <i class="fas fa-play" aria-hidden="true"></i>
        <span>Play now</span>
      </button>
    </div>
  `;
  spotlight.querySelector('[data-spotlight-id]')?.addEventListener('click', () => openGame(game));
}

function renderFeaturedRow(root, games, openGame) {
  const track = root.querySelector('#gamesFeaturedTrack');
  if (!track) return;
  const featured = getFeaturedGames(games, 10);
  if (!featured.length) {
    track.innerHTML = '<div class="games-empty-card">No featured games yet.</div>';
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
          <div class="games-featured-meta">★ ${Number(game.rating || 0).toFixed(1)} · ${game.multiplayer ? 'Multiplayer' : 'Single player'}</div>
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

function renderTopList(root, games, openGame) {
  const listEl = root.querySelector('#gamesTopList');
  if (!listEl) return;
  const featured = getFeaturedGames(games, 5);
  if (!featured.length) {
    listEl.innerHTML = '<div class="games-empty-card">No trending games yet.</div>';
    return;
  }
  listEl.innerHTML = featured.map((game, index) => `
    <button type="button" class="games-top-item" data-top-id="${escapeHtml(game.id)}">
      <span class="games-top-rank">#${index + 1}</span>
      ${game.image
        ? `<img class="games-top-thumb" src="${escapeHtml(game.image)}" alt="" loading="lazy">`
        : '<div class="games-top-thumb" aria-hidden="true"></div>'}
      <span class="games-top-copy">
        <span class="games-top-title">${escapeHtml(game.title)}</span>
        <span class="games-top-meta">
          <span>★ ${Number(game.rating || 0).toFixed(1)}</span>
          <span>${game.multiplayer ? 'Multi' : 'Solo'}</span>
        </span>
      </span>
    </button>
  `).join('');
  listEl.querySelectorAll('[data-top-id]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const game = featured.find((entry) => String(entry.id) === String(btn.dataset.topId));
      if (game) openGame(game);
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
  const resultsMetaEl = root.querySelector('#gamesResultsMeta');
  const resultsTitleEl = root.querySelector('#gamesResultsTitle');
  const modalEl = root.querySelector('#devGameModal');
  const modalTitleEl = root.querySelector('#devGameModalTitle');
  const modalFrameEl = root.querySelector('#devGameModalFrame');
  const modalCloseEl = root.querySelector('#devGameModalClose');
  const addForm = root.querySelector('#devAddGameForm');
  const formMsg = root.querySelector('#devFormMsg');

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

  function renderGrid() {
    const list = visibleGames();
    const category = data?.categories?.find((cat) => cat.key === activeCat);
    if (resultsTitleEl) {
      resultsTitleEl.textContent = category?.key && category.key !== '__all__'
        ? `${category.label} games`
        : 'Every game in the dev catalogue';
    }
    if (resultsMetaEl) {
      const queryLabel = query.trim() ? ` for "${query.trim()}"` : '';
      resultsMetaEl.textContent = `${list.length} result${list.length === 1 ? '' : 's'}${queryLabel}`;
    }
    gridEl.innerHTML = list.length
      ? list.map((game) => {
          const banner = game.image
            ? `<img class="dev-card-banner" src="${escapeHtml(game.image)}" alt="" loading="lazy">`
            : '<div class="dev-card-media-fallback" aria-hidden="true"></div>';
          const canPlay = Boolean(String(game.url || '').trim());
          const playBtn = canPlay
            ? `<button type="button" class="dev-card-play-chip games-grid-play" data-game-id="${escapeHtml(game.id)}">Play</button>`
            : '';
          const tags = Array.isArray(game.tags) ? game.tags.slice(0, 2) : [];
          const metaChips = [
            `★ ${Number(game.rating || 0).toFixed(1)}`,
            game.multiplayer ? 'Multiplayer' : 'Single player',
            ...tags
          ].slice(0, 3);
          return `<article class="dev-card">
            <div class="dev-card-media">
              ${banner}
              <div class="dev-card-play-overlay">${playBtn}</div>
            </div>
            <div class="dev-card-body">
              <h2 class="dev-card-title">${escapeHtml(game.title)}</h2>
              <p class="games-grid-tagline">${escapeHtml(gameSummary(game))}</p>
              <div class="dev-card-meta games-grid-meta">
                ${metaChips.map((chip) => `<span>${escapeHtml(chip)}</span>`).join('')}
              </div>
            </div>
          </article>`;
        }).join('')
      : '<div class="games-empty-card" style="grid-column:1/-1;">No games match this view yet.</div>';

    gridEl.querySelectorAll('[data-game-id]').forEach((btn) => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const game = list.find((g) => String(g.id) === String(btn.dataset.gameId));
        if (game) openGame(game);
      });
    });

    gridEl.querySelectorAll('.dev-card').forEach((card, index) => {
      const game = list[index];
      if (!game || !String(game.url || '').trim()) return;
      card.style.cursor = 'pointer';
      card.addEventListener('click', () => openGame(game));
      card.tabIndex = 0;
      card.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          openGame(game);
        }
      });
    });
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
      });
      catsEl.appendChild(btn);
    });
  }

  async function refreshCatalog() {
    data = await loadGamesCatalog();
    updateHeroStats(root, data);
    populateTagDatalist(root, data.tagOptions);
    renderSpotlight(root, data.games, openGame);
    renderFeaturedRow(root, data.games, openGame);
    renderTopList(root, data.games, openGame);
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
      renderSpotlight(root, data.games, openGame);
      renderFeaturedRow(root, data.games, openGame);
      renderTopList(root, data.games, openGame);
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
