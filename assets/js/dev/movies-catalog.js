import { getFirestoreDb } from './firebase-config.js';
import { escapeHtml } from './games-catalog.js';

const MAX_MOVIE_KEY_ID = 999999;
const MAX_MOVIE_CATEGORY_ID = 999;

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

function normalizeMovieCategory(value, pool) {
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

function normalizeMovieDoc(raw, fallbackId, categories, indexHint = 0) {
  const releaseYear = Number.parseInt(raw.releaseYear || raw.year || raw.release || raw.release_date, 10) || 0;
  const score = Number.parseFloat(raw.score ?? raw.rating ?? raw.voteAverage ?? 0) || 0;
  const category = normalizeMovieCategory(raw.category || raw.catagory || raw.genre, categories);
  const categoryCfg = (Array.isArray(categories) ? categories : []).find((cfg) => String(cfg.key) === String(category));
  const categoryId = toBoundedPositiveInt(raw.categoryId ?? categoryCfg?.categoryId, MAX_MOVIE_CATEGORY_ID);
  const fallbackMovieKey = (indexHint + 1) <= MAX_MOVIE_KEY_ID
    ? (indexHint + 1)
    : stableNumericKey(raw.id || fallbackId || raw.title || '', MAX_MOVIE_KEY_ID);
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
    description: String(raw.description || raw.desc || raw.synopsis || raw.overview || ''),
    url: String(raw.url || raw.movieUrl || raw.fullMovieUrl || '')
  };
}

function movieNewestComparator(a, b) {
  return (b.releaseYear || 0) - (a.releaseYear || 0)
    || (b.score || 0) - (a.score || 0)
    || String(a.title || '').localeCompare(String(b.title || ''));
}

function buildMoviesData(movieItems, categoryConfig) {
  const cfg = (Array.isArray(categoryConfig) && categoryConfig.length) ? categoryConfig : [];
  const moviesByCategory = {};
  cfg.forEach((c) => { moviesByCategory[c.key] = []; });
  (movieItems || []).forEach((movie, idx) => {
    const fixed = normalizeMovieDoc(movie, movie.id || `m-${idx}`, cfg, idx);
    if (!moviesByCategory[fixed.category]) moviesByCategory[fixed.category] = [];
    moviesByCategory[fixed.category].push(fixed);
  });
  Object.keys(moviesByCategory).forEach((cat) => moviesByCategory[cat].sort(movieNewestComparator));
  const inferredCategories = cfg.length
    ? cfg
    : Object.keys(moviesByCategory).map((key, idx) => ({
        key,
        categoryId: (idx + 1) <= MAX_MOVIE_CATEGORY_ID ? (idx + 1) : stableNumericKey(key, MAX_MOVIE_CATEGORY_ID),
        order: idx
      }));
  const normalizedCategories = inferredCategories.map((c, idx) => ({
    ...c,
    categoryId: toBoundedPositiveInt(c.categoryId, MAX_MOVIE_CATEGORY_ID)
      ?? ((idx + 1) <= MAX_MOVIE_CATEGORY_ID ? (idx + 1) : stableNumericKey(c.key || idx, MAX_MOVIE_CATEGORY_ID)),
    order: Number.isFinite(Number(c.order)) ? Number(c.order) : idx,
    count: (moviesByCategory[c.key] || []).length
  }));
  return { categories: normalizedCategories, moviesByCategory };
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

function watchPlayerHref(movie) {
  const u = resolveMovieUrl(movie.url);
  if (!u) return null;
  return new URL(`/movie/?u=${encodeURIComponent(u)}`, window.location.origin).toString();
}

async function loadMoviesCatalog() {
  const { db, fs } = await getFirestoreDb();
  let categoryRows = [];
  try {
    let snap;
    try {
      snap = await fs.getDocs(fs.query(fs.collection(db, 'movieCategories'), fs.orderBy('order', 'asc')));
    } catch (_) {
      snap = await fs.getDocs(fs.collection(db, 'movieCategories'));
    }
    categoryRows = snap.docs.map((d, i) => {
      const x = d.data() || {};
      const key = String(x.key || x.name || '').trim();
      if (!key) return null;
      const categoryId = toBoundedPositiveInt(x.categoryId ?? x.categoryKey ?? x.numericId, MAX_MOVIE_CATEGORY_ID);
      const fallbackCategoryId = (i + 1) <= MAX_MOVIE_CATEGORY_ID ? (i + 1) : stableNumericKey(key, MAX_MOVIE_CATEGORY_ID);
      return {
        id: d.id,
        key,
        categoryId: categoryId !== null ? categoryId : fallbackCategoryId,
        order: Number.isFinite(Number(x.order)) ? Number(x.order) : i
      };
    }).filter(Boolean).sort((a, b) => (a.order || 0) - (b.order || 0));
  } catch (e) {
    console.warn('movieCategories:', e);
  }

  const snap = await fs.getDocs(fs.collection(db, 'movies'));
  const movies = snap.docs.map((d, i) => normalizeMovieDoc({ id: d.id, ...d.data() }, d.id, categoryRows, i));
  return buildMoviesData(movies, categoryRows);
}

export function mountMoviesCatalog(root) {
  const statusEl = root.querySelector('#devStatus');
  const catsEl = root.querySelector('#devCats');
  const gridEl = root.querySelector('#devGrid');
  const qEl = root.querySelector('#devSearch');

  let data = null;
  let activeCat = '';
  let query = '';

  function visibleMovies() {
    const list = [...(data?.moviesByCategory?.[activeCat] || [])];
    const needle = query.trim().toLowerCase();
    if (!needle) return list;
    return list.filter((m) => String(m.title || '').toLowerCase().includes(needle));
  }

  function renderGrid() {
    const list = visibleMovies();
    gridEl.innerHTML = list.length
      ? list.map((movie) => {
          const href = watchPlayerHref(movie);
          const banner = movie.banner
            ? `<img class="dev-card-banner" src="${escapeHtml(movie.banner)}" alt="" loading="lazy">`
            : '<div class="dev-card-banner" aria-hidden="true"></div>';
          const action = href
            ? `<a class="dev-action dev-action-alt" href="${escapeHtml(href)}">Watch</a>`
            : '<button type="button" class="dev-action dev-action-alt" disabled>No URL</button>';
          return `<article class="dev-card">
            ${banner}
            <div class="dev-card-body">
              <h2 class="dev-card-title">${escapeHtml(movie.title)}</h2>
              <div class="dev-card-meta">${escapeHtml(movie.category)} · ${movie.releaseYear || '—'} · ★ ${movie.score || '—'}</div>
              <p class="dev-card-desc">${escapeHtml(movie.description || '')}</p>
              ${action}
            </div>
          </article>`;
        }).join('')
      : '<div class="dev-status" style="grid-column:1/-1;">No titles in this view.</div>';
  }

  function renderCats() {
    catsEl.innerHTML = '';
    data.categories.forEach((cat) => {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'dev-cat-btn' + (cat.key === activeCat ? ' active' : '');
      btn.textContent = `${cat.key} (${cat.count})`;
      btn.addEventListener('click', () => {
        activeCat = cat.key;
        catsEl.querySelectorAll('.dev-cat-btn').forEach((el) => el.classList.remove('active'));
        btn.classList.add('active');
        renderGrid();
      });
      catsEl.appendChild(btn);
    });
  }

  qEl?.addEventListener('input', () => {
    query = qEl.value || '';
    renderGrid();
  });

  loadMoviesCatalog()
    .then((catalog) => {
      data = catalog;
      if (!data.categories.length) throw new Error('No categories loaded.');
      activeCat = data.categories[0].key;
      statusEl?.remove();
      catsEl.hidden = false;
      gridEl.hidden = false;
      renderCats();
      renderGrid();
    })
    .catch((err) => {
      if (statusEl) {
        statusEl.textContent = 'Could not load movies: ' + (err?.message || String(err));
        statusEl.classList.add('err');
      }
    });
}
