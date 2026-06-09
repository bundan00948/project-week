import { getFirestoreDb } from './firebase-config.js';
import { escapeHtml } from './games-catalog.js';
import { bindFormToggle } from './access-gate.js';
import { nextNumericKey, showDevToast } from './catalog-utils.js';

const MAX_MOVIE_KEY_ID = 999999;
const MAX_MOVIE_CATEGORY_ID = 999;
const DEFAULT_GRADIENT = 'linear-gradient(135deg, #2f5ca3 0%, #1a2e58 100%)';

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
    titleImage: String(raw.titleImage || raw.titleLogo || raw.titleArt || ''),
    description: String(raw.description || raw.desc || raw.synopsis || raw.overview || ''),
    url: String(raw.url || raw.movieUrl || raw.fullMovieUrl || '')
  };
}

function movieNewestComparator(a, b) {
  return (b.releaseYear || 0) - (a.releaseYear || 0)
    || (b.score || 0) - (a.score || 0)
    || String(a.title || '').localeCompare(String(b.title || ''));
}

function movieTopComparator(a, b) {
  return (b.score || 0) - (a.score || 0)
    || (b.releaseYear || 0) - (a.releaseYear || 0)
    || String(a.title || '').localeCompare(String(b.title || ''));
}

function truncateText(text, max = 220) {
  const s = String(text || '').trim();
  if (!s) return '';
  return s.length > max ? `${s.slice(0, max - 3)}...` : s;
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
        gradient: DEFAULT_GRADIENT,
        art: '',
        artPosition: 'bottom',
        artScale: 100,
        order: idx
      }));

  const topMovies = inferredCategories
    .map((catCfg) => {
      const pick = [...(moviesByCategory[catCfg.key] || [])].sort(movieTopComparator)[0];
      return pick ? { ...pick, rankCategory: catCfg.key } : null;
    })
    .filter(Boolean);

  const normalizedCategories = inferredCategories.map((cfg, idx) => ({
    ...cfg,
    categoryId: toBoundedPositiveInt(cfg.categoryId, MAX_MOVIE_CATEGORY_ID)
      ?? ((idx + 1) <= MAX_MOVIE_CATEGORY_ID ? (idx + 1) : stableNumericKey(cfg.key || idx, MAX_MOVIE_CATEGORY_ID)),
    order: Number.isFinite(Number(cfg.order)) ? Number(cfg.order) : idx,
    count: (moviesByCategory[cfg.key] || []).length
  }));

  const movies = Object.values(moviesByCategory).flat();
  return { categories: normalizedCategories, moviesByCategory, movies, categoryRows: cfg, topMovies };
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

const DEV_MOVIES_RETURN_PATH = '/dev/movies';

function formatMovieRoutePart(value, width) {
  const n = Number.parseInt(value, 10);
  if (!Number.isFinite(n) || n < 0) return null;
  return String(n).padStart(width, '0');
}

function watchPlayerHref(movie) {
  const player = new URL('/movie/', window.location.origin);
  player.searchParams.set('return', DEV_MOVIES_RETURN_PATH);

  const streamUrl = resolveMovieUrl(movie.url);
  if (streamUrl) {
    player.searchParams.set('u', streamUrl);
    return player.toString();
  }

  const year = Number.parseInt(movie.releaseYear, 10);
  const categoryPart = formatMovieRoutePart(movie.categoryId, 3);
  const moviePart = formatMovieRoutePart(movie.movieKey, 6);
  if (Number.isFinite(year) && year >= 1900 && categoryPart && moviePart) {
    player.pathname = `/movie/${year}/${categoryPart}/${moviePart}`;
    return player.toString();
  }

  return null;
}

export async function loadMoviesCatalog() {
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
        gradient: String(x.gradient || DEFAULT_GRADIENT),
        art: String(x.art || ''),
        artPosition: String(x.artPosition || 'bottom').toLowerCase() === 'middle' ? 'middle' : 'bottom',
        artScale: [50, 75, 100, 125, 150].includes(Number(x.artScale)) ? Number(x.artScale) : 100,
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

async function addMovieToFirestore(formData, catalog) {
  const { db, fs } = await getFirestoreDb();
  const title = String(formData.get('title') || '').trim();
  const category = String(formData.get('category') || '').trim();
  const releaseYear = Number.parseInt(formData.get('releaseYear'), 10) || new Date().getFullYear();
  const scoreRaw = Number.parseFloat(formData.get('score'));
  const score = Number.isFinite(scoreRaw) ? Math.max(0, Math.min(10, Number(scoreRaw.toFixed(1)))) : 0;
  const banner = String(formData.get('banner') || '').trim();
  const titleImage = String(formData.get('titleImage') || '').trim();
  const url = String(formData.get('url') || '').trim();
  const description = String(formData.get('description') || '').trim();

  if (!title) throw new Error('Title is required.');
  if (!category) throw new Error('Category is required.');

  const categoryCfg = (catalog?.categoryRows || []).find((cat) => String(cat.key) === category);
  const categoryId = toBoundedPositiveInt(categoryCfg?.categoryId, MAX_MOVIE_CATEGORY_ID) ?? 0;
  const nextMovieKey = nextNumericKey(catalog?.movies || [], (movie) => movie.movieKey, MAX_MOVIE_KEY_ID, 1);
  const movieKey = nextMovieKey !== null ? nextMovieKey : stableNumericKey(title, MAX_MOVIE_KEY_ID);

  await fs.addDoc(fs.collection(db, 'movies'), {
    title,
    category,
    categoryId,
    movieKey,
    releaseYear,
    score,
    banner,
    titleImage,
    description,
    url,
    trailerUrl: '',
    createdAt: fs.serverTimestamp(),
    updatedAt: fs.serverTimestamp()
  });
}

function populateCategorySelect(root, categories) {
  const select = root.querySelector('#devMovieCategory');
  if (!select) return;
  select.innerHTML = categories.map((cat) =>
    `<option value="${escapeHtml(cat.key)}">${escapeHtml(cat.key)}</option>`
  ).join('');
}

export function mountMoviesCatalog(root) {
  if (root.__devMoviesCatalogMounted) return;
  root.__devMoviesCatalogMounted = true;

  const statusEl = root.querySelector('#devStatus');
  const carouselEl = root.querySelector('#cinemaCarousel');
  const genresEl = root.querySelector('#cinemaCategories');
  const gridEl = root.querySelector('#devGrid');
  const qEl = root.querySelector('#devSearch');
  const activeLabelEl = root.querySelector('#cinemaActiveCategory');
  const resultEl = root.querySelector('#cinemaResultCount');
  const addForm = root.querySelector('#devAddMovieForm');
  const formMsg = root.querySelector('#devFormMsg');
  const detailModal = root.querySelector('#cinemaDetailModal');
  const detailBackdrop = root.querySelector('#cinemaDetailBackdrop');
  const detailTitle = root.querySelector('#cinemaDetailTitle');
  const detailTitleImage = root.querySelector('#cinemaDetailTitleImage');
  const detailDesc = root.querySelector('#cinemaDetailDesc');
  const detailMeta = root.querySelector('#cinemaDetailMeta');
  const detailWatch = root.querySelector('#cinemaDetailWatch');
  const detailClose = root.querySelector('#cinemaDetailClose');

  let data = null;
  let activeCat = '';
  let query = '';
  let carouselIndex = 0;
  let carouselTimer = null;
  let activeDetailMovie = null;

  const formToggle = bindFormToggle(root, { backdropSelector: '[data-dev-form-backdrop]' });

  function allMoviesList() {
    return data?.movies || [];
  }

  function visibleMovies() {
    const list = [...(data?.moviesByCategory?.[activeCat] || [])];
    const needle = query.trim().toLowerCase();
    if (!needle) return list;
    return list.filter((m) => String(m.title || '').toLowerCase().includes(needle));
  }

  function findMovieById(id) {
    return allMoviesList().find((m) => String(m.id) === String(id)) || null;
  }

  function openDetailModal(movie) {
    if (!movie || !detailModal) return;
    activeDetailMovie = movie;
    const href = watchPlayerHref(movie);
    if (detailTitle) detailTitle.textContent = movie.title || 'Movie';
    if (detailTitleImage) {
      if (movie.titleImage) {
        detailTitleImage.src = movie.titleImage;
        detailTitleImage.alt = movie.title || '';
        detailTitleImage.hidden = false;
        detailTitle?.classList.add('has-image-fallback');
      } else {
        detailTitleImage.hidden = true;
        detailTitleImage.removeAttribute('src');
        detailTitleImage.alt = '';
        detailTitle?.classList.remove('has-image-fallback');
      }
    }
    if (detailDesc) {
      detailDesc.textContent = movie.description || 'No synopsis available yet.';
    }
    if (detailMeta) {
      detailMeta.innerHTML = `
        <span>${escapeHtml(movie.category)}</span>
        <span>${movie.releaseYear || 'N/A'}</span>
        <span>★ ${movie.score || 'N/A'}</span>
      `;
    }
    if (detailBackdrop) {
      detailBackdrop.style.backgroundImage = movie.banner
        ? `url("${String(movie.banner).replace(/"/g, '\\"')}")`
        : 'none';
    }
    if (detailWatch) {
      if (href) {
        detailWatch.href = href;
        detailWatch.style.display = '';
        detailWatch.textContent = 'Watch now';
        detailWatch.removeAttribute('aria-disabled');
      } else {
        detailWatch.removeAttribute('href');
        detailWatch.style.display = '';
        detailWatch.textContent = 'No stream link';
        detailWatch.setAttribute('aria-disabled', 'true');
      }
    }
    detailModal.classList.add('open');
    detailModal.setAttribute('aria-hidden', 'false');
    document.body.style.overflow = 'hidden';
  }

  function closeDetailModal() {
    detailModal?.classList.remove('open');
    detailModal?.setAttribute('aria-hidden', 'true');
    activeDetailMovie = null;
    if (!root.querySelector('.dev-form-panel.open')) {
      document.body.style.overflow = '';
    }
  }

  detailClose?.addEventListener('click', closeDetailModal);
  detailModal?.addEventListener('click', (e) => {
    if (e.target === detailModal) closeDetailModal();
  });
  detailWatch?.addEventListener('click', (e) => {
    if (!detailWatch.getAttribute('href')) e.preventDefault();
  });

  function showSlide(index) {
    if (!carouselEl) return;
    const slides = carouselEl.querySelectorAll('.cinema-slide');
    const dots = carouselEl.querySelectorAll('.cinema-dot');
    if (!slides.length) return;
    let i = index;
    if (i >= slides.length) i = 0;
    if (i < 0) i = slides.length - 1;
    slides.forEach((s) => s.classList.remove('active'));
    dots.forEach((d) => d.classList.remove('active'));
    slides[i]?.classList.add('active');
    dots[i]?.classList.add('active');
    carouselIndex = i;
  }

  function startCarousel() {
    clearInterval(carouselTimer);
    const count = carouselEl?.querySelectorAll('.cinema-slide').length || 0;
    if (count < 2) return;
    carouselTimer = setInterval(() => showSlide(carouselIndex + 1), 5600);
  }

  function renderCarousel() {
    if (!carouselEl) return;
    const topMovies = data?.topMovies || [];
    if (!topMovies.length) {
      carouselEl.innerHTML = '<div class="cinema-spotlight-empty">No featured titles yet.</div>';
      return;
    }
    carouselEl.innerHTML = topMovies.map((movie, idx) => {
      const bgImage = movie.banner
        ? `<img src="${escapeHtml(movie.banner)}" alt="" loading="${idx === 0 ? 'eager' : 'lazy'}">`
        : '';
      const titleBlock = movie.titleImage
        ? `<img class="cinema-slide-title-image" src="${escapeHtml(movie.titleImage)}" alt="${escapeHtml(movie.title)}" loading="${idx === 0 ? 'eager' : 'lazy'}"><h2 class="cinema-slide-title has-image-fallback">${escapeHtml(movie.title)}</h2>`
        : `<h2 class="cinema-slide-title">${escapeHtml(movie.title)}</h2>`;
      const preview = truncateText(movie.description) || 'Open details to read the synopsis and start watching.';
      return `
        <article class="cinema-slide ${idx === 0 ? 'active' : ''}">
          <div class="cinema-slide-bg">${bgImage}</div>
          <div class="cinema-slide-scrim"></div>
          <div class="cinema-slide-body">
            <p class="cinema-slide-kicker">Featured · ${escapeHtml(movie.rankCategory || movie.category)}</p>
            <div class="cinema-slide-meta">
              <span>${escapeHtml(movie.category)}</span>
              <span>${movie.releaseYear || 'N/A'}</span>
              <span>★ ${movie.score || 'N/A'}</span>
            </div>
            ${titleBlock}
            <p class="cinema-slide-desc">${escapeHtml(preview)}</p>
            <div class="cinema-slide-actions">
              <button type="button" class="cinema-watch-btn" data-spotlight-watch="${escapeHtml(movie.id)}">Watch now</button>
              <button type="button" class="cinema-info-btn" data-spotlight-info="${escapeHtml(movie.id)}">More info</button>
            </div>
          </div>
        </article>
      `;
    }).join('') + `
      <div class="cinema-carousel-dots">
        ${topMovies.map((_, i) => `<button type="button" class="cinema-dot ${i === 0 ? 'active' : ''}" data-slide="${i}" aria-label="Slide ${i + 1}"></button>`).join('')}
      </div>
    `;

    carouselEl.querySelectorAll('[data-slide]').forEach((dot) => {
      dot.addEventListener('click', () => showSlide(Number(dot.dataset.slide)));
    });
    carouselEl.querySelectorAll('[data-spotlight-info]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const movie = findMovieById(btn.dataset.spotlightInfo);
        if (movie) openDetailModal(movie);
      });
    });
    carouselEl.querySelectorAll('[data-spotlight-watch]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const movie = findMovieById(btn.dataset.spotlightWatch);
        const href = movie && watchPlayerHref(movie);
        if (href) window.location.assign(href);
        else if (movie) openDetailModal(movie);
      });
    });

    carouselIndex = 0;
    startCarousel();
  }

  function renderGenres() {
    if (!genresEl) return;
    genresEl.innerHTML = '';
    (data?.categories || []).forEach((cat) => {
      const artScale = cat.artScale || 100;
      const artSizePx = Math.round(64 * (artScale / 100));
      const artPos = cat.artPosition === 'middle' ? 'middle' : 'bottom';
      const artHtml = cat.art
        ? `<img class="cinema-genre-art ${artPos}" src="${escapeHtml(cat.art)}" alt="" loading="lazy" style="width:${artSizePx}px;height:${artSizePx}px;">`
        : '';
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'cinema-genre-card' + (cat.key === activeCat ? ' active' : '');
      btn.style.setProperty('--genre-gradient', cat.gradient || DEFAULT_GRADIENT);
      btn.innerHTML = `
        <div class="cinema-genre-title">${escapeHtml(cat.key)}</div>
        <span class="cinema-genre-count">${cat.count} title${cat.count === 1 ? '' : 's'}</span>
        ${artHtml}
      `;
      btn.addEventListener('click', () => {
        activeCat = cat.key;
        genresEl.querySelectorAll('.cinema-genre-card').forEach((el) => el.classList.remove('active'));
        btn.classList.add('active');
        renderGrid();
        document.getElementById('cinemaCatalogue')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      });
      genresEl.appendChild(btn);
    });
  }

  function renderGrid() {
    const list = visibleMovies();
    if (activeLabelEl) activeLabelEl.textContent = activeCat || 'Catalogue';
    if (resultEl) {
      const qText = query.trim() ? ` · "${query.trim()}"` : '';
      resultEl.textContent = `${list.length} title${list.length === 1 ? '' : 's'}${qText}`;
    }
    genresEl?.querySelectorAll('.cinema-genre-card').forEach((btn) => {
      btn.classList.toggle('active', btn.querySelector('.cinema-genre-title')?.textContent === activeCat);
    });

    gridEl.innerHTML = list.length
      ? list.map((movie) => {
          const banner = movie.banner
            ? `<img src="${escapeHtml(movie.banner)}" alt="" loading="lazy">`
            : '<div class="cinema-poster-banner-fallback"></div>';
          return `
            <article class="cinema-poster" data-movie-id="${escapeHtml(movie.id)}" tabindex="0" role="button">
              <div class="cinema-poster-banner">${banner}</div>
              <span class="cinema-poster-year">${movie.releaseYear || 'N/A'}</span>
              <span class="cinema-poster-score">★ ${movie.score || '—'}</span>
              <div class="cinema-poster-body">
                <h3 class="cinema-poster-title">${escapeHtml(movie.title)}</h3>
                <div class="cinema-poster-meta">${escapeHtml(movie.category)}</div>
              </div>
            </article>
          `;
        }).join('')
      : '<div class="cinema-empty">No titles match this genre or search.</div>';

    gridEl.querySelectorAll('[data-movie-id]').forEach((card) => {
      const open = () => {
        const movie = findMovieById(card.dataset.movieId);
        if (movie) openDetailModal(movie);
      };
      card.addEventListener('click', open);
      card.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          open();
        }
      });
    });
  }

  async function refreshCatalog() {
    data = await loadMoviesCatalog();
    if (!activeCat && data.categories.length) activeCat = data.categories[0].key;
    populateCategorySelect(root, data.categories);
    renderCarousel();
    renderGenres();
    renderGrid();
  }

  qEl?.addEventListener('input', () => {
    query = qEl.value || '';
    renderGrid();
  });

  addForm?.addEventListener('submit', async (e) => {
    e.preventDefault();
    if (formMsg) {
      formMsg.textContent = 'Submitting…';
      formMsg.classList.remove('err');
    }
    const submitBtn = addForm.querySelector('[type="submit"]');
    if (submitBtn) submitBtn.disabled = true;
    try {
      await addMovieToFirestore(new FormData(addForm), data);
      addForm.reset();
      if (data?.categories?.length) {
        const select = root.querySelector('#devMovieCategory');
        if (select) select.value = data.categories[0].key;
      }
      await refreshCatalog();
      formToggle?.close?.();
      if (formMsg) formMsg.textContent = '';
      showDevToast('Title suggestion submitted', 'success');
    } catch (err) {
      const message = err?.message || 'Could not submit suggestion.';
      if (formMsg) {
        formMsg.textContent = message;
        formMsg.classList.add('err');
      }
      showDevToast(message, 'error');
    } finally {
      if (submitBtn) submitBtn.disabled = false;
    }
  });

  loadMoviesCatalog()
    .then((catalog) => {
      data = catalog;
      if (!data.categories.length) throw new Error('No categories loaded.');
      activeCat = data.categories[0].key;
      statusEl?.remove();
      genresEl && (genresEl.hidden = false);
      gridEl && (gridEl.hidden = false);
      populateCategorySelect(root, data.categories);
      renderCarousel();
      renderGenres();
      renderGrid();
    })
    .catch((err) => {
      if (statusEl) {
        statusEl.textContent = 'Could not load movies: ' + (err?.message || String(err));
        statusEl.classList.add('err');
      }
    });
}
