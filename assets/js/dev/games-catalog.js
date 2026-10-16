/**
 * Games Universe Catalog - Performance Optimized
 * Implements batch chunking, lazy loading, and click-to-play evaluation.
 */

const BATCH_SIZE = 12;
let currentIndex = 0;
let allGames = [];

export async function initCatalog() {
  const container = document.getElementById('games-container');
  const sentinel = document.getElementById('scroll-sentinel');
  
  if (!container) return;

  try {
    // Fetch games metadata list asynchronously
    const response = await fetch('/games/among-run/games.json').catch(() => null);
    if (response && response.ok) {
      allGames = await response.json();
    }
  } catch (err) {
    allGames = [];
  }

  // Render the initial batch
  renderBatch(container);

  // Setup Intersection Observer for smooth infinite scrolling without lagging
  if (sentinel) {
    const observer = new IntersectionObserver((entries) => {
      if (entries[0].isIntersecting) {
        renderBatch(container);
      }
    }, { rootMargin: '300px' });
    
    observer.observe(sentinel);
  }
}

function renderBatch(container) {
  if (allGames.length > 0 && currentIndex >= allGames.length) return;

  const fragment = document.createDocumentFragment();
  const batch = allGames.length > 0 
    ? allGames.slice(currentIndex, currentIndex + BATCH_SIZE) 
    : getFallbackStaticCards();

  batch.forEach(game => {
    const card = document.createElement('div');
    card.className = 'game-card';
    card.innerHTML = `
      <div class="game-thumbnail-wrapper">
        <img 
          src="${game.image || '/images/games/2048.jpeg'}" 
          alt="${game.title || 'Game Panel'}" 
          loading="lazy" 
          decoding="async" 
          width="280" 
          height="180"
        >
      </div>
      <div class="game-meta">
        <h3>${game.title || 'Arcade Game'}</h3>
        <button class="play-trigger" data-url="${game.url || '#'}">Play Now</button>
      </div>
    `;

    // Click-to-Play: Hydrate iframe only when the user explicitly clicks play
    card.querySelector('.play-trigger').addEventListener('click', (e) => {
      const targetUrl = e.currentTarget.getAttribute('data-url');
      hydrateGameFrame(card, targetUrl);
    }, { once: true });

    fragment.appendChild(card);
  });

  container.appendChild(fragment);
  currentIndex += BATCH_SIZE;
}

function hydrateGameFrame(cardElement, gameUrl) {
  cardElement.innerHTML = `
    <iframe 
      src="${gameUrl}" 
      loading="lazy" 
      allowfullscreen 
      style="width: 100%; height: 100%; border: none;"
    ></iframe>
  `;
}

function getFallbackStaticCards() {
  return Array.from({ length: BATCH_SIZE }, (_, i) => ({
    title: `Game Panel ${currentIndex + i + 1}`,
    image: '/images/games/2048.jpeg',
    url: '#'
  }));
}

// Auto-initialize when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initCatalog);
} else {
  initCatalog();
}
