// Firn site — theme toggle, mobile nav, copy-to-clipboard for code blocks.
(function () {
  const root = document.documentElement;
  const saved = (() => { try { return localStorage.getItem('firn-theme'); } catch (e) { return null; } })();
  if (saved === 'light' || saved === 'dark') {
    root.setAttribute('data-theme', saved);
  } else if (window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches) {
    root.setAttribute('data-theme', 'light');
  }

  document.addEventListener('DOMContentLoaded', () => {
    const tt = document.querySelector('[data-theme-toggle]');
    if (tt) tt.addEventListener('click', () => {
      const next = root.getAttribute('data-theme') === 'light' ? 'dark' : 'light';
      root.setAttribute('data-theme', next);
      try { localStorage.setItem('firn-theme', next); } catch (e) { /* private mode */ }
    });

    const mt = document.querySelector('[data-menu-toggle]');
    const nav = document.querySelector('.nav');
    if (mt && nav) mt.addEventListener('click', () => nav.classList.toggle('open'));

    // Copy buttons on <pre class="code">
    document.querySelectorAll('pre.code').forEach((pre) => {
      const btn = document.createElement('button');
      btn.className = 'copy-btn';
      btn.type = 'button';
      btn.textContent = 'copy';
      btn.style.cssText = 'float:right;margin:0 0 8px 12px;font:600 .68rem ui-monospace,Menlo,monospace;letter-spacing:.08em;color:#8fd3ff;background:#131a33;border:1px solid rgba(148,163,184,.3);border-radius:6px;padding:3px 9px;cursor:pointer';
      btn.addEventListener('click', async () => {
        try {
          await navigator.clipboard.writeText(pre.innerText);
          btn.textContent = 'copied ✓';
          setTimeout(() => { btn.textContent = 'copy'; }, 1600);
        } catch (e) { btn.textContent = 'failed'; }
      });
      pre.prepend(btn);
    });
  });
})();
