'use strict';

// The guide remains readable without JavaScript. Enhance the two paths into tabs.
const guideTabs = Array.from(document.querySelectorAll('.guide-paths [role="tab"]'));
const guidePanels = guideTabs.map((tab) => document.getElementById(tab.getAttribute('aria-controls')));

function showGuidePath(panel, updateHash = false) {
  const selected = guideTabs.find((tab) => tab.getAttribute('aria-controls') === panel.id);
  guideTabs.forEach((tab) => {
    const active = tab === selected;
    tab.setAttribute('aria-selected', String(active));
    tab.tabIndex = active ? 0 : -1;
    document.getElementById(tab.getAttribute('aria-controls')).hidden = !active;
  });
  if (updateHash) history.replaceState(null, '', `#${panel.id}`);
}

function revealGuideHash(scroll = true) {
  let hash;
  try { hash = decodeURIComponent(location.hash.slice(1)); } catch { hash = ''; }
  const target = document.getElementById(hash);
  const panel = target?.closest('.guide-panel');
  if (panel) {
    showGuidePath(panel);
    if (scroll) target.scrollIntoView({ block: 'start', behavior: 'instant' });
  } else if (!guidePanels.some((item) => item.hidden)) {
    showGuidePath(guidePanels[0]);
  }
}

guideTabs.forEach((tab) => {
  // Shared script.js supplies roving focus and arrow/Home/End keyboard support.
  tab.addEventListener('click', () => showGuidePath(document.getElementById(tab.getAttribute('aria-controls')), true));
  tab.addEventListener('keydown', (event) => {
    if (['ArrowRight', 'ArrowLeft', 'Home', 'End'].includes(event.key)) {
      const active = guideTabs.find((item) => item.getAttribute('aria-selected') === 'true');
      if (active) history.replaceState(null, '', `#${active.getAttribute('aria-controls')}`);
    }
  });
});
window.addEventListener('hashchange', () => revealGuideHash());
revealGuideHash(Boolean(location.hash));

document.querySelectorAll('[data-copy]').forEach((button) => {
  let resetTimer;
  const originalLabel = button.textContent;
  button.addEventListener('click', async () => {
    const code = document.getElementById(button.dataset.copy);
    const status = document.getElementById('guide-copy-status');
    clearTimeout(resetTimer);
    try {
      if (!navigator.clipboard?.writeText) throw new Error('Clipboard unavailable');
      await navigator.clipboard.writeText(code.textContent);
      button.textContent = 'Copied';
      status.textContent = `${button.getAttribute('aria-label').replace(/^Copy /, '')} copied to clipboard.`;
    } catch {
      const range = document.createRange();
      range.selectNodeContents(code);
      const selection = window.getSelection();
      selection.removeAllRanges();
      selection.addRange(range);
      button.textContent = 'Select + copy';
      status.textContent = 'Clipboard access is unavailable. Commands are selected; press Control+C or Command+C to copy.';
    }
    resetTimer = setTimeout(() => { button.textContent = originalLabel; }, 5000);
  });
});
