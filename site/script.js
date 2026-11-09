'use strict';

// A small progressive enhancement layer. No services, analytics or API keys.
const menuButton = document.querySelector('.menu-toggle');
const navigation = document.querySelector('#main-nav');

function closeMenu() {
  menuButton.setAttribute('aria-expanded', 'false');
  navigation.classList.remove('open');
}

menuButton.addEventListener('click', () => {
  const expanded = menuButton.getAttribute('aria-expanded') === 'true';
  menuButton.setAttribute('aria-expanded', String(!expanded));
  navigation.classList.toggle('open', !expanded);
});
navigation.addEventListener('click', (event) => {
  if (event.target.closest('a')) closeMenu();
});
document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && menuButton.getAttribute('aria-expanded') === 'true') {
    closeMenu();
    menuButton.focus();
  }
});

document.querySelectorAll('[role="tablist"]').forEach((tablist) => {
  const tabs = Array.from(tablist.querySelectorAll('[role="tab"]'));
  function selectTab(selected, focus = false) {
    tabs.forEach((tab) => {
      const active = tab === selected;
      tab.setAttribute('aria-selected', String(active));
      tab.tabIndex = active ? 0 : -1;
      document.getElementById(tab.getAttribute('aria-controls')).hidden = !active;
    });
    if (focus) selected.focus();
    if (tablist.classList.contains('terminal-tabs')) {
      document.getElementById('copy-command').setAttribute('aria-label', `Copy ${selected.textContent.includes('API') ? 'API' : 'workspace'} development commands`);
      document.getElementById('copy-command').textContent = 'Copy';
    }
  }
  tabs.forEach((tab, index) => {
    tab.addEventListener('click', () => selectTab(tab));
    tab.addEventListener('keydown', (event) => {
      let nextIndex;
      if (event.key === 'ArrowRight') nextIndex = (index + 1) % tabs.length;
      if (event.key === 'ArrowLeft') nextIndex = (index - 1 + tabs.length) % tabs.length;
      if (event.key === 'Home') nextIndex = 0;
      if (event.key === 'End') nextIndex = tabs.length - 1;
      if (nextIndex !== undefined) {
        event.preventDefault();
        selectTab(tabs[nextIndex], true);
      }
    });
  });
});

const copyButton = document.getElementById('copy-command');
let copyResetTimer;
copyButton?.addEventListener('click', async () => {
  const panel = document.querySelector('.terminal [role="tabpanel"]:not([hidden])');
  const commands = panel.querySelector('code').textContent;
  const status = document.getElementById('copy-status');
  clearTimeout(copyResetTimer);
  try {
    if (!navigator.clipboard?.writeText) throw new Error('Clipboard unavailable');
    await navigator.clipboard.writeText(commands);
    copyButton.textContent = 'Copied';
    status.textContent = 'Commands copied to clipboard.';
  } catch {
    // Select the command text so copying remains possible if clipboard access is denied.
    const range = document.createRange();
    range.selectNodeContents(panel.querySelector('code'));
    const selection = window.getSelection();
    selection.removeAllRanges();
    selection.addRange(range);
    copyButton.textContent = 'Select + copy';
    status.textContent = 'Clipboard access is unavailable. Commands are selected; press Control+C or Command+C to copy.';
  }
  copyResetTimer = setTimeout(() => { copyButton.textContent = 'Copy'; }, 5000);
});
