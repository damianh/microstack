window.microstack = (() => {
    let openPicker;
    const scrollPositions = new Map();
    let currentUrl = location.href;
    function capture() {
        scrollPositions.set(currentUrl, {
            window: window.scrollY,
            index: document.querySelector('.resource-list')?.scrollTop ?? 0,
            records: document.querySelector('.records')?.scrollTop ?? 0
        });
        if (scrollPositions.size > 100) scrollPositions.delete(scrollPositions.keys().next().value);
    }
    const options = picker => Array.from(picker.querySelectorAll('[role="option"]'));
    function activate(picker, index, focus) {
        const items = options(picker);
        const current = items[Math.max(0, Math.min(index, items.length - 1))];
        items.forEach(item => item.tabIndex = item === current ? 0 : -1);
        const input = picker.querySelector('[role="combobox"]');
        if (current) {
            input.setAttribute('aria-activedescendant', current.id);
            if (focus) current.focus();
            current.scrollIntoView({ block: 'nearest' });
        } else input.removeAttribute('aria-activedescendant');
    }
    function close(picker, restore) {
        if (!picker) return;
        picker.querySelector('[data-picker-panel]').hidden = true;
        picker.querySelector('[data-picker-trigger]').setAttribute('aria-expanded', 'false');
        picker.querySelector('[role="combobox"]').setAttribute('aria-expanded', 'false');
        if (restore) picker.querySelector('[data-picker-trigger]').focus();
        if (openPicker === picker) openPicker = null;
    }
    function open(picker) {
        if (openPicker && openPicker !== picker) close(openPicker, false);
        openPicker = picker;
        picker.querySelector('[data-picker-panel]').hidden = false;
        picker.querySelector('[data-picker-trigger]').setAttribute('aria-expanded', 'true');
        picker.querySelector('[role="combobox"]').setAttribute('aria-expanded', 'true');
        activate(picker, 0, false);
        picker.querySelector('[role="combobox"]').focus();
    }
    document.addEventListener('click', event => {
        capture();
        const link = event.target.closest('a[href]');
        if (link && link.origin === location.origin && link.pathname === location.pathname &&
            new URL(link.href).searchParams.get('account') === new URL(currentUrl).searchParams.get('account') &&
            !scrollPositions.has(link.href))
            scrollPositions.set(link.href, scrollPositions.get(currentUrl));
        const picker = event.target.closest('[data-service-picker]');
        if (event.target.closest('[data-picker-trigger]')) {
            if (openPicker === picker) close(picker, true); else open(picker);
        } else if (event.target.closest('[role="option"]') && picker) close(picker, true);
        else if (openPicker && !openPicker.contains(event.target)) close(openPicker, !event.target.closest('a,button,input,select,textarea,[tabindex]'));
    });
    document.addEventListener('focusin', event => {
        if (openPicker && !openPicker.contains(event.target)) close(openPicker, false);
    });
    window.addEventListener('popstate', capture);
    document.addEventListener('keydown', event => {
        const picker = event.target.closest('[data-service-picker]');
        const trigger = event.target.closest('[data-picker-trigger]');
        if (trigger && (event.key === 'ArrowDown' || event.key === 'ArrowUp')) {
            event.preventDefault(); open(picker); return;
        }
        if (trigger && event.key.length === 1 && !event.ctrlKey && !event.metaKey && event.key !== ' ') {
            event.preventDefault(); open(picker);
            const input = picker.querySelector('input');
            input.value = event.key;
            input.dispatchEvent(new Event('input', { bubbles: true }));
            return;
        }
        if (picker && openPicker === picker) {
            if (event.key === 'Escape') { event.preventDefault(); close(picker, true); return; }
            const items = options(picker);
            const isInput = event.target.matches('input');
            let index = items.indexOf(event.target);
            if (isInput) index = items.findIndex(item => item.id === event.target.getAttribute('aria-activedescendant'));
            if (['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(event.key) && (!isInput || event.key.startsWith('Arrow'))) {
                event.preventDefault();
                activate(picker, event.key === 'Home' ? 0 : event.key === 'End' ? items.length - 1 :
                    event.key === 'ArrowDown' ? Math.min(index + 1, items.length - 1) : Math.max(index - 1, 0), !isInput);
            } else if (event.key === 'Enter' && isInput) {
                event.preventDefault(); items[Math.max(index, 0)]?.click();
            }
        }
        const tab = event.target.closest('[role="tab"]');
        if (tab && ['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) {
            event.preventDefault();
            const tabs = Array.from(tab.parentElement.querySelectorAll('[role="tab"]'));
            const index = tabs.indexOf(tab);
            const next = event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1 :
                (index + (event.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
            tabs[next].focus(); tabs[next].click();
        }
    });
    new MutationObserver(() => {
        if (openPicker && !document.contains(openPicker)) { openPicker = null; return; }
        if (openPicker && !options(openPicker).some(item => item.tabIndex === 0)) activate(openPicker, 0, false);
    }).observe(document.documentElement, { childList: true, subtree: true });
    return {
        copy: text => navigator.clipboard.writeText(text),
        focus: id => document.getElementById(id)?.focus(),
        restore: url => {
            const previous = new URL(currentUrl);
            const next = new URL(url);
            if (!scrollPositions.has(url) && previous.pathname === next.pathname &&
                previous.searchParams.get('account') === next.searchParams.get('account') && scrollPositions.has(currentUrl))
                scrollPositions.set(url, scrollPositions.get(currentUrl));
            currentUrl = url;
            const position = scrollPositions.get(url);
            if (!position) return;
            const index = document.querySelector('.resource-list');
            const records = document.querySelector('.records');
            if (index) index.scrollTop = position.index;
            if (records) records.scrollTop = position.records;
            window.scrollTo({ top: position.window, behavior: 'instant' });
        }
    };
})();
