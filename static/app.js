/* News Monkey — Dashboard JS (Vanilla) */

(function () {
    'use strict';

    // --- State ---
    const state = {
        events: [],
        filters: {
            time_range: '24h',
            impact: '',
            topic: '',
            geography: '',
            min_sources: 1,
            market_moving: false,
            keyword: '',
            custom_start: null,
            custom_end: null,
            hide_read: true,
        },
        currentView: 'timeline', // 'timeline' | 'detail'
        currentEventId: null,
        ws: null,
        wsReconnectDelay: 1000,
        wsReconnectTimer: null,
        lastUpdate: Date.now(),
        firstLoad: true,
        scrollY: 0,
        feedExpanded: true,
    };

    // --- Read Tracking (localStorage) ---
    const READ_STORAGE_KEY = 'news-monkey-read-events';

    function getReadEvents() {
        try {
            return JSON.parse(localStorage.getItem(READ_STORAGE_KEY) || '[]');
        } catch { return []; }
    }

    function markEventRead(eventId) {
        const read = getReadEvents();
        if (!read.includes(eventId)) {
            read.push(eventId);
            // Keep only last 500 read events to prevent unbounded growth
            if (read.length > 500) read.splice(0, read.length - 500);
            localStorage.setItem(READ_STORAGE_KEY, JSON.stringify(read));
            _invalidateReadCache();
        }
    }

    let _readCache = null;
    function _getReadSet() {
        if (!_readCache) _readCache = new Set(getReadEvents());
        return _readCache;
    }
    function _invalidateReadCache() { _readCache = null; }

    function isEventRead(eventId) {
        return _getReadSet().has(eventId);
    }

    function markAllEventsRead() {
        const read = getReadEvents();
        const newIds = state.events.map(e => e.id).filter(id => !_getReadSet().has(id));
        if (newIds.length === 0) return;
        const combined = [...read, ...newIds];
        // Keep only last 500 read events
        if (combined.length > 500) combined.splice(0, combined.length - 500);
        localStorage.setItem(READ_STORAGE_KEY, JSON.stringify(combined));
        _invalidateReadCache();
    }

    // --- DOM Refs ---
    const $ = (sel) => document.querySelector(sel);
    const $$ = (sel) => document.querySelectorAll(sel);

    // --- Utility ---
    function relativeTime(ts) {
        if (ts == null || isNaN(ts)) return '';
        const seconds = Math.floor((Date.now() / 1000) - ts);
        if (seconds < 60) return 'just now';
        if (seconds < 3600) return Math.floor(seconds / 60) + 'm ago';
        if (seconds < 86400) return Math.floor(seconds / 3600) + 'h ago';
        return Math.floor(seconds / 86400) + 'd ago';
    }

    function absoluteTime(ts) {
        if (ts == null || isNaN(ts)) return '';
        return new Date(ts * 1000).toLocaleString();
    }

    function escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str || '';
        return div.innerHTML;
    }

    function safeHref(url) {
        if (!url) return '';
        const trimmed = url.trim().toLowerCase();
        if (trimmed.startsWith('http://') || trimmed.startsWith('https://')) {
            return escapeHtml(url);
        }
        return '';
    }

    // --- API ---
    let _fetchController = null;

    async function fetchEvents() {
        // Cancel any in-flight request to prevent stale data overwrites
        if (_fetchController) _fetchController.abort();
        _fetchController = new AbortController();
        const signal = _fetchController.signal;

        const params = new URLSearchParams();
        const f = state.filters;
        params.set('time_range', f.time_range);
        if (f.impact) params.set('impact', f.impact);
        if (f.topic) params.set('topic', f.topic);
        if (f.geography) params.set('geography', f.geography);
        if (f.min_sources > 1) params.set('min_sources', f.min_sources);
        if (f.keyword) params.set('keyword', f.keyword);
        if (f.market_moving) params.set('market_moving', 'true');
        if (f.time_range === 'custom') {
            if (f.custom_start) params.set('custom_start', f.custom_start);
            if (f.custom_end) params.set('custom_end', f.custom_end);
        }

        try {
            const res = await fetch('/api/events?' + params.toString(), { signal });
            if (!res.ok) throw new Error('Failed to fetch events');
            const data = await res.json();
            state.events = data.events || [];
            state.lastUpdate = Date.now();
            state.firstLoad = false;
            renderTimeline();
            updateIndicator();
        } catch (err) {
            if (err.name === 'AbortError') return; // superseded by newer request
            console.error('Error fetching events:', err);
            if (state.firstLoad) {
                showFirstLoadError();
            }
        }
    }

    async function fetchEventDetail(eventId) {
        try {
            const res = await fetch('/api/events/' + encodeURIComponent(eventId));
            if (!res.ok) throw new Error('Event not found');
            return await res.json();
        } catch (err) {
            console.error('Error fetching event detail:', err);
            return null;
        }
    }

    // --- Rendering ---
    function showSkeletons() {
        const list = $('#event-list');
        let html = '';
        for (let i = 0; i < 5; i++) {
            html += `
                <div class="skeleton-card">
                    <div class="skeleton-line w60"></div>
                    <div class="skeleton-line w80"></div>
                    <div class="skeleton-line w40"></div>
                </div>`;
        }
        list.innerHTML = html;
    }

    function showFirstLoadError() {
        $('#first-load-state').classList.remove('hidden');
        $('#event-list').classList.add('hidden');
    }

    function renderTimeline() {
        const list = $('#event-list');
        const empty = $('#empty-state');
        const firstLoad = $('#first-load-state');

        firstLoad.classList.add('hidden');

        // Apply read filter client-side
        let displayEvents = state.events;
        if (state.filters.hide_read) {
            displayEvents = displayEvents.filter(e => !isEventRead(e.id));
        }

        if (displayEvents.length === 0) {
            list.classList.add('hidden');
            empty.classList.remove('hidden');
            // Show better message when empty due to read filter
            const emptyTitle = empty.querySelector('h2');
            const emptyMsg = empty.querySelector('p');
            if (state.filters.hide_read && state.events.length > 0) {
                if (emptyTitle) emptyTitle.textContent = 'All items read';
                if (emptyMsg) emptyMsg.textContent = 'All news items have been read. Uncheck "Hide Read Items" to see them again.';
            } else {
                if (emptyTitle) emptyTitle.textContent = 'No events found';
                if (emptyMsg) emptyMsg.textContent = 'Try adjusting your filters or expanding the time range.';
            }
            return;
        }

        empty.classList.add('hidden');
        list.classList.remove('hidden');

        const readCount = state.events.filter(e => isEventRead(e.id)).length;
        let html = '';

        // Collapsible section header (expanded by default per user feedback, preserved across re-renders)
        const expanded = state.feedExpanded;
        const unreadCount = state.events.length - readCount;
        html += `<div class="news-feed-header-row">
            <div class="news-feed-section-header" id="news-feed-toggle" role="button" tabindex="0" aria-expanded="${expanded}">
                <span class="news-feed-collapse-icon">${expanded ? '\u25BC' : '\u25B6'}</span>
                <span class="news-feed-count">${displayEvents.length} News Items${readCount > 0 ? ` (${readCount} read)` : ''}${expanded ? '' : ' (collapsed)'}</span>
            </div>
            ${unreadCount > 0 ? '<button class="btn btn-mark-all-read" id="mark-all-read-btn" title="Mark all items as read">Mark All Read</button>' : ''}
        </div>`;
        html += `<div id="news-feed-items"${expanded ? '' : ' class="hidden"'}>`;

        // Render each event as a compact one-line item with expandable detail
        displayEvents.forEach(event => {
            html += renderNewsItem(event);
        });

        html += '</div>';
        list.innerHTML = html;
    }

    // Event delegation for all news feed interactions (set up once, not per render)
    function initEventListDelegation() {
        const list = $('#event-list');

        list.addEventListener('click', (e) => {
            // Mark All Read button
            const markAllBtn = e.target.closest('#mark-all-read-btn');
            if (markAllBtn) {
                e.stopPropagation();
                markAllEventsRead();
                renderTimeline();
                return;
            }

            // Feed toggle
            const toggle = e.target.closest('#news-feed-toggle');
            if (toggle) {
                const items = list.querySelector('#news-feed-items');
                if (!items) return;
                items.classList.toggle('hidden');
                const expanded = !items.classList.contains('hidden');
                state.feedExpanded = expanded;
                toggle.setAttribute('aria-expanded', expanded);
                const collapseIcon = toggle.querySelector('.news-feed-collapse-icon');
                if (collapseIcon) collapseIcon.textContent = expanded ? '\u25BC' : '\u25B6';
                const displayEvents = state.filters.hide_read
                    ? state.events.filter(ev => !isEventRead(ev.id))
                    : state.events;
                const readCount = state.events.filter(ev => isEventRead(ev.id)).length;
                toggle.querySelector('.news-feed-count').textContent =
                    expanded
                        ? `${displayEvents.length} News Items${readCount > 0 ? ` (${readCount} read)` : ''}`
                        : `${displayEvents.length} News Items${readCount > 0 ? ` (${readCount} read)` : ''} (collapsed)`;
                return;
            }

            // Mark-as-read button
            const markBtn = e.target.closest('.mark-read-btn');
            if (markBtn) {
                e.stopPropagation();
                const eventId = markBtn.dataset.eventId;
                markEventRead(eventId);
                markBtn.classList.add('marked');
                markBtn.innerHTML = '&#10003;';
                markBtn.title = 'Already read';
                markBtn.setAttribute('aria-label', 'Already read');
                const card = markBtn.closest('.event-card');
                if (card) card.classList.add('event-read');
                const readCount = state.events.filter(ev => isEventRead(ev.id)).length;
                const countEl = document.querySelector('.news-feed-count');
                if (countEl) {
                    const displayCount = state.filters.hide_read
                        ? state.events.filter(ev => !isEventRead(ev.id)).length
                        : state.events.length;
                    countEl.textContent = `${displayCount} News Items${readCount > 0 ? ` (${readCount} read)` : ''}`;
                }
                if (state.filters.hide_read && card) card.remove();
                return;
            }

            // Source badge expand
            const badge = e.target.closest('.badge-sources');
            if (badge) {
                e.stopPropagation();
                const card = badge.closest('.event-card');
                const sourceList = card.querySelector('.source-list');
                if (sourceList) {
                    sourceList.classList.toggle('hidden');
                    badge.setAttribute('aria-expanded', String(!sourceList.classList.contains('hidden')));
                } else {
                    expandSources(card.dataset.eventId, badge);
                    badge.setAttribute('aria-expanded', 'true');
                }
                return;
            }

            // Related stories badge toggle
            const relatedBadge = e.target.closest('.badge-related');
            if (relatedBadge) {
                e.stopPropagation();
                const card = relatedBadge.closest('.event-card');
                const relatedList = card.querySelector('.related-stories-list');
                if (relatedList) {
                    relatedList.classList.toggle('hidden');
                    relatedBadge.setAttribute('aria-expanded', String(!relatedList.classList.contains('hidden')));
                }
                return;
            }

            // More/Less toggle
            const moreBtn = e.target.closest('.news-item-more');
            if (moreBtn) {
                e.stopPropagation();
                const wrapper = moreBtn.closest('.news-item-wrapper');
                if (!wrapper) return;
                const detail = wrapper.querySelector('.news-item-detail');
                if (!detail) return;
                detail.classList.toggle('hidden');
                const collapsed = detail.classList.contains('hidden');
                moreBtn.textContent = collapsed ? 'More' : 'Less';
                moreBtn.setAttribute('aria-expanded', String(!collapsed));
                return;
            }

            // Clickable entity tags
            const entityTag = e.target.closest('.clickable-entity');
            if (entityTag) {
                e.stopPropagation();
                const entity = entityTag.dataset.entity;
                if (entity) {
                    state.filters.keyword = entity;
                    $('#keyword-search').value = entity;
                    fetchEvents();
                }
                return;
            }

            // Headline external link — don't trigger detail view
            if (e.target.closest('.headline-link')) return;
            if (e.target.closest('.news-item-detail')) return;
            if (e.target.closest('.related-stories-list')) return;

            // Card click → detail view
            const card = e.target.closest('.event-card');
            if (card) {
                showDetail(card.dataset.eventId);
            }
        });

        list.addEventListener('keydown', (e) => {
            if (e.key !== 'Enter' && e.key !== ' ') return;
            const toggle = e.target.closest('#news-feed-toggle');
            if (toggle) {
                e.preventDefault();
                toggle.click();
                return;
            }
            const moreBtn = e.target.closest('.news-item-more');
            if (moreBtn) {
                e.preventDefault();
                moreBtn.click();
            }
        });
    }

    function renderNewsItem(event) {
        const impactClass = ['high','medium','low'].includes(event.impact) ? event.impact : 'medium';
        const topicTag = event.topic || 'General';
        const read = isEventRead(event.id);
        const readClass = read ? ' event-read' : '';
        const socialBadge = event.social_score > 0
            ? `<span class="badge badge-social" title="Social media coverage">${Math.round(event.social_score)} social</span>`
            : '';

        const confidencePct = event.confidence != null ? Math.round(event.confidence * 100) : null;
        const confidenceBadge = confidencePct != null
            ? `<span class="badge badge-confidence" title="Confidence score: ${confidencePct}%">${confidencePct}% conf</span>`
            : '';

        const impactScoreBadge = event.impact_score != null
            ? `<span class="badge badge-impact-score" title="Computed impact score">${event.impact_score}</span>`
            : '';

        const related = event.related_stories || [];
        const relatedBadge = related.length > 0
            ? `<span class="badge badge-related" role="button" tabindex="0" title="Click to see ${related.length} related stor${related.length === 1 ? 'y' : 'ies'} on the same topic">+${related.length} related</span>`
            : '';

        const entityTags = (event.entities || []).slice(0, 6).map(e =>
            `<span class="entity-tag clickable-entity" data-entity="${escapeHtml(e)}" title="Click to filter by '${escapeHtml(e)}'">${escapeHtml(e)}</span>`
        ).join('');

        const marketOddsBadge = event.market_odds != null
            ? `<span class="badge badge-market-odds" title="Market-implied probability of outcome (0-100%)">Prob. ${Math.round(event.market_odds * 100)}%</span>`
            : '';

        const safeId = escapeHtml(event.id);
        return `
            <article class="news-item-wrapper event-card${readClass}" data-event-id="${safeId}">
                <div class="news-item-row">
                    <button class="mark-read-btn${read ? ' marked' : ''}" data-event-id="${safeId}" title="${read ? 'Already read' : 'Mark as read'}" aria-label="${read ? 'Already read' : 'Mark as read'}">${read ? '&#10003;' : '&#9675;'}</button>
                    <span class="event-timestamp news-item-time" title="${absoluteTime(event.latest_timestamp)}">${relativeTime(event.latest_timestamp)}</span>
                    <h2 class="event-headline news-item-headline">${escapeHtml(event.neutral_headline || event.headline)}</h2>${event.source_url
                        ? `<a href="${safeHref(event.source_url)}" target="_blank" rel="noopener" class="headline-link" title="Open source article">&#8599;</a>`
                        : ''}
                    <span class="badge badge-sources news-item-source" role="button" tabindex="0" aria-expanded="false" title="Click to expand ${event.source_count || 0} deduplicated sources">from ${event.source_count || 0} sources</span>
                    <span class="badge badge-impact ${impactClass}" title="Impact level based on source count, authority, market signals, magnitude, and novelty">${impactClass.charAt(0).toUpperCase() + impactClass.slice(1)} Impact</span>
                    ${marketOddsBadge}
                    <span class="badge badge-topic news-item-tag">${escapeHtml(topicTag)}</span>
                    ${relatedBadge}
                    <span class="news-item-more" role="button" tabindex="0" aria-expanded="true">Less</span>
                </div>
                <div class="news-item-detail">
                    <p class="event-summary news-item-summary">${escapeHtml(event.summary)}</p>
                    <div class="event-meta">
                        ${impactScoreBadge}
                        ${(event.source_count || 0) > 1 ? '<span class="badge badge-dedup" title="Deduplicated event cluster">clustered</span>' : ''}
                        ${socialBadge}
                        ${confidenceBadge}
                        ${entityTags}
                    </div>
                </div>${related.length > 0 ? `
                <div class="related-stories-list hidden">
                    <div class="related-stories-header">Related stories on same topic:</div>
                    ${related.map(r => `<div class="related-story-item">
                        <span class="related-story-headline">${r.source_url ? `<a href="${safeHref(r.source_url)}" target="_blank" rel="noopener">${escapeHtml(r.headline)}</a>` : escapeHtml(r.headline)}</span>
                        <span class="badge badge-impact ${r.impact || 'low'}">${(r.impact || 'low').charAt(0).toUpperCase() + (r.impact || 'low').slice(1)}</span>
                        <span class="related-story-sources">from ${r.source_count || 0} sources</span>
                        <span class="related-story-time">${relativeTime(r.latest_timestamp)}</span>
                    </div>`).join('')}
                </div>` : ''}
            </article>`;
    }

    async function expandSources(eventId, badge) {
        const detail = await fetchEventDetail(eventId);
        if (!detail || !detail.articles) return;

        const card = badge.closest('.event-card');
        if (!card) return;
        const existing = card.querySelector('.source-list');
        if (existing) { existing.remove(); return; }

        // Sort earliest-first; primary = earliest source
        const sorted = [...detail.articles].sort((a, b) => a.timestamp - b.timestamp);
        const sourceHtml = sorted.map((a, idx) => {
            const isPrimary = idx === 0;
            const sourceTypeTag = a.source_type === 'social'
                ? '<span class="source-type-badge social-src">Social</span>'
                : (isPrimary ? '<span class="source-type-badge primary">Primary</span>' : '<span class="source-type-badge derivative">Derivative</span>');
            return `
            <div class="source-item ${isPrimary ? 'source-primary' : 'source-derivative'}">
                <div>
                    <span class="source-title">${a.url ? `<a href="${safeHref(a.url)}" target="_blank" rel="noopener">${escapeHtml(a.title)}</a>` : escapeHtml(a.title)}</span>
                    <span class="source-publisher">${escapeHtml(a.publisher)}</span>
                    <span class="source-time">${relativeTime(a.timestamp)}</span>
                    ${sourceTypeTag}
                </div>
                <span class="source-density">density: ${a.fact_density ? a.fact_density.toFixed(4) : '—'}</span>
            </div>`;
        }).join('');

        const div = document.createElement('div');
        div.className = 'source-list';
        div.innerHTML = sourceHtml;
        card.appendChild(div);
    }

    async function showDetail(eventId) {
        // Validate eventId to prevent selector/path injection
        if (!eventId || !/^[a-zA-Z0-9_-]+$/.test(eventId)) return;

        // Save scroll position for return
        state.scrollY = window.scrollY;
        state.currentView = 'detail';
        state.currentEventId = eventId;

        // Mark event as read
        markEventRead(eventId);
        const card = $(`.event-card[data-event-id="${eventId}"]`);
        if (card && !card.classList.contains('event-read')) {
            card.classList.add('event-read');
            const row = card.querySelector('.news-item-row');
            if (row && !row.querySelector('.read-indicator')) {
                row.insertAdjacentHTML('afterbegin', '<span class="read-indicator" title="Already read">&#10003;</span>');
            }
        }

        $('#timeline-view').classList.add('hidden');
        $('#detail-view').classList.remove('hidden');
        window.scrollTo(0, 0);

        const content = $('#detail-content');
        content.innerHTML = '<div class="skeleton-card"><div class="skeleton-line w80"></div><div class="skeleton-line w60"></div></div>';

        const data = await fetchEventDetail(eventId);
        if (!data) {
            content.innerHTML = '<p>Event not found.</p>';
            return;
        }

        const event = data.event;
        const articles = data.articles || [];
        const claims = data.claims || [];

        // Derive source_url from earliest article if not on the event
        let sourceUrl = event.source_url;
        if (!sourceUrl && articles.length > 0) {
            const earliest = [...articles].sort((a, b) => a.timestamp - b.timestamp)[0];
            if (earliest && earliest.url) sourceUrl = earliest.url;
        }

        let html = '';

        // Header
        const impactClass = ['high','medium','low'].includes(event.impact) ? event.impact : 'medium';
        const impactScoreHtml = event.impact_score != null
            ? `<span class="badge badge-impact-score" title="Computed impact score: source count + authority + market + magnitude + novelty">score ${event.impact_score}</span>`
            : '';
        html += `
            <div class="detail-header">
                <h1 class="detail-headline">${escapeHtml(event.neutral_headline || event.headline)}</h1>
                ${sourceUrl ? `<a href="${safeHref(sourceUrl)}" target="_blank" rel="noopener" class="detail-source-link">Read original article &#8599;</a>` : ''}
                <p class="detail-summary">${escapeHtml(event.summary)}</p>
                <div class="detail-meta">
                    <span class="badge badge-impact ${impactClass}">${impactClass.charAt(0).toUpperCase() + impactClass.slice(1)} Impact</span>
                    ${impactScoreHtml}
                    <span class="badge badge-sources">from ${event.source_count} sources</span>
                    ${event.social_score > 0 ? `<span class="badge badge-social" title="Social media coverage score">${Math.round(event.social_score)} social</span>` : ''}
                    ${event.confidence != null ? `<span class="badge badge-confidence">${Math.round(event.confidence * 100)}% confidence</span>` : ''}
                    <span class="event-timestamp" title="${absoluteTime(event.latest_timestamp)}">Last updated ${relativeTime(event.latest_timestamp)}</span>
                    ${(event.entities || []).map(e => `<span class="entity-tag">${escapeHtml(e)}</span>`).join('')}
                </div>
            </div>`;

        // Social coverage section
        const socialArticles = articles.filter(a => a.source_type === 'social');
        if (socialArticles.length > 0) {
            const totalCoverage = socialArticles.reduce((sum, a) => sum + (a.social_coverage || 0), 0);
            const platforms = [...new Set(socialArticles.map(a => (a.publisher || '').split('/')[0]))];
            html += `
                <div class="social-section detail-collapsible">
                    <h3 class="detail-section-toggle">Social Coverage <span class="collapse-icon">&#9660;</span></h3>
                    <div class="detail-section-body">
                    <div class="social-summary">
                        <span class="social-stat">${socialArticles.length} social source${socialArticles.length > 1 ? 's' : ''}</span>
                        <span class="social-stat">${totalCoverage.toLocaleString()} interactions</span>
                        <span class="social-stat">Platforms: ${platforms.map(p => escapeHtml(p)).join(', ')}</span>
                    </div>
                    </div>
                </div>`;
        }

        // Fact sheet (claims)
        if (claims.length > 0) {
            html += `<div class="fact-sheet detail-collapsible"><h3 class="detail-section-toggle">Verified Facts <span class="collapse-icon">&#9660;</span></h3><div class="detail-section-body">`;
            claims.forEach(c => {
                html += `<div class="fact-row">`;
                if (c.who) html += `<span class="fact-label">Who</span><span>${escapeHtml(c.who)}</span>`;
                html += `</div>`;
                if (c.what) {
                    html += `<div class="fact-row"><span class="fact-label">What</span><span>${escapeHtml(c.what)}</span></div>`;
                }
                if (c.when_occurred) {
                    html += `<div class="fact-row"><span class="fact-label">When</span><span>${escapeHtml(c.when_occurred)}</span></div>`;
                }
                if (c.where_occurred) {
                    html += `<div class="fact-row"><span class="fact-label">Where</span><span>${escapeHtml(c.where_occurred)}</span></div>`;
                }
                if (c.numbers && c.numbers.length > 0) {
                    html += `<div class="fact-row"><span class="fact-label">Numbers</span><span>${c.numbers.map(n => escapeHtml(n)).join(', ')}</span></div>`;
                }
                if (c.direct_quotes && c.direct_quotes.length > 0) {
                    html += `<div class="fact-row fact-quotes"><span class="fact-label">Quotes</span><span>${c.direct_quotes.map(q => `<q>${escapeHtml(q)}</q>`).join(' ')}</span></div>`;
                }
                if (c.uncertainty) {
                    html += `<div class="fact-row"><span class="fact-label">Note</span><span class="uncertainty-note">${escapeHtml(c.uncertainty)}</span></div>`;
                }
            });
            html += `</div></div>`;
        }

        // Development timeline
        const timeline = event.timeline || [];
        if (timeline.length > 0) {
            html += `<div class="timeline-section detail-collapsible"><h3 class="detail-section-toggle">Development Timeline <span class="collapse-icon">&#9660;</span></h3><div class="detail-section-body">`;
            timeline.forEach(t => {
                html += `
                    <div class="timeline-item">
                        <span class="timeline-time">${relativeTime(t.timestamp)}</span>
                        <span class="timeline-text">${escapeHtml(t.text)}</span>
                    </div>`;
            });
            html += `</div></div>`;
        }

        // Disputed claims
        const disputed = event.disputed_claims || [];
        if (disputed.length > 0) {
            html += `<div class="disputed-section detail-collapsible"><h3 class="detail-section-toggle">Disputed Claims <span class="collapse-icon">&#9660;</span></h3><div class="detail-section-body">`;
            disputed.forEach(d => {
                html += `<div class="disputed-row">
                    <div class="disputed-claim-text"><span class="disputed-flag">Disputed</span>${escapeHtml(d.claim || '')}</div>
                    <div class="disputed-contradiction">${escapeHtml(d.contradiction || '')}</div>
                </div>`;
            });
            html += `</div></div>`;
        }

        // Novel facts — diff highlighting for new information
        const novel = event.novel_facts || [];
        if (novel.length > 0) {
            html += `<div class="novel-facts detail-collapsible"><h3 class="detail-section-toggle">What's New <span class="collapse-icon">&#9660;</span></h3><div class="detail-section-body"><ul>`;
            novel.forEach(f => { html += `<li><span class="diff-added">+ ${escapeHtml(f)}</span></li>`; });
            html += `</ul></div></div>`;
        }

        // Source comparison table (sorted earliest-first, primary = earliest)
        if (articles.length > 0) {
            const sortedArticles = [...articles].sort((a, b) => a.timestamp - b.timestamp);
            const hasLowDensity = sortedArticles.some(a => a.low_density);
            html += `
                <div class="source-comparison detail-collapsible">
                    <h3 class="detail-section-toggle">Source Comparison <span class="collapse-icon">&#9660;</span></h3>
                    <div class="detail-section-body">
                    ${hasLowDensity ? '<label class="suppress-toggle"><input type="checkbox" id="suppress-low-density" checked> Suppress low-density articles</label>' : ''}
                    <table class="source-table">
                        <thead>
                            <tr>
                                <th>Publisher</th>
                                <th>Type</th>
                                <th>Headline</th>
                                <th>Time</th>
                                <th>Density</th>
                                <th>Sensationalism</th>
                                <th>Unique Claims</th>
                                <th>Link</th>
                            </tr>
                        </thead>
                        <tbody>`;
            sortedArticles.forEach((a, idx) => {
                const isPrimary = idx === 0;
                const sourceType = isPrimary ? 'primary' : 'derivative';
                const sourceLabel = isPrimary ? '<span class="source-type-badge primary">Primary</span>' : '<span class="source-type-badge derivative">Derivative</span>';
                const sourceTypeTag = a.source_type === 'social'
                    ? '<span class="source-type-badge social-src">Social</span>'
                    : sourceLabel;
                const lowDensityFlag = a.low_density ? '<span class="low-density-flag" title="Low information density — opinion piece or rewrite with no new claims">Low density</span>' : '';
                const sensWarn = a.sensationalism_score > 0.5 ? ' sensationalism-high' : '';
                const neutralRewrite = (a.neutral_title && a.sensationalism_score > 0.3)
                    ? `<div class="neutral-rewrite" title="Neutral rewrite of sensational headline">${escapeHtml(a.neutral_title)}</div>`
                    : '';
                html += `
                    <tr class="source-row ${sourceType}${a.low_density ? ' low-density suppressed' : ''}">
                        <td>${escapeHtml(a.publisher)}</td>
                        <td>${sourceTypeTag}</td>
                        <td>${escapeHtml(a.title)} ${lowDensityFlag}${neutralRewrite}</td>
                        <td>${relativeTime(a.timestamp)}</td>
                        <td>${a.fact_density != null ? a.fact_density.toFixed(4) : '—'}</td>
                        <td class="sensationalism-cell${sensWarn}">${a.sensationalism_score != null ? a.sensationalism_score.toFixed(3) : '—'}</td>
                        <td>${a.unique_claims != null ? a.unique_claims : 0}</td>
                        <td>${a.url ? `<a href="${safeHref(a.url)}" target="_blank" rel="noopener">View</a>` : '<span class="text-muted">—</span>'}</td>
                    </tr>`;
            });
            html += `</tbody></table></div></div>`;
        }

        // Narrative Evolution — how coverage framing changed over time
        const narrativeEvolution = data.narrative_evolution || [];
        if (narrativeEvolution.length > 1) {
            html += `<div class="narrative-evolution detail-collapsible"><h3 class="detail-section-toggle">Narrative Evolution <span class="collapse-icon">&#9660;</span></h3>
                <div class="detail-section-body">
                <p class="section-subtitle">How coverage framing changed over time</p>
                <div class="narrative-timeline">`;
            narrativeEvolution.forEach(entry => {
                const shiftBadge = entry.framing_shift
                    ? `<span class="badge badge-framing-shift ${entry.framing_shift === 'more sensational' ? 'shift-sensational' : 'shift-neutral'}">${escapeHtml(entry.framing_shift)}</span>`
                    : '';
                html += `
                    <div class="narrative-entry">
                        <div class="narrative-time">${relativeTime(entry.timestamp)}</div>
                        <div class="narrative-publisher">${escapeHtml(entry.publisher)}</div>
                        <div class="narrative-headline">${escapeHtml(entry.headline)}</div>
                        <div class="narrative-meta">
                            <span class="badge badge-source-type">${escapeHtml(entry.source_type)}</span>
                            <span class="sensationalism-mini">sens: ${entry.sensationalism_score != null ? entry.sensationalism_score.toFixed(2) : '—'}</span>
                            ${shiftBadge}
                        </div>
                    </div>`;
            });
            html += `</div></div></div>`;
        }

        // Publisher Bias Comparison
        const publisherBias = data.publisher_bias || [];
        if (publisherBias.length > 1) {
            html += `<div class="publisher-bias detail-collapsible"><h3 class="detail-section-toggle">Publisher Bias Comparison <span class="collapse-icon">&#9660;</span></h3>
                <div class="detail-section-body">
                <p class="section-subtitle">Sensationalism and source type by outlet</p>
                <table class="bias-table"><thead><tr>
                    <th>Publisher</th><th>Articles</th><th>Avg Sensationalism</th><th>Primary Source</th><th>Types</th>
                </tr></thead><tbody>`;
            publisherBias.forEach(b => {
                const avgSens = b.avg_sensationalism != null ? b.avg_sensationalism : 0;
                const sensClass = avgSens > 0.5 ? ' high-sens' : avgSens > 0.3 ? ' med-sens' : '';
                const types = Array.isArray(b.source_types) ? b.source_types : [];
                html += `<tr class="bias-row${sensClass}">
                    <td>${escapeHtml(b.publisher)}</td>
                    <td>${b.article_count}</td>
                    <td>${avgSens.toFixed(3)}</td>
                    <td>${b.is_primary_source ? '<span class="badge badge-primary">Primary</span>' : 'Derivative'}</td>
                    <td>${types.map(t => `<span class="badge badge-source-type">${escapeHtml(t)}</span>`).join(' ')}</td>
                </tr>`;
            });
            html += `</tbody></table></div></div>`;
        }

        content.innerHTML = html;

        // Attach collapsible toggles for detail sections
        content.querySelectorAll('.detail-section-toggle').forEach(toggle => {
            toggle.style.cursor = 'pointer';
            toggle.setAttribute('role', 'button');
            toggle.setAttribute('tabindex', '0');
            toggle.setAttribute('aria-expanded', 'true');
            const handler = () => {
                const body = toggle.nextElementSibling;
                if (body && body.classList.contains('detail-section-body')) {
                    body.classList.toggle('collapsed');
                    const collapsed = body.classList.contains('collapsed');
                    const icon = toggle.querySelector('.collapse-icon');
                    if (icon) icon.textContent = collapsed ? '\u25B6' : '\u25BC';
                    toggle.setAttribute('aria-expanded', !collapsed);
                }
            };
            toggle.addEventListener('click', handler);
            toggle.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); handler(); }
            });
        });

        // Low-density suppression toggle
        const suppressToggle = content.querySelector('#suppress-low-density');
        if (suppressToggle) {
            suppressToggle.addEventListener('change', (e) => {
                content.querySelectorAll('.source-row.low-density').forEach(row => {
                    row.classList.toggle('suppressed', e.target.checked);
                });
            });
        }

    }

    function showTimeline() {
        state.currentView = 'timeline';
        state.currentEventId = null;
        $('#detail-view').classList.add('hidden');
        $('#timeline-view').classList.remove('hidden');
        // Restore scroll position
        window.scrollTo(0, state.scrollY);
    }

    function updateIndicator() {
        const seconds = Math.floor((Date.now() - state.lastUpdate) / 1000);
        let text = 'updated just now';
        if (seconds > 60) text = `updated ${Math.floor(seconds / 60)}m ago`;
        if (seconds > 3600) text = `updated ${Math.floor(seconds / 3600)}h ago`;
        $('#update-indicator').textContent = text;
    }

    // --- WebSocket ---
    function connectWS() {
        // Guard against duplicate connections
        if (state.ws && state.ws.readyState <= WebSocket.OPEN) return;

        const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
        const ws = new WebSocket(protocol + '//' + location.host + '/ws');

        ws.onopen = () => {
            state.ws = ws;
            state.wsReconnectDelay = 1000;
            const indicator = $('#ws-status');
            indicator.className = 'ws-status connected';
            indicator.title = 'Live updates connected';
        };

        ws.onmessage = (e) => {
            try {
                const msg = JSON.parse(e.data);
                handleWSMessage(msg);
            } catch (err) {
                console.error('WS parse error:', err);
            }
        };

        ws.onclose = () => {
            state.ws = null;
            const indicator = $('#ws-status');
            indicator.className = 'ws-status disconnected';
            indicator.title = 'Disconnected — reconnecting...';

            // Clear any pending reconnect timer
            if (state.wsReconnectTimer) clearTimeout(state.wsReconnectTimer);
            state.wsReconnectTimer = setTimeout(() => {
                indicator.className = 'ws-status reconnecting';
                connectWS();
            }, state.wsReconnectDelay);

            state.wsReconnectDelay = Math.min(state.wsReconnectDelay * 2, 30000);
        };

        ws.onerror = () => {
            ws.close();
        };
    }

    function handleWSMessage(msg) {
        switch (msg.type) {
            case 'event_created':
                fetchEvents();
                break;
            case 'article_added':
                if (state.currentView === 'detail' && state.currentEventId === msg.event_id) {
                    showDetail(msg.event_id);
                }
                fetchEvents();
                break;
            case 'pong':
                break;
            default:
                console.log('Unknown WS message type:', msg.type);
        }
    }

    // --- Filters (auto-apply on change — no Apply button needed) ---
    let _keywordTimer = null;

    function applyFilters() {
        if (state.filters.time_range === 'custom') {
            const startEl = $('#custom-start');
            const endEl = $('#custom-end');
            if (startEl.value) state.filters.custom_start = new Date(startEl.value).getTime() / 1000;
            if (endEl.value) state.filters.custom_end = new Date(endEl.value).getTime() / 1000;
            if (state.filters.custom_start && state.filters.custom_end && state.filters.custom_start > state.filters.custom_end) {
                const tmp = state.filters.custom_start;
                state.filters.custom_start = state.filters.custom_end;
                state.filters.custom_end = tmp;
            }
        }
        fetchEvents();
    }

    function initFilters() {
        $('#time-range').addEventListener('change', (e) => {
            state.filters.time_range = e.target.value;
            const customRange = $('#custom-range');
            if (e.target.value === 'custom') {
                customRange.classList.remove('hidden');
            } else {
                customRange.classList.add('hidden');
                applyFilters();
            }
        });

        $('#impact-filter').addEventListener('change', (e) => {
            state.filters.impact = e.target.value;
            applyFilters();
        });

        $('#topic-filter').addEventListener('change', (e) => {
            state.filters.topic = e.target.value;
            // Sync quick-filter buttons
            $$('.quick-filter').forEach(b => b.classList.remove('active'));
            const matching = [...$$('.quick-filter')].find(b => b.dataset.filter === e.target.value);
            if (matching) matching.classList.add('active');
            applyFilters();
        });

        $('#geography-filter').addEventListener('change', (e) => {
            state.filters.geography = e.target.value;
            applyFilters();
        });

        $('#min-sources').addEventListener('change', (e) => {
            state.filters.min_sources = parseInt(e.target.value) || 1;
            applyFilters();
        });

        $('#market-moving').addEventListener('change', (e) => {
            state.filters.market_moving = e.target.checked;
            applyFilters();
        });

        // Hide read toggle
        const hideReadEl = $('#hide-read');
        if (hideReadEl) {
            hideReadEl.addEventListener('change', (e) => {
                state.filters.hide_read = e.target.checked;
                renderTimeline();
            });
        }

        // Debounced keyword search — auto-applies after 400ms of no typing
        $('#keyword-search').addEventListener('input', (e) => {
            state.filters.keyword = e.target.value.trim();
            clearTimeout(_keywordTimer);
            _keywordTimer = setTimeout(applyFilters, 400);
        });

        // Custom date range: apply when either date changes
        $('#custom-start').addEventListener('change', applyFilters);
        $('#custom-end').addEventListener('change', applyFilters);

        $('#reset-filters').addEventListener('click', resetFilters);
        $('#reset-from-empty').addEventListener('click', resetFilters);

        // Sidebar toggle for mobile
        $('#filter-toggle').addEventListener('click', () => {
            const sidebar = $('#sidebar');
            sidebar.classList.toggle('open');
            const btn = $('#filter-toggle');
            btn.setAttribute('aria-expanded', String(sidebar.classList.contains('open')));
        });
    }

    function resetFilters() {
        state.filters = {
            time_range: '24h',
            impact: '',
            topic: '',
            geography: '',
            min_sources: 1,
            market_moving: false,
            keyword: '',
            custom_start: null,
            custom_end: null,
            hide_read: true,
        };
        $('#time-range').value = '24h';
        $('#impact-filter').value = '';
        $('#topic-filter').value = '';
        $('#geography-filter').value = '';
        $('#min-sources').value = '1';
        $('#market-moving').checked = false;
        $('#keyword-search').value = '';
        $('#custom-range').classList.add('hidden');
        const hideReadEl = $('#hide-read');
        if (hideReadEl) hideReadEl.checked = true;
        fetchEvents();
    }

    // --- Quick Filters ---
    function initQuickFilters() {
        $$('.quick-filter').forEach(btn => {
            btn.addEventListener('click', () => {
                $$('.quick-filter').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                const topic = btn.dataset.filter;
                state.filters.topic = topic;
                $('#topic-filter').value = topic;
                fetchEvents();
            });
        });
    }

    // --- Navigation ---
    function initNavigation() {
        $('#back-to-timeline').addEventListener('click', showTimeline);
    }

    // --- Init ---
    function init() {
        showSkeletons();
        initFilters();
        initQuickFilters();
        initNavigation();
        initEventListDelegation();
        fetchEvents();
        connectWS();

        // Handle hash-based deep links (e.g., /#event-{id} from markets page)
        const hash = location.hash;
        if (hash && hash.startsWith('#event-')) {
            const eventId = hash.replace('#event-', '');
            // Validate eventId is alphanumeric/UUID to prevent selector injection
            if (eventId && /^[a-zA-Z0-9_-]+$/.test(eventId)) {
                setTimeout(() => showDetail(eventId), 500);
            }
        }

        // Clean up WebSocket and intervals on page unload
        window.addEventListener('beforeunload', () => {
            if (state.wsReconnectTimer) clearTimeout(state.wsReconnectTimer);
            if (state._indicatorInterval) clearInterval(state._indicatorInterval);
            if (state._fetchInterval) clearInterval(state._fetchInterval);
            if (state._pingInterval) clearInterval(state._pingInterval);
            if (state.ws) { state.ws.onclose = null; state.ws.close(); }
        });

        // Update indicator periodically
        state._indicatorInterval = setInterval(updateIndicator, 10000);

        // Auto-refresh events every 60s
        state._fetchInterval = setInterval(fetchEvents, 60000);

        // WS keepalive ping every 30s
        state._pingInterval = setInterval(() => {
            if (state.ws && state.ws.readyState === WebSocket.OPEN) {
                state.ws.send('ping');
            }
        }, 30000);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
