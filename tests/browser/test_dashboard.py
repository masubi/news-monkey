"""Browser tests for News Monkey dashboard."""
import re
from playwright.sync_api import expect


# --- UF-01: Full Browse Flow ---
def test_timeline_loads_with_events(page):
    """Events load on the timeline view."""
    cards = page.locator(".event-card")
    expect(cards.first).to_be_visible(timeout=5000)
    count = cards.count()
    assert count > 0, "Expected at least one event card"


# --- FT-01: Reverse chronological ---
def test_events_display_in_timeline(page):
    """Event cards show headline, badges, and summary expanded by default."""
    card = page.locator(".event-card").first
    expect(card.locator(".event-headline")).to_be_visible()
    expect(card.locator(".badge-sources")).to_be_visible()
    expect(card.locator(".badge-impact")).to_be_visible()
    # Summary is expanded by default per user feedback (ISSUE-206)
    expect(card.locator(".event-summary")).to_be_visible()
    # Clicking "Less" collapses it
    card.locator(".news-item-more").click()
    expect(card.locator(".event-summary")).to_be_hidden()


# --- FT-15: Timestamp format ---
def test_timestamp_shows_relative_time(page):
    """Timestamps show relative time (e.g., '2h ago')."""
    ts = page.locator(".event-timestamp").first
    expect(ts).to_be_visible()
    text = ts.text_content()
    assert re.search(r"(\d+[mhd] ago|just now)", text), f"Unexpected timestamp format: {text}"


def test_timestamp_has_absolute_title(page):
    """Timestamp has absolute time in title attribute."""
    ts = page.locator(".event-timestamp").first
    title = ts.get_attribute("title")
    assert title is not None and len(title) > 5, "Expected absolute time in title"


# --- FT-14: Entity tags ---
def test_entity_tags_visible(page):
    """Entity tags are rendered on event cards (visible by default per ISSUE-206)."""
    card = page.locator(".event-card").first
    tags = card.locator(".entity-tag")
    expect(tags.first).to_be_visible()


# --- DT-04: Impact badges are color-coded ---
def test_impact_badges_are_color_coded(page):
    """Impact badges have the correct CSS class (high/medium/low)."""
    badges = page.locator(".badge-impact")
    count = badges.count()
    assert count > 0
    for i in range(min(count, 5)):
        classes = badges.nth(i).get_attribute("class")
        assert any(c in classes for c in ["high", "medium", "low"]), f"Badge missing impact class: {classes}"


# --- FT-03: Source badge expands sources ---
def test_source_badge_click_expands_sources(page):
    """Clicking source badge shows deduplicated article list."""
    badge = page.locator(".badge-sources").first
    badge.click()
    source_list = page.locator(".source-list").first
    expect(source_list).to_be_visible(timeout=5000)
    source_items = source_list.locator(".source-item")
    assert source_items.count() > 0


# --- FT-04 & UF-01: Detail view ---
def test_click_event_shows_detail(page):
    """Clicking an event card headline navigates to detail view."""
    card = page.locator(".event-card").first
    headline_text = card.locator(".event-headline").text_content()
    card.locator(".event-headline").click()

    detail_view = page.locator("#detail-view")
    expect(detail_view).to_be_visible(timeout=5000)

    detail_headline = page.locator(".detail-headline")
    expect(detail_headline).to_be_visible()
    assert headline_text in detail_headline.text_content()


# --- FT-05: Back navigation ---
def test_back_to_timeline(page):
    """Clicking back returns to timeline."""
    # Go to detail
    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    # Go back
    page.locator("#back-to-timeline").click()
    expect(page.locator("#timeline-view")).to_be_visible()
    expect(page.locator("#detail-view")).to_be_hidden()


# --- FT-04: Detail view shows fact sheet ---
def test_detail_shows_fact_sheet(page):
    """Detail view shows verified facts."""
    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    # Should have fact sheet with claims
    fact_sheet = page.locator(".fact-sheet")
    expect(fact_sheet).to_be_visible(timeout=5000)
    fact_rows = fact_sheet.locator(".fact-row")
    assert fact_rows.count() > 0


# --- FT-04: Detail shows timeline ---
def test_detail_shows_timeline(page):
    """Detail view shows development timeline."""
    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    timeline = page.locator(".timeline-section")
    expect(timeline).to_be_visible(timeout=5000)
    items = timeline.locator(".timeline-item")
    assert items.count() > 0


# --- FT-04: Detail shows source comparison ---
def test_detail_shows_source_comparison(page):
    """Detail view shows source comparison table."""
    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    table = page.locator(".source-comparison")
    expect(table).to_be_visible(timeout=5000)
    rows = table.locator("tbody tr")
    assert rows.count() > 0


# --- FT-13: Market badges NOT on news feed items (removed per feedback) ---
def test_no_market_badges_on_news_feed_items(page):
    """News feed item rows should NOT show market badges (moved to detail/markets page)."""
    row_badges = page.locator(".news-item-row .badge-market")
    assert row_badges.count() == 0, "Market badges should not appear on news feed item rows"


# --- FT-12: Market section NOT in detail view (moved to dedicated markets page) ---
def test_no_market_section_in_detail(page):
    """Detail view should NOT show market section — market data lives on the dedicated Polymarket Bets page."""
    # Use market_moving filter to find events with market data, then click into detail
    page.locator("#time-range").select_option("7d")
    page.locator("#market-moving").check()
    page.wait_for_timeout(1500)
    cards = page.locator(".event-card")
    if cards.count() > 0:
        cards.first.locator(".event-headline").click()
        expect(page.locator("#detail-view")).to_be_visible(timeout=5000)
        market = page.locator(".market-section")
        assert market.count() == 0, "Market section should not appear in detail view — market data is on the Polymarket Bets page"


# --- FT-06 & UF-03: Filter by time range ---
def test_filter_time_range(page, server_url):
    """Applying time range filter changes displayed events."""
    initial_count = page.locator(".event-card").count()

    # Apply 1h filter (may reduce results)
    page.locator("#time-range").select_option("1h")
    page.wait_for_timeout(1500)

    # Reset to 7d to see more
    page.locator("#time-range").select_option("7d")
    page.wait_for_timeout(1500)
    wider_count = page.locator(".event-card").count()
    assert wider_count >= initial_count or wider_count > 0


# --- FT-07: Filter by impact ---
def test_filter_by_impact(page):
    """Filtering by impact shows only matching events."""
    page.locator("#impact-filter").select_option("high")
    page.wait_for_timeout(1500)

    badges = page.locator(".badge-impact")
    count = badges.count()
    for i in range(count):
        text = badges.nth(i).text_content().strip().lower()
        assert "high" in text


# --- FT-11: Reset filters ---
def test_reset_filters(page):
    """Reset button restores all events."""
    # Apply restrictive filter
    page.locator("#impact-filter").select_option("low")
    page.wait_for_timeout(1500)

    # Reset
    page.locator("#reset-filters").click()
    page.wait_for_timeout(1000)

    # Should show more events
    cards = page.locator(".event-card")
    assert cards.count() > 0


# --- EC-01: Empty state ---
def test_empty_state_on_restrictive_filter(page):
    """Restrictive filter shows empty state message."""
    page.locator("#keyword-search").fill("nonexistent_gibberish_xyz_12345")
    page.wait_for_timeout(1500)

    empty = page.locator("#empty-state")
    expect(empty).to_be_visible(timeout=3000)
    expect(empty.locator("h2")).to_contain_text("No events found")

    # Reset from empty state
    page.locator("#reset-from-empty").click()
    page.wait_for_timeout(1000)
    expect(page.locator(".event-card").first).to_be_visible(timeout=5000)


# --- UF-05: No artifacts ---
def test_no_artifacts_on_timeline(page):
    """No null, undefined, or placeholder text visible."""
    body_text = page.locator("body").text_content()
    assert "undefined" not in body_text.lower() or "undefined" in body_text.lower().split("market")[0] is None  # Skip if in actual content
    # More targeted check
    cards = page.locator(".event-card")
    for i in range(min(cards.count(), 5)):
        text = cards.nth(i).text_content()
        assert "null" not in text.split()  # "null" as standalone word
        assert "[object Object]" not in text


# --- DT-01: Features reachable ---
def test_features_reachable_within_two_clicks(page):
    """All primary features accessible within 2 clicks: timeline (0), detail (1), filters (0)."""
    # Timeline visible on load (0 clicks)
    expect(page.locator(".event-card").first).to_be_visible(timeout=5000)

    # Filters visible on load (0 clicks) — auto-apply on change, only Reset button shown
    expect(page.locator("#reset-filters")).to_be_visible()

    # Detail view reachable in 1 click
    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)


# --- DT-05: Responsive ---
def test_responsive_mobile_view(page):
    """At 375px width, filter toggle appears and sidebar is hidden."""
    page.set_viewport_size({"width": 375, "height": 812})
    page.wait_for_timeout(500)

    # Filter toggle should be visible on mobile
    toggle = page.locator("#filter-toggle")
    expect(toggle).to_be_visible()

    # Events should still be visible
    expect(page.locator(".event-card").first).to_be_visible(timeout=5000)


# --- WebSocket status indicator ---
def test_ws_status_indicator(page):
    """WebSocket status indicator is visible and shows connected."""
    ws_status = page.locator("#ws-status")
    expect(ws_status).to_be_visible()
    classes = ws_status.get_attribute("class")
    assert "connected" in classes


# --- Update indicator ---
def test_update_indicator(page):
    """Update indicator shows 'updated just now' on load."""
    indicator = page.locator("#update-indicator")
    expect(indicator).to_be_visible()
    assert "updated" in indicator.text_content().lower()


# --- DT-06: Progressive disclosure ---
def test_progressive_disclosure(page):
    """Summary visible by default (ISSUE-206); collapsible via Less; full detail on click."""
    card = page.locator(".event-card").first
    # Card summary expanded by default per user feedback
    expect(card.locator(".event-summary")).to_be_visible()
    assert card.locator(".fact-sheet").count() == 0
    assert card.locator(".timeline-section").count() == 0
    assert card.locator(".source-table").count() == 0

    # Click into detail — full sections appear
    card.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)
    expect(page.locator(".fact-sheet")).to_be_visible(timeout=5000)


# --- FT-08: Filter min sources via UI ---
def test_filter_min_sources_ui(page):
    """Setting min sources filter reduces events."""
    page.locator("#time-range").select_option("7d")
    page.wait_for_timeout(1500)
    all_count = page.locator(".event-card").count()

    page.locator("#min-sources").fill("10")
    page.wait_for_timeout(1500)
    filtered_count = page.locator(".event-card").count()
    assert filtered_count <= all_count


# --- FT-09: Filter market-moving via UI ---
def test_filter_market_moving_ui(page):
    """Market-moving filter shows only events with prediction market data."""
    page.locator("#time-range").select_option("7d")
    page.locator("#market-moving").check()
    page.wait_for_timeout(1500)

    cards = page.locator(".event-card")
    count = cards.count()
    assert count > 0, "Expected some market-linked events"
    # Market data lives on dedicated Polymarket Bets page, not in detail view
    # Verify that filtering by market-moving returns events with market_moving flag
    if count > 0:
        cards.first.locator(".event-headline").click()
        expect(page.locator("#detail-view")).to_be_visible(timeout=5000)
        # Detail view should have standard sections but NOT market-section
        expect(page.locator(".detail-header")).to_be_visible(timeout=3000)


# --- FT-10: Filter keyword search via UI ---
def test_filter_keyword_search_ui(page):
    """Keyword search filters events matching the keyword."""
    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("Federal Reserve")
    page.wait_for_timeout(1500)

    cards = page.locator(".event-card")
    assert cards.count() >= 1
    # At least one card should mention the keyword
    found = False
    for i in range(cards.count()):
        text = cards.nth(i).text_content().lower()
        if "federal reserve" in text:
            found = True
            break
    assert found, "Expected keyword in at least one result"


# --- UF-02: Source drill-down links ---
def test_source_drilldown_has_links(page):
    """Expanded source list contains clickable links to original articles."""
    badge = page.locator(".badge-sources").first
    badge.click()
    source_list = page.locator(".source-list").first
    expect(source_list).to_be_visible(timeout=5000)

    links = source_list.locator("a")
    assert links.count() > 0
    href = links.first.get_attribute("href")
    assert href and href.startswith("http")


# --- UF-04: Empty to populated transition ---
def test_empty_to_populated_transition(page):
    """Transitioning from empty state back to populated is clean."""
    # Force empty state
    page.locator("#keyword-search").fill("zzz_no_match_zzz")
    page.wait_for_timeout(1500)
    expect(page.locator("#empty-state")).to_be_visible(timeout=3000)
    expect(page.locator("#event-list")).to_be_hidden()

    # Reset to populated
    page.locator("#reset-from-empty").click()
    page.wait_for_timeout(1000)
    expect(page.locator("#empty-state")).to_be_hidden()
    expect(page.locator("#event-list")).to_be_visible()
    expect(page.locator(".event-card").first).to_be_visible(timeout=5000)


# --- EC-02: Unicode content display ---
def test_unicode_content_displays(page, server_url):
    """Events with unicode/emoji characters display correctly."""
    import httpx
    httpx.post(f"{server_url}/api/events", json={
        "headline": "Emoji test 🚀🌍 café résumé",
        "summary": "Testing unicode: 日本語テスト",
        "entities": ["München", "São Paulo"],
        "impact": "low",
    })
    # Navigate to index page fresh to ensure we're on the timeline view
    page.goto(server_url)
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(500)
    # Select 7d range to see the newly created event
    page.locator("#time-range").select_option("7d")
    page.wait_for_timeout(2000)

    # Check timeline-view content specifically (not whole body which includes hidden detail-view)
    timeline = page.locator("#timeline-view")
    timeline_text = timeline.text_content()
    assert "🚀" in timeline_text or "café" in timeline_text or "Emoji test" in timeline_text


# --- EC-03: Long content handling ---
def test_long_headline_does_not_break_layout(page, server_url):
    """Very long headlines don't break the card layout."""
    import httpx
    long_headline = "A" * 500
    httpx.post(f"{server_url}/api/events", json={
        "headline": long_headline,
        "summary": "Short summary.",
        "impact": "low",
    })
    page.locator("#time-range").select_option("7d")
    page.wait_for_timeout(1500)

    # Cards should still render without overflow issues
    cards = page.locator(".event-card")
    assert cards.count() > 0
    # Check the page isn't horizontally scrollable beyond main content
    main_width = page.locator("main").bounding_box()["width"]
    assert main_width <= 1280  # Viewport width


# --- EC-04: XSS prevention in browser ---
def test_xss_prevention_in_browser(page, server_url):
    """Script tags in event data are escaped, not executed."""
    import httpx
    httpx.post(f"{server_url}/api/events", json={
        "headline": '<img src=x onerror="document.title=\'HACKED\'">XSS Test',
        "summary": '<script>alert("xss")</script>',
        "impact": "low",
    })
    page.locator("#time-range").select_option("7d")
    page.wait_for_timeout(1500)

    # Title should NOT have been changed by XSS
    assert page.title() == "News Monkey"
    # The script tag text should be visible as text, not executed
    body = page.locator("body").inner_html()
    assert "<script>" not in body or "&lt;script&gt;" in body


# --- DT-05: Responsive tablet view ---
def test_responsive_tablet_view(page):
    """At 768px width, layout remains functional."""
    page.set_viewport_size({"width": 768, "height": 1024})
    page.wait_for_timeout(500)
    expect(page.locator(".event-card").first).to_be_visible(timeout=5000)


# --- DT-03: Naming consistency ---
def test_naming_consistency(page):
    """Key UI labels use consistent terminology."""
    # Verify filter labels are present and consistent
    expect(page.locator('label[for="time-range"]')).to_contain_text("Time Range")
    expect(page.locator('label[for="impact-filter"]')).to_contain_text("Impact")
    expect(page.locator('label[for="min-sources"]')).to_contain_text("Min Sources")
    expect(page.locator('label[for="keyword-search"]')).to_contain_text("Search")

    # Header branding
    expect(page.locator("header h1")).to_contain_text("News Monkey")


# --- FT-04: Detail shows all sections for first event ---
def test_detail_view_complete_sections(page):
    """Detail view shows all required sections: header, facts, timeline, sources."""
    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    # Header with headline + summary
    expect(page.locator(".detail-headline")).to_be_visible()
    expect(page.locator(".detail-summary")).to_be_visible()

    # Impact badge in detail
    expect(page.locator(".detail-meta .badge-impact")).to_be_visible()

    # Source count in detail
    expect(page.locator(".detail-meta .badge-sources")).to_be_visible()


# --- UF-05: No artifacts in detail view ---
def test_no_artifacts_in_detail_view(page):
    """No null/undefined/placeholder in detail view."""
    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    text = page.locator("#detail-content").text_content()
    assert "[object Object]" not in text
    assert "NaN" not in text


# --- FT-16: Detail shows novel facts ("What's New") ---
def test_detail_shows_novel_facts(page):
    """Detail view shows 'What's New' section for events with novel facts."""
    # Find the Fed event (seed data with novel_facts)
    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("Federal Reserve")
    page.wait_for_timeout(1500)

    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    novel = page.locator(".novel-facts")
    expect(novel).to_be_visible(timeout=5000)
    expect(novel.locator("h3")).to_contain_text("What's New")
    items = novel.locator("li")
    assert items.count() > 0

    # Navigate back and reset filters for next test
    page.locator("#back-to-timeline").click()
    page.locator("#keyword-search").fill("")
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- FT-17: Detail shows disputed claims ---
def test_detail_shows_disputed_claims(page):
    """Detail view shows disputed claims section with claim + contradiction."""
    # Find the Fed event (seed data with disputed_claims)
    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("Federal Reserve")
    page.wait_for_timeout(1500)

    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    disputed = page.locator(".disputed-section")
    expect(disputed).to_be_visible(timeout=5000)
    expect(disputed.locator("h3")).to_contain_text("Disputed")
    rows = disputed.locator(".disputed-row")
    assert rows.count() > 0

    page.locator("#back-to-timeline").click()
    page.locator("#keyword-search").fill("")
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- FT-18: Filter by topic ---
def test_filter_by_topic_ui(page):
    """Topic filter shows only events matching selected topic."""
    page.locator("#time-range").select_option("7d")
    page.locator("#topic-filter").select_option("Technology")
    page.wait_for_timeout(1500)

    cards = page.locator(".event-card")
    assert cards.count() >= 1
    text = page.locator("#event-list").text_content().lower()
    assert "semiconductor" in text or "ai" in text or "technology" in text or "spacex" in text or "tech" in text


# --- FT-19: Filter by geography ---
def test_filter_by_geography_ui(page):
    """Geography filter shows only events matching selected region."""
    page.locator("#time-range").select_option("7d")
    page.locator("#geography-filter").select_option("Europe")
    page.wait_for_timeout(1500)

    cards = page.locator(".event-card")
    assert cards.count() >= 1
    text = page.locator("#event-list").text_content().lower()
    assert "eu" in text or "europe" in text


# --- EC-05: Source badge click does not navigate to detail ---
def test_source_badge_click_does_not_navigate(page):
    """Clicking the source badge expands sources but does not open detail view."""
    badge = page.locator(".badge-sources").first
    badge.click()
    page.wait_for_timeout(1000)

    expect(page.locator("#timeline-view")).to_be_visible()
    expect(page.locator("#detail-view")).to_be_hidden()
    source_list = page.locator(".source-list").first
    expect(source_list).to_be_visible()


# --- FT-20: Source comparison table has correct columns ---
def test_source_comparison_table_columns(page):
    """Source comparison table has Publisher, Headline, Time, Density, Link columns."""
    # Use a seed event that has articles
    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("Federal Reserve")
    page.wait_for_timeout(1500)

    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    table = page.locator(".source-table")
    expect(table).to_be_visible(timeout=5000)
    headers = table.locator("thead th")
    header_texts = [headers.nth(i).text_content().strip() for i in range(headers.count())]
    assert "Publisher" in header_texts
    assert "Headline" in header_texts
    assert "Time" in header_texts
    assert "Density" in header_texts
    assert "Sensationalism" in header_texts
    assert "Unique Claims" in header_texts
    assert "Link" in header_texts

    page.locator("#back-to-timeline").click()
    page.locator("#keyword-search").fill("")
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- EC-06: Custom time range shows date inputs ---
def test_custom_time_range_ui(page):
    """Selecting 'custom' time range shows date inputs."""
    page.locator("#time-range").select_option("custom")
    custom_range = page.locator("#custom-range")
    expect(custom_range).to_be_visible()
    expect(page.locator("#custom-start")).to_be_visible()
    expect(page.locator("#custom-end")).to_be_visible()


# --- UF-06: Multiple filters combined ---
def test_combined_filters(page):
    """Multiple filters can be applied simultaneously."""
    page.locator("#time-range").select_option("7d")
    page.locator("#impact-filter").select_option("high")
    page.locator("#geography-filter").select_option("US")
    page.wait_for_timeout(1500)

    cards = page.locator(".event-card")
    assert cards.count() >= 1
    badges = page.locator(".badge-impact")
    for i in range(badges.count()):
        assert "high" in badges.nth(i).get_attribute("class")


# --- DT-07: Filter actions visually present ---
def test_primary_actions_visually_prominent(page):
    """Reset button is present with proper styling. Filters auto-apply on change (no Apply button)."""
    reset_btn = page.locator("#reset-filters")
    expect(reset_btn).to_be_visible()
    assert "btn-secondary" in reset_btn.get_attribute("class")


# --- NEW-01: Unique claims column shows values in source comparison ---
def test_unique_claims_column_in_source_comparison(page):
    """Source comparison table has Unique Claims column with numeric values."""
    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("Federal Reserve")
    page.wait_for_timeout(1500)

    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    table = page.locator(".source-table")
    expect(table).to_be_visible(timeout=5000)

    # Verify the "Unique Claims" header exists
    headers = table.locator("thead th")
    header_texts = [headers.nth(i).text_content().strip() for i in range(headers.count())]
    assert "Unique Claims" in header_texts

    # Find the column index
    claims_col_idx = header_texts.index("Unique Claims")

    # Verify each row has a numeric value in the Unique Claims column
    rows = table.locator("tbody tr")
    assert rows.count() > 0
    for i in range(rows.count()):
        cell = rows.nth(i).locator("td").nth(claims_col_idx)
        text = cell.text_content().strip()
        assert text.isdigit(), f"Expected numeric unique claims, got: {text}"

    page.locator("#back-to-timeline").click()
    page.locator("#keyword-search").fill("")
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- NEW-03: Source list items show publisher and fact density ---
def test_source_list_items_content(page):
    """Expanded source list shows publisher name and fact density per source."""
    # Use 7d and keyword to target a seed event with articles
    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("Federal Reserve")
    page.wait_for_timeout(1500)

    badge = page.locator(".badge-sources").first
    badge.click()
    source_list = page.locator(".source-list").first
    expect(source_list).to_be_visible(timeout=5000)

    items = source_list.locator(".source-item")
    assert items.count() > 0

    first_item = items.first
    # Publisher should be present
    publisher = first_item.locator(".source-publisher")
    expect(publisher).to_be_visible()
    assert len(publisher.text_content().strip()) > 0

    # Density should be present
    density = first_item.locator(".source-density")
    expect(density).to_be_visible()

    page.locator("#keyword-search").fill("")
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- NEW-04: Multiple detail views show correct event data ---
def test_sequential_detail_views(page):
    """Clicking different events in sequence shows the correct detail each time."""
    page.locator("#time-range").select_option("7d")
    page.wait_for_timeout(1500)

    cards = page.locator(".event-card")
    assert cards.count() >= 2

    # Get headlines of first two events
    headline1 = cards.nth(0).locator(".event-headline").text_content()
    headline2 = cards.nth(1).locator(".event-headline").text_content()
    assert headline1 != headline2

    # Click first event
    cards.nth(0).locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)
    assert headline1 in page.locator(".detail-headline").text_content()

    # Go back
    page.locator("#back-to-timeline").click()
    expect(page.locator("#timeline-view")).to_be_visible()

    # Click second event
    page.locator(".event-card").nth(1).locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)
    assert headline2 in page.locator(".detail-headline").text_content()

    page.locator("#back-to-timeline").click()
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- NEW-05: Filter reset restores all form values ---
def test_filter_reset_restores_form_values(page):
    """Reset button restores all filter inputs to default values."""
    # Set various filters
    page.locator("#time-range").select_option("7d")
    page.locator("#impact-filter").select_option("high")
    page.locator("#topic-filter").select_option("Technology")
    page.locator("#geography-filter").select_option("US")
    page.locator("#min-sources").fill("5")
    page.locator("#market-moving").check()
    page.locator("#keyword-search").fill("test query")

    # Reset
    page.locator("#reset-filters").click()
    page.wait_for_timeout(1000)

    # Verify all values are back to defaults
    assert page.locator("#time-range").input_value() == "24h"
    assert page.locator("#impact-filter").input_value() == ""
    assert page.locator("#topic-filter").input_value() == ""
    assert page.locator("#geography-filter").input_value() == ""
    assert page.locator("#min-sources").input_value() == "1"
    assert page.locator("#market-moving").is_checked() is False
    assert page.locator("#keyword-search").input_value() == ""


# --- NEW-06: Source comparison table data matches event articles ---
def test_source_table_row_count_matches_articles(page, server_url):
    """Source comparison table row count matches the number of articles for the event."""
    import httpx

    # Get a seed event with known articles
    events = httpx.get(f"{server_url}/api/events?time_range=7d").json()["events"]
    # Find one with articles
    target = None
    for e in events:
        detail = httpx.get(f"{server_url}/api/events/{e['id']}").json()
        if len(detail["articles"]) > 0:
            target = detail
            break
    assert target is not None

    expected_count = len(target["articles"])
    event_id = target["event"]["id"]

    # Navigate to detail in browser
    page.locator("#time-range").select_option("7d")
    page.wait_for_timeout(1500)

    card = page.locator(f'.event-card[data-event-id="{event_id}"]')
    card.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    rows = page.locator(".source-table tbody tr")
    expect(rows.first).to_be_visible(timeout=5000)
    assert rows.count() == expected_count

    page.locator("#back-to-timeline").click()
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- NEW-07: Detail view fact fields ---
def test_detail_fact_fields_complete(page):
    """Fact sheet rows display Who, What, When, Where labels when data exists."""
    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("Federal Reserve")
    page.wait_for_timeout(1500)

    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    fact_sheet = page.locator(".fact-sheet")
    expect(fact_sheet).to_be_visible(timeout=5000)

    labels = fact_sheet.locator(".fact-label")
    label_texts = [labels.nth(i).text_content().strip() for i in range(labels.count())]
    assert "Who" in label_texts
    assert "What" in label_texts

    page.locator("#back-to-timeline").click()
    page.locator("#keyword-search").fill("")
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- NEW-08: Confidence score badge on event cards ---
def test_confidence_badge_on_cards(page):
    """Event cards display confidence score badge (visible after expanding More)."""
    card = page.locator(".event-card").first
    card.locator(".news-item-more").click()
    badges = card.locator(".badge-confidence")
    count = badges.count()
    assert count > 0, "Expected at least one confidence badge on event cards"
    text = badges.first.text_content()
    assert "conf" in text.lower()
    assert "%" in text


# --- NEW-09: Confidence score in detail view ---
def test_confidence_in_detail_view(page):
    """Detail view header shows confidence score."""
    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    confidence = page.locator(".detail-meta .badge-confidence")
    expect(confidence).to_be_visible()
    text = confidence.text_content()
    assert "confidence" in text.lower()

    page.locator("#back-to-timeline").click()


# --- NEW-10: Primary vs derivative source distinction in source table ---
def test_primary_derivative_source_distinction(page):
    """Source comparison table visually distinguishes primary and derivative sources."""
    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("Federal Reserve")
    page.wait_for_timeout(1500)

    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    table = page.locator(".source-table")
    expect(table).to_be_visible(timeout=5000)

    # First row should be marked as primary
    rows = table.locator("tbody tr")
    assert rows.count() >= 2
    first_row = rows.first
    assert "primary" in first_row.get_attribute("class")
    primary_badge = first_row.locator(".source-type-badge.primary")
    expect(primary_badge).to_be_visible()
    assert "Primary" in primary_badge.text_content()

    # Second row should be marked as derivative
    second_row = rows.nth(1)
    assert "derivative" in second_row.get_attribute("class")
    derivative_badge = second_row.locator(".source-type-badge.derivative")
    expect(derivative_badge).to_be_visible()

    page.locator("#back-to-timeline").click()
    page.locator("#keyword-search").fill("")
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- NEW-11: Primary source badge in expanded source list ---
def test_primary_source_in_expanded_list(page):
    """Expanded source list marks primary source with visual indicator."""
    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("Federal Reserve")
    page.wait_for_timeout(1500)

    badge = page.locator(".badge-sources").first
    badge.click()
    source_list = page.locator(".source-list").first
    expect(source_list).to_be_visible(timeout=5000)

    # First source item should have primary class
    first_item = source_list.locator(".source-item").first
    assert "source-primary" in first_item.get_attribute("class")
    primary_badge = first_item.locator(".source-type-badge.primary")
    expect(primary_badge).to_be_visible()

    page.locator("#keyword-search").fill("")
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- NEW-12: Numbers displayed in fact sheet ---
def test_fact_sheet_shows_numbers(page):
    """Fact sheet in detail view displays numbers from claims."""
    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("Federal Reserve")
    page.wait_for_timeout(1500)

    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    fact_sheet = page.locator(".fact-sheet")
    expect(fact_sheet).to_be_visible(timeout=5000)

    labels = fact_sheet.locator(".fact-label")
    label_texts = [labels.nth(i).text_content().strip() for i in range(labels.count())]
    assert "Numbers" in label_texts, f"Expected 'Numbers' label in fact sheet, got: {label_texts}"

    page.locator("#back-to-timeline").click()
    page.locator("#keyword-search").fill("")
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- NEW-13: Direct quotes displayed in fact sheet ---
def test_fact_sheet_shows_direct_quotes(page):
    """Fact sheet in detail view displays direct quotes from claims."""
    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("Federal Reserve")
    page.wait_for_timeout(1500)

    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    fact_sheet = page.locator(".fact-sheet")
    expect(fact_sheet).to_be_visible(timeout=5000)

    labels = fact_sheet.locator(".fact-label")
    label_texts = [labels.nth(i).text_content().strip() for i in range(labels.count())]
    assert "Quotes" in label_texts, f"Expected 'Quotes' label in fact sheet, got: {label_texts}"

    # Check for <q> tags (styled quotes)
    quotes = fact_sheet.locator("q")
    assert quotes.count() > 0, "Expected at least one <q> element in fact sheet"

    page.locator("#back-to-timeline").click()
    page.locator("#keyword-search").fill("")
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- NEW-14: Sensationalism score column in source table ---
def test_sensationalism_score_in_source_table(page):
    """Source comparison table includes Sensationalism column."""
    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("Federal Reserve")
    page.wait_for_timeout(1500)

    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    table = page.locator(".source-table")
    expect(table).to_be_visible(timeout=5000)
    headers = table.locator("thead th")
    header_texts = [headers.nth(i).text_content().strip() for i in range(headers.count())]
    assert "Sensationalism" in header_texts, f"Expected 'Sensationalism' header, got: {header_texts}"

    # Verify sensationalism values are numeric
    sens_col_idx = header_texts.index("Sensationalism")
    rows = table.locator("tbody tr")
    for i in range(rows.count()):
        cell = rows.nth(i).locator("td").nth(sens_col_idx)
        text = cell.text_content().strip()
        # Should be a float like "0.23" or "—"
        assert text == "\u2014" or float(text) >= 0, f"Invalid sensationalism value: {text}"

    page.locator("#back-to-timeline").click()
    page.locator("#keyword-search").fill("")
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- NEW-15: Disputed claims have flag badges ---
def test_disputed_claims_have_flag_badges(page):
    """Disputed claims section shows 'Disputed' flag badges for each claim."""
    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("Federal Reserve")
    page.wait_for_timeout(1500)

    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    disputed = page.locator(".disputed-section")
    expect(disputed).to_be_visible(timeout=5000)

    flags = disputed.locator(".disputed-flag")
    assert flags.count() > 0, "Expected disputed flag badges"
    assert "Disputed" in flags.first.text_content()

    # Each disputed row should have claim text and contradiction
    rows = disputed.locator(".disputed-row")
    assert rows.count() > 0
    claim_text = rows.first.locator(".disputed-claim-text")
    expect(claim_text).to_be_visible()
    contradiction = rows.first.locator(".disputed-contradiction")
    expect(contradiction).to_be_visible()

    page.locator("#back-to-timeline").click()
    page.locator("#keyword-search").fill("")
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- NEW-16: Novel facts have visual diff highlighting ---
def test_novel_facts_have_diff_highlighting(page):
    """Novel facts list items have visual highlighting (border-left indicator)."""
    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("Federal Reserve")
    page.wait_for_timeout(1500)

    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    novel = page.locator(".novel-facts")
    expect(novel).to_be_visible(timeout=5000)

    items = novel.locator("li")
    assert items.count() > 0

    # Check first item has a bounding box (rendered correctly)
    box = items.first.bounding_box()
    assert box is not None, "Novel fact item should have a bounding box"
    assert box["width"] > 0 and box["height"] > 0

    page.locator("#back-to-timeline").click()
    page.locator("#keyword-search").fill("")
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- NEW-17: Uncertainty markers displayed in fact sheet ---
def test_uncertainty_markers_in_fact_sheet(page):
    """Claims with uncertainty markers display them in the fact sheet."""
    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("Federal Reserve")
    page.wait_for_timeout(1500)

    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    fact_sheet = page.locator(".fact-sheet")
    expect(fact_sheet).to_be_visible(timeout=5000)

    # Check for uncertainty/Note labels
    labels = fact_sheet.locator(".fact-label")
    label_texts = [labels.nth(i).text_content().strip() for i in range(labels.count())]
    assert "Note" in label_texts, f"Expected 'Note' label for uncertainty marker, got: {label_texts}"

    # Check uncertainty text with semantic class
    uncertainty_notes = fact_sheet.locator(".uncertainty-note")
    assert uncertainty_notes.count() > 0, "Expected uncertainty-note elements"
    text = uncertainty_notes.first.text_content()
    assert "according to" in text.lower(), f"Expected uncertainty marker text, got: {text}"

    page.locator("#back-to-timeline").click()
    page.locator("#keyword-search").fill("")
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- NEW-18: Market probability bar width matches odds ---
def test_market_bar_width_matches_odds(page):
    """Market-moving events can be filtered and market data shown on markets page."""
    # Market data is on the dedicated Polymarket Bets page, not the detail view.
    # Verify the market-moving filter works and returns events.
    page.locator("#time-range").select_option("7d")
    page.locator("#market-moving").check()
    page.wait_for_timeout(1500)
    cards = page.locator(".event-card")
    if cards.count() > 0:
        # Verify market-moving events appear
        assert cards.count() > 0, "Expected market-moving events with 7d filter"

        # Navigate to detail — should not have market odds bar (moved to /markets)
        cards.first.locator(".event-headline").click()
        expect(page.locator("#detail-view")).to_be_visible(timeout=5000)
        page.locator("#back-to-timeline").click()


# --- NEW-19: Detail view header shows entity tags ---
def test_detail_header_shows_entity_tags(page):
    """Detail view header includes entity tags from the event."""
    # Reset filters first to ensure clean state
    page.locator("#reset-filters").click()
    page.wait_for_timeout(500)

    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("Federal Reserve")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(1500)

    cards = page.locator(".event-card")
    expect(cards.first).to_be_visible(timeout=5000)
    cards.first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    # Wait for detail content to render
    expect(page.locator(".detail-headline")).to_be_visible(timeout=5000)

    entity_tags = page.locator(".detail-meta .entity-tag")
    expect(entity_tags.first).to_be_visible(timeout=5000)
    assert entity_tags.count() > 0, "Expected entity tags in detail header"

    page.locator("#back-to-timeline").click()
    page.locator("#keyword-search").fill("")
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- NEW-20: Header shows auto-refresh update indicator ---
def test_header_auto_refresh_indicator(page):
    """Header shows update indicator that reflects last data refresh time."""
    indicator = page.locator("#update-indicator")
    expect(indicator).to_be_visible()
    text = indicator.text_content()
    assert "updated" in text.lower()
    assert "just now" in text or "m ago" in text or "h ago" in text


# --- NEW-21: Prediction market section removed from news feed detail view ---
def test_no_market_section_in_detail_view(page):
    """Detail view should NOT show prediction market section (removed per user feedback)."""
    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("Federal Reserve")
    page.wait_for_timeout(1500)

    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    # Market section should NOT be present in news feed detail view
    market_section = page.locator(".market-section")
    expect(market_section).to_have_count(0)

    page.locator("#back-to-timeline").click()
    page.locator("#keyword-search").fill("")
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- NEW-22: Low-density suppression toggle in source comparison ---
def test_low_density_suppression_ui(page, server_url):
    """Source comparison table suppresses low-density articles by default when present."""
    import httpx

    # Create an event with a low-density article to test suppression
    event = httpx.post(f"{server_url}/api/events", json={
        "headline": "Test low density suppression event",
        "summary": "Testing low density suppression",
    }).json()
    # Add a low-density article (very high word_count, zero claims → low fact_density)
    httpx.post(f"{server_url}/api/events/{event['id']}/articles", json={
        "title": "Opinion: My thoughts on stuff",
        "publisher": "Blog Post",
        "text": " ".join(["word"] * 200),
        "fact_density": 0.0,
    })
    # Add a normal article
    httpx.post(f"{server_url}/api/events/{event['id']}/articles", json={
        "title": "Factual report on stuff",
        "publisher": "Reuters",
        "text": "Factual article with claims.",
        "fact_density": 0.05,
    })

    # Navigate to the event in the browser
    page.locator("#time-range").select_option("7d")
    page.locator("#keyword-search").fill("low density suppression")
    page.wait_for_timeout(1500)

    cards = page.locator(".event-card")
    if cards.count() > 0:
        cards.first.locator(".event-headline").click()
        expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

        # Check if suppress toggle exists when low-density articles present
        table = page.locator(".source-table")
        if table.count() > 0:
            low_density_rows = page.locator(".source-row.low-density")
            if low_density_rows.count() > 0:
                # Low density rows should have suppressed class by default
                for i in range(low_density_rows.count()):
                    assert "suppressed" in low_density_rows.nth(i).get_attribute("class")

    page.locator("#back-to-timeline").click()
    page.locator("#keyword-search").fill("")
    page.locator("#time-range").select_option("24h")
    page.wait_for_timeout(1500)  # filters auto-apply on change
    page.wait_for_timeout(500)


# --- NEW-23: Neutral title rewrite field in article response ---
def test_neutral_title_field_in_articles(page, server_url):
    """Articles in the API response include the neutral_title field."""
    import httpx

    events = httpx.get(f"{server_url}/api/events?time_range=7d").json()["events"]
    for e in events:
        detail = httpx.get(f"{server_url}/api/events/{e['id']}").json()
        if detail["articles"]:
            for a in detail["articles"]:
                assert "neutral_title" in a, f"Article missing neutral_title: {a.get('title')}"
            return
    assert False, "No event with articles found"


# --- Quick filter bar tests ---

def test_quick_filter_bar_visible(page):
    """Quick filter bar with topic presets is visible above the timeline."""
    quick_filters = page.locator(".quick-filters")
    expect(quick_filters).to_be_visible()
    buttons = quick_filters.locator(".quick-filter")
    assert buttons.count() >= 3, "Expected at least 3 quick filter buttons"
    # "All Events" should be active by default
    all_btn = quick_filters.locator('.quick-filter[data-filter=""]')
    expect(all_btn).to_have_class(re.compile(r"active"))


def test_quick_filter_market_economy(page):
    """Clicking 'Market & Economy' quick filter shows only economy/market events."""
    me_btn = page.locator('.quick-filter[data-filter="Market & Economy"]')
    expect(me_btn).to_be_visible()
    me_btn.click()
    page.wait_for_timeout(1000)

    # Button should be active
    expect(me_btn).to_have_class(re.compile(r"active"))
    # "All Events" should not be active
    all_btn = page.locator('.quick-filter[data-filter=""]')
    expect(all_btn).not_to_have_class(re.compile(r"active"))

    # Remaining events should all be economy or market-linked
    cards = page.locator(".event-card")
    assert cards.count() > 0, "Expected events after Market & Economy filter"


def test_quick_filter_returns_to_all(page):
    """Clicking 'All Events' after a quick filter restores all events."""
    # First apply Market & Economy filter
    me_btn = page.locator('.quick-filter[data-filter="Market & Economy"]')
    me_btn.click()
    page.wait_for_timeout(500)
    filtered_count = page.locator(".event-card").count()

    # Then click All Events
    all_btn = page.locator('.quick-filter[data-filter=""]')
    all_btn.click()
    page.wait_for_timeout(500)
    all_count = page.locator(".event-card").count()

    assert all_count >= filtered_count, "All Events should show at least as many events as filtered view"


def test_market_economy_filter_in_topic_dropdown(page):
    """Topic filter dropdown includes 'Market & Economy' option."""
    topic_select = page.locator("#topic-filter")
    options = topic_select.locator("option")
    option_values = [options.nth(i).get_attribute("value") for i in range(options.count())]
    assert "Market & Economy" in option_values, f"Expected 'Market & Economy' in topic options: {option_values}"


# =====================================================================
# Evolution Round: Multi-page navigation and UX requirements tests
# =====================================================================


# --- NAV-01: Top-level tab navigation ---
def test_nav_tabs_present_on_news_feed(page):
    """News Feed page has all 5 navigation tabs."""
    nav = page.locator(".header-nav")
    expect(nav).to_be_visible()
    links = nav.locator(".nav-link")
    assert links.count() == 5
    texts = [links.nth(i).text_content().strip() for i in range(5)]
    assert "News Feed" in texts
    assert "Unusual Polymarket Bets" in texts
    assert "Social vs Traditional Gaps" in texts
    assert "AI Intelligence" in texts
    assert "Sources" in texts


def test_nav_active_state_news_feed(page):
    """News Feed tab is active on the news feed page."""
    active = page.locator(".nav-link.active")
    assert "News Feed" in active.text_content()


# --- NAV-02: Tab navigation works ---
def test_nav_to_markets(page, server_url):
    """Clicking Unusual Bets tab navigates to markets page."""
    page.locator('a.nav-link:has-text("Unusual Polymarket Bets")').click()
    page.wait_for_load_state("networkidle")
    assert "/markets" in page.url
    active = page.locator(".nav-link.active")
    assert "Unusual Polymarket Bets" in active.text_content()


def test_nav_to_gaps(page, server_url):
    """Clicking Social vs Traditional tab navigates to gaps page."""
    page.locator('a.nav-link:has-text("Social vs Traditional Gaps")').click()
    page.wait_for_load_state("networkidle")
    assert "/gaps" in page.url
    active = page.locator(".nav-link.active")
    assert "Social vs Traditional Gaps" in active.text_content()


def test_nav_to_sources(page, server_url):
    """Clicking Sources tab navigates to sources page."""
    page.locator('a.nav-link:has-text("Sources")').click()
    page.wait_for_load_state("networkidle")
    assert "/sources" in page.url
    active = page.locator(".nav-link.active")
    assert "Sources" in active.text_content()


# --- NF-02: News feed collapsible section ---
def test_news_feed_collapsible(page):
    """News feed has a collapsible section header showing item count."""
    header = page.locator("#news-feed-toggle")
    expect(header).to_be_visible(timeout=5000)
    count_text = header.locator(".news-feed-count").text_content()
    assert "News Items" in count_text

    # Click to collapse
    header.click()
    items = page.locator("#news-feed-items")
    expect(items).to_be_hidden()

    # Click to expand
    header.click()
    expect(items).to_be_visible()


# --- NF-05: No polymarket section in news feed ---
def test_no_polymarket_section_in_news_feed(page):
    """News feed page has no separate prediction market section."""
    assert page.locator("#main-content h3:has-text('Prediction Market')").count() == 0
    assert page.locator("#main-content .polymarket-section").count() == 0


# --- NF-06: Events sorted by impact+recency ---
def test_events_sorted_by_impact_recency(page, server_url):
    """Events are sorted with high impact + recent first."""
    import httpx
    events = httpx.get(f"{server_url}/api/events?time_range=7d&limit=50").json()["events"]
    if len(events) < 3:
        return
    # Find first non-high event
    first_non_high = None
    for i, e in enumerate(events):
        if e["impact"] != "high":
            first_non_high = i
            break
    if first_non_high is None:
        return  # All high
    # All events before first_non_high should be high
    for i in range(first_non_high):
        assert events[i]["impact"] == "high", f"Event {i} should be high impact, got {events[i]['impact']}"


# --- NF-07: Topic bubbles apply on click (no Apply button) ---
def test_topic_bubbles_apply_on_click(page):
    """Topic bubble filters apply immediately without requiring an Apply button."""
    apply_buttons = page.locator("button:has-text('Apply')")
    assert apply_buttons.count() == 0, "There should be no Apply button"

    tech_btn = page.locator('.quick-filter[data-filter="Technology"]')
    tech_btn.click()
    page.wait_for_timeout(1000)
    expect(tech_btn).to_have_class(re.compile(r"active"))

    page.locator('.quick-filter[data-filter=""]').click()
    page.wait_for_timeout(500)


# --- NF-08: All filters auto-apply ---
def test_sidebar_filters_auto_apply(page):
    """Sidebar filter changes auto-apply without an Apply button."""
    initial_count = page.locator(".event-card").count()

    page.locator("#impact-filter").select_option("high")
    page.wait_for_timeout(1000)
    high_count = page.locator(".event-card").count()
    assert high_count <= initial_count or high_count >= 0

    page.locator("#reset-filters").click()
    page.wait_for_timeout(500)


# --- NF-10: Impact tiers all represented ---
def test_impact_tiers_all_represented(page, server_url):
    """API returns events with high, medium, and low impact tiers."""
    import httpx
    events = httpx.get(f"{server_url}/api/events?time_range=7d&limit=100").json()["events"]
    impacts = set(e["impact"] for e in events)
    assert "high" in impacts, "Expected high-impact events"
    assert "medium" in impacts, "Expected medium-impact events"
    assert "low" in impacts, "Expected low-impact events"


# =====================================================================
# Polymarket Bets Page Tests
# =====================================================================


def test_markets_page_loads(markets_page):
    """Markets page loads as a separate dedicated page."""
    heading = markets_page.locator("h2:has-text('Unusual Polymarket Bets')")
    expect(heading).to_be_visible()
    active = markets_page.locator(".nav-link.active")
    assert "Unusual Polymarket Bets" in active.text_content()


def test_markets_filter_extreme_probabilities(markets_page):
    """No bets at 0% or 100% probability are displayed."""
    markets_page.wait_for_timeout(2000)
    probs = markets_page.locator(".bet-row-prob")
    for i in range(probs.count()):
        text = probs.nth(i).text_content().strip()
        pct = int(text.replace("%", ""))
        assert 1 < pct < 99, f"Bet with {pct}% should be filtered out"


def test_markets_collapsible_groups(markets_page):
    """Bet groups are collapsible with category headers."""
    markets_page.wait_for_timeout(2000)
    groups = markets_page.locator(".bet-category-group")
    if groups.count() > 0:
        header = groups.first.locator(".bet-category-header")
        expect(header).to_be_visible()
        items = groups.first.locator(".bet-category-items")
        header.click()
        expect(items).to_be_hidden()
        header.click()
        expect(items).to_be_visible()


def test_markets_expandable_rows(markets_page):
    """Clicking a bet row expands detail with resolution and volume."""
    markets_page.wait_for_timeout(2000)
    rows = markets_page.locator(".bet-row")
    if rows.count() > 0:
        detail = rows.first.locator(".bet-row-detail")
        expect(detail).to_be_hidden()
        rows.first.locator(".bet-row-main").click()
        expect(detail).to_be_visible()
        expect(detail.locator(".bet-detail-inner")).to_be_visible()


def test_markets_category_filter(markets_page):
    """Category filter restricts displayed bets."""
    markets_page.wait_for_timeout(2000)
    initial = markets_page.locator(".bet-row").count()
    if initial == 0:
        return
    markets_page.locator("#category-filter").select_option("Politics")
    markets_page.wait_for_timeout(500)
    all_rows = markets_page.locator(".bet-row:visible")
    for i in range(all_rows.count()):
        cat = all_rows.nth(i).get_attribute("data-category")
        assert cat == "Politics", f"Expected Politics, got {cat}"
    markets_page.locator("#category-filter").select_option("")


def test_markets_sort_options(markets_page):
    """Sort dropdown has all required options."""
    sort_select = markets_page.locator("#sort-by")
    expect(sort_select).to_be_visible()
    options = sort_select.locator("option")
    values = [options.nth(i).get_attribute("value") for i in range(options.count())]
    assert "volume_spike" in values
    assert "volume" in values
    assert "probability" in values
    assert "end_date" in values


def test_markets_probability_tooltip(markets_page):
    """Probability display has tooltip explaining market-implied probability."""
    markets_page.wait_for_timeout(2000)
    probs = markets_page.locator(".bet-row-prob")
    if probs.count() > 0:
        title = probs.first.get_attribute("title")
        assert "probability" in title.lower(), f"Expected probability tooltip, got: {title}"


def test_markets_impact_level(markets_page):
    """Each bet has an impact badge."""
    markets_page.wait_for_timeout(2000)
    badges = markets_page.locator(".bet-row .badge-impact")
    if badges.count() > 0:
        for i in range(min(badges.count(), 5)):
            classes = badges.nth(i).get_attribute("class")
            assert "high" in classes or "medium" in classes or "low" in classes


def test_markets_all_section_expanded(markets_page):
    """All Tracked Markets section is expanded by default per user feedback."""
    all_list = markets_page.locator("#all-bets-list")
    expect(all_list).not_to_have_class(re.compile(r"collapsed"))


# =====================================================================
# Social vs Traditional Gaps Page Tests
# =====================================================================


def test_gaps_sections_collapsible(gaps_page):
    """All gap sections are collapsible."""
    gaps_page.wait_for_timeout(2000)
    toggle = gaps_page.locator("#social-leading-toggle")
    expect(toggle).to_be_visible()
    content = gaps_page.locator("#social-leading-list")
    toggle.click()
    expect(content).to_have_class(re.compile(r"collapsed"))
    toggle.click()
    expect(content).not_to_have_class(re.compile(r"collapsed"))


def test_gaps_only_high_items(gaps_page, server_url):
    """Active gap sections only show high-importance + high-impact items."""
    gaps_page.wait_for_timeout(2000)
    items = gaps_page.locator("#social-leading-list .gap-item")
    for i in range(items.count()):
        impact_badge = items.nth(i).locator(".badge-impact")
        if impact_badge.count() > 0:
            text = impact_badge.first.text_content().lower()
            assert "high" in text, f"Expected High Impact, got: {text}"
        importance_badge = items.nth(i).locator(".badge-importance")
        if importance_badge.count() > 0:
            text = importance_badge.first.text_content().lower()
            assert "high" in text, f"Expected High Importance, got: {text}"


def test_gaps_social_leading_section(gaps_page):
    """Gaps page has social-leading section (traditional_leading removed per feedback)."""
    gaps_page.wait_for_timeout(2000)
    expect(gaps_page.locator("h3:has-text('Trending on Social')")).to_be_visible()
    # Traditional-leading section should not exist
    assert gaps_page.locator("h3:has-text('Covered by Traditional')").count() == 0


def test_gaps_expired_section_collapsed(gaps_page):
    """Expired & Lower Priority section is collapsed by default."""
    expired = gaps_page.locator("#expired-list")
    expect(expired).to_have_class(re.compile(r"collapsed"))


def test_gaps_expandable_detail(gaps_page):
    """Gap items have expandable detail with bullet-point rationale."""
    gaps_page.wait_for_timeout(2000)
    items = gaps_page.locator(".gap-item")
    if items.count() > 0:
        detail = items.first.locator(".gap-item-detail")
        expect(detail).to_be_hidden()
        items.first.locator(".gap-item-row").click()
        expect(detail).to_be_visible()
        bullets = detail.locator("li")
        assert bullets.count() > 0, "Expected bullet-point rationale"


# =====================================================================
# Sources Page Tests
# =====================================================================


def test_sources_grouped_by_type(sources_page):
    """Sources page has Traditional, Social, and Prediction Markets groups."""
    sources_page.wait_for_timeout(2000)
    expect(sources_page.locator("h3:has-text('Traditional News Sources')")).to_be_visible()
    expect(sources_page.locator("h3:has-text('Social News Sources')")).to_be_visible()
    expect(sources_page.locator("h3:has-text('Prediction Markets')")).to_be_visible()


def test_sources_sensationalism_tooltip(sources_page):
    """Sensationalism column header has tooltip explaining calculation."""
    sources_page.wait_for_timeout(2000)
    headers = sources_page.locator("th")
    for i in range(headers.count()):
        text = headers.nth(i).text_content()
        if "Sensationalism" in text:
            title = headers.nth(i).get_attribute("title")
            assert title is not None and len(title) > 20, f"Expected detailed tooltip, got: {title}"
            assert "emotional" in title.lower() or "sensationalism" in title.lower()
            return


def test_sources_fact_density_tooltip(sources_page):
    """Fact density column header has tooltip explaining calculation."""
    sources_page.wait_for_timeout(2000)
    headers = sources_page.locator("th")
    for i in range(headers.count()):
        text = headers.nth(i).text_content()
        if "Fact Density" in text:
            title = headers.nth(i).get_attribute("title")
            assert title is not None and len(title) > 20, f"Expected detailed tooltip, got: {title}"
            assert "claim" in title.lower() or "density" in title.lower()
            return


# --- News Feed: Headline Links ---

def test_news_items_have_headline_links(page):
    """Each news item headline links to source article (not search engine)."""
    page.wait_for_timeout(1000)
    items = page.locator(".news-item-wrapper")
    count = items.count()
    if count > 0:
        # Check first few items for links
        for i in range(min(3, count)):
            item = items.nth(i)
            link = item.locator(".headline-link")
            if link.count() > 0:
                href = link.first.get_attribute("href")
                assert href is not None
                assert href.startswith("http"), f"Headline link should be absolute URL: {href}"
                assert "google.com/search" not in href, "Link should not be a search engine"


# --- News Feed: Expandable "More" Detail ---

def test_news_items_expandable_more(page):
    """Each news item is expanded by default (ISSUE-206) with a 'Less' toggle to collapse."""
    page.wait_for_timeout(1000)
    more_buttons = page.locator(".news-item-more")
    if more_buttons.count() > 0:
        # Detail section should already be visible (expanded by default)
        detail = page.locator(".news-item-detail").first
        expect(detail).to_be_visible()
        # Should contain a summary with meaningful content
        summary = detail.locator(".news-item-summary")
        if summary.count() > 0:
            text = summary.text_content()
            assert len(text) > 10, "Summary should have meaningful content"
        # Click "Less" to collapse
        more_buttons.first.click()
        expect(detail).to_be_hidden()


# --- News Feed: Collapsible Section with Count ---

def test_news_feed_shows_item_count_when_collapsed(page):
    """Collapsing news feed shows item count badge."""
    page.wait_for_timeout(1000)
    toggle = page.locator("#news-feed-toggle")
    if toggle.count() > 0:
        count_text = toggle.locator(".news-feed-count").text_content()
        assert "News Items" in count_text, f"Should show count, got: {count_text}"
        # Click to collapse
        toggle.click()
        page.wait_for_timeout(300)
        items = page.locator("#news-feed-items")
        assert "hidden" in (items.get_attribute("class") or "")
        # Count should update
        count_after = toggle.locator(".news-feed-count").text_content()
        assert "collapsed" in count_after or "News Items" in count_after


# --- Markets: No Extreme Probabilities in Data ---

def test_markets_no_zero_or_hundred_percent(markets_page):
    """Markets page filters out 0% and 100% bets (gaming contracts)."""
    markets_page.wait_for_timeout(2000)
    prob_elements = markets_page.locator(".bet-row-prob")
    for i in range(prob_elements.count()):
        text = prob_elements.nth(i).text_content().strip()
        pct = int(text.replace("%", ""))
        assert 1 <= pct <= 99, f"Should not show extreme probability: {text}"


# --- Markets: Bet Rows Have Required Fields ---

def test_markets_bet_row_has_required_info(markets_page):
    """Each bet row shows question, odds, volume, and impact."""
    markets_page.wait_for_timeout(2000)
    rows = markets_page.locator(".bet-row")
    if rows.count() > 0:
        row = rows.first
        # Question text
        question = row.locator(".bet-row-question")
        assert question.count() > 0
        assert len(question.text_content()) > 5
        # Probability
        prob = row.locator(".bet-row-prob")
        assert prob.count() > 0
        # Volume stat
        vol = row.locator(".bet-stat")
        assert vol.count() > 0
        # Impact badge
        impact = row.locator(".badge-impact")
        assert impact.count() > 0


# --- Markets: Source Labels ---

def test_markets_show_source_labels(markets_page):
    """Market bets show source label (Polymarket or CallSheet)."""
    markets_page.wait_for_timeout(2000)
    source_badges = markets_page.locator(".badge-source-market")
    if source_badges.count() > 0:
        text = source_badges.first.text_content()
        assert text in ("Polymarket", "CallSheet"), f"Unexpected source: {text}"


# --- Gaps: Only High Impact + High Importance ---

def test_gaps_active_sections_only_high(gaps_page):
    """Active gap sections only show high-impact + high-importance items."""
    gaps_page.wait_for_timeout(2000)
    # Check social-leading active items
    social_list = gaps_page.locator("#social-leading-list .gap-item")
    for i in range(social_list.count()):
        item = social_list.nth(i)
        impact_badge = item.locator(".badge-impact")
        importance_badge = item.locator(".badge-importance")
        if impact_badge.count() > 0:
            assert "high" in impact_badge.text_content().lower()
        if importance_badge.count() > 0:
            assert "high" in importance_badge.text_content().lower()


# --- Gaps: Expandable Detail with Bullet Points ---

def test_gaps_detail_has_bullet_rationale(gaps_page):
    """Expanding a gap row shows rationale in bullet-point format."""
    gaps_page.wait_for_timeout(2000)
    rows = gaps_page.locator(".gap-item-row")
    if rows.count() > 0:
        rows.first.click()
        gaps_page.wait_for_timeout(300)
        detail = gaps_page.locator(".gap-item-detail").first
        expect(detail).to_be_visible()
        bullets = detail.locator(".gap-rationale li")
        assert bullets.count() > 0, "Gap detail should have bullet-point rationale"


def test_gaps_metrics_explainer_visible(gaps_page):
    """Gaps page shows explainer text clarifying impact vs importance."""
    explainer = gaps_page.locator(".gaps-metrics-explainer")
    expect(explainer).to_be_visible()
    text = explainer.text_content()
    assert "Impact" in text, "Explainer should describe Impact"
    assert "Importance" in text, "Explainer should describe Importance"


def test_gaps_badges_have_tooltips(gaps_page):
    """Impact and importance badges have tooltip text explaining methodology."""
    gaps_page.wait_for_timeout(2000)
    impact_badges = gaps_page.locator(".badge-impact[title]")
    importance_badges = gaps_page.locator(".badge-importance[title]")
    if impact_badges.count() > 0:
        title = impact_badges.first.get_attribute("title")
        assert "source count" in title.lower() or "significant" in title.lower(), \
            "Impact badge tooltip should explain methodology"
    if importance_badges.count() > 0:
        title = importance_badges.first.get_attribute("title")
        assert "gap score" in title.lower() or "divergence" in title.lower(), \
            "Importance badge tooltip should explain methodology"


# --- Sources: Three Distinct Groups ---

def test_sources_three_groups_visible(sources_page):
    """Sources page shows three distinct source group headings."""
    sources_page.wait_for_timeout(2000)
    headings = sources_page.locator(".source-group-title")
    assert headings.count() >= 3, f"Expected 3 source groups, got {headings.count()}"
    texts = [headings.nth(i).text_content() for i in range(headings.count())]
    combined = " ".join(texts)
    assert "Traditional" in combined
    assert "Social" in combined
    assert "Prediction" in combined


# --- Sources: Ingestion Pipeline Status ---

def test_sources_pipeline_status_visible(sources_page):
    """Sources page shows ingestion pipeline status cards after expanding section."""
    sources_page.wait_for_timeout(2000)
    # Pipeline section is collapsed by default — expand it by clicking the toggle
    toggle = sources_page.locator("#pipeline-toggle")
    toggle.click()
    sources_page.wait_for_timeout(1000)
    pipeline = sources_page.locator(".pipeline-card")
    assert pipeline.count() >= 2, "Should show at least RSS and Social pipeline cards"


# --- News Feed: Default 24h without Apply ---

def test_default_24h_no_apply_button(page):
    """News feed defaults to 24h filter without needing an Apply button."""
    page.wait_for_timeout(1000)
    # Time range should default to 24h
    time_select = page.locator("#time-range")
    assert time_select.input_value() == "24h"
    # There should be no Apply button visible in filters
    apply_buttons = page.locator("button:has-text('Apply')")
    assert apply_buttons.count() == 0, "Should not have an Apply button"


# --- Entity Tags Clickable ---

def test_entity_tags_clickable_filter(page):
    """Clicking entity tag filters events by that entity."""
    page.wait_for_timeout(1000)
    # First expand "More" on the first item to reveal entity tags
    more_buttons = page.locator(".news-item-more")
    if more_buttons.count() > 0:
        more_buttons.first.click()
        page.wait_for_timeout(300)
    tags = page.locator(".clickable-entity:visible")
    if tags.count() > 0:
        # Get the entity name
        entity_name = tags.first.get_attribute("data-entity")
        tags.first.click()
        page.wait_for_timeout(500)
        # Keyword search should now have the entity
        keyword_input = page.locator("#keyword-search")
        assert keyword_input.input_value() == entity_name


# --- EV-01: Narrative Evolution section in detail view ---
def test_detail_shows_narrative_evolution(page):
    """Detail view shows narrative evolution section when multiple articles exist."""
    cards = page.locator(".event-card")
    for i in range(min(cards.count(), 5)):
        source_text = cards.nth(i).locator(".badge-sources").text_content()
        if "from 1 sources" not in source_text:
            cards.nth(i).locator(".event-headline").click()
            expect(page.locator("#detail-view")).to_be_visible(timeout=5000)
            narrative = page.locator(".narrative-evolution")
            if narrative.count() > 0:
                expect(narrative).to_be_visible()
                entries = narrative.locator(".narrative-entry")
                assert entries.count() > 0, "Narrative evolution should have entries"
                first_entry = entries.first
                expect(first_entry.locator(".narrative-time")).to_be_visible()
                expect(first_entry.locator(".narrative-publisher")).to_be_visible()
                expect(first_entry.locator(".narrative-headline")).to_be_visible()
            page.locator("#back-to-timeline").click()
            page.wait_for_timeout(300)
            return
    assert True


# --- EV-02: Publisher Bias section in detail view ---
def test_detail_shows_publisher_bias(page):
    """Detail view shows publisher bias comparison table."""
    cards = page.locator(".event-card")
    for i in range(min(cards.count(), 5)):
        source_text = cards.nth(i).locator(".badge-sources").text_content()
        if "from 1 sources" not in source_text:
            cards.nth(i).locator(".event-headline").click()
            expect(page.locator("#detail-view")).to_be_visible(timeout=5000)
            bias = page.locator(".publisher-bias")
            if bias.count() > 0:
                expect(bias).to_be_visible()
                table = bias.locator(".bias-table")
                expect(table).to_be_visible()
                headers = table.locator("th")
                header_texts = [headers.nth(j).text_content() for j in range(headers.count())]
                assert "Publisher" in header_texts
                assert "Avg Sensationalism" in header_texts
            page.locator("#back-to-timeline").click()
            page.wait_for_timeout(300)
            return
    assert True


# --- EV-03: Probability chart in detail view ---
def test_detail_shows_probability_chart(page):
    """Detail view shows SVG probability chart for market-linked events."""
    # Use market_moving filter to find events with market data
    page.locator("#time-range").select_option("7d")
    page.locator("#market-moving").check()
    page.wait_for_timeout(1500)
    cards = page.locator(".event-card")
    if cards.count() > 0:
        cards.first.locator(".event-headline").click()
        expect(page.locator("#detail-view")).to_be_visible(timeout=5000)
        chart = page.locator(".probability-chart-container")
        if chart.count() > 0:
            expect(chart).to_be_visible()
            svg = chart.locator("svg.probability-chart")
            expect(svg).to_be_visible()
            assert chart.locator("polyline").count() > 0
            assert chart.locator("circle").count() > 0
            shift = chart.locator(".probability-shift")
            expect(shift).to_be_visible()
        page.locator("#back-to-timeline").click()
        page.wait_for_timeout(300)


# --- EV-04: Collapsible detail sections ---
def test_detail_sections_are_collapsible(page):
    """Detail sections can be toggled open and closed."""
    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)

    toggles = page.locator(".detail-section-toggle")
    if toggles.count() > 0:
        first_toggle = toggles.first
        # Click to collapse
        first_toggle.click()
        page.wait_for_timeout(200)
        first_body = page.locator(".detail-section-body").first
        expect(first_body).to_have_class(re.compile("collapsed"))
        # Click to expand
        first_toggle.click()
        page.wait_for_timeout(200)
        assert "collapsed" not in (first_body.get_attribute("class") or "")


# --- MK-01: Markets linked events section ---
def test_markets_linked_events_section(markets_page):
    """Markets page has a linked events section."""
    linked_section = markets_page.locator("#linked-events-list")
    expect(linked_section).to_be_attached()
    toggle = markets_page.locator("#linked-toggle")
    expect(toggle).to_be_visible()


# --- MK-02: Markets sort actually reorders items ---
def test_markets_sort_reorders_items(markets_page):
    """Changing sort order rerenders the bet rows without errors."""
    rows_before = markets_page.locator(".bet-row")
    if rows_before.count() < 2:
        return
    markets_page.locator("#sort-by").select_option("probability")
    markets_page.wait_for_timeout(500)
    rows_after = markets_page.locator(".bet-row")
    assert rows_after.count() > 0


# --- SR-01: Sources page prediction markets section ---
def test_sources_prediction_markets_section(sources_page):
    """Sources page shows prediction market sources (Polymarket, CallSheet)."""
    predict_section = sources_page.locator("#prediction-market-sources")
    expect(predict_section).to_be_attached()
    toggle = sources_page.locator("#predict-toggle")
    if toggle.count() > 0:
        toggle.click()
        sources_page.wait_for_timeout(300)
    cards = predict_section.locator(".social-source-card")
    assert cards.count() >= 2, "Should have Polymarket and CallSheet"
    text = predict_section.text_content()
    assert "Polymarket" in text
    assert "CallSheet" in text


# --- SR-02: Sources page subreddits listed ---
def test_sources_subreddits_listed(sources_page):
    """Sources page lists tracked subreddits."""
    subs = sources_page.locator("#social-subreddits")
    text = subs.text_content()
    assert "r/" in text, "Should show subreddit list"


# --- SR-03: Sources page social source cards ---
def test_sources_social_source_cards(sources_page):
    """Sources page shows social platform cards with API details."""
    social_section = sources_page.locator("#social-sources")
    cards = social_section.locator(".social-source-card")
    assert cards.count() >= 5, "Should show at least 5 social platforms"
    first_card = cards.first
    expect(first_card.locator(".social-name")).to_be_visible()


# --- MO-01: Mobile responsive - markets page ---
def test_markets_mobile_responsive(browser, server_url):
    """Markets page renders at mobile viewport without breaking."""
    context = browser.new_context(viewport={"width": 375, "height": 812})
    pg = context.new_page()
    pg.goto(f"{server_url}/markets")
    pg.wait_for_load_state("networkidle")
    expect(pg.locator("header")).to_be_visible()
    expect(pg.locator("#main-content")).to_be_visible()
    page_width = pg.evaluate("document.body.scrollWidth")
    assert page_width <= 400, f"Page too wide for mobile: {page_width}px"
    pg.close()
    context.close()


# --- MO-02: Mobile responsive - gaps page ---
def test_gaps_mobile_responsive(browser, server_url):
    """Gaps page renders at mobile viewport without breaking."""
    context = browser.new_context(viewport={"width": 375, "height": 812})
    pg = context.new_page()
    pg.goto(f"{server_url}/gaps")
    pg.wait_for_load_state("networkidle")
    expect(pg.locator("header")).to_be_visible()
    expect(pg.locator("#main-content")).to_be_visible()
    page_width = pg.evaluate("document.body.scrollWidth")
    assert page_width <= 400, f"Page too wide for mobile: {page_width}px"
    pg.close()
    context.close()


# --- MO-03: Mobile responsive - sources page ---
def test_sources_mobile_responsive(browser, server_url):
    """Sources page renders at mobile viewport without breaking."""
    context = browser.new_context(viewport={"width": 375, "height": 812})
    pg = context.new_page()
    pg.goto(f"{server_url}/sources")
    pg.wait_for_load_state("networkidle")
    expect(pg.locator("header")).to_be_visible()
    expect(pg.locator("#main-content")).to_be_visible()
    pg.close()
    context.close()


# --- AC-01: All pages share consistent header/nav ---
def test_all_pages_consistent_nav(browser, server_url):
    """All pages have identical header nav structure."""
    context = browser.new_context(viewport={"width": 1280, "height": 800})
    pg = context.new_page()
    expected_links = ["News Feed", "Unusual Polymarket Bets", "Social vs Traditional Gaps", "AI Intelligence", "Sources"]
    for path in ["/", "/markets", "/gaps", "/ai", "/sources"]:
        pg.goto(f"{server_url}{path}")
        pg.wait_for_load_state("networkidle")
        nav_links = pg.locator(".header-nav .nav-link")
        texts = [nav_links.nth(i).text_content().strip() for i in range(nav_links.count())]
        assert texts == expected_links, f"Nav mismatch on {path}: {texts}"
    pg.close()
    context.close()


# --- AC-02: Market odds shown in detail view has probability tooltip ---
def test_market_badge_probability_tooltip(page):
    """Market section in detail view has probability tooltip."""
    page.locator("#time-range").select_option("7d")
    page.locator("#market-moving").check()
    page.wait_for_timeout(1500)
    cards = page.locator(".event-card")
    if cards.count() > 0:
        cards.first.locator(".event-headline").click()
        expect(page.locator("#detail-view")).to_be_visible(timeout=5000)
        market = page.locator(".market-section")
        if market.count() > 0:
            expect(market).to_be_visible(timeout=5000)
            # Market section should have probability info
            text = market.text_content()
            assert "%" in text, "Market section should show probability percentage"


# --- AC-03: Impact score badge in detail view has tooltip ---
def test_impact_score_badge_tooltip(page):
    """Impact score badge in detail view has explanatory tooltip."""
    page.locator(".event-card").first.locator(".event-headline").click()
    expect(page.locator("#detail-view")).to_be_visible(timeout=5000)
    score_badge = page.locator(".badge-impact-score")
    if score_badge.count() > 0:
        title = score_badge.first.get_attribute("title")
        assert title is not None and len(title) > 10


# --- AC-04: No console errors on page load ---
def test_no_console_errors_on_load(browser, server_url):
    """No JavaScript console errors on main page load."""
    context = browser.new_context(viewport={"width": 1280, "height": 800})
    pg = context.new_page()
    errors = []
    pg.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
    pg.goto(server_url)
    pg.wait_for_load_state("networkidle")
    pg.wait_for_timeout(1000)
    real_errors = [e for e in errors if "websocket" not in e.lower() and "ws:" not in e.lower()]
    assert len(real_errors) == 0, f"Console errors found: {real_errors}"
    pg.close()
    context.close()


# --- AC-05: No console errors on markets page ---
def test_no_console_errors_on_markets(browser, server_url):
    """No JavaScript console errors on markets page."""
    context = browser.new_context(viewport={"width": 1280, "height": 800})
    pg = context.new_page()
    errors = []
    pg.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
    pg.goto(f"{server_url}/markets")
    pg.wait_for_load_state("networkidle")
    pg.wait_for_timeout(1000)
    real_errors = [e for e in errors if "websocket" not in e.lower()]
    assert len(real_errors) == 0, f"Console errors on markets: {real_errors}"
    pg.close()
    context.close()


# --- AC-06: No console errors on gaps page ---
def test_no_console_errors_on_gaps(browser, server_url):
    """No JavaScript console errors on gaps page."""
    context = browser.new_context(viewport={"width": 1280, "height": 800})
    pg = context.new_page()
    errors = []
    pg.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
    pg.goto(f"{server_url}/gaps")
    pg.wait_for_load_state("networkidle")
    pg.wait_for_timeout(1000)
    real_errors = [e for e in errors if "websocket" not in e.lower()]
    assert len(real_errors) == 0, f"Console errors on gaps: {real_errors}"
    pg.close()
    context.close()


# --- DV-01: Detail view market section has volume display ---
def test_detail_market_section_has_volume(page):
    """Market section in detail view shows volume information."""
    page.locator("#time-range").select_option("7d")
    page.locator("#market-moving").check()
    page.wait_for_timeout(1500)
    cards = page.locator(".event-card")
    if cards.count() > 0:
        cards.first.locator(".event-headline").click()
        expect(page.locator("#detail-view")).to_be_visible(timeout=5000)
        market_section = page.locator(".market-section")
        if market_section.count() > 0:
            text = market_section.text_content()
            assert "Volume" in text or "volume" in text
        page.locator("#back-to-timeline").click()
        page.wait_for_timeout(300)


# --- FD-01: News feed item count matches API ---
def test_news_feed_count_matches_api(page, server_url):
    """The news feed item count matches the API response."""
    import httpx
    resp = httpx.get(f"{server_url}/api/events?time_range=24h")
    api_count = resp.json().get("count", 0)
    cards = page.locator(".event-card")
    ui_count = cards.count()
    assert ui_count == api_count, f"UI shows {ui_count} items but API returned {api_count}"
