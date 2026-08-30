"""Glinet Discord Bot - Web Admin GUI Variant 2.

Theme: "Command Center"
- Top bar (no sidebar). Compact, dense, info-first.
- Top bar has: search box, quick-action palette (Ctrl+K), server selector, theme
- Home page is a single scrolling page with collapsible accordion sections grouped by
  domain (Bot, Servers, Tools, Admin). Each section is a grid of clickable
  tiles with status indicators.
- Login page uses a centered card on a darker background.
- All non-home pages render their legacy body content inside a top-bar shell.
"""

from __future__ import annotations

PAGE_TEMPLATE = """\
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="csrf-token" content="{{ csrf_token }}">
  <title>{{ title }} · Glinet Bot</title>
  <link rel="icon" href="{{ url_for('static', filename='favicon.png') }}">
  <link rel="apple-touch-icon" sizes="180x180" href="{{ url_for('static', filename='favicon.png') }}">
  <link property="og:image" content="{{ url_for('static', filename='favicon.png') }}">
  {% if page == "status_public" and status_refresh_seconds and status_refresh_seconds > 0 %}
  <meta http-equiv="refresh" content="{{ status_refresh_seconds }}">
  {% endif %}
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css" rel="stylesheet">
  <style>
    :root {
      --bg: #0a0d12; --bg-2: #11151c; --bg-3: #1a1f2a;
      --fg: #e7edf7; --muted: #93a3b8; --border: #2a3142;
      --accent: #38bdf8; --accent-2: #7dd3fc;
      --success: #4ade80; --warn: #fbbf24; --danger: #f87171; --info: #60a5fa;
    }
    body[data-theme="light"] {
      --bg: #f8fafc; --bg-2: #ffffff; --bg-3: #f1f5f9;
      --fg: #0f172a; --muted: #64748b; --border: #e2e8f0;
    }
    * { box-sizing: border-box; }
    html, body { background: var(--bg); color: var(--fg); margin: 0; }
    body { font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; font-size: 14px; min-height: 100vh; }
    a { color: var(--accent-2); text-decoration: none; }
    a:hover { color: var(--accent); }

    .cc-topbar {
      position: sticky; top: 0; z-index: 100;
      background: var(--bg-2); border-bottom: 1px solid var(--border);
      padding: 10px 18px;
      display: flex; align-items: center; gap: 14px;
    }
    .cc-topbar .cc-brand { display: flex; align-items: center; gap: 8px; font-weight: 700; }
    .cc-topbar .cc-brand img { width: 26px; height: 26px; border-radius: 6px; }
    .cc-topbar .cc-brand a { color: var(--fg); }
    .cc-topbar .cc-search {
      flex: 1; max-width: 480px;
      display: flex; align-items: center; gap: 6px;
      background: var(--bg-3); border: 1px solid var(--border);
      border-radius: 8px; padding: 6px 10px;
    }
    .cc-topbar .cc-search i { color: var(--muted); }
    .cc-topbar .cc-search input {
      background: transparent; border: 0; color: var(--fg); outline: none;
      flex: 1; font-size: 13px;
    }
    .cc-topbar .cc-search kbd {
      background: var(--bg-2); border: 1px solid var(--border);
      padding: 1px 5px; border-radius: 4px; font-size: 11px; color: var(--muted);
    }
    .cc-topbar .cc-right { display: flex; align-items: center; gap: 10px; margin-left: auto; }

    .cc-palette-toggle {
      background: var(--bg-3); border: 1px solid var(--border); color: var(--fg);
      padding: 6px 10px; border-radius: 8px; cursor: pointer; font-size: 12px;
      display: flex; align-items: center; gap: 6px;
    }
    .cc-palette-toggle:hover { background: var(--accent); color: white; border-color: var(--accent); }

    .cc-main { padding: 24px; max-width: 1400px; margin: 0 auto; }
    .cc-section { background: var(--bg-2); border: 1px solid var(--border); border-radius: 12px; margin-bottom: 18px; overflow: hidden; }
    .cc-section-header {
      display: flex; align-items: center; gap: 10px;
      padding: 14px 18px; cursor: pointer; user-select: none;
      background: var(--bg-2); border-bottom: 1px solid var(--border);
    }
    .cc-section-header:hover { background: var(--bg-3); }
    .cc-section-header h2 { margin: 0; font-size: 14px; font-weight: 700; letter-spacing: .3px; flex: 1; }
    .cc-section-header .cc-pill { font-size: 11px; color: var(--muted); }
    .cc-section-header .cc-chevron { transition: transform .2s; }
    .cc-section.collapsed .cc-chevron { transform: rotate(-90deg); }
    .cc-section.collapsed .cc-section-body { display: none; }
    .cc-section-body { padding: 14px 18px 18px; }

    .cc-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 10px; }
    .cc-tile {
      display: block; padding: 12px 14px;
      background: var(--bg-3); border: 1px solid var(--border); border-radius: 8px;
      color: var(--fg); transition: border-color .12s ease, transform .12s ease, background .12s ease;
      text-decoration: none;
    }
    .cc-tile:hover { border-color: var(--accent); transform: translateY(-1px); background: var(--bg-2); }
    .cc-tile-title { display: flex; align-items: center; gap: 8px; font-weight: 600; font-size: 13px; }
    .cc-tile-title i { color: var(--accent-2); }
    .cc-tile-desc { font-size: 12px; color: var(--muted); margin-top: 4px; line-height: 1.4; }
    .cc-tile-status { display: inline-block; width: 6px; height: 6px; border-radius: 50%; background: var(--success); margin-left: auto; }
    .cc-tile-status.warn { background: var(--warn); }
    .cc-tile-status.danger { background: var(--danger); }
    .cc-tile-status.muted { background: var(--muted); }

    .cc-form-control, .cc-main .form-control, .cc-main .form-select {
      background: var(--bg-2); color: var(--fg); border: 1px solid var(--border);
    }
    .cc-form-control:focus, .cc-main .form-control:focus, .cc-main .form-select:focus {
      background: var(--bg-2); color: var(--fg); border-color: var(--accent);
      box-shadow: 0 0 0 2px rgba(56, 189, 248, .15);
    }
    .cc-form-label, .cc-main .form-label { color: var(--fg); }
    .cc-main .table { color: var(--fg); }
    .cc-main .table > :not(caption) > * > * { background: transparent; color: var(--fg); border-bottom-color: var(--border); }
    .cc-main .modal-content { background: var(--bg-2); color: var(--fg); }

    .cc-btn, .cc-main .btn-primary { background: var(--accent); border-color: var(--accent); color: #001520; }
    .cc-btn:hover, .cc-main .btn-primary:hover { background: var(--accent-2); border-color: var(--accent-2); }
    .cc-btn-secondary, .cc-main .btn-secondary { background: var(--bg-3); color: var(--fg); border-color: var(--border); }
    .cc-btn-danger, .cc-main .btn-danger { background: var(--danger); border-color: var(--danger); }

    .cc-flash { padding: 10px 14px; border-radius: 8px; margin-bottom: 14px; border: 1px solid var(--border); }
    .cc-flash-success { background: rgba(74, 222, 128, .1); border-color: var(--success); color: #bbf7d0; }
    .cc-flash-danger { background: rgba(248, 113, 113, .1); border-color: var(--danger); color: #fecaca; }
    .cc-flash-warning { background: rgba(251, 191, 36, .1); border-color: var(--warn); color: #fde68a; }
    .cc-flash-info { background: rgba(96, 165, 250, .1); border-color: var(--info); color: #bfdbfe; }

    /* Command palette */
    .cc-palette {
      position: fixed; inset: 0; z-index: 200;
      background: rgba(0, 0, 0, .55); display: none;
      align-items: flex-start; justify-content: center; padding-top: 12vh;
    }
    .cc-palette.open { display: flex; }
    .cc-palette-inner {
      background: var(--bg-2); border: 1px solid var(--border);
      border-radius: 12px; width: 580px; max-width: 92vw; max-height: 60vh; overflow: hidden;
      box-shadow: 0 20px 60px rgba(0, 0, 0, .5);
      display: flex; flex-direction: column;
    }
    .cc-palette-input {
      width: 100%; padding: 14px 16px; background: transparent; border: 0; border-bottom: 1px solid var(--border);
      color: var(--fg); font-size: 16px; outline: none;
    }
    .cc-palette-list { overflow-y: auto; flex: 1; }
    .cc-palette-item {
      display: flex; align-items: center; gap: 10px;
      padding: 10px 16px; cursor: pointer; color: var(--fg); text-decoration: none;
    }
    .cc-palette-item:hover, .cc-palette-item.active { background: var(--accent); color: #001520; }
    .cc-palette-item:hover i, .cc-palette-item.active i { color: #001520; }
    .cc-palette-item i { color: var(--accent-2); width: 18px; }
    .cc-palette-empty { padding: 24px; text-align: center; color: var(--muted); }

    .cc-stat-row { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 18px; }
    .cc-stat {
      flex: 1; min-width: 160px; background: var(--bg-2); border: 1px solid var(--border);
      border-radius: 10px; padding: 14px 18px;
    }
    .cc-stat-label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 1px; }
    .cc-stat-value { font-size: 24px; font-weight: 700; margin-top: 4px; }

    .cc-section-title { font-size: 18px; font-weight: 700; margin: 0 0 12px; }
    .cc-section-desc { color: var(--muted); font-size: 13px; margin: 0 0 18px; }

    .cc-theme-toggle { display: flex; gap: 4px; background: var(--bg-3); border: 1px solid var(--border); border-radius: 7px; padding: 3px; }
    .cc-theme-toggle button { background: transparent; border: 0; color: var(--muted); padding: 4px 8px; border-radius: 5px; cursor: pointer; font-size: 12px; }
    .cc-theme-toggle button.active { background: var(--accent); color: #001520; }

    .cc-login-page { min-height: 100vh; display: flex; align-items: center; justify-content: center; background: radial-gradient(ellipse at top, #1a2030 0%, #0a0d12 60%); }
    .cc-login-card { background: var(--bg-2); border: 1px solid var(--border); border-radius: 12px; padding: 32px; width: 100%; max-width: 380px; }
  </style>
</head>
<body data-theme="dark">
  {% set is_authed = session.get("user") %}
  {% set is_login_page = page in ["login", "forgot_password", "reset_password", "status_public", "public_status", "public_status_everything"] %}

  {% if is_login_page or not is_authed %}
    <div style="margin:0; padding:0;">
      {% if page == "login" %}
        <div class="cc-login-page">
          <div class="cc-login-card">
            <div style="text-align:center; margin-bottom: 24px;">
              <img src="{{ url_for('static', filename='favicon.png') }}" alt="logo" style="width:60px;height:60px;border-radius:12px;margin-bottom:10px;">
              <h1 style="margin:0; font-size: 20px;">Glinet Bot</h1>
              <p style="color: var(--muted); margin: 4px 0 0; font-size: 12px;">Admin login</p>
            </div>
            {% with messages = get_flashed_messages(with_categories=true) %}
              {% for cat, msg in messages %}
                <div class="cc-flash cc-flash-{{ cat if cat in ['success','danger','warning'] else 'info' }}">{{ msg }}</div>
              {% endfor %}
            {% endwith %}
            <form method="post" autocomplete="on">
              <input type="hidden" name="csrf_token" value="{{ csrf_token }}">
              <div class="mb-3">
                <label class="form-label cc-form-label">Email</label>
                <input class="form-control cc-form-control" type="email" name="email" required autofocus>
              </div>
              <div class="mb-3">
                <label class="form-label cc-form-label">Password</label>
                <input class="form-control cc-form-control" type="password" name="password" required>
              </div>
              <button class="btn btn-primary w-100" type="submit">Sign in</button>
            </form>
          </div>
        </div>
      {% else %}
        {{ legacy_body|default("", true) | safe }}
      {% endif %}
    </div>
  {% else %}
    <div style="margin:0; padding:0;">
      <header class="cc-topbar">
        <div class="cc-brand">
          <img src="{{ url_for('static', filename='favicon.png') }}" alt="logo">
          <a href="{{ url_for('home') }}">Glinet Bot</a>
        </div>
        <div class="cc-search">
          <i class="bi bi-search"></i>
          <input id="ccPaletteInput" type="text" placeholder="Search pages..." autocomplete="off">
          <kbd>Ctrl+K</kbd>
        </div>
        <div class="cc-right">
          <div class="cc-theme-toggle">
            <button type="button" data-set-theme="dark" class="active">Dark</button>
            <button type="button" data-set-theme="light">Light</button>
          </div>
          <a class="btn btn-sm btn-secondary" href="{{ url_for('account') }}"><i class="bi bi-person"></i></a>
          <a class="btn btn-sm btn-secondary" href="{{ url_for('logout') }}">Logout</a>
        </div>
      </header>

      {% if page == "home" %}
        <main class="cc-main">
          <div class="cc-section">
            <div class="cc-section-body">
              <div class="cc-stat-row">
                <div class="cc-stat">
                  <div class="cc-stat-label">Status</div>
                  <div class="cc-stat-value">{{ status_summary.ready or "unknown" }}</div>
                </div>
                <div class="cc-stat">
                  <div class="cc-stat-label">Guilds</div>
                  <div class="cc-stat-value">{{ status_summary.managed_guild_count or 0 }}</div>
                </div>
                <div class="cc-stat">
                  <div class="cc-stat-label">Latency</div>
                  <div class="cc-stat-value">{{ status_summary.latency_ms or "—" }}<small style="color:var(--muted)"> ms</small></div>
                </div>
              </div>
            </div>
          </div>

          <div class="cc-section">
            <div class="cc-section-header" data-toggle="collapse" data-target="#sec-core">
              <h2>Core</h2>
              <span class="cc-pill">Bot, Servers, Dashboard</span>
              <i class="bi bi-chevron-down cc-chevron"></i>
            </div>
            <div class="cc-section-body collapse show" id="sec-core">
              <div class="cc-grid">
                <a class="cc-tile" href="{{ url_for('dashboard') }}">
                  <div class="cc-tile-title"><i class="bi bi-speedometer2"></i> Dashboard</div>
                  <div class="cc-tile-desc">Overview, status cards, recent actions.</div>
                  <span class="cc-tile-status"></span>
                </a>
                <a class="cc-tile" href="{{ url_for('guilds_page') }}">
                  <div class="cc-tile-title"><i class="bi bi-hdd-rack"></i> Servers</div>
                  <div class="cc-tile-desc">Managed Discord servers.</div>
                  <span class="cc-tile-status"></span>
                </a>
                <a class="cc-tile" href="{{ url_for('command_permissions') }}">
                  <div class="cc-tile-title"><i class="bi bi-shield-check"></i> Command Permissions</div>
                  <div class="cc-tile-desc">Allowlist and overrides.</div>
                  <span class="cc-tile-status"></span>
                </a>
                <a class="cc-tile" href="{{ url_for('bot_profile') }}">
                  <div class="cc-tile-title"><i class="bi bi-robot"></i> Bot Profile</div>
                  <div class="cc-tile-desc">Username, nickname, avatar.</div>
                  <span class="cc-tile-status"></span>
                </a>
              </div>
            </div>
          </div>

          <div class="cc-section">
            <div class="cc-section-header" data-toggle="collapse" data-target="#sec-tools">
              <h2>Tools</h2>
              <span class="cc-pill">Feeds, Translation, IRC</span>
              <i class="bi bi-chevron-down cc-chevron"></i>
            </div>
            <div class="cc-section-body collapse show" id="sec-tools">
              <div class="cc-grid">
                <a class="cc-tile" href="{{ url_for('reddit_feeds') }}">
                  <div class="cc-tile-title"><i class="bi bi-reddit"></i> Reddit Feeds</div>
                  <div class="cc-tile-desc">Subreddit to channel feeds.</div>
                  <span class="cc-tile-status"></span>
                </a>
                <a class="cc-tile" href="{{ url_for('youtube_subscriptions') }}">
                  <div class="cc-tile-title"><i class="bi bi-youtube"></i> YouTube</div>
                  <div class="cc-tile-desc">Channel and community posts.</div>
                  <span class="cc-tile-status"></span>
                </a>
                <a class="cc-tile" href="{{ url_for('translate_channels_page') }}">
                  <div class="cc-tile-title"><i class="bi bi-translate"></i> Auto-Translate</div>
                  <div class="cc-tile-desc">Channel translation mappings.</div>
                  <span class="cc-tile-status"></span>
                </a>
                <a class="cc-tile" href="{{ url_for('irc_bridge_page') }}">
                  <div class="cc-tile-title"><i class="bi bi-chat-dots"></i> IRC Bridge</div>
                  <div class="cc-tile-desc">Two-way IRC ↔ Discord chat.</div>
                  <span class="cc-tile-status"></span>
                </a>
              </div>
            </div>
          </div>

          <div class="cc-section">
            <div class="cc-section-header" data-toggle="collapse" data-target="#sec-admin">
              <h2>Admin</h2>
              <span class="cc-pill">Users, Settings, Logs</span>
              <i class="bi bi-chevron-down cc-chevron"></i>
            </div>
            <div class="cc-section-body collapse show" id="sec-admin">
              <div class="cc-grid">
                <a class="cc-tile" href="{{ url_for('members_page') }}">
                  <div class="cc-tile-title"><i class="bi bi-people"></i> Members</div>
                  <div class="cc-tile-desc">Member management and roles.</div>
                  <span class="cc-tile-status"></span>
                </a>
                <a class="cc-tile" href="{{ url_for('moderation_page') }}">
                  <div class="cc-tile-title"><i class="bi bi-shield-lock"></i> Moderation</div>
                  <div class="cc-tile-desc">Bad word filter and actions.</div>
                  <span class="cc-tile-status"></span>
                </a>
                <a class="cc-tile" href="{{ url_for('logs') }}">
                  <div class="cc-tile-title"><i class="bi bi-file-text"></i> Logs</div>
                  <div class="cc-tile-desc">Live logs and export.</div>
                  <span class="cc-tile-status"></span>
                </a>
                <a class="cc-tile" href="{{ url_for('account') }}">
                  <div class="cc-tile-title"><i class="bi bi-person"></i> My Account</div>
                  <div class="cc-tile-desc">Profile and password.</div>
                  <span class="cc-tile-status"></span>
                </a>
              </div>
            </div>
          </div>
        </main>
      {% else %}
        <main class="cc-main">
          {% with messages = get_flashed_messages(with_categories=true) %}
            {% for cat, msg in messages %}
              <div class="cc-flash cc-flash-{{ cat if cat in ['success','danger','warning'] else 'info' }}">{{ msg }}</div>
            {% endfor %}
          {% endwith %}
          {{ legacy_body|default("", true) | safe }}
        </main>
      {% endif %}
    </div>
  {% endif %}

  <div class="cc-palette" id="ccPalette">
    <div class="cc-palette-inner">
      <input class="cc-palette-input" id="ccPaletteInput2" placeholder="Type a page name..." autocomplete="off">
      <div class="cc-palette-list" id="ccPaletteList"></div>
    </div>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
  <script>
    const PALETTE = [
      { name: "Home", url: "{{ url_for('home') }}", icon: "bi-house" },
      { name: "Dashboard", url: "{{ url_for('dashboard') }}", icon: "bi-speedometer2" },
      { name: "Status", url: "{{ url_for('status_page') }}", icon: "bi-activity" },
      { name: "Servers", url: "{{ url_for('guilds_page') }}", icon: "bi-hdd-rack" },
      { name: "Guild Settings", url: "{{ url_for('guild_settings') }}", icon: "bi-gear" },
      { name: "Command Permissions", url: "{{ url_for('command_permissions') }}", icon: "bi-shield-check" },
      { name: "YouTube", url: "{{ url_for('youtube_subscriptions') }}", icon: "bi-youtube" },
      { name: "Reddit", url: "{{ url_for('reddit_feeds') }}", icon: "bi-reddit" },
      { name: "Auto-Translate", url: "{{ url_for('translate_channels_page') }}", icon: "bi-translate" },
      { name: "IRC Bridge", url: "{{ url_for('irc_bridge_page') }}", icon: "bi-chat-dots" },
      { name: "Honeypot", url: "{{ url_for('honeypot_page') }}", icon: "bi-bug" },
      { name: "Reaction Roles", url: "{{ url_for('reaction_roles') }}", icon: "bi-emoji-smile" },
      { name: "Tag Responses", url: "{{ url_for('tag_responses') }}", icon: "bi-tags" },
      { name: "Member Activity", url: "{{ url_for('member_activity_page') }}", icon: "bi-people" },
      { name: "Action Log", url: "{{ url_for('actions') }}", icon: "bi-list-ul" },
      { name: "Logs", url: "{{ url_for('logs') }}", icon: "bi-file-text" },
      { name: "My Account", url: "{{ url_for('account') }}", icon: "bi-person" },
    ];
    function openPalette() {
      document.getElementById('ccPalette').classList.add('open');
      setTimeout(() => document.getElementById('ccPaletteInput2').focus(), 50);
    }
    function closePalette() {
      document.getElementById('ccPalette').classList.remove('open');
    }
    function filterPalette(q) {
      q = (q || '').toLowerCase();
      const filtered = PALETTE.filter(p => p.name.toLowerCase().includes(q));
      const list = document.getElementById('ccPaletteList');
      if (!filtered.length) {
        list.innerHTML = '<div class="cc-palette-empty">No matches</div>';
        return;
      }
      list.innerHTML = filtered.map(p =>
        '<a class="cc-palette-item" href="' + p.url + '"><i class="bi ' + p.icon + '"></i>' + p.name + '</a>'
      ).join('');
    }
    filterPalette('');
    document.addEventListener('keydown', function(e) {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        if (document.getElementById('ccPalette').classList.contains('open')) closePalette();
        else openPalette();
      } else if (e.key === 'Escape') {
        closePalette();
      }
    });
    document.querySelectorAll('[data-set-theme]').forEach(btn => {
      btn.addEventListener('click', () => {
        const theme = btn.getAttribute('data-set-theme');
        document.body.setAttribute('data-theme', theme);
        document.querySelectorAll('[data-set-theme]').forEach(b => b.classList.toggle('active', b === btn));
        try { localStorage.setItem('cc-theme', theme); } catch (e) {}
      });
    });
    try { const t = localStorage.getItem('cc-theme'); if (t) { document.body.setAttribute('data-theme', t); document.querySelectorAll('[data-set-theme]').forEach(b => b.classList.toggle('active', b.getAttribute('data-set-theme') === t)); } } catch (e) {}
  </script>
</body>
</html>
"""
