# Reverse Proxy Web GUI

Use this guide when exposing the web admin UI through a reverse proxy.

## Goals

- Terminate HTTPS at the proxy.
- Forward the original host/protocol headers correctly.
- Keep CSRF and same-origin protections enabled.
- Avoid direct public exposure of the container port.

## Required Environment Settings

Set these values in `.env`:

```env
WEB_ENABLED=true
WEB_BIND_HOST=0.0.0.0
WEB_PORT=8080
WEB_PUBLIC_BASE_URL=https://discord-admin.example.com/
WEB_TRUST_PROXY_HEADERS=true
WEB_SESSION_COOKIE_SECURE=true
WEB_ENFORCE_CSRF=true
WEB_ENFORCE_SAME_ORIGIN_POSTS=true
```

Notes:
- `WEB_PUBLIC_BASE_URL` must match the external URL users open in their browser.
- Keep a trailing slash on `WEB_PUBLIC_BASE_URL` for consistency.
- Leave `WEB_TRUST_PROXY_HEADERS=true` only when the proxy is trusted and under your control.

## Docker/Network Recommendation

- Bind the app to an internal network only.
- Expose the web admin service to the proxy, not directly to the internet.
- If you must publish a host port, restrict it via firewall or localhost bind.

Example (localhost-only mapping):

```yaml
ports:
  - "127.0.0.1:8080:8080"
```

## Nginx Example

Replace `discord-admin.example.com` with your real domain.

```nginx
server {
    listen 80;
    server_name discord-admin.example.com;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl http2;
    server_name discord-admin.example.com;

    ssl_certificate     /etc/letsencrypt/live/discord-admin.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/discord-admin.example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;

        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Host $host;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Real-IP $remote_addr;

        proxy_read_timeout 60s;
        proxy_connect_timeout 10s;
    }
}
```

## Caddy Example

```caddy
discord-admin.example.com {
    reverse_proxy 127.0.0.1:8080 {
        header_up Host {host}
        header_up X-Forwarded-Host {host}
        header_up X-Forwarded-Proto {scheme}
        header_up X-Forwarded-For {remote_host}
        header_up X-Real-IP {remote_host}
    }
}
```

## Traefik Example (Docker Labels)

```yaml
services:
  bot:
    labels:
      - "traefik.enable=true"
      - "traefik.http.routers.bot-web.rule=Host(`discord-admin.example.com`)"
      - "traefik.http.routers.bot-web.entrypoints=websecure"
      - "traefik.http.routers.bot-web.tls=true"
      - "traefik.http.services.bot-web.loadbalancer.server.port=8080"
      - "traefik.http.middlewares.bot-web-headers.headers.customrequestheaders.X-Forwarded-Proto=https"
      - "traefik.http.routers.bot-web.middlewares=bot-web-headers"
```

If you terminate TLS at Traefik, keep router entrypoint on your HTTPS entrypoint (commonly `websecure`).

## Apache HTTPD Example

Enable modules:

- `proxy`
- `proxy_http`
- `ssl`
- `headers`
- `rewrite`

Virtual host example:

```apache
<VirtualHost *:80>
    ServerName discord-admin.example.com
    RewriteEngine On
    RewriteRule ^/(.*)$ https://discord-admin.example.com/$1 [R=301,L]
</VirtualHost>

<VirtualHost *:443>
    ServerName discord-admin.example.com

    SSLEngine on
    SSLCertificateFile /etc/letsencrypt/live/discord-admin.example.com/fullchain.pem
    SSLCertificateKeyFile /etc/letsencrypt/live/discord-admin.example.com/privkey.pem

    ProxyPreserveHost On
    RequestHeader set X-Forwarded-Proto "https"
    RequestHeader set X-Forwarded-Host "%{HTTP_HOST}s"

    ProxyPass / http://127.0.0.1:8080/
    ProxyPassReverse / http://127.0.0.1:8080/
</VirtualHost>
```

## HAProxy Example

```haproxy
frontend https_in
    bind *:443 ssl crt /etc/letsencrypt/live/discord-admin.example.com/haproxy.pem
    mode http
    option forwardfor
    http-request set-header X-Forwarded-Proto https
    http-request set-header X-Forwarded-Host %[req.hdr(host)]
    use_backend bot_web if { hdr(host) -i discord-admin.example.com }

backend bot_web
    mode http
    server bot_local 127.0.0.1:8080 check
```

## Troubleshooting

If login fails with `Blocked request due to origin policy.`:

1. Confirm `WEB_PUBLIC_BASE_URL` matches the exact external host users access.
2. Confirm proxy forwards `Host` and `X-Forwarded-Host`.
3. Confirm `WEB_TRUST_PROXY_HEADERS=true`.
4. Confirm browser is using the same domain as `WEB_PUBLIC_BASE_URL`.

If secure cookies do not persist:

1. Confirm users access the site over `https://`.
2. Keep `WEB_SESSION_COOKIE_SECURE=true`.
3. Confirm `X-Forwarded-Proto` is passed as `https`.

## Validation Checklist

- Login works through proxy.
- POST actions (save settings, create users, update profile) succeed.
- No origin-policy errors in logs.
- Cookies show `HttpOnly`, `Secure`, and `SameSite=Strict`.
- Direct container port is not publicly reachable.

## Related Pages

- [Web Admin Interface](Web-Admin-Interface)
- [Environment Variables](Environment-Variables)
- [Security Hardening](Security-Hardening)
