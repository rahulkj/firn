#!/usr/bin/env bash
# firn installer — interactive setup of firnd as a system service.
# Linux (systemd) and macOS (launchd). Windows: use WSL2 or the OCI image.
# Usage:  sudo ./install.sh [--version v0.5.1] [--unattended] [--help]
set -euo pipefail

FIRN_VERSION="${FIRN_VERSION:-v0.5.1}"
UNATTENDED=0
while [ $# -gt 0 ]; do
  case "$1" in
    --version) FIRN_VERSION="${2:?--version needs a value}"; shift 2 ;;
    --unattended) UNATTENDED=1; shift ;;
    --help|-h) sed -n '2,7p' "$0"; exit 0 ;;
    *) die "unknown argument: $1 (try --help)" ;;
  esac
done

say()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m✓\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m✗ %s\033[0m\n' "$*" >&2; exit 1; }

ask() { # ask VAR "Question" [default]
  local var="$1" q="$2" default="${3:-}" reply
  if [ "$UNATTENDED" = 1 ]; then eval "$var=\"${default}\""; return; fi
  if [ -n "$default" ]; then read -r -p "  $q [$default]: " reply; reply="${reply:-$default}"
  else read -r -p "  $q: " reply; fi
  eval "$var=\"$reply\""
}
ask_yn() { # ask_yn VAR "Question" [default y|n]
  local var="$1" q="$2" default="${3:-n}" reply
  if [ "$UNATTENDED" = 1 ]; then eval "$var=\"$default\""; return; fi
  read -r -p "  $q [${default}]: " reply; reply="${reply:-$default}"
  case "$reply" in y|Y|yes|YES) eval "$var=y" ;; *) eval "$var=n" ;; esac
}

[ "$(id -u)" -eq 0 ] || die "run as root (sudo). firnd needs a system user and a service unit."
say "firn $FIRN_VERSION installer"

# ---- detect OS -----------------------------------------------------------
OS="$(uname -s)"
case "$OS" in
  Linux)  command -v systemctl >/dev/null || die "no systemd found. firn supports systemd Linux; on other inits, run firnd manually or use the OCI image." ;;
  Darwin) ;;
  *)      die "unsupported OS ($OS). Windows: use WSL2 or docker/podman (see deploy/compose/)." ;;
esac

# ---- install binaries ----------------------------------------------------
say "Binaries"
if command -v firnd >/dev/null && command -v firn-cli >/dev/null; then
  ok "firnd/firn-cli already on PATH ($(firnd -h 2>&1 | head -0; command -v firnd))"
else
  arch=$(uname -m); case "$arch" in x86_64) GOARCH=amd64;; aarch64|arm64) GOARCH=arm64;; *) die "unsupported arch $arch";; esac
  base="https://github.com/rahulkj/firn/releases/download/${FIRN_VERSION}"
  tarball="firn-${FIRN_VERSION}-$( [ "$OS" = Darwin ] && echo darwin || echo linux )-${GOARCH}.tar.gz"
  tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
  say "Downloading $tarball"
  curl -fL --retry 3 -o "$tmp/$tarball" "$base/$tarball" || die "download failed: $base/$tarball (check the tag exists)"
  curl -fL --retry 3 -o "$tmp/SHA256SUMS" "$base/SHA256SUMS" || warn "SHA256SUMS missing — skipping integrity check"
  if [ -f "$tmp/SHA256SUMS" ]; then
    (cd "$tmp" && grep "$tarball" SHA256SUMS | shasum -a 256 -c -) || die "checksum mismatch — refusing to install"
    ok "checksum verified"
  fi
  tar -C "$tmp" -xzf "$tmp/$tarball"
  install -m 0755 "$tmp/firnd" /usr/local/bin/firnd
  install -m 0755 "$tmp/firn-cli" /usr/local/bin/firn-cli
  ok "installed firnd + firn-cli to /usr/local/bin"
fi

# ---- service user & dirs -------------------------------------------------
if [ "$OS" = Linux ]; then
  id firn >/dev/null 2>&1 || useradd -r -s /usr/sbin/nologin -d /var/lib/firn firn
fi
ask DATA_DIR "Data directory" "/var/lib/firn"
mkdir -p "$DATA_DIR"; chmod 700 "$DATA_DIR"
[ "$OS" = Linux ] && chown -R firn:firn "$DATA_DIR"
ok "data dir $DATA_DIR (mode 700)"

# ---- listeners -----------------------------------------------------------
ask HTTP_ADDR "HTTP admin address" ":9460"
ask GRPC_ADDR "Client gRPC address" ":9461"

# ---- TLS -----------------------------------------------------------------
TLS=n
ask_yn TLS "Enable TLS on both listeners? (provide cert paths next)" n
TLS_CERT=""; TLS_KEY=""
if [ "$TLS" = y ]; then
  ask TLS_CERT "Path to TLS certificate PEM"
  ask TLS_KEY  "Path to TLS private key PEM"
  [ -f "$TLS_CERT" ] && [ -f "$TLS_KEY" ] || die "TLS cert/key not found"
fi

# ---- mode ----------------------------------------------------------------
MODE=n
ask MODE "Install mode — single node, or 3-node cluster? [single/cluster]" "single"
NODE_ID=""; PEERS=""
MESH_CA_BUNDLE=""; MESH_CA_KEY=""
if [ "$MODE" = cluster ]; then
  ask NODE_ID "This node's ID (n1, n2, n3…)"
  ask PEERS    "Peer list including self — e.g. n1=192.0.2.11:9471,n2=192.0.2.12:9471,n3=192.0.2.13:9471"
  if [ -z "$NODE_ID" ] || [ -z "$PEERS" ]; then die "cluster mode requires -node-id and -peers"; fi
  case "$PEERS" in *"$NODE_ID="*) : ;; *) die "peers must include this node: $NODE_ID=<host>:<mesh-port>" ;; esac
  mkdir -p "$DATA_DIR/mesh-ca"
  ask_yn GEN_CA "Generate a shared mesh CA now into $DATA_DIR/mesh-ca? (choose n to copy an existing CA yourself)" y
  if [ "$GEN_CA" = y ]; then
    openssl ecparam -genkey -name prime256v1 -out "$DATA_DIR/mesh-ca/ca.key" >/dev/null 2>&1
    openssl req -x509 -new -nodes -key "$DATA_DIR/mesh-ca/ca.key" -days 3650 \
      -subj "/CN=firn-mesh-ca" -out "$DATA_DIR/mesh-ca/ca.crt" >/dev/null 2>&1
    chmod 600 "$DATA_DIR/mesh-ca/ca.key"; chmod 644 "$DATA_DIR/mesh-ca/ca.crt"
    [ "$OS" = Linux ] && chown -R firn:firn "$DATA_DIR/mesh-ca"
    ok "mesh CA written to $DATA_DIR/mesh-ca — copy ca.crt + ca.key (mode 600) to the other nodes"
  else
    ask MESH_CA_BUNDLE "Path to existing mesh CA bundle PEM (shared with peers)"
    ask MESH_CA_KEY    "Path to existing mesh CA private key PEM"
    [ -f "$MESH_CA_BUNDLE" ] && [ -f "$MESH_CA_KEY" ] || die "mesh CA bundle/key not found"
  fi
fi

# ---- retention (optional) --------------------------------------------------
ask RET_TTL "Retention TTL for whole segments, e.g. 720h (0 = disabled)" "0"

# ---- assemble flags -------------------------------------------------------
FLAGS="-data-dir $DATA_DIR -http-addr $HTTP_ADDR -grpc-addr $GRPC_ADDR -retention-ttl $RET_TTL"
[ -n "$TLS_CERT" ] && FLAGS="$FLAGS -tls-cert $TLS_CERT -tls-key $TLS_KEY"
if [ "$MODE" = cluster ]; then
  FLAGS="$FLAGS -node-id $NODE_ID -peers $PEERS"
  [ -n "$MESH_CA_BUNDLE" ] && FLAGS="$FLAGS -mesh-ca-bundle $MESH_CA_BUNDLE -mesh-ca-key $MESH_CA_KEY"
fi

# ---- write unit + env -----------------------------------------------------
mkdir -p /etc/firn
printf 'FIRND_FLAGS="%s"\n' "$FLAGS" > /etc/firn/firnd.env
chmod 600 /etc/firn/firnd.env

UNIT_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ "$OS" = Linux ]; then
  if [ -f "$UNIT_SRC/systemd/firnd.service" ]; then
    install -m 0644 "$UNIT_SRC/systemd/firnd.service" /etc/systemd/system/firnd.service
  else
    cat > /etc/systemd/system/firnd.service <<'UNIT'
[Unit]
Description=firn event broker
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
User=firn
Group=firn
EnvironmentFile=/etc/firn/firnd.env
ExecStart=/usr/local/bin/firnd $FIRND_FLAGS
Restart=on-failure
RestartSec=2
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true
PrivateTmp=true
[Install]
WantedBy=multi-user.target
UNIT
  fi
  systemctl daemon-reload
  systemctl enable --now firnd
  ok "firnd enabled and started (systemd)"
  say "Next steps"
  cat <<EOF
  1. Watch the log — the first boot prints a one-time BOOTSTRAP TOKEN:
       journalctl -u firnd -f
  2. Mint the first (admin) key:
       curl -s -X POST $HTTP_ADDR/apikeys -H "Authorization: Bearer <TOKEN>" \\
         -H 'Content-Type: application/json' -d '{"name":"root","role":"admin"}'
  3. Health check:
       curl $HTTP_ADDR/healthz
  Docs: https://rahulkj.github.io/firn/ · config: docs/configuration.md
EOF
else
  PLIST=/Library/LaunchDaemons/org.rahulkj.firnd.plist
  # Generate the plist with one <string> per argument of FLAGS.
  ARGS_XML=""
  for a in $FLAGS; do
    esc=$(printf '%s' "$a" | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g')
    ARGS_XML+="    <string>${esc}</string>\n"
  done
  cat > "$PLIST" <<UNIT
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>org.rahulkj.firnd</string>
  <key>ProgramArguments</key><array>
    <string>/usr/local/bin/firnd</string>
$(printf '%b' "$ARGS_XML")  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/var/log/firnd.stdout.log</string>
  <key>StandardErrorPath</key><string>/var/log/firnd.stderr.log</string>
</dict></plist>
UNIT
  launchctl unload "$PLIST" 2>/dev/null || true
  launchctl load -w "$PLIST"
  ok "firnd loaded (launchd) — logs: tail -f /var/log/firnd.stderr.log"
fi
warn "The data directory $DATA_DIR holds all state. Back it up (broker stopped) before upgrades."
