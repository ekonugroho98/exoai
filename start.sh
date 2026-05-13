#!/usr/bin/env bash
# start.sh — one-command Bifrost startup with the zai-web provider built in.
#
# Usage:
#   ./start.sh             # build (if needed) and run on default port (8080)
#   ./start.sh --rebuild   # force a fresh build before running
#   ./start.sh --port 9090 # use a different port
#   ./start.sh --dev       # hot-reload dev mode (UI + API, requires Node.js)
#   ./start.sh --help      # print this help

set -euo pipefail

# ----------------------------------------------------------------------------
# colors
# ----------------------------------------------------------------------------
if [[ -t 1 ]]; then
  C_RED=$'\033[31m'
  C_GREEN=$'\033[32m'
  C_YELLOW=$'\033[33m'
  C_CYAN=$'\033[36m'
  C_DIM=$'\033[2m'
  C_RESET=$'\033[0m'
else
  C_RED=""; C_GREEN=""; C_YELLOW=""; C_CYAN=""; C_DIM=""; C_RESET=""
fi

info()    { echo "${C_CYAN}▸${C_RESET} $*"; }
success() { echo "${C_GREEN}✓${C_RESET} $*"; }
warn()    { echo "${C_YELLOW}!${C_RESET} $*"; }
err()     { echo "${C_RED}✗${C_RESET} $*" >&2; }

# ----------------------------------------------------------------------------
# arg parsing
# ----------------------------------------------------------------------------
REBUILD=0
DEV=0
PORT=8080

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rebuild) REBUILD=1; shift ;;
    --dev)     DEV=1; shift ;;
    --port)    PORT="$2"; shift 2 ;;
    --help|-h)
      grep -E '^#( |$)' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      err "unknown option: $1 (try --help)"
      exit 1
      ;;
  esac
done

# ----------------------------------------------------------------------------
# always run from the script's directory
# ----------------------------------------------------------------------------
cd "$(dirname "$0")"

# ----------------------------------------------------------------------------
# Go version check (need 1.26+)
# ----------------------------------------------------------------------------
if ! command -v go >/dev/null 2>&1; then
  err "Go not installed. Install Go 1.26+ from https://go.dev/dl/"
  exit 1
fi

GO_VER=$(go version | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?' | head -1)
GO_MAJOR=${GO_VER%%.*}
GO_MINOR=$(echo "$GO_VER" | cut -d. -f2)

if (( GO_MAJOR < 1 )) || { (( GO_MAJOR == 1 )) && (( GO_MINOR < 26 )); }; then
  err "Go $GO_VER is too old. Need 1.26+. Install from https://go.dev/dl/"
  exit 1
fi
info "Go $GO_VER detected"

# ----------------------------------------------------------------------------
# dev mode: delegate to `make dev`
# ----------------------------------------------------------------------------
if (( DEV == 1 )); then
  info "Starting in dev mode (hot reload)..."
  exec make dev
fi

# ----------------------------------------------------------------------------
# ensure go.work exists (workspace points to local modules incl. zai-web)
# ----------------------------------------------------------------------------
if [[ ! -f go.work ]]; then
  info "go.work missing — running setup-workspace..."
  make setup-workspace
  success "workspace ready"
else
  echo "${C_DIM}  go.work already exists — skipping setup${C_RESET}"
fi

# ----------------------------------------------------------------------------
# decide whether to build:
#   - --rebuild flag forces it
#   - binary doesn't exist
#   - any .go file under core/ or transports/ is newer than the binary
# ----------------------------------------------------------------------------
BIN=tmp/bifrost-http
NEED_BUILD=0

if (( REBUILD == 1 )); then
  NEED_BUILD=1
  info "rebuild requested"
elif [[ ! -x "$BIN" ]]; then
  NEED_BUILD=1
  info "binary not found — will build"
else
  # Find any .go file newer than the binary (excluding test files)
  if find core transports -name "*.go" ! -name "*_test.go" -newer "$BIN" -print -quit 2>/dev/null | grep -q .; then
    NEED_BUILD=1
    info "source changed since last build — will rebuild"
  fi
fi

if (( NEED_BUILD == 1 )); then
  info "building bifrost-http (this may take 1-3 minutes on first run)..."
  make build LOCAL=1
  success "build complete: $BIN"
else
  echo "${C_DIM}  binary up-to-date — skipping build${C_RESET}"
fi

# ----------------------------------------------------------------------------
# zai-web browser worker (optional — auto-starts if cookies file exists)
# ----------------------------------------------------------------------------
ZAI_COOKIES="${ZAI_COOKIES:-chat.z.ai_cookies.json}"
ZAI_WORKER_PORT="${ZAI_WORKER_PORT:-9001}"
ZAI_WORKER_PID=""

cleanup() {
  if [[ -n "$ZAI_WORKER_PID" ]]; then
    info "stopping zai worker (pid $ZAI_WORKER_PID)..."
    kill "$ZAI_WORKER_PID" 2>/dev/null || true
    wait "$ZAI_WORKER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

if [[ -f "$ZAI_COOKIES" ]]; then
  # Check Python + dependencies
  if command -v python3 >/dev/null 2>&1; then
    ZAI_SCRIPT="transports/bifrost-http/handlers/zai_browser_worker.py"
    if [[ -f "$ZAI_SCRIPT" ]]; then
      info "starting zai-web browser worker (port ${ZAI_WORKER_PORT}, headless)..."
      python3 "$ZAI_SCRIPT" \
        --port "$ZAI_WORKER_PORT" \
        --profile ~/.bifrost/zai-profiles/default \
        --no-geoip \
        --headless \
        --cookies "$ZAI_COOKIES" &
      ZAI_WORKER_PID=$!
      export BIFROST_ZAI_BROWSER_POOL_URL="http://127.0.0.1:${ZAI_WORKER_PORT}"

      # Wait for worker to be ready (max 15s)
      for i in $(seq 1 15); do
        sleep 1
        if curl -s "http://127.0.0.1:${ZAI_WORKER_PORT}/status" >/dev/null 2>&1; then
          success "zai worker ready (pid $ZAI_WORKER_PID)"
          break
        fi
        if ! kill -0 "$ZAI_WORKER_PID" 2>/dev/null; then
          warn "zai worker exited — zai-web provider will use direct mode"
          ZAI_WORKER_PID=""
          unset BIFROST_ZAI_BROWSER_POOL_URL
          break
        fi
      done
    fi
  else
    warn "python3 not found — skipping zai-web worker"
  fi
else
  echo "${C_DIM}  no cookies file ($ZAI_COOKIES) — zai-web worker skipped${C_RESET}"
  echo "${C_DIM}  tip: export cookies from browser → $ZAI_COOKIES to enable${C_RESET}"
fi

# ----------------------------------------------------------------------------
# run
# ----------------------------------------------------------------------------
echo
success "starting bifrost-http on http://localhost:${PORT}"
echo "${C_DIM}  UI:  http://localhost:${PORT}/workspace/providers${C_RESET}"
echo "${C_DIM}  API: http://localhost:${PORT}/v1/chat/completions${C_RESET}"
[[ -n "$ZAI_WORKER_PID" ]] && echo "${C_DIM}  Zai: http://127.0.0.1:${ZAI_WORKER_PORT}/status${C_RESET}"
echo "${C_DIM}  Stop with Ctrl+C${C_RESET}"
echo

exec "$BIN" -port "$PORT"
