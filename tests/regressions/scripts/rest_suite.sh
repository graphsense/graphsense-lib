#!/usr/bin/env bash
# REST regression suite runner.
#
# Compares a REFERENCE server against a CURRENT server, where each side is
# either a hosted deployment (URL) or a version of this repo (git ref /
# working tree) that gets built into a Docker image and served locally.
#
# Invoked by `make rest REF=... CUR=... DEPTH=...` or directly:
#   scripts/rest_suite.sh <reference> <current> <depth>

set -u -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGRESSIONS_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(cd "$REGRESSIONS_DIR/../.." && pwd)"
GSLIB_REPO="https://github.com/graphsense/graphsense-lib.git"

CURRENT_PORT="${REST_CURRENT_PORT:-19100}"
BASELINE_PORT="${REST_BASELINE_PORT:-19101}"
CONFIG_FILE="${REST_CONFIG_FILE:-$REPO_ROOT/instance/config.yaml}"

usage() {
    cat <<'EOF'
Usage: make rest REF=<reference> CUR=<current> DEPTH=<quick|standard|full>
       scripts/rest_suite.sh <reference> <current> <depth>

Compares two versions of the GraphSense REST API endpoint-by-endpoint.
REFERENCE is the trusted side (baseline), CURRENT is the side under test.
Differences are reported per endpoint, plus a timing report
(reports/regression_timing_report.json).

REFERENCE / CURRENT accept any of:
  api.iknaio.com          hosted production  (needs GS_API_KEY in env)
  api.test.iknaio.com     hosted test        (needs GS_API_KEY in env)
  http://host:port        any already-running server
  local                   this working tree, incl. uncommitted changes
                          (built into a Docker image, served locally)
  <tag|branch|commit>     e.g. v25.11.18, master, 837b88df
                          (built into a Docker image, served locally)

DEPTH selects which suites run (cumulative):
  quick     manual suite (hand-written edge cases)   ~40 calls     ~1-2 min
  standard  quick + fuzz (endpoint families sweep)   ~85 calls     ~3-5 min
  full      standard + loki (replayed prod calls)    ~14k calls    ~15-30 min
            (loki runs with LOKI_WORKERS parallel pytest workers, default 8;
            the timing report only covers part of the calls in that mode)

  Estimates assume warm servers; add ~10-15 min once per git-ref/local side
  for a cold Docker image build (seconds when the layer cache is warm).
  Built servers are left running so re-runs skip the build entirely
  (stop them with: make rest-stop).

Environment overrides:
  REST_CURRENT_PORT / REST_BASELINE_PORT   local server ports (19100/19101)
  REST_CONFIG_FILE     config for locally served sides (instance/config.yaml)
  GS_API_KEY           API key for *.iknaio.com sides
  TAGSTORE_URL         used to derive the tagstore DSN for local sides
  REBUILD=1            force docker rebuild even if the image exists
  LOKI_WORKERS         parallel pytest workers for the loki suite (default 8)

Examples:
  make rest REF=api.iknaio.com CUR=local DEPTH=quick
  make rest REF=v25.11.18 CUR=local DEPTH=standard
  make rest REF=api.iknaio.com CUR=api.test.iknaio.com DEPTH=full
  make rest REF=837b88df CUR=feature/clustering2 DEPTH=quick

Notes:
  - CUR=api.iknaio.com fails the tag-obfuscation test by design: it sends
    an anonymous request, which the production gateway rejects.
  - Suites run against live keyspaces; results depend on the configured
    Cassandra data being reachable from both sides.
EOF
}

log() { printf '\033[36m[rest-suite]\033[0m %s\n' "$*"; }
die() { printf '\033[31m[rest-suite] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

port_in_use() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

if [ "$#" -eq 0 ] || [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
    usage
    exit 0
fi
[ "$#" -eq 3 ] || { usage; die "expected exactly 3 arguments, got $#"; }

REF_SPEC="$1"
CUR_SPEC="$2"
DEPTH="$3"

case "$DEPTH" in
    quick|standard|full) ;;
    small) DEPTH=quick ;;
    medium) DEPTH=standard ;;
    large) DEPTH=full ;;
    *) usage; die "DEPTH must be quick, standard or full (got '$DEPTH')" ;;
esac

if [ "$DEPTH" = "full" ] && [ ! -f "$REGRESSIONS_DIR/tests/rest/test_loki_generated.py" ]; then
    die "DEPTH=full needs tests/rest/test_loki_generated.py — run 'make generate-loki' first"
fi

# ---------------------------------------------------------------------------
# Image building (git ref / working tree -> docker image)
# ---------------------------------------------------------------------------

resolve_commit() { # <spec> -> full sha on stdout, or fail
    git -C "$REPO_ROOT" rev-parse --verify --quiet "$1^{commit}" && return 0
    log "'$1' not found locally, fetching from origin..." >&2
    git -C "$REPO_ROOT" fetch --quiet --tags origin >&2 || true
    git -C "$REPO_ROOT" rev-parse --verify --quiet "$1^{commit}"
}

scm_version() { # <checkout dir> -> PEP440 version on stdout
    # the Dockerfile COPYs the tree without .git, so the version has to be
    # computed on the host and passed in (same as the root Makefile does)
    (cd "$1" && uv run --no-project --with "setuptools>=77" --with "setuptools-scm>=8" python -m setuptools_scm 2>/dev/null)
}

build_image() { # <image> <context dir> <what>
    local version
    version=$(scm_version "$2")
    [ -n "$version" ] || die "could not compute setuptools-scm version for $3"
    docker build -t "$1" \
        --build-arg "SETUPTOOLS_SCM_PRETEND_VERSION_FOR_GRAPHSENSE_LIB=$version" \
        "$2" >&2
}

ensure_image() { # <spec> -> image tag on stdout
    if [ "$1" = "local" ]; then
        local image="gslib-rest:local"
        log "building $image from working tree $REPO_ROOT (incl. uncommitted changes)..." >&2
        build_image "$image" "$REPO_ROOT" "working tree" || die "docker build of working tree failed"
        echo "$image"
        return 0
    fi

    local sha
    sha=$(resolve_commit "$1") || die "'$1' is neither a known URL form nor a resolvable git ref"
    local image="gslib-rest:${sha:0:12}"

    if [ "${REBUILD:-}" != "1" ] && docker image inspect "$image" >/dev/null 2>&1; then
        log "image $image for '$1' already built, reusing (REBUILD=1 to force)" >&2
        echo "$image"
        return 0
    fi

    local tmp
    tmp=$(mktemp -d /tmp/gslib-rest-build-XXXXXX)
    log "building $image from $1 ($sha) — cold builds take ~10-15 min..." >&2
    # local clone is fast (hardlinked objects) and keeps setuptools-scm happy
    git clone --quiet "$REPO_ROOT" "$tmp" >&2 || { rm -rf "$tmp"; die "git clone failed"; }
    if ! git -C "$tmp" checkout --quiet "$sha" 2>/dev/null; then
        git -C "$tmp" fetch --quiet --tags "$GSLIB_REPO" >&2
        git -C "$tmp" checkout --quiet "$sha" || { rm -rf "$tmp"; die "cannot check out $sha"; }
    fi
    build_image "$image" "$tmp" "$1" || { rm -rf "$tmp"; die "docker build of $1 failed"; }
    rm -rf "$tmp"
    echo "$image"
}

# ---------------------------------------------------------------------------
# Local serving
# ---------------------------------------------------------------------------

tagstore_async_url() {
    if [ -n "${GS_TAGSTORE_ASYNC_URL:-}" ]; then
        echo "$GS_TAGSTORE_ASYNC_URL"
    elif [ -n "${TAGSTORE_URL:-}" ]; then
        echo "$TAGSTORE_URL" | sed 's|^postgresql://|postgresql+asyncpg://|'
    fi
}

ensure_server() { # <role: current|baseline> <port> <image>
    local role="$1" port="$2" image="$3"
    local name="gs-rest-$role"

    # Compare image IDs, not tag strings: after a rebuild the tag (e.g.
    # gslib-rest:local) points at a new image while the running container
    # keeps the old one — a tag comparison would silently reuse stale code.
    local running_image_id running_port desired_image_id
    running_image_id=$(docker inspect --format '{{.Image}}' "$name" 2>/dev/null || true)
    running_port=$(docker inspect --format '{{index .Config.Labels "gs-rest-port"}}' "$name" 2>/dev/null || true)
    if [ -n "$running_image_id" ]; then
        desired_image_id=$(docker image inspect --format '{{.Id}}' "$image" 2>/dev/null || true)
        if [ "$running_image_id" = "$desired_image_id" ] && [ "$running_port" = "$port" ]; then
            log "reusing running $name ($image on :$port)"
            return 0
        fi
        log "replacing $name (stale image or port -> $image on :$port)"
        docker rm -f "$name" >/dev/null 2>&1
    fi

    if port_in_use "$port"; then
        die "port $port is already in use by something other than $name — stop that process or set REST_CURRENT_PORT / REST_BASELINE_PORT to a free port"
    fi
    [ -f "$CONFIG_FILE" ] || [ -L "$CONFIG_FILE" ] || die "config not found at $CONFIG_FILE (set REST_CONFIG_FILE)"

    local real_config tagstore_dsn
    real_config=$(readlink -f "$CONFIG_FILE")
    tagstore_dsn=$(tagstore_async_url)
    local extra_env=()
    [ -n "$tagstore_dsn" ] && extra_env+=(-e "GS_TAGSTORE_ASYNC_URL=$tagstore_dsn")

    log "starting $name ($image) on port $port..."
    docker run -d --name "$name" --rm --network=host \
        --label "gs-rest-port=$port" \
        -v "$real_config:/srv/graphsense-rest/instance/config.yaml:ro" \
        -e CONFIG_FILE=/srv/graphsense-rest/instance/config.yaml \
        -e NUM_WORKERS=1 -e NUM_THREADS=1 -e TZ=UTC \
        "${extra_env[@]}" \
        "$image" \
        sh -c "gunicorn -c /opt/gunicorn-conf.py --bind 0.0.0.0:$port 'graphsenselib.web.app:create_app(\"/srv/graphsense-rest/instance/config.yaml\")' --worker-class uvicorn.workers.UvicornWorker" \
        >/dev/null || die "docker run of $name failed"

    local i
    for i in $(seq 1 60); do
        if curl -s -o /dev/null "http://localhost:$port/stats"; then
            return 0
        fi
        sleep 2
    done
    echo "--- $name logs ---" >&2
    docker logs "$name" 2>&1 | tail -20 >&2
    die "$name did not become ready within 120s"
}

# ---------------------------------------------------------------------------
# Side resolution: spec -> URL + auth + gateway headers
# ---------------------------------------------------------------------------

setup_side() { # <role: current|baseline> <spec> <port>; sets <ROLE>_URL/_AUTH/_HDRS
    local role="$1" spec="$2" port="$3"
    local url auth hdrs

    case "$spec" in
        http://*|https://*)
            url="$spec" ;;
        *.iknaio.com)
            url="https://$spec" ;;
        *)
            local image
            image=$(ensure_image "$spec") || exit 1
            ensure_server "$role" "$port" "$image"
            url="http://localhost:$port"
            ;;
    esac

    if [[ "$url" == *"iknaio.com"* ]]; then
        [ -n "${GS_API_KEY:-}" ] || die "$role=$spec needs GS_API_KEY in the environment"
        auth="$GS_API_KEY"
        hdrs="{}"
    else
        # replay what the production API gateway injects, so tag visibility
        # matches a hosted deployment
        auth="test"
        hdrs='{"X-Consumer-Groups":"tags-private"}'
    fi

    curl -fsS -o /dev/null --max-time 20 -H "Authorization: $auth" "$url/stats" \
        || die "$role server $url is not responding on /stats"

    if [ "$role" = "current" ]; then
        CUR_URL="$url"; CUR_AUTH="$auth"; CUR_HDRS="$hdrs"
    else
        REF_URL="$url"; REF_AUTH="$auth"; REF_HDRS="$hdrs"
    fi
}

setup_side baseline "$REF_SPEC" "$BASELINE_PORT"
setup_side current "$CUR_SPEC" "$CURRENT_PORT"

# ---------------------------------------------------------------------------
# Test execution
# ---------------------------------------------------------------------------

# don't ping Slack from interactive comparison runs unless explicitly configured
if [ -z "${GRAPHSENSE_CONFIG_YAML:-}" ]; then
    NO_SLACK_YAML=$(mktemp /tmp/gs-rest-suite-noslack-XXXXXX.yaml)
    echo "slack_topics: {}" > "$NO_SLACK_YAML"
    export GRAPHSENSE_CONFIG_YAML="$NO_SLACK_YAML"
fi

export BASELINE_SERVER="$REF_URL" CURRENT_SERVER="$CUR_URL"
export BASELINE_AUTH="${BASELINE_AUTH:-$REF_AUTH}" CURRENT_AUTH="${CURRENT_AUTH:-$CUR_AUTH}"
export BASELINE_HEADERS="${BASELINE_HEADERS:-$REF_HDRS}" CURRENT_HEADERS="${CURRENT_HEADERS:-$CUR_HDRS}"

log "reference (baseline): $REF_SPEC -> $REF_URL"
log "current  (under test): $CUR_SPEC -> $CUR_URL"
log "depth: $DEPTH"

run_suite() { # <label> <pytest args...>
    local label="$1"; shift
    log "running $label suite..."
    local start=$SECONDS
    if (cd "$REGRESSIONS_DIR" && uv run pytest "$@"); then
        RESULTS+=("$label: PASSED ($((SECONDS - start))s)")
    else
        RESULTS+=("$label: FAILED ($((SECONDS - start))s)")
        RET=1
    fi
}

RESULTS=()
RET=0
run_suite manual tests/rest/test_manual_regression.py -v -m regression
if [ "$DEPTH" = "standard" ] || [ "$DEPTH" = "full" ]; then
    run_suite fuzz tests/rest/test_baseline_regression.py -v -m regression
fi
if [ "$DEPTH" = "full" ]; then
    run_suite loki tests/rest/test_loki_generated.py -m loki_generated -n "${LOKI_WORKERS:-8}"
fi

echo
log "=== summary ($REF_SPEC vs $CUR_SPEC, $DEPTH) ==="
for r in "${RESULTS[@]}"; do log "  $r"; done
log "timing report: $REGRESSIONS_DIR/reports/regression_timing_report.json"
if docker ps --format '{{.Names}}' | grep -q '^gs-rest-'; then
    log "local servers left running for fast re-runs — stop with: make rest-stop"
fi
exit $RET
