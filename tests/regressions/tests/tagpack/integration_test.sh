#!/bin/bash
set -e

# Integration test for tagpack pipeline
# This script:
# 1. Starts a PostgreSQL container
# 2. Initializes the tagstore schema (using graphsense-lib)
# 3. Clones graphsense-tagpacks repository
# 4. Validates and inserts all tagpacks
# 5. Runs verification queries
#
# Environment variables:
#   GRAPHSENSE_LIB_LOCAL - Path to local graphsense-lib repo (default: clones from GitHub)
#   GRAPHSENSE_LIB_VERSION - Version/branch/tag to use (default: master)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NIGHTLY_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# graphsense-lib location (source for CLI tools)
GRAPHSENSE_LIB_REPO="https://github.com/graphsense/graphsense-lib.git"
GRAPHSENSE_LIB_LOCAL="${GRAPHSENSE_LIB_LOCAL:-}"
GRAPHSENSE_LIB_VERSION="${GRAPHSENSE_LIB_VERSION:-master}"

TAGPACKS_DIR="$SCRIPT_DIR/test-tagpacks"
TAGPACKS_REPO="https://github.com/graphsense/graphsense-tagpacks.git"
DB_URL="postgresql://graphsense:graphsense123@localhost:5432/tagstore"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    log_info "Cleaning up..."
    cd "$SCRIPT_DIR"
    docker compose down -v 2>/dev/null || true
    # Data directory is owned by root from PostgreSQL container, use docker to remove
    if [ -d "./data" ]; then
        docker run --rm -v "$SCRIPT_DIR/data:/data" alpine sh -c "rm -rf /data/*" 2>/dev/null || true
        rmdir "./data" 2>/dev/null || true
    fi
    rm -rf "$TAGPACKS_DIR" 2>/dev/null || true
    rm -f "$SCRIPT_DIR/.env" 2>/dev/null || true
    rm -f "$SCRIPT_DIR/postgres-conf.sql" 2>/dev/null || true
    # Clean up cloned graphsense-lib if we cloned it
    if [ -n "$CLONED_GRAPHSENSE_LIB" ] && [ -d "$CLONED_GRAPHSENSE_LIB" ]; then
        rm -rf "$CLONED_GRAPHSENSE_LIB" 2>/dev/null || true
    fi
}

# Trap to ensure cleanup on exit
trap cleanup EXIT

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v docker &> /dev/null; then
        log_error "docker is not installed"
        exit 1
    fi

    if ! command -v git &> /dev/null; then
        log_error "git is not installed"
        exit 1
    fi

    if ! command -v uv &> /dev/null; then
        log_error "uv is not installed"
        exit 1
    fi
}

# Setup graphsense-lib (clone if not provided)
setup_graphsense_lib() {
    if [ -n "$GRAPHSENSE_LIB_LOCAL" ] && [ -d "$GRAPHSENSE_LIB_LOCAL" ]; then
        log_info "Using local graphsense-lib at: $GRAPHSENSE_LIB_LOCAL"
        REPO_ROOT="$GRAPHSENSE_LIB_LOCAL"
    else
        log_info "Cloning graphsense-lib (version: $GRAPHSENSE_LIB_VERSION)..."
        CLONED_GRAPHSENSE_LIB="$SCRIPT_DIR/graphsense-lib"
        if [ -d "$CLONED_GRAPHSENSE_LIB" ]; then
            rm -rf "$CLONED_GRAPHSENSE_LIB"
        fi
        git clone --depth 1 --branch "$GRAPHSENSE_LIB_VERSION" "$GRAPHSENSE_LIB_REPO" "$CLONED_GRAPHSENSE_LIB" 2>/dev/null || \
            (git clone "$GRAPHSENSE_LIB_REPO" "$CLONED_GRAPHSENSE_LIB" && cd "$CLONED_GRAPHSENSE_LIB" && git checkout "$GRAPHSENSE_LIB_VERSION")
        REPO_ROOT="$CLONED_GRAPHSENSE_LIB"
    fi

    log_info "graphsense-lib ready at: $REPO_ROOT"
}

# Setup environment
setup_environment() {
    log_info "Setting up environment..."
    cd "$SCRIPT_DIR"

    # Create .env file
    cat > .env << EOF
LOCAL_DATA_DIR=./data
POSTGRES_HOST=localhost
POSTGRES_USER=postgres
POSTGRES_DB=tagstore
POSTGRES_PASSWORD=testpassword
POSTGRES_USER_TAGSTORE=graphsense
POSTGRES_PASSWORD_TAGSTORE=graphsense123
POSTGRES_PASSWORD_TAGSTORE_READONLY=readonly123
EOF

    # Create empty postgres-conf.sql
    touch postgres-conf.sql

    # Create docker network if it doesn't exist
    docker network create graphsense 2>/dev/null || true
}

# Start PostgreSQL container
start_database() {
    log_info "Starting PostgreSQL container..."
    cd "$SCRIPT_DIR"

    # Remove old data if exists (use docker for root-owned files)
    if [ -d "./data" ]; then
        docker run --rm -v "$SCRIPT_DIR/data:/data" alpine sh -c "rm -rf /data/*" 2>/dev/null || true
        rmdir ./data 2>/dev/null || true
    fi
    mkdir -p ./data

    docker compose up -d

    # Wait for database to be ready
    log_info "Waiting for database to be ready..."
    for i in {1..30}; do
        if docker compose exec -T db pg_isready -U postgres -d tagstore &>/dev/null; then
            log_info "Database is ready!"
            return 0
        fi
        echo -n "."
        sleep 2
    done

    log_error "Database failed to start within timeout"
    docker compose logs db
    exit 1
}

# Initialize tagstore schema
init_tagstore() {
    log_info "Initializing tagstore schema..."
    cd "$REPO_ROOT"

    # Run tagstore init
    uv run graphsense-cli tagstore init --db-url "$DB_URL"

    log_info "Tagstore schema initialized"
}

# Clone tagpacks repository
clone_tagpacks() {
    log_info "Cloning graphsense-tagpacks repository..."

    if [ -d "$TAGPACKS_DIR" ]; then
        log_info "Tagpacks directory exists, pulling latest..."
        cd "$TAGPACKS_DIR"
        git pull
    else
        git clone --depth 1 "$TAGPACKS_REPO" "$TAGPACKS_DIR"
    fi

    log_info "Tagpacks repository ready"
}

# Validate tagpacks
validate_tagpacks() {
    log_info "Validating tagpacks..."
    cd "$REPO_ROOT"

    uv run graphsense-cli tagpack-tool tagpack validate "$TAGPACKS_DIR/packs" --no-address-validation

    log_info "Tagpack validation completed"
}

# Insert actorpacks (must be done before tagpacks)
insert_actorpacks() {
    log_info "Inserting actorpacks into tagstore..."
    cd "$REPO_ROOT"

    # Check if actors directory exists (graphsense-tagpacks uses 'actors' folder)
    if [ -d "$TAGPACKS_DIR/actors" ]; then
        if ! uv run graphsense-cli tagpack-tool actorpack insert "$TAGPACKS_DIR/actors" \
            --url "$DB_URL" \
            --no-strict-check \
            --no-git \
            --force 2>&1 | tee /tmp/actorpack_insert.log; then
            log_error "Actorpack insertion failed"
            return 1
        fi

        # Check for errors in the output
        if grep -q "FAILED" /tmp/actorpack_insert.log; then
            log_error "Some actorpacks failed to insert"
            return 1
        fi

        log_info "Actorpack insertion completed"
    else
        log_error "No actors directory found at $TAGPACKS_DIR/actors"
        return 1
    fi
}

# Insert tagpacks
insert_tagpacks() {
    log_info "Inserting tagpacks into tagstore..."
    cd "$REPO_ROOT"

    if ! uv run graphsense-cli tagpack-tool tagpack insert "$TAGPACKS_DIR/packs" \
        --url "$DB_URL" \
        --no-strict-check \
        --no-git \
        --no-validation \
        --force \
        --n-workers 1 2>&1 | tee /tmp/tagpack_insert.log; then
        log_error "Tagpack insertion command failed"
        return 1
    fi

    # Check for errors in the output
    if grep -q "FAILED" /tmp/tagpack_insert.log; then
        FAILED_COUNT=$(grep -c "FAILED" /tmp/tagpack_insert.log)
        log_error "$FAILED_COUNT tagpacks failed to insert"
        return 1
    fi

    log_info "Tagpack insertion completed successfully"
}

# Refresh database views
refresh_views() {
    log_info "Refreshing database views..."
    cd "$REPO_ROOT"

    uv run graphsense-cli tagpack-tool tagstore refresh-views --url "$DB_URL"

    log_info "Database views refreshed"
}

# Verify insertion
verify_insertion() {
    log_info "Verifying tagpack insertion..."
    cd "$SCRIPT_DIR"

    # Count records in database
    ACTOR_COUNT=$(docker compose exec -T db psql -U graphsense -d tagstore -t -c "SELECT COUNT(*) FROM actor;" | tr -d ' \n')
    TAG_COUNT=$(docker compose exec -T db psql -U graphsense -d tagstore -t -c "SELECT COUNT(*) FROM tag;" | tr -d ' \n')
    TAGPACK_COUNT=$(docker compose exec -T db psql -U graphsense -d tagstore -t -c "SELECT COUNT(*) FROM tagpack;" | tr -d ' \n')

    log_info "Actors in database: $ACTOR_COUNT"
    log_info "Tags in database: $TAG_COUNT"
    log_info "Tagpacks in database: $TAGPACK_COUNT"

    if [ "$TAG_COUNT" -gt 0 ] && [ "$TAGPACK_COUNT" -gt 0 ] && [ "$ACTOR_COUNT" -gt 0 ]; then
        log_info "✓ Verification passed: Database contains actors, tags and tagpacks"
    else
        log_error "✗ Verification failed: Database is missing data"
        return 1
    fi

    # Query tags for a specific known address
    TEST_ADDRESS="1Archive1n2C579dMsAu3iC6tWzuQJz8dN"
    log_info "Querying tags for address: $TEST_ADDRESS"
    docker compose exec -T db psql -U graphsense -d tagstore -c \
        "SELECT t.identifier, t.label, t.source, tp.title as tagpack_title
         FROM tag t
         JOIN tagpack tp ON t.tagpack = tp.id
         WHERE t.identifier = '$TEST_ADDRESS';"

    # Verify at least one tag exists for this address
    TAG_EXISTS=$(docker compose exec -T db psql -U graphsense -d tagstore -t -c \
        "SELECT COUNT(*) FROM tag WHERE identifier = '$TEST_ADDRESS';" | tr -d ' \n')

    if [ "$TAG_EXISTS" -gt 0 ]; then
        log_info "✓ Found $TAG_EXISTS tag(s) for test address"
        return 0
    else
        log_error "✗ No tags found for test address $TEST_ADDRESS"
        return 1
    fi
}

# Main execution
main() {
    log_info "Starting tagpack integration test..."

    check_prerequisites
    setup_graphsense_lib
    setup_environment
    start_database
    init_tagstore
    clone_tagpacks
    validate_tagpacks
    insert_actorpacks
    insert_tagpacks
    refresh_views
    verify_insertion

    log_info "========================================="
    log_info "Integration test completed successfully!"
    log_info "========================================="
}

# Run main function
main "$@"
