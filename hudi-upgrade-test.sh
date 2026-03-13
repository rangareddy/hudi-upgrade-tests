#!/bin/bash
#
# Hudi Upgrade Test Runner - Complete Workflow
# Usage: hudi-upgrade-test.sh <TABLE_TYPE> <IS_CDC>
# 
# Generates table → Baseline tests → Upgrade → Post-upgrade tests
#

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

WORKING_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_GENERATOR="${WORKING_DIR}/hudi_upgrade_data_generator.py"
QUERY_TESTER="${WORKING_DIR}/hudi_upgrade_test_queries.py"
TABLE_TYPE="${1:-COPY_ON_WRITE}"
IS_CDC="${2:-false}"

SOURCE_HUDI_VERSION=0.12.3
SCALA_VERSION=2.12
TARGET_HUDI_VERSION=0.15.0

log() { echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $*" >&2; }
success() { echo -e "${GREEN}[SUCCESS]${NC} $*" >&2; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*" >&2; }

spark_run() {
    local spark_version="$1"
    local hudi_version="$2"
    local scala_version="$3"
    local script="$4"
    shift 4
    local args=("$@")
    local spark_version_str="${spark_version//./}"
    local spark_home_var="SPARK${spark_version_str}_HOME"
    local spark_home="${!spark_home_var:-}"

    if [[ -n "$spark_home" && -d "$spark_home" ]]; then
        log "Using SPARK_HOME: $spark_home"
        export SPARK_HOME="$spark_home"
    else
        log "No SPARK${spark_version_str}_HOME found"
        exit 1
    fi
    
    local hudi_package="org.apache.hudi:hudi-spark${spark_version}-bundle_${scala_version}:${hudi_version}"
    log "🚀 Running: spark-submit --packages ${hudi_package} ${script} ${args[*]}"
    
    spark-submit \
        --packages "${hudi_package}" \
        --driver-memory 2g \
        --executor-memory 2g \
        "${script}" \
        "${args[@]}"
    
    success "$(basename "${script}") completed"
}

main() {
    local table_type_upper=$(echo "$TABLE_TYPE" | tr '[:lower:]' '[:upper:]')
    
    log "🎯 Hudi Upgrade Test: ${table_type_upper} (CDC: ${IS_CDC})"
    log "================================================================"
    
    local baseline_versions=(
        "3.1,${SOURCE_HUDI_VERSION}"
        "3.2,${SOURCE_HUDI_VERSION}"
        "3.3,${SOURCE_HUDI_VERSION}"
    )
    local upgrade_versions=(
        "3.4,${TARGET_HUDI_VERSION}"
        "3.5,${TARGET_HUDI_VERSION}"
    )

    local all_versions=("${baseline_versions[@]}" "${upgrade_versions[@]}")

    # Step 1: Generate table with OLD Hudi
    log "📊 STEP 1: Generate table with Hudi ${SOURCE_HUDI_VERSION}"
    spark_run "3.1" "${SOURCE_HUDI_VERSION}" "${SCALA_VERSION}" \
              "${DATA_GENERATOR}" \
              "${TABLE_TYPE}" "${IS_CDC}" "init"

     # Step 2: Baseline tests (pre-upgrade)
    log "🔍 STEP 2: Baseline Query Tests (Pre-Upgrade)"
    for version_pair in "${all_versions[@]}"; do
        IFS=',' read -r spark_version hudi_version <<< "$version_pair"
        log "Spark ${spark_version} + Hudi ${hudi_version}"
        spark_run "${spark_version}" "${hudi_version}" "${SCALA_VERSION}" \
                  "${QUERY_TESTER}" \
                  "${TABLE_TYPE}" "${IS_CDC}" "baseline"
    done

     # Step 3: UPGRADE Hudi table
    log "🔄 STEP 3: UPGRADE Hudi Table to ${TARGET_HUDI_VERSION}"
    spark_run "3.5" "${TARGET_HUDI_VERSION}" "${SCALA_VERSION}" \
              "${DATA_GENERATOR}" \
              "${TABLE_TYPE}" "${IS_CDC}" "upgrade"
    
    # Step 4: Post-upgrade tests
    log "✅ STEP 4: Post-Upgrade Query Tests"
    for version_pair in "${all_versions[@]}"; do
        IFS=',' read -r spark_version hudi_version <<< "$version_pair"
        log "Spark ${spark_version} + Hudi ${hudi_version}"
        spark_run "${spark_version}" "${hudi_version}" "${SCALA_VERSION}" \
                  "${QUERY_TESTER}" \
                  "${TABLE_TYPE}" "${IS_CDC}" "upgrade"
    done
    
    success "End to end hudi upgrade test completed successfully and results are stored in the directory: ${WORKING_DIR}/results!"
}

# Main execution
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    if [[ $# -lt 1 ]]; then
        cat << EOF
Usage: $0 TABLE_TYPE [IS_CDC]
  TABLE_TYPE: COPY_ON_WRITE | MERGE_ON_READ
  IS_CDC:    true | false (default: false)

Examples:
  $0 COPY_ON_WRITE                    # COW baseline+upgrade
  $0 MERGE_ON_READ true               # MOR CDC tables
EOF
        exit 1
    fi
    
    main "$@"
fi