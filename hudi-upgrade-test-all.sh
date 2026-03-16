#!/bin/bash
#
# Hudi Upgrade Test Runner - All table types and CDC combinations
# Runs hudi-upgrade-test.sh for COPY_ON_WRITE and MERGE_ON_READ, each with
# IS_CDC false and true. Uses same hudi-upgrade.properties as hudi-upgrade-test.sh.
#
# Usage: bash hudi-upgrade-test-all.sh
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROPERTIES_FILE="${SCRIPT_DIR}/hudi-upgrade.properties"
if [[ -f "$PROPERTIES_FILE" ]]; then
  set -a
  # shellcheck source=hudi-upgrade.properties
  source "$PROPERTIES_FILE"
  set +a
fi

declare TABLE_TYPES=("COPY_ON_WRITE" "MERGE_ON_READ")
declare IS_CDCS=("false" "true")

for TABLE_TYPE in "${TABLE_TYPES[@]}"; do
    for IS_CDC in "${IS_CDCS[@]}"; do
        echo "Running test for table type: ${TABLE_TYPE} and is cdc: ${IS_CDC}"
        bash "${SCRIPT_DIR}/hudi-upgrade-test.sh" "${TABLE_TYPE}" "${IS_CDC}"
    done
done