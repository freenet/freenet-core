#!/bin/bash
# check-endpoints.sh - Check all known Freenet endpoints and report status

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

TIMEOUT=5

check_endpoint() {
    local name="$1"
    local url="$2"
    local start=$(date +%s%N)
    local http_code
    http_code=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout "$TIMEOUT" "$url" 2>/dev/null)
    local end=$(date +%s%N)
    local ms=$(( (end - start) / 1000000 ))

    if [ "$http_code" -ge 200 ] 2>/dev/null && [ "$http_code" -lt 400 ]; then
        printf "  ${GREEN}%-30s${NC}  %s  %4dms\n" "$name" "$http_code" "$ms"
    elif [ "$http_code" = "000" ]; then
        printf "  ${RED}%-30s${NC}  DOWN  %4dms\n" "$name" "$ms"
    else
        printf "  ${YELLOW}%-30s${NC}  %s  %4dms\n" "$name" "$http_code" "$ms"
    fi
}

echo ""
echo "Freenet Endpoint Status"
echo "======================="
echo ""
printf "  %-30s  %s  %s\n" "ENDPOINT" "CODE" "LATENCY"
printf "  %-30s  %s  %s\n" "--------" "----" "-------"

# Run all checks in parallel, collect output
{
    check_endpoint "nova gateway (HTTP)"      "http://5.9.111.215:31337" &
    check_endpoint "nova gateway (HTTPS)"     "https://freenet.org"      &
    check_endpoint "vega gateway (HTTP)"      "http://vega.locut.us:31337" &
    check_endpoint "river.freenet.org"        "https://river.freenet.org" &
    check_endpoint "nova telemetry health"    "http://5.9.111.215:13133/health" &
    check_endpoint "nova telemetry dashboard" "http://nova.locut.us:3133" &
    wait
}

echo ""
