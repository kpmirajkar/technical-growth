#!/usr/bin/env bash
#
# Smoke-tests order-service's REST API against a running docker compose stack.
# Start the stack first: docker compose up --build -d
# Then run:              ./scripts/smoke-test.sh
#
# Scope: checks that POST /orders responds correctly for the happy path and
# both failure-stock scenarios. It does NOT verify the downstream Kafka outcome
# (InventoryReserved/InventoryFailed) — that still needs `docker compose logs`
# or Redpanda Console, since order-service's response only confirms the publish,
# not the inventory decision.

set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
RESPONSE_FILE="$(mktemp)"
trap 'rm -f "$RESPONSE_FILE"' EXIT
PASS=0
FAIL=0

check() {
  local name="$1" expected_status="$2" body="$3" expect_substr="$4"
  local status response_body

  status="$(curl -s -o "$RESPONSE_FILE" -w '%{http_code}' -X POST "$BASE_URL/orders" \
    -H "Content-Type: application/json" \
    -d "$body")"
  response_body="$(cat "$RESPONSE_FILE")"

  if [[ "$status" == "$expected_status" && "$response_body" == *"$expect_substr"* ]]; then
    echo "PASS: $name (HTTP $status)"
    PASS=$((PASS + 1))
  else
    echo "FAIL: $name (expected HTTP $expected_status containing '$expect_substr', got HTTP $status)"
    echo "  response: $response_body"
    FAIL=$((FAIL + 1))
  fi
}

echo "Running smoke tests against $BASE_URL ..."
echo

check "happy path (WIDGET-1, enough stock)" 200 \
  '{"customer_id": "cust-smoke-1", "sku": "WIDGET-1", "quantity": 1}' \
  '"status":"published"'

check "insufficient stock (WIDGET-2, qty > 5)" 200 \
  '{"customer_id": "cust-smoke-2", "sku": "WIDGET-2", "quantity": 999}' \
  '"status":"published"'

check "zero-stock sku (WIDGET-3)" 200 \
  '{"customer_id": "cust-smoke-3", "sku": "WIDGET-3", "quantity": 1}' \
  '"status":"published"'

echo
echo "Results: $PASS passed, $FAIL failed"
[[ "$FAIL" -eq 0 ]]
