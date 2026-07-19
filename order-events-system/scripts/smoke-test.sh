#!/usr/bin/env bash
#
# Smoke-tests order-service's REST API against a running docker compose stack.
# Start the stack first: docker compose up --build -d
# Then run:              ./scripts/smoke-test.sh
#
# Scope: checks POST /orders (happy path + both failure-stock scenarios) and
# GET /orders/{id} (order persisted with status). It does NOT verify the
# downstream Kafka outcome (InventoryReserved/InventoryFailed) — that still
# needs `docker compose logs` or Redpanda Console. Since the Week 2 outbox,
# POST means "accepted and durably recorded"; publishing happens async.

set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
RESPONSE_FILE="$(mktemp)"
trap 'rm -f "$RESPONSE_FILE"' EXIT
PASS=0
FAIL=0

check() {
  local name="$1" expected_status="$2" method="$3" path="$4" body="$5" expect_substr="$6"
  local status response_body

  if [[ "$method" == "GET" ]]; then
    status="$(curl -s -o "$RESPONSE_FILE" -w '%{http_code}' "$BASE_URL$path")"
  else
    status="$(curl -s -o "$RESPONSE_FILE" -w '%{http_code}' -X POST "$BASE_URL$path" \
      -H "Content-Type: application/json" \
      -d "$body")"
  fi
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

check "happy path (WIDGET-1, enough stock)" 200 POST /orders \
  '{"customer_id": "cust-smoke-1", "sku": "WIDGET-1", "quantity": 1}' \
  '"status":"accepted"'

# The happy-path response embeds the order_id — read it for the GET check.
ORDER_ID="$(python3 -c "import json,sys; print(json.load(sys.stdin)['event']['order_id'])" < "$RESPONSE_FILE")"

check "insufficient stock (WIDGET-2, qty > 5)" 200 POST /orders \
  '{"customer_id": "cust-smoke-2", "sku": "WIDGET-2", "quantity": 999}' \
  '"status":"accepted"'

check "zero-stock sku (WIDGET-3)" 200 POST /orders \
  '{"customer_id": "cust-smoke-3", "sku": "WIDGET-3", "quantity": 1}' \
  '"status":"accepted"'

check "order persisted (GET /orders/{id})" 200 GET "/orders/$ORDER_ID" "" \
  '"status":"PLACED"'

check "unknown order id is 404" 404 GET "/orders/does-not-exist" "" ""

echo
echo "Results: $PASS passed, $FAIL failed"
[[ "$FAIL" -eq 0 ]]
