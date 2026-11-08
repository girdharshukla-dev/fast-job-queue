#!/bin/sh

BASE_URL="http://localhost:8080"

echo "### Submit jobs"

JOB1=$(curl -s -X POST "$BASE_URL/jobs" \
  -H "Content-Type: application/json" \
  -d '{"type":"random","payload":{"note":"hello"}}')

echo "$JOB1" | jq .

JOB2=$(curl -s -X POST "$BASE_URL/jobs" \
  -H "Content-Type: application/json" \
  -d '{"type":"sleep","payload":{"ms":2000},"maxAttempts":2,"timeoutMs":10000}')

echo "$JOB2" | jq .

JOB3=$(curl -s -X POST "$BASE_URL/jobs" \
  -H "Content-Type: application/json" \
  -d '{"type":"fail","payload":{"reason":"test"},"maxAttempts":2}')

echo "$JOB3" | jq .

# Extract IDs
ID1=$(echo "$JOB1" | jq -r '.id')
ID2=$(echo "$JOB2" | jq -r '.id')
ID3=$(echo "$JOB3" | jq -r '.id')

echo "### Validation"

curl -s -i -X POST "$BASE_URL/jobs" \
  -H "Content-Type: application/json" \
  -d '{"payload":{"x":1}}'

curl -s -i -X POST "$BASE_URL/jobs" \
  -H "Content-Type: application/json" \
  -d '{invalid json'

echo "### Get job"

curl -s "$BASE_URL/jobs/$ID1" | jq .
curl -s "$BASE_URL/jobs/$ID2" | jq .
curl -s "$BASE_URL/jobs/$ID3" | jq .

curl -s -i "$BASE_URL/jobs/does-not-exist"

echo "### List jobs"

curl -s "$BASE_URL/jobs?status=PENDING" | jq .
curl -s "$BASE_URL/jobs?status=RUNNING" | jq .
curl -s "$BASE_URL/jobs?status=COMPLETED" | jq .
curl -s "$BASE_URL/jobs?status=DEAD" | jq .

echo "### Invalid status"

curl -s -i "$BASE_URL/jobs?status=BOGUS"