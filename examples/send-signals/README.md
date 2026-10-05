# Send signals fixture (P1)

Against a locally running worker (`npx wrangler dev` in otel-worker/, port 24318):

```sh
TOKEN=default-token  # value of AUTH_TOKEN in otel-worker/.dev.vars
curl -X POST http://localhost:24318/v1/traces  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' --data-binary @../../otel-worker/examples/send-trace/trace.json
curl -X POST http://localhost:24318/v1/logs    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' --data-binary @logs.json
curl -X POST http://localhost:24318/v1/metrics -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' --data-binary @metrics.json

curl http://localhost:24318/v1/logs    -H "Authorization: Bearer $TOKEN"
curl http://localhost:24318/v1/metrics -H "Authorization: Bearer $TOKEN"
# logs correlated to the fixture trace:
curl http://localhost:24318/v1/traces/2b76e003e3cff12e054bcd0ca6879ee4/logs -H "Authorization: Bearer $TOKEN"
```

Expected: 2 logs, 2 metric samples (1 gauge queue.depth=7, 1 histogram http.server.duration sum=42.5).
