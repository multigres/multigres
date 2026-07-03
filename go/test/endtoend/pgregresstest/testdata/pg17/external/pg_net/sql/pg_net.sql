-- pg_net compatibility suite.
--
-- Upstream pg_net ships pytest/SQLAlchemy tests that manage bespoke web
-- servers and ALTER SYSTEM worker-restart cases. This SQL suite keeps the
-- multigateway compatibility signal deterministic: load the extension through
-- the gateway, queue one async HTTP request, let autocommit expose it to the
-- preloaded background worker, then collect the response from the local
-- httpbin-compatible server the harness serves on 127.0.0.1:9080.
CREATE EXTENSION pg_net;

SELECT true AS ok
FROM (SELECT net.check_worker_is_up()) AS worker;

CREATE TEMP TABLE pg_net_get_request AS
SELECT net.http_get('http://127.0.0.1:9080/get?source=pg_net') AS request_id;

SELECT request_id > 0 AS ok
FROM pg_net_get_request;

SELECT status = 'SUCCESS'::net.request_status
       AND message = 'ok'
       AND (response).status_code = 200
       AND (response).body LIKE '%"method":"GET"%'
       AND (response).body LIKE '%"source":"pg_net"%' AS ok
FROM net._http_collect_response(
  (SELECT request_id FROM pg_net_get_request),
  async := false
);

SELECT count(*) = 1 AS ok
FROM net._http_response;
