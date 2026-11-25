# Normalize a `go test -json` JSONL stream for dorny/test-reporter (golang-json).
#
# The reporter groups events by (Package, Test), takes each group's LAST event,
# and aborts the whole job with "missing elapsed on final test event" if that
# event has no Elapsed field. Only terminal events (pass/fail/skip) carry
# Elapsed; run/output/pause/cont do not. Stray async output from the integration
# suite — attributed by test2json to the most recent test after its terminal
# event — or a truncated/killed stream can leave a group ending on an
# elapsed-less line even when every test passed, turning a green run red.
#
# This reads raw lines (jq -R), echoes every parseable line unchanged, drops
# unparseable ones (a truncated final line), and appends a terminal event
# carrying Elapsed for any group whose last event lacked one: the real result if
# a terminal was seen (a pass stays a pass, a fail stays a fail), else a
# synthetic fail (an interrupted test is a failure, not a silent pass). Genuine
# failures are preserved; the go-test exit code, not this filter, gates the job.
#
# Invoke with `jq -R -n -r -f` and --arg mode "stream" (default: the normalized
# stream) or "interrupted" (the count of groups marked failed for lack of any
# terminal, for a CI warning).

[inputs]
| map({raw: ., o: (try fromjson catch null)}) as $events
| (
    reduce ($events[] | select(.o != null and ((.o.Test // "") != ""))) as $e (
      {order: [], g: {}};
      ($e.o | [.Package, .Test] | tojson) as $k
      | (if .g[$k] == null
           then .order += [$k] | .g[$k] = {pkg: $e.o.Package, test: $e.o.Test, hasElapsed: false, term: null}
           else . end)
      | .g[$k].hasElapsed = ($e.o.Elapsed != null)
      | (if ($e.o.Elapsed != null) and (["pass", "fail", "skip"] | index($e.o.Action))
           then .g[$k].term = {Action: $e.o.Action, Elapsed: $e.o.Elapsed}
           else . end)
    )
  ) as $s
# Groups whose last event lacked Elapsed, in first-seen order — each needs a
# terminal event appended.
| [$s.order[] | $s.g[.] | select(.hasElapsed | not)] as $repair
| if $mode == "interrupted"
    then ($repair | map(select(.term == null)) | length)
    else
      ($events[] | select(.o != null) | .raw),
      ($repair[]
       | if .term != null
           then {Action: .term.Action, Package: .pkg, Test: .test, Elapsed: .term.Elapsed}
           else {Action: "fail", Package: .pkg, Test: .test, Elapsed: 0} end
       | tojson)
  end
