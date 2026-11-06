import httpx

from obp_sentinel.collector import LOKI_SINCE, Collector
from obp_sentinel.config import Config
from obp_sentinel.loki_client import LokiClient
from obp_sentinel.store import Store

# Lines as they come out of Loki for OBP Bank Node (read 2026-11-06)
NODE_WARN = ('{"timestamp":"2026-11-06T16:31:27.357523Z","level":"WARN","fields":{"message":"USD/fiat leg fell back",'
             '"currency":"KES","used":"er-api","failed":"pyth: 401 Unauthorized"},"target":"obp_bank_node::fx"}')
UI_WARN = "\x1b[2m2026-11-06T16:29:59.448244Z\x1b[0m \x1b[33m WARN\x1b[0m \x1b[2mobp_bank_node_ui\x1b[0m\x1b[2m:\x1b[0m cookie missing"


def loki(pages: list[list[tuple[str, dict, list[tuple[int, str]]]]], seen: list | None = None) -> LokiClient:
    """A Loki that answers each query with the next page: [(app, labels, [(ts_ns, line)])]."""
    pages = list(pages)

    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        streams = pages.pop(0) if pages else []
        result = [{"stream": {"app": app, **labels}, "values": [[str(ts), line] for ts, line in values]}
                  for app, labels, values in streams]
        return httpx.Response(200, json={"status": "success", "data": {"resultType": "streams", "result": result}})

    return LokiClient("http://loki.test", "1", '{namespace="bank-node"}', httpx.Client(transport=httpx.MockTransport(handler)))


def test_query_selects_the_levels_counted_and_sends_the_tenant():
    seen = []
    client = loki([[]], seen)
    client.entries(["error", "warning"], 5, 100)
    request = seen[0]
    assert request.headers["X-Scope-OrgID"] == "1"
    assert request.url.params["query"] == '{namespace="bank-node"} | detected_level=~"critical|error|fatal|warn|warning"'
    assert request.url.params["direction"] == "forward" and request.url.params["start"] == "5"
    assert loki([]).query(["warning"]).endswith('detected_level=~"warn|warning"')


def test_entries_of_all_streams_come_back_in_time_order_with_ansi_removed():
    client = loki([[("node-a", {"detected_level": "warn"}, [(30, NODE_WARN)]),
                    ("ui-a", {"detected_level": "warn"}, [(20, UI_WARN)]),
                    ("node-b", {"detected_level": "info"}, [(10, "not asked for")])]])
    entries = client.entries(["error", "warning"], 0, 100)
    assert [(e.ts_ns, e.level) for e in entries] == [(20, "warning"), (30, "warning")]
    assert "\x1b" not in entries[0].line and "cookie missing" in entries[0].line


def test_a_full_page_is_followed_by_the_next_one():
    page1 = [("node-a", {"detected_level": "error"}, [(1, "a"), (2, "b")])]
    page2 = [("node-a", {"detected_level": "error"}, [(3, "c")])]
    seen = []
    entries = loki([page1, page2], seen).entries(["error"], 0, 2)
    assert [e.line for e in entries] == ["a", "b", "c"]
    assert seen[1].url.params["start"] == "3"


def test_an_error_from_loki_is_raised():
    client = LokiClient("http://loki.test", "1", "{}", httpx.Client(transport=httpx.MockTransport(
        lambda r: httpx.Response(400, text="parse error"))))
    try:
        client.entries(["error"], 0, 10)
    except RuntimeError as e:
        assert "400" in str(e)
    else:
        raise AssertionError("no error")


def collector(tmp_path, client: LokiClient) -> tuple[Collector, Store]:
    store = Store(str(tmp_path / "s.db"))
    config = Config.from_env()
    return Collector(config, store, client=None, loki=client), store


def test_poll_counts_new_lines_once(tmp_path):
    first = [[("node-a", {"detected_level": "warn"}, [(1_000_000_000_000, NODE_WARN), (2_000_000_000_000, NODE_WARN)])]]
    c, store = collector(tmp_path, loki(first + [[]]))
    assert c.poll_loki() == "loki +2"
    assert store.get_state(LOKI_SINCE, None) == 2_000_000_000_000
    assert c.poll_loki() == "loki +0"  # the next poll starts after the last line and finds nothing
    (total,) = store.db.execute("SELECT total_count FROM signatures").fetchone()
    assert total == 2
    assert [tuple(r) for r in store.db.execute("SELECT level, logger, template FROM signatures")] == [
        ("warning", "obp_bank_node::fx", "USD/fiat leg fell back")]


def test_a_failed_poll_is_recorded_and_keeps_the_place(tmp_path):
    client = LokiClient("http://loki.test", "1", "{}", httpx.Client(transport=httpx.MockTransport(
        lambda r: httpx.Response(503, text="down"))))
    c, store = collector(tmp_path, client)
    store.set_state(LOKI_SINCE, 42)
    assert c.poll_loki() == "loki failed"
    assert store.get_state(LOKI_SINCE, None) == 42
    assert [tuple(r) for r in store.db.execute("SELECT source, ok FROM polls")] == [("loki", 0)]
