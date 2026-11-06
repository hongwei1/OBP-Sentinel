from obp_sentinel.signatures import normalise_path, parse_line, signature_of


def line(msg: str, ts="2026-10-04 09:12:01+02", logger="code.api.util.APIUtil") -> str:
    return f"[{ts}] [http4s-io-3] [{logger}] {msg}"


def test_parse_line():
    parsed = parse_line(line("Something failed\njava.lang.IllegalStateException: boom"))
    assert parsed.logger == "code.api.util.APIUtil"
    assert parsed.thread == "http4s-io-3"
    assert parsed.message == "Something failed"
    assert parsed.detail == "java.lang.IllegalStateException: boom"
    assert parsed.ts.isoformat() == "2026-10-04T09:12:01+02:00"


def test_parse_line_utc_and_unparsed():
    assert parse_line(line("x", ts="2026-10-04 07:12:01Z")).ts.utcoffset().total_seconds() == 0
    parsed = parse_line("no brackets here")
    assert parsed.ts is None and parsed.message == "no brackets here"


def test_same_problem_different_ids_is_one_signature():
    a = signature_of("ERROR", parse_line(line(
        "Account not found for BANK_ID 'gh.29.uk' ACCOUNT_ID 'a1b2c3' at /obp/v5.1.0/banks/gh.29.uk/accounts/a1b2c3/owner/account"
    )))
    b = signature_of("ERROR", parse_line(line(
        "Account not found for BANK_ID 'rbs.12' ACCOUNT_ID 'zz99' at /obp/v5.1.0/banks/rbs.12/accounts/zz99/owner/account",
        ts="2026-10-04 11:00:00+02",
    )))
    assert a.id == b.id
    assert a.endpoint == "/obp/v5.1.0/banks/{id}/accounts/{id}/owner/account"


def test_normalises_uuids_numbers_emails_ips():
    sig = signature_of("warning", parse_line(line(
        "User 9b2f6e2a-1c3d-4e5f-8a9b-0c1d2e3f4a5b (bob@example.com) from 10.0.0.12 took 1234 ms"
    )))
    assert sig.template == "User <uuid> (<email>) from <ip> took <n> ms"


def test_different_loggers_or_levels_are_different_signatures():
    a = signature_of("error", parse_line(line("Timeout")))
    b = signature_of("error", parse_line(line("Timeout", logger="code.bankconnectors.Connector")))
    c = signature_of("warning", parse_line(line("Timeout")))
    assert len({a.id, b.id, c.id}) == 3


def test_exception_from_detail_line():
    sig = signature_of("error", parse_line(line("Failed\njava.sql.SQLTransientConnectionException: pool exhausted")))
    assert sig.exception == "java.sql.SQLTransientConnectionException"


def test_normalise_path_keeps_literals_and_version():
    assert normalise_path("/obp/v7.0.0/management/telemetry") == "/obp/v7.0.0/management/telemetry"
    assert normalise_path("/obp/v5.1.0/users/user_id/9b2f6e2a") == "/obp/v5.1.0/users/{id}/{id}"


# Lines as OBP Bank Node's node writes them (read from Loki, 2026-11-06)
NODE_FINAL = ('{"timestamp":"2026-11-06T16:32:30.013267Z","level":"INFO","fields":{"message":"settlement FINAL",'
              '"idempotency_key":"79880e67-d4eb-46b9-b058-b2101644fdfa","tx_id":"eb872a9fde2a8767862d33e77eaf05cec8534f1872bfb480fcb503cda6dd84cc",'
              '"depth":2},"target":"obp_bank_node::finality"}')


def node_warn(currency: str, ts="2026-11-06T16:31:27.357523Z") -> str:
    return ('{"timestamp":"' + ts + '","level":"WARN","fields":{"message":"USD/fiat leg fell back","currency":"' + currency
            + '","used":"er-api","failed":"pyth: transaction rejected: fx: pyth: HTTP status client error (401 Unauthorized)"},'
            '"target":"obp_bank_node::fx"}')


def test_parse_json_line():
    parsed = parse_line(node_warn("KES"))
    assert parsed.level == "warn"
    assert parsed.logger == "obp_bank_node::fx"
    assert parsed.thread == ""
    assert parsed.message == "USD/fiat leg fell back"
    assert parsed.ts.isoformat() == "2026-11-06T16:31:27.357523+00:00"
    # the other fields are the detail, in the order the node wrote them
    assert parsed.detail.splitlines() == [
        "currency=KES", "used=er-api",
        "failed=pyth: transaction rejected: fx: pyth: HTTP status client error (401 Unauthorized)"]


def test_json_error_field_comes_first_in_detail():
    raw = ('{"timestamp":"2026-11-06T16:49:10.154360Z","level":"WARN","fields":{"message":"routing-scheme registry refresh failed",'
           '"attempt":3,"error":"OBP API transport error: OBP-API returned 401 Unauthorized"},"target":"obp_bank_node::routing"}')
    assert parse_line(raw).detail.splitlines() == ["error=OBP API transport error: OBP-API returned 401 Unauthorized", "attempt=3"]


def test_json_lines_with_different_values_are_one_signature():
    a = parse_line(node_warn("KES"))
    b = parse_line(node_warn("TZS", ts="2026-11-06T17:00:00.000000Z"))
    assert signature_of(a.level, a).id == signature_of(b.level, b).id
    assert signature_of(a.level, a).template == "USD/fiat leg fell back"


def test_json_line_without_an_exception_class():
    parsed = parse_line(NODE_FINAL)
    assert signature_of(parsed.level, parsed).exception == ""
    assert signature_of(parsed.level, parsed).level == "info"


def test_json_that_is_not_a_tracing_line_stays_plain_text():
    for raw in ('{"not": "a log line"}', '{"level":"INFO","fields":{"x":1}}', '{broken', '[1, 2]'):
        parsed = parse_line(raw)
        assert parsed.ts is None and parsed.message == raw and parsed.level == ""
