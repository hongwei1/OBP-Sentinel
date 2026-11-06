"""Reading container logs from Loki, for what is not OBP-API and serves no log cache (OBP Bank Node).

Alloy writes every Pod's stdout to Loki, one stream per container, and Loki derives `detected_level` for each
line. Sentinel asks for the lines of the levels it counts (warnings and errors) after the last one it read,
oldest first. The gateway needs the tenant header and no token.
"""

import re
from dataclasses import dataclass

import httpx

# Loki's detected_level, as Sentinel names the levels it counts (the log cache's "error" and "warning")
SENTINEL_LEVEL = {"warn": "warning", "warning": "warning", "error": "error", "critical": "error", "fatal": "error"}
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
# Most pages one poll reads: past that, the rest is read by the next poll
MAX_PAGES = 5


@dataclass(frozen=True)
class LokiEntry:
    ts_ns: int
    line: str
    level: str  # "error" or "warning"


class LokiClient:
    def __init__(self, url: str, tenant: str, selector: str, http: httpx.Client | None = None):
        self.url = url.rstrip("/")
        self.selector = selector  # a stream selector, e.g. {namespace="bank-node"}
        self.http = http or httpx.Client(timeout=30)
        self.headers = {"X-Scope-OrgID": tenant} if tenant else {}

    def query(self, levels: list[str]) -> str:
        names = sorted(name for name, level in SENTINEL_LEVEL.items() if level in levels)
        return f'{self.selector} | detected_level=~"{"|".join(names)}"'

    def entries(self, levels: list[str], start_ns: int, limit: int) -> list[LokiEntry]:
        """The lines of `levels` logged at or after `start_ns`, oldest first. Reads on while pages come back full."""
        found: list[LokiEntry] = []
        for _ in range(MAX_PAGES):
            response = self.http.get(
                f"{self.url}/loki/api/v1/query_range", headers=self.headers,
                params={"query": self.query(levels), "start": str(start_ns), "limit": str(limit), "direction": "forward"},
            )
            if response.status_code != 200:
                raise RuntimeError(f"GET {self.url}/loki/api/v1/query_range failed ({response.status_code}): {response.text[:300]}")
            page = []
            for stream in response.json()["data"]["result"]:
                level = SENTINEL_LEVEL.get(stream["stream"].get("detected_level", ""), "")
                page += [LokiEntry(int(ts), ANSI.sub("", line), level) for ts, line in stream["values"] if level]
            # Streams come one after the other: put their lines in time order
            page.sort(key=lambda e: e.ts_ns)
            found += page
            if len(page) < limit:
                break
            start_ns = page[-1].ts_ns + 1
        return found
