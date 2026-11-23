"""Retry behaviour for upstream HTTP calls."""

from __future__ import annotations

import requests
import pytest

from ratelimit import RateLimiter, get_json


class FakeResponse:
    def __init__(self, status: int, payload: dict | None = None) -> None:
        self.status_code = status
        self._payload = payload or {"ok": True}
        self.headers: dict[str, str] = {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")

    def json(self) -> dict:
        return self._payload


def _no_sleep(monkeypatch) -> None:
    monkeypatch.setattr("ratelimit.time.sleep", lambda _seconds: None)


def test_retries_temporary_503(monkeypatch) -> None:
    _no_sleep(monkeypatch)
    responses = [FakeResponse(503), FakeResponse(200, {"ok": "yes"})]
    monkeypatch.setattr("ratelimit.requests.get", lambda *a, **k: responses.pop(0))

    limiter = RateLimiter(3_600_000)
    assert get_json(limiter, "http://example.test", max_retries=2) == {"ok": "yes"}
    assert responses == []


def test_retries_timeout(monkeypatch) -> None:
    _no_sleep(monkeypatch)
    calls = {"count": 0}

    def fake_get(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise requests.exceptions.Timeout("slow")
        return FakeResponse(200, {"ok": "after-timeout"})

    monkeypatch.setattr("ratelimit.requests.get", fake_get)
    limiter = RateLimiter(3_600_000)
    assert get_json(limiter, "http://example.test", max_retries=2)["ok"] == "after-timeout"
    assert calls["count"] == 2


def test_gives_up_after_retries(monkeypatch) -> None:
    _no_sleep(monkeypatch)
    monkeypatch.setattr(
        "ratelimit.requests.get", lambda *a, **k: FakeResponse(503)
    )
    limiter = RateLimiter(3_600_000)
    with pytest.raises(requests.HTTPError):
        get_json(limiter, "http://example.test", max_retries=1)
