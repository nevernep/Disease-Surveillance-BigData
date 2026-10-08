"""download_disease_full: versioned raw files, no duplicates, no partial files.

The network is replaced by a fake requests.get; no token or internet needed.
"""

import pytest

from src.extract import extract_disease


class FakeResponse:
    def __init__(self, payload=None, content=b"", length=None):
        self._payload = payload
        self._content = content
        self.headers = {"Content-Length": str(len(content) if length is None else length)}

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload

    def iter_content(self, chunk_size):
        yield self._content

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.fixture
def fake_source(monkeypatch):
    """Serve `state["content"]` as the resource file."""
    state = {"content": b"header\nrow1\n", "length": None}

    def fake_get(url, **kwargs):
        if url == extract_disease.RESOURCE_SHOW_URL:
            return FakeResponse(payload={"result": {"url": "https://example/file.csv",
                                                     "last_modified": "2026-09-04"}})
        return FakeResponse(content=state["content"], length=state["length"])

    monkeypatch.setenv("DATA_GO_TH_TOKEN", "test-token")
    monkeypatch.setattr(extract_disease.requests, "get", fake_get)
    return state


def test_first_download_creates_a_dated_version(fake_source, tmp_path):
    status, path = extract_disease.download_disease_full("2569", "rid", raw_dir=tmp_path, version="20261008")

    assert status == "new"
    assert path.name == "disease_cases_2569_full_20261008.csv"
    assert path.read_bytes() == fake_source["content"]


def test_same_content_is_not_stored_again(fake_source, tmp_path):
    extract_disease.download_disease_full("2569", "rid", raw_dir=tmp_path, version="20261008")

    status, path = extract_disease.download_disease_full("2569", "rid", raw_dir=tmp_path, version="20261015")

    assert status == "unchanged"
    assert path.name == "disease_cases_2569_full_20261008.csv"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["disease_cases_2569_full_20261008.csv"]


def test_changed_content_becomes_a_new_version(fake_source, tmp_path):
    extract_disease.download_disease_full("2569", "rid", raw_dir=tmp_path, version="20261008")
    fake_source["content"] = b"header\nrow1\nrow2\n"

    status, path = extract_disease.download_disease_full("2569", "rid", raw_dir=tmp_path, version="20261015")

    assert status == "new"
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "disease_cases_2569_full_20261008.csv",
        "disease_cases_2569_full_20261015.csv",
    ]


def test_truncated_download_leaves_no_file(fake_source, tmp_path):
    fake_source["length"] = 999  # server announced more bytes than it sent

    with pytest.raises(RuntimeError, match="size mismatch"):
        extract_disease.download_disease_full("2569", "rid", raw_dir=tmp_path, version="20261008")

    assert list(tmp_path.iterdir()) == []


def test_missing_token_is_reported(monkeypatch, tmp_path):
    monkeypatch.delenv("DATA_GO_TH_TOKEN", raising=False)

    with pytest.raises(ValueError):
        extract_disease.download_disease_full("2569", "rid", raw_dir=tmp_path, version="20261008")
