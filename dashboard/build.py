"""Build the dashboard page from dashboard/template.html.

    python dashboard/build.py
        -> dashboard/index.html, a full HTML document served by app.py; it loads
           live data from /api/data.

    python dashboard/build.py --embed data.json --out page.html
        -> a self-contained page body with the data embedded (for sharing as a
           static page); data.json is a saved /api/data response.

Edit template.html, then rebuild; index.html is generated.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
TEMPLATE = HERE / "template.html"
HEAD_END = "</style>"
BASE_CSS = "<style>html, body { margin: 0; } [hidden] { display: none !important; } img { max-width: 100%; }</style>"


def build_index(template: str) -> str:
    split = template.index(HEAD_END) + len(HEAD_END)
    head, body = template[:split], template[split:]
    return (
        "<!doctype html>\n<html lang=\"th\">\n<head>\n<meta charset=\"utf-8\">\n"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1, viewport-fit=cover\">\n"
        f"{BASE_CSS}\n{head}\n</head>\n<body>{body}\n</body>\n</html>\n"
    )


def build_embedded(template: str, data: dict) -> str:
    # "</" is escaped so the JSON can never close the script element.
    blob = json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    marker = "<script>\nfunction start(DATA) {"
    if template.count(marker) != 1:
        raise ValueError("template script marker not found")
    return template.replace(marker, f"<script>window.__DASHBOARD_DATA__ = {blob};</script>\n{marker}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--embed", type=Path, help="saved /api/data JSON to embed")
    parser.add_argument("--out", type=Path, help="output path for --embed")
    args = parser.parse_args()

    template = TEMPLATE.read_text(encoding="utf-8")
    if args.embed:
        if not args.out:
            parser.error("--embed needs --out")
        data = json.loads(args.embed.read_text(encoding="utf-8"))
        args.out.write_text(build_embedded(template, data), encoding="utf-8", newline="\n")
        print(f"Wrote {args.out}")
    else:
        index = HERE / "index.html"
        index.write_text(build_index(template), encoding="utf-8", newline="\n")
        print(f"Wrote {index}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
