#!/usr/bin/env python3
"""
Generate styled HTML study pages from the Markdown notes.

    python3 notes/build.py

The Markdown files are the SINGLE SOURCE OF TRUTH — never hand-edit anything
in notes/html/, it is regenerated wholesale and your changes will be lost.
Edit the .md, re-run this, commit both.

No third-party dependencies on purpose: a study-notes builder that needs a
pip install is a builder you won't run. Mermaid diagrams are rendered
client-side from a CDN, so viewing the pages needs an internet connection
(the Markdown originals render fine on GitHub either way).
"""

import html
import re
from pathlib import Path

NOTES_DIR = Path(__file__).resolve().parent
OUT_DIR = NOTES_DIR / "html"

# Order matters — this is the reading order shown in the nav.
PAGES = [
    ("00-start-here.md", "Start Here"),
    ("week-1-concepts.md", "Week 1"),
    ("week-2-concepts.md", "Week 2"),
]

RAW_HTML_PREFIXES = ("<details", "</details", "<summary", "</summary", "<div", "</div")


def slug(text: str) -> str:
    s = re.sub(r"<[^>]+>", "", text).lower()
    s = re.sub(r"[^\w\s-]", "", s)
    return re.sub(r"[\s_]+", "-", s).strip("-")


def inline(text: str) -> str:
    """Inline Markdown → HTML. Code spans are stashed so nothing rewrites them."""
    spans: list[str] = []

    def stash(m):
        spans.append(m.group(1))
        return f"\x00{len(spans) - 1}\x00"

    text = re.sub(r"`([^`]+)`", stash, text)
    text = html.escape(text, quote=False)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', text)
    # non-greedy so bold may contain italics: **bold with *emphasis* inside**
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"(?<!\*)\*([^*\n]+)\*(?!\*)", r"<em>\1</em>", text)

    def unstash(m):
        return f"<code>{html.escape(spans[int(m.group(1))], quote=False)}</code>"

    return re.sub(r"\x00(\d+)\x00", unstash, text)


def render_table(rows: list[str]) -> str:
    def cells(row):
        return [c.strip() for c in row.strip().strip("|").split("|")]

    head, body = cells(rows[0]), rows[2:]
    out = ["<table><thead><tr>"]
    out += [f"<th>{inline(c)}</th>" for c in head]
    out.append("</tr></thead><tbody>")
    for row in body:
        out.append("<tr>" + "".join(f"<td>{inline(c)}</td>" for c in cells(row)) + "</tr>")
    out.append("</tbody></table>")
    return "".join(out)


def convert(md: str) -> tuple[str, list[tuple[str, str]]]:
    lines = md.split("\n")
    out: list[str] = []
    toc: list[tuple[str, str]] = []
    i, n = 0, len(lines)

    while i < n:
        line = lines[i]
        stripped = line.strip()

        # fenced code / mermaid
        if stripped.startswith("```"):
            lang = stripped[3:].strip()
            i += 1
            buf = []
            while i < n and not lines[i].strip().startswith("```"):
                buf.append(lines[i])
                i += 1
            i += 1
            body = "\n".join(buf)
            if lang == "mermaid":
                out.append(f'<pre class="mermaid">{html.escape(body, quote=False)}</pre>')
            else:
                cls = f' class="lang-{lang}"' if lang else ""
                out.append(f"<pre><code{cls}>{html.escape(body, quote=False)}</code></pre>")
            continue

        # raw HTML passthrough (details/summary blocks)
        if stripped.startswith(RAW_HTML_PREFIXES):
            out.append(stripped)
            i += 1
            continue

        # headings
        m = re.match(r"^(#{1,4})\s+(.*)$", line)
        if m:
            level, text = len(m.group(1)), m.group(2).strip()
            anchor = slug(text)
            out.append(f'<h{level} id="{anchor}">{inline(text)}</h{level}>')
            if level in (1, 2):
                toc.append((anchor, re.sub(r"<[^>]+>", "", inline(text))))
            i += 1
            continue

        # horizontal rule
        if re.fullmatch(r"-{3,}|\*{3,}", stripped):
            out.append("<hr>")
            i += 1
            continue

        # table
        if stripped.startswith("|") and i + 1 < n and re.match(r"^\|[\s:|-]+\|$", lines[i + 1].strip()):
            rows = []
            while i < n and lines[i].strip().startswith("|"):
                rows.append(lines[i])
                i += 1
            out.append(render_table(rows))
            continue

        # blockquote
        if stripped.startswith(">"):
            buf = []
            while i < n and lines[i].strip().startswith(">"):
                buf.append(re.sub(r"^\s*>\s?", "", lines[i]))
                i += 1
            inner, _ = convert("\n".join(buf))
            out.append(f"<blockquote>{inner}</blockquote>")
            continue

        # lists (one level; deeper indentation is treated as item continuation)
        m = re.match(r"^(\s*)([-*]|\d+\.)\s+(.*)$", line)
        if m:
            ordered = not m.group(2) in ("-", "*")
            items: list[str] = []
            while i < n:
                mm = re.match(r"^(\s*)([-*]|\d+\.)\s+(.*)$", lines[i])
                if mm:
                    items.append(mm.group(3))
                    i += 1
                elif lines[i].strip() and lines[i].startswith((" ", "\t")) and items:
                    items[-1] += " " + lines[i].strip()
                    i += 1
                else:
                    break
            tag = "ol" if ordered else "ul"
            out.append(f"<{tag}>" + "".join(f"<li>{inline(t)}</li>" for t in items) + f"</{tag}>")
            continue

        # blank
        if not stripped:
            i += 1
            continue

        # paragraph
        buf = []
        while i < n and lines[i].strip() and not re.match(
            r"^(#{1,4}\s|```|\||>|\s*([-*]|\d+\.)\s|-{3,}$)", lines[i]
        ) and not lines[i].strip().startswith(RAW_HTML_PREFIXES):
            buf.append(lines[i].strip())
            i += 1
        if buf:
            out.append(f"<p>{inline(' '.join(buf))}</p>")

    return "\n".join(out), toc


CSS = """
:root{--bg:#fdfdfc;--fg:#1f2328;--muted:#5b6470;--line:#e3e5e8;--accent:#0b6cc4;
--code-bg:#f4f5f7;--card:#fff;--warn-bg:#fff8e6;--warn-br:#e8b931;--quote:#f0f4f9}
@media (prefers-color-scheme:dark){:root{--bg:#14171a;--fg:#e6e8ea;--muted:#9aa4b0;
--line:#2c3238;--accent:#5ab0ff;--code-bg:#1d2227;--card:#181c20;--warn-bg:#2a2410;
--warn-br:#8a7320;--quote:#1a2028}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);
font:16px/1.68 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,Roboto,sans-serif;
-webkit-text-size-adjust:100%}
.wrap{max-width:860px;margin:0 auto;padding:0 24px 96px}
nav.top{position:sticky;top:0;z-index:9;background:var(--bg);border-bottom:1px solid var(--line);
padding:12px 0;margin-bottom:8px;display:flex;gap:8px;flex-wrap:wrap}
nav.top a{font-size:14px;text-decoration:none;color:var(--muted);padding:5px 11px;
border:1px solid var(--line);border-radius:99px}
nav.top a:hover{color:var(--accent);border-color:var(--accent)}
nav.top a.active{background:var(--accent);border-color:var(--accent);color:#fff}
h1{font-size:2em;line-height:1.25;margin:1.2em 0 .5em;letter-spacing:-.02em}
h2{font-size:1.42em;margin:1.9em 0 .5em;padding-bottom:.28em;border-bottom:1px solid var(--line);
letter-spacing:-.01em}
h3{font-size:1.13em;margin:1.7em 0 .4em}
h1:first-of-type{margin-top:.6em}
p{margin:.85em 0}
a{color:var(--accent)}
ul,ol{padding-left:1.4em;margin:.8em 0}
li{margin:.42em 0}
code{background:var(--code-bg);padding:.14em .38em;border-radius:4px;font-size:.88em;
font-family:ui-monospace,SFMono-Regular,"SF Mono",Menlo,monospace}
pre{background:var(--code-bg);border:1px solid var(--line);border-radius:8px;padding:14px 16px;
overflow-x:auto;margin:1.1em 0}
pre code{background:none;padding:0;font-size:.85em;line-height:1.55}
pre.mermaid{background:var(--card);text-align:center;line-height:1.4}
blockquote{margin:1.2em 0;padding:.9em 1.1em;background:var(--quote);
border-left:3px solid var(--accent);border-radius:0 8px 8px 0}
blockquote p{margin:.4em 0}
table{border-collapse:collapse;width:100%;margin:1.2em 0;font-size:.94em;display:block;
overflow-x:auto}
th,td{border:1px solid var(--line);padding:9px 12px;text-align:left;vertical-align:top}
th{background:var(--code-bg);font-weight:600}
hr{border:0;border-top:1px solid var(--line);margin:2.6em 0}
details{background:var(--card);border:1px solid var(--line);border-radius:8px;
padding:12px 16px;margin:1.2em 0}
summary{cursor:pointer;font-weight:600}
details[open] summary{margin-bottom:.6em;padding-bottom:.6em;border-bottom:1px solid var(--line)}
.toc{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:14px 18px;
margin:1.6em 0}
.toc-title{font-size:12px;text-transform:uppercase;letter-spacing:.09em;color:var(--muted);
margin-bottom:8px}
.toc ul{margin:0;padding-left:1.1em}
.toc li{margin:.2em 0;font-size:.93em}
.built{color:var(--muted);font-size:13px;margin-top:3em;padding-top:1.2em;
border-top:1px solid var(--line)}
@media(max-width:640px){body{font-size:15px}.wrap{padding:0 16px 64px}h1{font-size:1.6em}}
"""

TEMPLATE = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>
<style>{css}</style>
</head><body>
<div class="wrap">
<nav class="top">{nav}</nav>
<div class="toc"><div class="toc-title">On this page</div><ul>{toc}</ul></div>
{body}
<p class="built">Generated from <code>notes/{src}</code> by <code>notes/build.py</code>.
Edit the Markdown, not this file.</p>
</div>
<script type="module">
import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
const dark = window.matchMedia('(prefers-color-scheme: dark)').matches;
mermaid.initialize({{startOnLoad:true, theme: dark ? 'dark' : 'default',
  themeVariables:{{fontSize:'14px'}}}});
</script>
</body></html>
"""


def main() -> None:
    OUT_DIR.mkdir(exist_ok=True)
    for src, label in PAGES:
        md_path = NOTES_DIR / src
        if not md_path.exists():
            print(f"  skip {src} (missing)")
            continue
        body, toc = convert(md_path.read_text())
        title = next((t for a, t in toc), label)

        nav = "".join(
            f'<a href="{Path(s).stem}.html"{" class=\'active\'" if s == src else ""}>{l}</a>'
            for s, l in PAGES
        )
        toc_html = "".join(f'<li><a href="#{a}">{t}</a></li>' for a, t in toc[1:])

        out = TEMPLATE.format(
            title=title, css=CSS, nav=nav, toc=toc_html, body=body, src=src
        )
        dest = OUT_DIR / f"{md_path.stem}.html"
        dest.write_text(out)
        print(f"  {src} -> notes/html/{dest.name}  ({len(out):,} bytes)")

    index = OUT_DIR / "index.html"
    index.write_text(
        f'<!doctype html><meta charset="utf-8">'
        f'<meta http-equiv="refresh" content="0;url={Path(PAGES[0][0]).stem}.html">'
    )
    print(f"  index.html -> redirects to {Path(PAGES[0][0]).stem}.html")


if __name__ == "__main__":
    main()
