#!/usr/bin/env python3
"""
Load a job-details CSV (from scrape_rabota_it.py --with-details) and print rows
matching optional filters (remote, company, title/description search).
"""

from __future__ import annotations

import argparse
import curses
import csv
import io
import pydoc
import re
import sys
import textwrap
from pathlib import Path


def _norm(s: str) -> str:
    return s.strip().lower()


def _is_remote(location: str) -> bool:
    t = _norm(location)
    if not t:
        return False
    if "remote" in t:
        return True
    if "distan" in t:  # distanță / distanț
        return True
    if "la distan" in t:
        return True
    return False


def _is_hybrid(location: str) -> bool:
    t = _norm(location)
    return "hibrid" in t or "hybrid" in t


def _is_onsite(location: str) -> bool:
    t = _norm(location)
    if not t:
        return False
    if _is_remote(location) or _is_hybrid(location):
        return False
    return "angajator" in t or "locația" in t or "locatia" in t


def _row_matches(
    row: dict[str, str],
    *,
    remote: bool,
    hybrid: bool,
    onsite: bool,
    without_description: bool,
    company: str | None,
    title_substr: str | None,
    search: str | None,
) -> bool:
    loc = row.get("location", "") or ""
    if remote and not _is_remote(loc):
        return False
    if hybrid and not _is_hybrid(loc):
        return False
    if onsite and not _is_onsite(loc):
        return False

    if without_description and (row.get("description", "") or "").strip():
        return False

    if company is not None:
        if company.lower() not in _norm(row.get("company", "")):
            return False

    if title_substr is not None:
        if title_substr.lower() not in _norm(row.get("job_title", "")):
            return False

    if search is not None:
        q = search.lower()
        hay = " ".join(
            [
                row.get("job_title", "") or "",
                row.get("company", "") or "",
                row.get("location", "") or "",
                row.get("address", "") or "",
                row.get("description", "") or "",
            ]
        ).lower()
        if q not in hay:
            return False

    return True


def load_rows(path: Path) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        raw = list(csv.DictReader(f))
    return normalize_rows(raw)


def _pick_first(row: dict[str, str], *keys: str) -> str:
    for k in keys:
        v = (row.get(k, "") or "").strip()
        if v:
            return v
    return ""


def normalize_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """
    Normalize details rows across old/new CSV schemas.
    """
    out: list[dict[str, str]] = []
    for row in rows:
        r = dict(row)
        r["job_title"] = _pick_first(r, "job_title", "title")
        r["company"] = _pick_first(r, "company")
        r["location"] = _pick_first(r, "location", "workplace", "locul_de_munca")
        r["address"] = _pick_first(r, "address", "office", "oficiul_principal")
        r["description"] = _pick_first(r, "description", "about")
        r["url"] = _pick_first(r, "url", "job_url")
        out.append(r)
    return out


def _extract_job_id(url: str) -> str:
    m = re.search(r"/(\d+)(?:/)?$", (url or "").strip())
    return m.group(1) if m else ""


def load_published_map(path: Path) -> dict[str, dict[str, str]]:
    """
    Load first CSV (rabota_it_jobs.csv) as mapping vacancy_id -> published fields.
    """
    if not path.is_file():
        return {}
    out: dict[str, dict[str, str]] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            vid = (row.get("vacancy_id") or "").strip()
            url = (row.get("url") or "").strip()
            url_vid = _extract_job_id(url)
            key = vid or url_vid
            if not key:
                continue
            posted_label = (
                (row.get("posted_label") or "").strip()
                or (row.get("posted_time") or "").strip()
            )
            payload = {
                "posted_label": posted_label,
                "approx_posted_at": (row.get("approx_posted_at") or "").strip(),
                "calendar_day": (row.get("calendar_day") or "").strip(),
            }
            out[key] = payload
            if vid and vid != key:
                out[vid] = payload
            if url_vid and url_vid != key:
                out[url_vid] = payload
    return out


def enrich_rows_with_publish_data(
    rows: list[dict[str, str]],
    publish_map: dict[str, dict[str, str]],
) -> list[dict[str, str]]:
    enriched: list[dict[str, str]] = []
    for idx, row in enumerate(rows, start=1):
        copy = dict(row)
        copy["csv_index"] = str(idx)
        job_id = _extract_job_id(copy.get("url", ""))
        copy["job_id"] = job_id
        pub = publish_map.get(job_id, {})
        copy["posted_label"] = pub.get("posted_label", "")
        copy["approx_posted_at"] = pub.get("approx_posted_at", "")
        copy["calendar_day"] = pub.get("calendar_day", "")
        enriched.append(copy)
    return enriched


def _format_about_lines(text: str, width: int) -> list[str]:
    """Wrap description text to keep terminal output readable."""
    if not text.strip():
        return []
    w = max(40, width)
    collapsed = " ".join(text.split())
    return textwrap.wrap(
        collapsed,
        width=w,
        break_long_words=True,
        break_on_hyphens=True,
    )


def format_jobs_output(
    rows: list[dict[str, str]],
    max_rows: int | None,
    *,
    wrap_width: int,
) -> str:
    """Build the full text shown for matching jobs (used with pager or stdout)."""
    n_total = len(rows)
    if max_rows is not None:
        rows = rows[:max_rows]
    body_indent = "      "
    buf = io.StringIO()
    for i, r in enumerate(rows, start=1):
        title = r.get("job_title", "").strip()
        company = r.get("company", "").strip()
        location = r.get("location", "").strip()
        address = r.get("address", "").strip()
        url = r.get("url", "").strip()
        posted_label = r.get("posted_label", "").strip()
        approx_posted_at = r.get("approx_posted_at", "").strip()
        desc = (r.get("description", "") or "").strip()

        print(f"[{i}] {title}", file=buf)
        print(f"    Company:  {company}", file=buf)
        print(f"    Location: {location}", file=buf)
        if address:
            print(f"    Address:  {address}", file=buf)
        if posted_label or approx_posted_at:
            print(f"    Published:{' ' if posted_label else ''}{posted_label}", file=buf)
        if r.get("csv_index", "").strip():
            print(f"    Index:    {r.get('csv_index', '').strip()}", file=buf)
        print(f"    URL:      {url}", file=buf)
        about_lines = _format_about_lines(desc, wrap_width)
        if about_lines:
            print("    About:", file=buf)
            for line in about_lines:
                print(f"      {line}", file=buf)
        print(file=buf)

    if max_rows is not None and n_total > len(rows):
        print(
            f"(… {n_total - len(rows)} more not shown; increase --max-rows)",
            file=buf,
        )
    return buf.getvalue()


def emit_output(text: str, *, use_pager: bool) -> None:
    """Send `text` through the system pager (usually less) or stdout."""
    if not text.strip():
        return
    if use_pager:
        pydoc.pager(text)
    else:
        sys.stdout.write(text)


def _interactive_row_label(row: dict[str, str]) -> str:
    title = row.get("job_title", "").strip()
    return title


def _interactive_detail_lines(
    row: dict[str, str],
    *,
    wrap_width: int,
    show_description: bool,
) -> list[str]:
    lines: list[str] = []
    lines.append(f"Title:     {row.get('job_title', '').strip()}")
    lines.append(f"Company:   {row.get('company', '').strip()}")
    lines.append(f"Location:  {row.get('location', '').strip()}")
    address = row.get("address", "").strip()
    if address:
        lines.append(f"Address:   {address}")
    lines.append(f"Published: {row.get('posted_label', '').strip() or '-'}")
    lines.append(f"Index:     {row.get('csv_index', '').strip() or '-'}")
    lines.append(f"URL:       {row.get('url', '').strip()}")
    lines.append("")

    if show_description:
        desc = (row.get("description", "") or "").strip()
        about = _format_about_lines(desc, max(40, wrap_width))
        lines.append("About:")
        if about:
            lines.extend(about)
        else:
            lines.append("(empty)")
    else:
        lines.append("About: (hidden) Press Enter to toggle")
    return lines


def browse_jobs_interactive(
    rows: list[dict[str, str]],
    *,
    wrap_width: int,
) -> int:
    if not rows:
        print("No rows to browse.", file=sys.stderr)
        return 0
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        print("--interactive requires a TTY terminal.", file=sys.stderr)
        return 2

    def _run(stdscr) -> int:
        curses.curs_set(0)
        stdscr.keypad(True)
        use_theme_colors = False
        if curses.has_colors():
            try:
                curses.start_color()
                curses.use_default_colors()
                # Use terminal default background (-1) so app matches terminal theme.
                if curses.COLORS >= 256:
                    # Pair 1: bright white text, terminal background
                    # Pair 2: bright yellow text, terminal background
                    curses.init_pair(1, 15, -1)
                    curses.init_pair(2, 226, -1)
                else:
                    # Fallback for basic terminals with default background.
                    curses.init_pair(1, curses.COLOR_WHITE, -1)
                    curses.init_pair(2, curses.COLOR_YELLOW, -1)
                use_theme_colors = True
            except curses.error:
                use_theme_colors = False

        color_normal = curses.color_pair(1) if use_theme_colors else 0
        # Keep same background; highlight selected row via brighter text + bold/underline.
        color_selected = (
            curses.color_pair(2) | curses.A_BOLD | curses.A_UNDERLINE
            if use_theme_colors
            else curses.A_REVERSE
        )

        selected = 0
        scroll = 0
        detail_scroll = 0
        show_desc: set[int] = set()

        while True:
            stdscr.erase()
            h, w = stdscr.getmaxyx()
            list_h = max(5, h // 2)
            detail_y = list_h + 1
            help_line = "q:quit  j/k or arrows:move  Enter:toggle desc  J/K or PgDn/PgUp:scroll desc  g/G:top/bottom"
            stdscr.addnstr(0, 0, help_line, w - 1, color_normal)

            visible_count = max(1, list_h - 2)
            if selected < scroll:
                scroll = selected
            if selected >= scroll + visible_count:
                scroll = selected - visible_count + 1

            for i in range(visible_count):
                idx = scroll + i
                if idx >= len(rows):
                    break
                y = 1 + i
                label = _interactive_row_label(rows[idx])
                if idx == selected:
                    stdscr.attron(color_selected)
                    stdscr.addnstr(y, 0, label, w - 1)
                    stdscr.attroff(color_selected)
                else:
                    stdscr.addnstr(y, 0, label, w - 1, color_normal)

            stdscr.hline(list_h, 0, "-", w - 1, color_normal)

            detail_lines = _interactive_detail_lines(
                rows[selected],
                wrap_width=wrap_width,
                show_description=selected in show_desc,
            )
            max_detail_lines = h - detail_y
            max_detail_scroll = max(0, len(detail_lines) - max_detail_lines)
            if detail_scroll > max_detail_scroll:
                detail_scroll = max_detail_scroll
            if detail_scroll < 0:
                detail_scroll = 0
            visible_detail = detail_lines[detail_scroll : detail_scroll + max_detail_lines]
            for i, line in enumerate(visible_detail):
                stdscr.addnstr(detail_y + i, 0, line, w - 1, color_normal)

            stdscr.refresh()
            ch = stdscr.getch()
            if ch in (ord("q"), 27):
                return 0
            if ch in (curses.KEY_UP, ord("k")):
                old = selected
                selected = max(0, selected - 1)
                if selected != old:
                    detail_scroll = 0
            elif ch in (curses.KEY_DOWN, ord("j")):
                old = selected
                selected = min(len(rows) - 1, selected + 1)
                if selected != old:
                    detail_scroll = 0
            elif ch in (ord("g"),):
                selected = 0
                detail_scroll = 0
            elif ch in (ord("G"),):
                selected = len(rows) - 1
                detail_scroll = 0
            elif ch in (ord("J"), curses.KEY_NPAGE):
                detail_scroll = min(max_detail_scroll, detail_scroll + max(1, max_detail_lines // 2))
            elif ch in (ord("K"), curses.KEY_PPAGE):
                detail_scroll = max(0, detail_scroll - max(1, max_detail_lines // 2))
            elif ch in (10, 13, curses.KEY_ENTER):
                if selected in show_desc:
                    show_desc.remove(selected)
                else:
                    show_desc.add(selected)
                detail_scroll = 0

    return curses.wrapper(_run)


def main() -> int:
    p = argparse.ArgumentParser(
        description="Filter job-details CSV and print matching jobs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --csv rabota_it_job_details.csv --remote
  %(prog)s --company Enter
  %(prog)s --search "backend"
  %(prog)s --title "developer" --remote
        """.strip(),
    )
    p.add_argument(
        "--csv",
        type=Path,
        default=Path("rabota_it_jobs_details.csv"),
        help="Path to job details CSV (default: rabota_it_jobs_details.csv)",
    )
    p.add_argument("--remote", action="store_true", help="Location looks remote / la distanță")
    p.add_argument("--hybrid", action="store_true", help="Location mentions hybrid / hibrid")
    p.add_argument(
        "--onsite",
        action="store_true",
        help="Location looks on-site (not remote/hybrid heuristics)",
    )
    p.add_argument("--company", metavar="TEXT", help="Company name contains TEXT (case-insensitive)")
    p.add_argument("--title", metavar="TEXT", help="Job title contains TEXT (case-insensitive)")
    p.add_argument(
        "--search",
        metavar="TEXT",
        help="TEXT appears in title, company, location, address, or description (case-insensitive)",
    )
    p.add_argument(
        "--without-description",
        action="store_true",
        help="Show only jobs where description is empty",
    )
    p.add_argument(
        "--jobs-csv",
        type=Path,
        default=Path("rabota_it_jobs.csv"),
        help="Path to first jobs CSV for published time join (default: rabota_it_jobs.csv)",
    )
    p.add_argument(
        "--max-rows",
        type=int,
        default=None,
        metavar="N",
        help="Print at most N jobs (after filtering)",
    )
    p.add_argument(
        "--width",
        type=int,
        default=88,
        metavar="COLS",
        help="Wrap description ('About') to this many columns (default: 88)",
    )
    p.add_argument(
        "--interactive",
        action="store_true",
        help="Interactive browser mode (vim-like navigation in terminal).",
    )
    pager = p.add_mutually_exclusive_group()
    pager.add_argument(
        "--pager",
        action="store_true",
        help="Open results in the system pager (less/more).",
    )
    pager.add_argument(
        "--no-pager",
        action="store_true",
        help="Print all results to stdout at once (no less).",
    )
    args = p.parse_args()

    if not args.csv.is_file():
        print(f"File not found: {args.csv}", file=sys.stderr)
        return 1

    if sum(bool(x) for x in (args.remote, args.hybrid, args.onsite)) > 1:
        print("Use at most one of --remote, --hybrid, --onsite", file=sys.stderr)
        return 2

    all_rows = load_rows(args.csv)
    publish_map = load_published_map(args.jobs_csv)
    all_rows = enrich_rows_with_publish_data(all_rows, publish_map)

    matched = [
        r
        for r in all_rows
        if _row_matches(
            r,
            remote=args.remote,
            hybrid=args.hybrid,
            onsite=args.onsite,
            without_description=args.without_description,
            company=args.company,
            title_substr=args.title,
            search=args.search,
        )
    ]

    print(f"Matched {len(matched)} of {len(all_rows)} rows from {args.csv}\n", file=sys.stderr)

    if args.interactive:
        return browse_jobs_interactive(
            matched[: args.max_rows] if args.max_rows is not None else matched,
            wrap_width=args.width,
        )

    if args.no_pager:
        use_pager = False
    elif args.pager:
        use_pager = True
    else:
        # Interactive terminal: page like `less` by default; pipes: dump full text.
        use_pager = sys.stdout.isatty()

    body = format_jobs_output(
        matched,
        args.max_rows,
        wrap_width=args.width,
    )
    emit_output(body, use_pager=use_pager)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
