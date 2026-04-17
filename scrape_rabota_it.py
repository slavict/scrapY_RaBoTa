#!/usr/bin/env python3
"""
Extract IT jobs from rabota.md category feed and write CSV with only:
vacancy_id, title, company, posted_time, url
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin

import scrapy
from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess

BASE = "https://www.rabota.md"
CATEGORY_PATH = "/ro/vacancies/category/it"

logger = logging.getLogger(__name__)

SESSION_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
}


@dataclass(frozen=True)
class VacancyRow:
    vacancy_id: str
    title: str
    company: str
    posted_time: str
    url: str


@dataclass(frozen=True)
class VacancyDetailRow:
    job_title: str
    company: str
    location: str
    address: str
    description: str
    url: str


def _posted_time_age_days(posted_time: str) -> float | None:
    """
    Convert Romanian relative date text to approximate age in days.
    Returns None for unknown/unparseable values.
    """
    t = posted_time.strip().lower()
    if not t:
        return None
    if t == "astăzi":
        return 0.0
    if t == "ieri":
        return 1.0

    m = re.match(r"^(\d+)\s+(minut|minute)\s+în urmă$", t)
    if m:
        return int(m.group(1)) / (24 * 60)

    m = re.match(r"^(\d+)\s+(oră|ore)\s+în urmă$", t)
    if m:
        return int(m.group(1)) / 24

    m = re.match(r"^(\d+)\s+(zi|zile)\s+în urmă$", t)
    if m:
        return float(int(m.group(1)))

    m = re.match(r"^(\d+)\s+(lună|luni)\s+în urmă$", t)
    if m:
        return float(int(m.group(1)) * 30)

    return None


def _is_not_older_than_one_day(posted_time: str) -> bool:
    age = _posted_time_age_days(posted_time)
    if age is None:
        return False
    # Keep only "today" jobs (under 24 hours).
    return age < 1.0


def _clean_text(s: str) -> str:
    return " ".join((s or "").split())


def _extract_labeled_value(soup: BeautifulSoup, labels: tuple[str, ...]) -> str:
    patterns = [re.compile(rf"^\s*{re.escape(label)}\s*:?\s*(.*)$", re.IGNORECASE) for label in labels]

    # 1) Direct "Label: Value" text in common block nodes.
    for el in soup.find_all(["p", "li", "div", "span", "dt", "dd"]):
        txt = _clean_text(el.get_text(" ", strip=True))
        if not txt:
            continue
        for rx in patterns:
            m = rx.match(txt)
            if not m:
                continue
            val = _clean_text(m.group(1))
            if val:
                return val

    # 2) Label and value split across nested tags / siblings.
    for node in soup.find_all(string=True):
        txt = _clean_text(str(node))
        if not txt:
            continue
        for rx in patterns:
            m = rx.match(txt)
            if m:
                val = _clean_text(m.group(1))
                if val:
                    return val
                parent = getattr(node, "parent", None)
                if parent is not None:
                    for owner in (parent, getattr(parent, "parent", None)):
                        if owner is None:
                            continue
                        sib = owner.find_next_sibling()
                        if sib is None:
                            continue
                        sib_txt = _clean_text(sib.get_text(" ", strip=True))
                        if sib_txt:
                            return sib_txt

            parent = getattr(node, "parent", None)
            if parent is None:
                continue
            merged = _clean_text(parent.get_text(" ", strip=True))
            for rx2 in patterns:
                m2 = rx2.match(merged)
                if not m2:
                    continue
                val = _clean_text(m2.group(1))
                if val:
                    return val
    return ""


def _parse_detail_row_from_html(row: VacancyRow, html: str) -> VacancyDetailRow:
    soup = BeautifulSoup(html, "html.parser")

    job_title = row.title
    h1 = soup.select_one("h1")
    if h1:
        t = _clean_text(h1.get_text(" ", strip=True))
        if t:
            job_title = t

    company = row.company
    company_el = soup.select_one("a.vip_company--name, .companyName a, .company-name a")
    if company_el:
        c = _clean_text(company_el.get_text(" ", strip=True))
        if c:
            company = c

    location = _extract_labeled_value(
        soup,
        ("Locul de muncă", "Locul de munca"),
    )
    address = _extract_labeled_value(
        soup,
        ("Oficiul principal", "Oficiul central", "Adresa"),
    )

    desc = ""
    desc_selectors = [
        "div.vacancy-content[data-js-vacancy-content]",
        "div[data-js-vacancy-content]",
        "div.vacancy-content",
        "section.vacancy-content",
        "div[class*='vacancy-content']",
        "div[class*='description']",
        "section[class*='description']",
    ]
    for sel in desc_selectors:
        desc_el = soup.select_one(sel)
        if not desc_el:
            continue
        candidate = _clean_text(desc_el.get_text(" ", strip=True))
        if candidate:
            desc = candidate
            break

    # Last fallback: meta description from page head.
    if not desc:
        meta = soup.select_one("meta[name='description'][content]")
        if meta:
            desc = _clean_text(meta.get("content", ""))
    return VacancyDetailRow(
        job_title=job_title,
        company=company,
        location=location,
        address=address,
        description=desc,
        url=row.url,
    )


def details_output_path(main_output_path: str) -> str:
    p = Path(main_output_path)
    if p.suffix.lower() == ".csv":
        return str(p.with_name(f"{p.stem}_details.csv"))
    return str(p.with_name(f"{p.name}_details.csv"))


def write_details_csv(path: str, rows: list[VacancyDetailRow]) -> None:
    fieldnames = ["job_title", "company", "location", "address", "description", "url"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(
                {
                    "job_title": r.job_title,
                    "company": r.company,
                    "location": r.location,
                    "address": r.address,
                    "description": r.description,
                    "url": r.url,
                }
            )


def next_page_path(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.select_one("div.js-page-container[data-next]")
    if not container:
        return None
    nxt = container.get("data-next")
    if not nxt or not str(nxt).strip():
        return None
    return str(nxt).strip()


def _company_name_from_block(block) -> str:
    a = block.select_one("a.vip_company--name")
    if a:
        return a.get_text(strip=True)
    return ""


def _posted_time_from_block(block) -> str:
    # Must come from: <div class="date text-sm text-gray-400">...</div>
    date_el = block.select_one("div.date.text-sm.text-gray-400")
    if date_el:
        return date_el.get_text(strip=True)
    # Fallback to any .date inside the company block.
    date_el = block.select_one("div.date")
    return date_el.get_text(strip=True) if date_el else ""


def _extract_row_from_li(li, company: str, posted_time: str):
    vacancy_id = ""
    raw_id = li.get("id", "")
    if raw_id.startswith("vacID-"):
        vacancy_id = raw_id.replace("vacID-", "", 1)

    link = li.select_one("a.vacancyShowPopup[href]")
    if not link:
        return None
    href = link.get("href", "")
    if "/locuri-de-munca/" not in href:
        return None

    title = link.get_text(strip=True) or (link.get("title") or "").strip()
    url = urljoin(BASE, href)
    return VacancyRow(
        vacancy_id=vacancy_id,
        title=title,
        company=company,
        posted_time=posted_time,
        url=url,
    )


def _nearest_company_and_time(li, container):
    """
    For b_info10/vacancy-list-like layouts, try nearest parent first.
    """
    company = ""
    posted_time = ""
    for p in li.parents:
        if p == container:
            break
        if not company:
            c = p.select_one("a.vip_company--name")
            if c:
                company = c.get_text(strip=True)
        if not posted_time:
            d = p.select_one("div.date.text-sm.text-gray-400") or p.select_one("div.date")
            if d:
                posted_time = d.get_text(strip=True)
        if company and posted_time:
            break
    return company, posted_time


def iter_vacancies_from_page(html: str):
    soup = BeautifulSoup(html, "html.parser")

    for block in soup.select("div.categoryVacancy.preview"):
        company = _company_name_from_block(block)
        posted_time = _posted_time_from_block(block)
        for li in block.select("li.vacancyRow"):
            row = _extract_row_from_li(li, company, posted_time)
            if row:
                yield row

    for card in soup.select("div.vacancyCardItem.previewCard.noPaddings"):
        vacancy_id = (card.get("data-vacancyid") or card.get("data-vacancyId") or "").strip()

        link = card.select_one("a.vacancyShowPopup[href]")
        if not link:
            continue
        href = (link.get("href") or "").strip()
        if not href:
            continue

        title = link.get_text(" ", strip=True) or (link.get("title") or "").strip()
        if not title:
            continue

        # For previewCard layout, company is usually the first label in metadata row.
        company = ""
        meta_spans = card.select("div.text-black.flex.items-center.gap-x-6 span")
        for span in meta_spans:
            txt = span.get_text(" ", strip=True)
            if txt:
                company = txt
                break

        posted_time = ""
        time_el = card.select_one("p.text-sm.font-normal.text-gray-400")
        if time_el:
            posted_time = time_el.get_text(" ", strip=True)
            posted_time = re.sub(r"^\s*actualizat:\s*", "", posted_time, flags=re.IGNORECASE)
        if not posted_time:
            date_el = card.select_one("div.date.text-sm.text-gray-400, div.date")
            posted_time = date_el.get_text(" ", strip=True) if date_el else ""

        yield VacancyRow(
            vacancy_id=vacancy_id,
            title=title,
            company=company,
            posted_time=posted_time,
            url=urljoin(BASE, href),
        )


class RabotaItSpider(scrapy.Spider):
    name = "rabota_it"

    def __init__(
        self,
        start_url: str,
        out_rows: list[VacancyRow],
        *,
        with_details: bool = False,
        out_detail_rows: list[VacancyDetailRow] | None = None,
        max_jobs: int | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.start_url = start_url
        self.out_rows = out_rows
        self.with_details = with_details
        self.out_detail_rows = out_detail_rows if out_detail_rows is not None else []
        self.max_jobs = max_jobs if (max_jobs is not None and max_jobs > 0) else None
        self._seen_keys: set[str] = set()
        self._details_seen_urls: set[str] = set()

    def start_requests(self):
        yield scrapy.Request(self.start_url, headers=SESSION_HEADERS, callback=self.parse_page)

    def _collect_rows_from_html(self, html: str) -> list[VacancyRow]:
        added: list[VacancyRow] = []
        for row in (iter_vacancies_from_page(html) or []):
            if self.max_jobs is not None and len(self.out_rows) >= self.max_jobs:
                break
            key = row.vacancy_id or row.url
            if key in self._seen_keys:
                continue
            if not _is_not_older_than_one_day(row.posted_time):
                continue
            self._seen_keys.add(key)
            self.out_rows.append(row)
            added.append(row)
        return added

    @staticmethod
    def _fallback_detail(row: VacancyRow) -> VacancyDetailRow:
        return VacancyDetailRow(
            job_title=row.title,
            company=row.company,
            location="",
            address="",
            description="",
            url=row.url,
        )

    def _detail_request(self, row: VacancyRow):
        if row.url in self._details_seen_urls:
            return None
        self._details_seen_urls.add(row.url)
        return scrapy.Request(
            row.url,
            headers=SESSION_HEADERS,
            callback=self.parse_detail,
            errback=self.parse_detail_error,
            meta={"vacancy_row": row},
            dont_filter=True,
        )

    def parse_detail(self, response):
        row: VacancyRow = response.meta["vacancy_row"]
        self.out_detail_rows.append(_parse_detail_row_from_html(row, response.text))

    def parse_detail_error(self, failure):
        request = failure.request
        row: VacancyRow = request.meta["vacancy_row"]
        logger.warning("Failed details fetch %s: %s", row.url, failure.value)
        self.out_detail_rows.append(self._fallback_detail(row))

    def parse_page(self, response):
        rows = self._collect_rows_from_html(response.text)
        if self.with_details:
            for row in rows:
                req = self._detail_request(row)
                if req is not None:
                    yield req
        if self.max_jobs is not None and len(self.out_rows) >= self.max_jobs:
            return

        yield scrapy.Request(
            response.url,
            method="POST",
            headers=SESSION_HEADERS,
            callback=self.parse_ajax,
            dont_filter=True,
        )

        nxt = next_page_path(response.text)
        if nxt:
            yield scrapy.Request(
                urljoin(BASE, nxt),
                headers=SESSION_HEADERS,
                callback=self.parse_page,
            )

    def parse_ajax(self, response):
        try:
            payload = json.loads(response.text)
        except ValueError:
            return
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            return
        content = data.get("content")
        if not isinstance(content, str) or not content.strip():
            return
        rows = self._collect_rows_from_html(content)
        if self.with_details:
            for row in rows:
                req = self._detail_request(row)
                if req is not None:
                    yield req


def scrape_all(
    start_url: str,
    pause_s: float,
    with_details: bool = False,
    details_concurrency: int = 12,
    max_jobs: int | None = None,
) -> tuple[list[VacancyRow], list[VacancyDetailRow]]:
    out_rows: list[VacancyRow] = []
    out_detail_rows: list[VacancyDetailRow] = []
    process = CrawlerProcess(
        settings={
            "LOG_ENABLED": False,
            "DOWNLOAD_DELAY": max(0.0, pause_s),
            "CONCURRENT_REQUESTS": max(8, details_concurrency if with_details else 8),
            "ROBOTSTXT_OBEY": False,
        }
    )
    process.crawl(
        RabotaItSpider,
        start_url=start_url,
        out_rows=out_rows,
        with_details=with_details,
        out_detail_rows=out_detail_rows,
        max_jobs=max_jobs,
    )
    process.start()
    return out_rows, out_detail_rows


def main() -> int:
    p = argparse.ArgumentParser(
        description="Scrape rabota.md IT feed to CSV: vacancy_id,title,company,posted_time,url"
    )
    p.add_argument(
        "-o",
        "--output",
        default="rabota_it_jobs.csv",
        help="Output CSV path (default: rabota_it_jobs.csv)",
    )
    p.add_argument(
        "--pause",
        type=float,
        default=1,
        help="Seconds to sleep between paginated requests (default: 1)",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    p.add_argument(
        "--with-details",
        action="store_true",
        help="Also fetch each vacancy page and write *_details.csv",
    )
    p.add_argument(
        "--details-concurrency",
        type=int,
        default=12,
        help="Concurrent detail requests when --with-details is set (default: 12)",
    )
    p.add_argument(
        "--max-jobs",
        type=int,
        default=None,
        help="Limit scraping to first N jobs (useful for testing details parsing)",
    )
    args = p.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    start = urljoin(BASE, CATEGORY_PATH)
    try:
        rows, detail_rows = scrape_all(
            start,
            args.pause,
            with_details=args.with_details,
            details_concurrency=args.details_concurrency,
            max_jobs=args.max_jobs,
        )
    except Exception as e:
        logger.error("Scrapy crawl error: %s", e)
        return 1

    fieldnames = ["vacancy_id", "title", "company", "posted_time", "url"]
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(
                {
                    "vacancy_id": r.vacancy_id,
                    "title": r.title,
                    "company": r.company,
                    "posted_time": r.posted_time,
                    "url": r.url,
                }
            )
    logger.info("Wrote %d rows to %s", len(rows), args.output)
    if args.with_details:
        details_path = details_output_path(args.output)
        write_details_csv(details_path, detail_rows)
        logger.info("Wrote %d detail rows to %s", len(detail_rows), details_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
