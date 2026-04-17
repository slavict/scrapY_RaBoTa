# rabota.md IT scraper

This project contains:

- `scrape_rabota_it.py` — scrapes IT jobs from [rabota.md IT category](https://www.rabota.md/ro/vacancies/category/it)
- `filter_jobs.py` — filters/browses a **details CSV**

## Requirements

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)

## Setup

```bash
uv venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
uv pip install -r requirements.txt
```

## Scrape IT jobs

Run:

```bash
uv run python scrape_rabota_it.py
```

Options:

```bash
uv run python scrape_rabota_it.py -o my_jobs.csv
uv run python scrape_rabota_it.py --pause 0.5
uv run python scrape_rabota_it.py --log-level DEBUG
uv run python scrape_rabota_it.py --with-details --details-concurrency 20
```

### What it extracts

The scraper parses jobs from:

- `div.b_info7.categoryVacanciesFeed`

For each vacancy it writes only these columns:

- `vacancy_id`
- `title`
- `company`
- `posted_time`
- `url`

`posted_time` is taken directly from:

- `<div class="date text-sm text-gray-400">...</div>`

No additional fields are added.

## Filter details CSV (optional)

`filter_jobs.py` works with a details CSV that contains columns such as:
`job_title`, `company`, `location`, `address`, `description`, `url`.

It can also read `rabota_it_jobs.csv` to show published time in the output.

Examples:

```bash
uv run python filter_jobs.py --csv rabota_it_jobs_details.csv --remote
uv run python filter_jobs.py --company "Enter"
uv run python filter_jobs.py --search "backend"
uv run python filter_jobs.py --without-description
uv run python filter_jobs.py --interactive
```

Interactive mode keys:

- `j/k` or arrow keys: move
- `Enter`: show/hide description
- `J/K` or `PgDn/PgUp`: scroll description pane
- `g/G`: jump top/bottom
- `q`: quit
