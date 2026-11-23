# US CPI local API

Run the backend with Python 3.11 and the frontend with Node.js 24 LTS and npm in two separate terminals.

## First-time setup

Clone both repositories into the same parent directory.

```sh
git clone https://github.com/MoonishMaredia/inflation-backend.git
git clone https://github.com/MoonishMaredia/inflation-dashboard.git
cd inflation-backend
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

On Windows, use `py -3.11 -m venv .venv` and activate it with `.venv\Scripts\Activate.ps1` in PowerShell.

The repository includes `inflation_database.db` with verified monthly data through October 2026, so you can start immediately. The database path is anchored to this repository. API indexes and monthly weights preserve official missing observations.

## Start the backend

From `inflation-backend`, activate the environment and run the API. Keep this terminal open.

```sh
source .venv/bin/activate
python -m uvicorn app:app --host 127.0.0.1 --port 8000
```

Open [the API documentation](http://127.0.0.1:8000/docs). The [latest-data endpoint](http://127.0.0.1:8000/api/v1/maxDate) should return `"2026-10-01"` for the included database.

## Start the frontend

In a second terminal, run the following from the parent directory containing both repositories.

```sh
cd inflation-dashboard
npm ci
npm start
```

Open [the dashboard](http://localhost:3000). It calls the backend at `http://127.0.0.1:8000`. If an existing frontend environment file points elsewhere, put `REACT_APP_INFLATION_URL=http://127.0.0.1:8000` in `inflation-dashboard/.env.local` and restart React.

For subsequent runs, start the backend and run `npm start` in the frontend terminal. Dependencies need installing again when their requirements change. Stop either service with Ctrl+C. The [frontend README](https://github.com/MoonishMaredia/inflation-dashboard#readme) has additional UI and test guidance.

## Refreshes

After BLS publishes the month you want, activate the Python environment and run this from the backend repository. Replace the example month with the latest released month. The refresh is run manually locally. January refreshes automatically obtain the new basket's official December anchor.

```sh
python -m API.update_data 2026-10
```

The command reconciles monthly indexes back to January 1999 so that rebasings stay continuous. It verifies all requested series and calendar months, skips annual averages, and imports December Table 1 weights for 2024 onward. Raw API responses and HTML are recorded in `.bls-cache/`. Use `--reuse-cache` for intentional replay of those responses. A successful refresh takes a SQLite backup in `.db-backups/` and writes the verified rows in one transaction. If an annual table changes layout or a series is missing unexpectedly, the refresh stops before writing.

The BLS API supports unregistered use with up to 25 series per request, 10 years per request, and 25 queries per day. Optionally export `BLS_API_KEY` to use registered limits of 50 series, 20 years, and 500 queries per day. Bureau registration sends the key by email and requires renewal at least annually. [BLS API FAQ](https://www.bls.gov/developers/api_faqs.htm)

If starting with no database, run `python -m database.create_local_db` first. It loads the series hierarchy, historical index and monthly weight snapshots through May 2024, and annual anchors through December 2023. It refuses to replace an existing database.

## Sources and calculations

- Index levels are the 57 existing CPI-U, U.S. city average, not seasonally adjusted series. Monthly and annual changes compare the exact dates one or twelve calendar months apart. The original history was checked against the live API. BLS rebased tobacco and smoking products to December 2024 = 100, and the refreshed index history includes that revision back to January 1999. [BLS time-series API](https://www.bls.gov/developers/) [BLS rebasing notice](https://www.bls.gov/cpi/notices/2026/rebasing.htm)
- The manual annual step populated `base_weight` from the December CPI-U Table 1. The refresh selects the new expenditure weights, with the December 2023, 2024, and 2025 tables using 2022, 2023, and 2024 spending and anchoring the 2024, 2025, and 2026 CPI baskets respectively. The original 57 December 2023 anchors match the official table. The explicit series-to-table label mapping lives in `datafiles/bls_weight_labels.csv`. [December 2023](https://www.bls.gov/cpi/tables/relative-importance/2023.htm) [December 2024](https://www.bls.gov/cpi/tables/relative-importance/2024.htm) [December 2025](https://www.bls.gov/cpi/tables/relative-importance/2025.htm)
- Each month's estimated relative importance is the prior December anchor multiplied by the category's index ratio since December and divided by the all-items index ratio over the same period, rounded to three decimals. The database retains the historical monthly weight snapshot through December 2023. The published annual anchors are relative importance percentages. Monthly weights are price-updated estimates and may differ slightly from the BLS release tables due to rounding. [BLS relative importance methodology](https://www.bls.gov/cpi/tables/relative-importance/)
- All 57 tracked October 2025 indexes were marked unavailable by the API. November 2025 motor vehicle insurance is also unpublished. Missing observations propagate to comparisons that require them, including the November 2025 monthly rates, October 2026 annual rates, and December 2025 motor vehicle insurance monthly rate. The dashboard keeps the missing calendar months visible. [BLS shutdown FAQ](https://www.bls.gov/cpi/additional-resources/2025-federal-government-shutdown-impact-cpi-faq.htm) [November 2025 release](https://www.bls.gov/news.release/archives/cpi_12182025.htm) [October 2026 release](https://www.bls.gov/news.release/cpi.htm)
- The waterfall multiplies each major category's price change by its starting relative importance. The sum is an approximate change for a fixed starting basket. The actual all-items CPI change uses the published headline indexes and is displayed separately. BLS updates expenditure weights annually, so a comparison spanning a reweighting can give different results. The waterfall requires both endpoints and all eight starting weights.
