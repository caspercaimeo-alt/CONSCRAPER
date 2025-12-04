import requests
from time import sleep
import csv
from datetime import datetime
from collections import defaultdict
import argparse
from tqdm import tqdm
import logging

# --- Logging setup ---
log_filename = "CONSCRAPERV1.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_filename, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logging.info("=== Starting CONSCRAPERV1 ===")

# --- Fetch awards with retries ---
def get_awards_last_5_years(CGAC=None, delay=0.2, max_pages=None, recipient_keyword=None, agency_keyword=None, max_retries=3):
    url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
    output = []
    page = 1
    page_size = 50

    current_year = datetime.now().year
    start_year = current_year - 5
    time_period = [{"start_date": f"{start_year}-01-01", "end_date": f"{current_year}-12-31"}]

    logging.info(f"Fetching awards for CGAC {CGAC} from {start_year} to {current_year}...")

    pbar = tqdm(desc="Pages fetched", unit="page")

    while True:
        payload = {
            "filters": {
                "awarding_agency_codes": [CGAC] if CGAC else [],
                "time_period": time_period
            },
            "fields": ["award_id", "recipient_name", "action_date", "federal_action_obligation", "awarding_agency_code", "awarding_agency_name"],
            "page": page,
            "limit": page_size
        }

        retries = 0
        success = False
        while retries < max_retries and not success:
            try:
                response = requests.post(url, json=payload, timeout=20)
                response.raise_for_status()
                data = response.json()
                success = True
            except requests.RequestException as e:
                retries += 1
                logging.warning(f"Request failed on page {page} (attempt {retries}/{max_retries}): {e}")
                sleep(2)
            except ValueError:
                logging.error(f"Invalid JSON response on page {page}")
                break

        if not success:
            logging.error(f"Page {page} failed after {max_retries} retries. Skipping.")
            break

        results = data.get("results", [])
        if not results:
            logging.info(f"No results on page {page}, stopping pagination.")
            break

        if recipient_keyword:
            results = [r for r in results if recipient_keyword.lower() in r.get("recipient_name", "").lower()]
        if agency_keyword:
            results = [r for r in results if agency_keyword.lower() in r.get("awarding_agency_name", "").lower()]

        output.extend(results)
        page += 1
        pbar.update(1)
        sleep(delay)

        if max_pages and page > max_pages:
            logging.info(f"Reached max_pages={max_pages}, stopping early.")
            break

    pbar.close()
    logging.info(f"Finished fetching {len(output)} total awards for CGAC {CGAC}")
    return output

# --- Filtering function ---
def filter_20_percent_change(data):
    filtered = []
    award_years = defaultdict(lambda: defaultdict(float))
    award_info = {}

    current_year = datetime.now().year
    last_5_years = [current_year - i for i in range(5, 0, -1)]

    for award in data:
        try:
            year = int(award['action_date'][:4])
            award_years[award['award_id']][year] += float(award['federal_action_obligation'])
            award_info[award['award_id']] = {
                "recipient_name": award.get("recipient_name", ""),
                "awarding_agency_code": award.get("awarding_agency_code", "")
            }
        except (KeyError, ValueError, TypeError):
            continue

    for award_id, years in award_years.items():
        sorted_years = sorted(years.items())
        last_3_years = sorted_years[-3:]
        if len(last_3_years) < 2:
            continue

        first_year, first_amount = last_3_years[0]
        last_year, last_amount = last_3_years[-1]

        if first_amount == 0:
            continue

        change = (last_amount - first_amount) / first_amount
        total_5yr = sum(years.get(y, 0) for y in last_5_years)

        if abs(change) >= 0.2:
            row = {
                "award_id": award_id,
                "recipient_name": award_info[award_id]["recipient_name"],
                "awarding_agency_code": award_info[award_id]["awarding_agency_code"],
                "first_year": first_year,
                "first_amount": first_amount,
                "last_year": last_year,
                "last_amount": last_amount,
                "change_percent": round(change * 100, 2),
                "trend": "Increase" if change > 0 else "Decrease",
                "total_5yr_funding": round(total_5yr, 2)
            }
            for y in last_5_years:
                row[f"funding_{y}"] = round(years.get(y, 0), 2)
            row["abs_change"] = abs(last_amount - first_amount)
            filtered.append(row)

    filtered.sort(key=lambda x: x['change_percent'], reverse=True)
    logging.info(f"{len(filtered)} awards match the ≥20% change filter")
    return filtered, last_5_years

# --- CSV functions ---
def save_to_csv(data, filename="filtered_awards.csv"):
    if not data:
        logging.warning("No data to save.")
        return
    headers = data[0].keys()
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)
    logging.info(f"Data saved to {filename}")

def save_top_increases_decreases(data, top_n=10):
    increases = [d for d in data if d['trend'] == "Increase"][:top_n]
    decreases = [d for d in data if d['trend'] == "Decrease"][:top_n]
    save_to_csv(increases, "top_10_increases.csv")
    save_to_csv(decreases, "top_10_decreases.csv")

def save_biggest_absolute_change(data):
    if not data:
        return
    biggest = max(data, key=lambda x: x['abs_change'])
    save_to_csv([biggest], "biggest_absolute_change.csv")

def save_summary_csv(data, last_5_years):
    summary = []
    totals_by_year = {y: 0 for y in last_5_years}
    total_funding = 0
    for award in data:
        for y in last_5_years:
            totals_by_year[y] += award.get(f"funding_{y}", 0)
        total_funding += award.get("total_5yr_funding", 0)
    summary.append({"metric": "Total Funding"})
    for y in last_5_years:
        summary[0][f"funding_{y}"] = round(totals_by_year[y], 2)
    summary[0]["total_5yr_funding"] = round(total_funding, 2)
    summary[0]["award_count"] = len(data)
    headers = summary[0].keys()
    with open("summary_awards.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(summary)
    logging.info("Summary CSV saved to summary_awards.csv")

def save_recipient_summary(data, last_5_years):
    recipients = defaultdict(lambda: {"award_count": 0, "total_5yr_funding": 0, "sum_change_percent": 0, "trends": [], **{f"funding_{y}":0 for y in last_5_years}})
    for award in data:
        rec = award["recipient_name"]
        recipients[rec]["award_count"] += 1
        recipients[rec]["total_5yr_funding"] += award.get("total_5yr_funding",0)
        recipients[rec]["sum_change_percent"] += award.get("change_percent",0)
        recipients[rec]["trends"].append(award.get("trend",""))
        for y in last_5_years:
            recipients[rec][f"funding_{y}"] += award.get(f"funding_{y}",0)
    pivot_data = []
    for rec, info in recipients.items():
        avg_change = round(info["sum_change_percent"]/info["award_count"],2) if info["award_count"] else 0
        trend = max(set(info["trends"]), key=info["trends"].count) if info["trends"] else ""
        row = {
            "recipient_name": rec,
            "award_count": info["award_count"],
            "total_5yr_funding": round(info["total_5yr_funding"],2),
            "average_change_percent": avg_change,
            "major_trend": trend
        }
        for y in last_5_years:
            row[f"funding_{y}"] = round(info[f"funding_{y}"],2)
        pivot_data.append(row)
    pivot_data.sort(key=lambda x: x["total_5yr_funding"], reverse=True)
    save_to_csv(pivot_data, "recipient_summary.csv")
    logging.info("Recipient summary CSV saved to recipient_summary.csv")

# --- Main ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch USAspending awards for the last 5 years with optional filters and retries")
    parser.add_argument("--cgac", type=str, default="097", help="3-digit agency code")
    parser.add_argument("--recipient", type=str, default=None, help="Recipient keyword filter")
    parser.add_argument("--agency", type=str, default=None, help="Agency keyword filter")
    parser.add_argument("--max_pages", type=int, default=None, help="Max pages to fetch")
    parser.add_argument("--trend_only", type=str, choices=["increase","decrease","both"], default="both")
    args = parser.parse_args()

    try:
        all_awards = get_awards_last_5_years(
            CGAC=args.cgac,
            max_pages=args.max_pages,
            recipient_keyword=args.recipient,
            agency_keyword=args.agency
        )

        if not all_awards:
            logging.warning("No awards fetched. Check API status or filters.")
        else:
            filtered_awards, last_5_years = filter_20_percent_change(all_awards)

            if args.trend_only != "both":
                filtered_awards = [a for a in filtered_awards if a["trend"].lower() == args.trend_only]
                logging.info(f"After applying trend_only={args.trend_only}, {len(filtered_awards)} awards remain")

            save_to_csv(filtered_awards, f"DoD_awards_20percent_change_filtered_{args.cgac}.csv")
            save_top_increases_decreases(filtered_awards)
            save_biggest_absolute_change(filtered_awards)
            save_summary_csv(filtered_awards, last_5_years)
            save_recipient_summary(filtered_awards, last_5_years)
            logging.info(f"Sample filtered award: {filtered_awards[0] if filtered_awards else 'No results'}")

    except Exception as e:
        logging.error(f"Script failed: {e}")