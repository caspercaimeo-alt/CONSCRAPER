import requests
from time import sleep
import csv
from datetime import datetime
from collections import defaultdict
import pandas as pd
import os
import json
import logging
import argparse
from tqdm import tqdm

# ---------------------- SCRAPER ----------------------

def get_awards_last_5_years(CGAC=None, delay=0.2, max_pages=None):
    url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
    output = []
    page = 1
    page_size = 50

    current_year = datetime.now().year
    start_year = current_year - 5
    time_period = [{"start_date": f"{start_year}-01-01", "end_date": f"{current_year}-12-31"}]

    print(f"Fetching awards for CGAC {CGAC} from {start_year} to {current_year}...")

    pbar = tqdm(desc="Pages fetched", unit="page")

    while True:
        payload = {
            "filters": {
                "awarding_agency_codes": [CGAC] if CGAC else [],
                "time_period": time_period
            },
            "fields": ["award_id", "recipient_name", "action_date", "federal_action_obligation", "awarding_agency_code", "awarding_agency_name", "trend"],
            "page": page,
            "limit": page_size
        }

        try:
            response = requests.post(url, json=payload, timeout=20)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            print(f"\n⚠️ Request failed on page {page}: {e}")
            break

        results = data.get("results", [])
        if not results:
            break

        output.extend(results)
        page += 1
        pbar.update(1)
        sleep(delay)

        if max_pages and page > max_pages:
            break

    pbar.close()
    print(f"✅ Finished fetching {len(output)} total awards for CGAC {CGAC} in the last 5 years")
    return output

# ---------------------- FILTER 20% CHANGE ----------------------

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
                "awarding_agency_code": award.get("awarding_agency_code", ""),
                "trend": award.get("trend", "")
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
    print(f"✅ {len(filtered)} awards match the ≥20% change filter")
    return filtered, last_5_years

# ---------------------- DASHBOARD HTML ----------------------

def create_dashboard_with_charts(csv_files, output_file="dashboard.html"):
    tabs_html = ""
    content_html = ""
    tab_buttons = ""
    
    for i, csv_file in enumerate(csv_files):
        if not os.path.exists(csv_file):
            logging.warning(f"CSV file {csv_file} not found, skipping in dashboard")
            continue

        df = pd.read_csv(csv_file)
        tab_id = f"tab{i}"

        # Add chart column
        funding_cols = [c for c in df.columns if c.startswith("funding_")]
        if funding_cols:
            chart_divs = [f"chart_{i}_{idx}" for idx in df.index]
            df["chart"] = chart_divs

        table_html = df.to_html(index=False, escape=False, classes="display nowrap", table_id=f"table{i}")

        active_class = "active" if i == 0 else ""
        tab_buttons += f'<button class="tablinks {active_class}" onclick="openTab(event, \'{tab_id}\')">{os.path.basename(csv_file)}</button>\n'
        display_style = "block" if i == 0 else "none"
        content_html += f'<div id="{tab_id}" class="tabcontent" style="display:{display_style}">\n<h2>{os.path.basename(csv_file)}</h2>\n{table_html}\n</div>\n'

    html_content = f"""
<html>
<head>
<meta charset="utf-8">
<title>CONSCRAPERV1 Dashboard</title>
<link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css"/>
<style>
body {{ font-family: Arial, sans-serif; margin: 20px; }}
.tab {{ overflow: hidden; border-bottom: 1px solid #ccc; margin-bottom: 10px; }}
.tab button {{ background-color: inherit; border: none; outline: none; padding: 10px 20px; cursor: pointer; }}
.tab button.active {{ background-color: #ddd; }}
.tabcontent {{ display: none; }}
</style>
<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.plot.ly/plotly-2.30.0.min.js"></script>
</head>
<body>
<h1>CONSCRAPERV1 Dashboard</h1>
<div class="tab">
{tab_buttons}
</div>
{content_html}

<script>
function openTab(evt, tabId) {{
    var tabcontent = document.getElementsByClassName("tabcontent");
    for (var i = 0; i < tabcontent.length; i++) {{
        tabcontent[i].style.display = "none";
    }}
    var tablinks = document.getElementsByClassName("tablinks");
    for (var i = 0; i < tablinks.length; i++) {{
        tablinks[i].className = tablinks[i].className.replace(" active", "");
    }}
    document.getElementById(tabId).style.display = "block";
    evt.currentTarget.className += " active";
}}
$(document).ready(function() {{ $("table.display").DataTable({{ scrollX: true }}); }});
</script>
<script>
document.addEventListener("DOMContentLoaded", function() {{
"""

    for i, csv_file in enumerate(csv_files):
        if not os.path.exists(csv_file):
            continue
        df = pd.read_csv(csv_file)
        funding_cols = [c for c in df.columns if c.startswith("funding_")]
        if not funding_cols or "chart" not in df.columns:
            continue
        for idx, row in df.iterrows():
            chart_id = row["chart"]
            values = [row[c] for c in funding_cols]
            years = funding_cols
            bar_colors = ["green" if row.get("trend","".lower())=="increase" else "red" for _ in values]
            html_content += f"""
    Plotly.newPlot('{chart_id}', [{{
        x: {json.dumps(years)},
        y: {json.dumps(values)},
        type: 'bar',
        marker: {{ color: {bar_colors} }}
    }}], {{margin: {{t:0, b:30, l:30, r:10}}, height:150, width:300}});
"""

    html_content += """
}});
</script>
</body>
</html>
"""

    with open("dashboard.html", "w", encoding="utf-8") as f:
        f.write(html_content)
    logging.info("✅ Dashboard with charts created: dashboard.html")

# ---------------------- MAIN ----------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cgac", type=str, default="097")
    parser.add_argument("--trend_only", type=str, choices=["increase","decrease","both"], default="both")
    parser.add_argument("--max_pages", type=int, default=None)
    args = parser.parse_args()

    all_awards = get_awards_last_5_years(CGAC=args.cgac, max_pages=args.max_pages)

    if not all_awards:
        print("⚠️ No awards fetched. Check API status or filters.")
    else:
        filtered_awards, last_5_years = filter_20_percent_change(all_awards)
        if args.trend_only != "both":
            filtered_awards = [a for a in filtered_awards if a["trend"].lower() == args.trend_only]

        # Save CSVs
        df_filtered = pd.DataFrame(filtered_awards)
        df_filtered.to_csv(f"DoD_awards_20percent_change_filtered_{args.cgac}.csv", index=False)
        df_filtered.to_csv("top_10_increases.csv", index=False)
        df_filtered.to_csv("top_10_decreases.csv", index=False)
        df_filtered.to_csv("summary_awards.csv", index=False)
        df_filtered.to_csv("recipient_summary.csv", index=False)

        # Generate HTML dashboard
        csv_files = [
            f"DoD_awards_20percent_change_filtered_{args.cgac}.csv",
            "top_10_increases.csv",
            "top_10_decreases.csv",
            "summary_awards.csv",
            "recipient_summary.csv"
        ]
        create_dashboard_with_charts(csv_files)
