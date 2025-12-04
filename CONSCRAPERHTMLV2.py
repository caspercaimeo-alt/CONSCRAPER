import pandas as pd
import os
import json
import logging

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

        # Add a 'chart' column for inline bar charts if funding columns exist
        funding_cols = [c for c in df.columns if c.startswith("funding_")]
        if funding_cols:
            chart_divs = [f"chart_{i}_{idx}" for idx in df.index]
            df["chart"] = chart_divs

        # Render table
        table_html = df.to_html(index=False, escape=False, classes="display nowrap", table_id=f"table{i}")

        # Tab button
        active_class = "active" if i == 0 else ""
        tab_buttons += f'<button class="tablinks {active_class}" onclick="openTab(event, \'{tab_id}\')">{os.path.basename(csv_file)}</button>\n'

        # Tab content
        display_style = "block" if i == 0 else "none"
        content_html += f'<div id="{tab_id}" class="tabcontent" style="display:{display_style}">\n<h2>{os.path.basename(csv_file)}</h2>\n{table_html}\n</div>\n'

    # Full HTML template
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
// Tab switching
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

$(document).ready(function() {{
    $("table.display").DataTable({{ scrollX: true }});
}});
</script>

<script>
document.addEventListener("DOMContentLoaded", function() {{
"""

    # Add Plotly bar charts per row
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
});
</script>
</body>
</html>
"""

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_content)
    logging.info(f"✅ Dashboard with charts created: {output_file}")

# --- Example usage ---
csv_files = [
    f"DoD_awards_20percent_change_filtered_097.csv",
    "top_10_increases.csv",
    "top_10_decreases.csv",
    "summary_awards.csv",
    "recipient_summary.csv"
]
create_dashboard_with_charts(csv_files)