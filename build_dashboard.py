#!/usr/bin/env python3
"""Generate the POS-DERP 3.0 dashboard on Google Sheets via the Sheets API."""

from __future__ import annotations

import collections
import os
import random
import sys
import time
from datetime import datetime, timezone, timedelta
from functools import wraps
from statistics import mean
from typing import Dict, List, Tuple, Callable, Any

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Load environment variables
load_dotenv()

SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")
MASTER_COPY_SPREADSHEET_ID = os.getenv("MASTER_COPY_SPREADSHEET_ID")
HEIFER_SPREADSHEET_ID = os.getenv("HEIFER_SPREADSHEET_ID")
DATA_SHEET = "Data"
DASHBOARD_SHEET = "Dashboard"
PIVOT_SHEET = "Pivot_Data"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# API rate limiting configuration
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0
MAX_REQUEST_SIZE_MB = 9  # Keep under 10MB limit
CHUNK_SIZE = 10000  # Fetch data in 10k record chunks

# API call tracking
api_call_count = 0

HEX_COLORS = {
    "background": "F5F7FB",
    "kpi_fill": "E3ECFF",
    "kpi_text": "1B1E36",
    "table_header": "D8E2EF",
    "table_band": "EEF3FB",
    "accent_primary": "5C8DFF",
    "accent_secondary": "7BC5B2",
    "accent_tertiary": "9A8CFF",
}


def log_api_call(operation: str):
    """Track API calls for monitoring."""
    global api_call_count
    api_call_count += 1
    print(f"  API Call #{api_call_count}: {operation}")


def estimate_request_size(data: Any) -> float:
    """Estimate request size in MB."""
    import json
    try:
        size_bytes = len(json.dumps(data, default=str).encode('utf-8'))
        return size_bytes / (1024 * 1024)
    except:
        return 0


def execute_with_retry(func: Callable, operation_name: str, max_retries: int = MAX_RETRIES) -> Any:
    """
    Execute API call with exponential backoff retry logic.
    Handles rate limits (429), server errors (503), timeouts, and other transient failures.
    """
    for attempt in range(max_retries):
        try:
            log_api_call(operation_name)
            result = func()
            return result
        except HttpError as e:
            status = e.resp.status

            # Rate limit or server error - retry with backoff
            if status in [429, 503]:
                if attempt == max_retries - 1:
                    print(f"  ✗ Max retries reached for {operation_name}")
                    raise

                # Exponential backoff with jitter
                wait_time = (INITIAL_BACKOFF * (2 ** attempt)) + (random.random() * 0.1)
                print(f"  ⚠ Rate limit hit (attempt {attempt + 1}/{max_retries}), waiting {wait_time:.2f}s...")
                time.sleep(wait_time)
            else:
                # Non-retryable error
                print(f"  ✗ Error in {operation_name}: HTTP {status}")
                raise
        except TimeoutError as e:
            # Network timeout - retry with backoff
            if attempt == max_retries - 1:
                print(f"  ✗ Max retries reached for {operation_name} after timeout")
                raise

            wait_time = (INITIAL_BACKOFF * (2 ** attempt)) + (random.random() * 0.1)
            print(f"  ⚠ Timeout on {operation_name} (attempt {attempt + 1}/{max_retries}), retrying in {wait_time:.2f}s...")
            time.sleep(wait_time)
        except Exception as e:
            # Check if it's a timeout-related exception
            error_str = str(e).lower()
            if 'timeout' in error_str or 'timed out' in error_str:
                if attempt == max_retries - 1:
                    print(f"  ✗ Max retries reached for {operation_name} after timeout")
                    raise

                wait_time = (INITIAL_BACKOFF * (2 ** attempt)) + (random.random() * 0.1)
                print(f"  ⚠ Timeout on {operation_name} (attempt {attempt + 1}/{max_retries}), retrying in {wait_time:.2f}s...")
                time.sleep(wait_time)
            else:
                # Other unexpected error - don't retry
                print(f"  ✗ Unexpected error in {operation_name}: {e}")
                raise

    raise Exception(f"Failed to execute {operation_name} after {max_retries} attempts")


def hex_to_rgb(color: str) -> Dict[str, float]:
    color = color.lstrip("#")
    return {
        "red": int(color[0:2], 16) / 255.0,
        "green": int(color[2:4], 16) / 255.0,
        "blue": int(color[4:6], 16) / 255.0,
    }


def get_service():
    """
    Get Google Sheets service with credentials.
    Supports both local (file-based) and GitHub Actions (env var JSON) authentication.
    """
    # Check if running in GitHub Actions (service account JSON in env var)
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

    if service_account_json:
        # GitHub Actions mode: parse JSON from environment variable
        import json
        service_account_info = json.loads(service_account_json)
        creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    else:
        # Local mode: use file path from .env
        if not SERVICE_ACCOUNT_FILE or not os.path.exists(SERVICE_ACCOUNT_FILE):
            raise ValueError(
                "No credentials found. Either set GOOGLE_SERVICE_ACCOUNT_JSON env var "
                "or provide SERVICE_ACCOUNT_FILE path in .env"
            )
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    return build("sheets", "v4", credentials=creds)


def get_sheet_id(service, sheet_name: str, spreadsheet_id: str) -> int:
    """Get the sheet ID for a given sheet name in a spreadsheet."""
    def _get():
        return service.spreadsheets().get(
            spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))"
        ).execute()

    spreadsheet = execute_with_retry(_get, f"get_sheet_id({sheet_name})")
    for sheet in spreadsheet.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == sheet_name:
            return props["sheetId"]
    raise ValueError(f"Sheet '{sheet_name}' not found in spreadsheet {spreadsheet_id}.")


def create_sheet_if_not_exists(service, spreadsheet_id: str, sheet_name: str) -> int:
    """Create a sheet if it doesn't exist and return its ID."""
    try:
        return get_sheet_id(service, sheet_name, spreadsheet_id)
    except ValueError:
        # Sheet doesn't exist, create it
        print(f"  Creating '{sheet_name}' sheet...")
        request = {
            "requests": [{
                "addSheet": {
                    "properties": {
                        "title": sheet_name
                    }
                }
            }]
        }
        def _create():
            return service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=request
            ).execute()

        response = execute_with_retry(_create, f"create_sheet({sheet_name})")
        sheet_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]
        print(f"  ✓ Created '{sheet_name}' sheet")
        return sheet_id


def fetch_data(service) -> Tuple[List[str], List[List[str]]]:
    """
    Fetch all data from the Data sheet starting from row 3.
    Uses pagination to handle large datasets efficiently.
    """
    print("  Fetching data with pagination for large datasets...")

    # First, get just the header row
    def _get_header():
        return service.spreadsheets().values().get(
            spreadsheetId=MASTER_COPY_SPREADSHEET_ID,
            range=f"{DATA_SHEET}!A3:T3"
        ).execute()

    header_result = execute_with_retry(_get_header, "fetch_header")
    header_values = header_result.get("values", [])

    if not header_values:
        raise ValueError("No header found in Data sheet")

    header = header_values[0]
    all_data_rows = []

    # Fetch data in chunks
    start_row = 4  # Start after header (row 3)
    chunk_num = 0

    while True:
        chunk_num += 1
        end_row = start_row + CHUNK_SIZE - 1
        range_str = f"{DATA_SHEET}!A{start_row}:T{end_row}"

        def _get_chunk():
            return service.spreadsheets().values().get(
                spreadsheetId=MASTER_COPY_SPREADSHEET_ID,
                range=range_str
            ).execute()

        result = execute_with_retry(_get_chunk, f"fetch_data_chunk_{chunk_num}")
        values = result.get("values", [])

        if not values:
            # No more data
            break

        print(f"    Chunk {chunk_num}: Fetched {len(values)} rows (rows {start_row}-{start_row + len(values) - 1})")

        # Process rows in this chunk
        for row in values:
            # Skip completely empty rows
            if not any(str(cell).strip() for cell in row):
                continue
            # Only include rows with at least some non-blank data
            if any(cell.strip() if isinstance(cell, str) else cell for cell in row):
                while len(row) < len(header):
                    row.append("")
                all_data_rows.append(row)

        # If we got fewer rows than CHUNK_SIZE, we've reached the end
        if len(values) < CHUNK_SIZE:
            break

        start_row = end_row + 1

    print(f"  ✓ Fetched {len(all_data_rows)} total data rows across {chunk_num} chunks")
    return header, all_data_rows


def normalize_data(header: List[str], rows: List[List[str]]) -> List[Dict[str, str]]:
    """Convert rows to dictionaries and normalize values."""
    normalized = []
    
    SEX_MAP = {"F": "Female", "M": "Male"}
    MARITAL_MAP = {"M": "Married", "S": "Single", "D": "Divorced", "W": "Widowed"}
    EDUCATION_MAP = {
        "0": "No Schooling",
        "1": "Primary",
        "2": "Secondary",
        "3": "Diploma",
        "4": "Degree",
        "5": "Postgraduate",
    }
    
    for row in rows:
        # Skip rows where all cells are empty/blank
        if not any(str(cell).strip() for cell in row):
            continue
        record = dict(zip(header, row))
        
        sex_raw = record.get("SEX", "").strip()
        record["Sex"] = SEX_MAP.get(sex_raw, "Other" if sex_raw else "Not Reported")
        
        marital_raw = record.get("MARITAL STATUS\nM/S/D/W", "").strip()
        record["Marital_Status"] = MARITAL_MAP.get(marital_raw, "Other" if marital_raw else "Not Reported")
        
        edu_raw = record.get("EDUCATIONAL LEVEL", "").strip()
        record["Education"] = EDUCATION_MAP.get(edu_raw, "Other" if edu_raw else "Not Reported")
        
        emp_raw = record.get("EMPLOYMENT STATUS ", "").strip().title()
        if emp_raw in {"Self-Employed", "Self-Empolyed", "Self Employeed"}:
            emp_raw = "Self-Employed"
        record["Employment"] = emp_raw if emp_raw else "Not Reported"
        
        record["Age_Range"] = record.get("AGE RANGE", "").strip() or "Not Reported"
        record["Cluster"] = record.get("CLUSTER / ASSOCIATION NAME", "").strip() or "Not Reported"
        record["State"] = record.get("STATE", "").strip() or "Not Reported"
        record["LGA"] = record.get("CLUSTER/ASSOCIATION LGA", "").strip() or "Not Reported"
        
        try:
            record["Age_Num"] = float(record.get("AGE ", "").strip() or 0)
        except:
            record["Age_Num"] = 0
            
        try:
            record["Household_Num"] = float(record.get("NO IN HOUSEHOLD", "").strip() or 0)
        except:
            record["Household_Num"] = 0
            
        try:
            record["Years_Num"] = float(record.get("YEARS IN POULTRY BUSSINESS", "").strip() or 0)
        except:
            record["Years_Num"] = 0
            
        try:
            record["Birds_Num"] = float(record.get("NO OF BIRDS ON FARM", "").strip() or 0)
        except:
            record["Birds_Num"] = 0
        
        normalized.append(record)
    
    return normalized


def aggregate_data(data: List[Dict[str, str]]) -> Dict:
    """Create all pivot tables and aggregations."""
    
    total_participants = len(data)
    
    gender_dist = collections.Counter(r["Sex"] for r in data)
    gender_table = [["Gender", "Count"]]
    for gender, count in sorted(gender_dist.items(), key=lambda x: -x[1]):
        gender_table.append([str(gender), int(count)])
    
    age_dist = collections.Counter(r["Age_Range"] for r in data)
    age_table = [["Age Range", "Count"]]
    for age_range, count in sorted(age_dist.items(), key=lambda x: -x[1]):
        age_table.append([str(age_range), int(count)])
    
    marital_dist = collections.Counter(r["Marital_Status"] for r in data)
    marital_table = [["Marital Status", "Count"]]
    for status, count in sorted(marital_dist.items(), key=lambda x: -x[1]):
        marital_table.append([str(status), int(count)])
    
    education_dist = collections.Counter(r["Education"] for r in data)
    education_table = [["Education", "Count"]]
    for edu, count in sorted(education_dist.items(), key=lambda x: -x[1]):
        education_table.append([str(edu), int(count)])
    
    employment_dist = collections.Counter(r["Employment"] for r in data)
    employment_table = [["Employment Status", "Count"]]
    for emp, count in sorted(employment_dist.items(), key=lambda x: -x[1]):
        employment_table.append([str(emp), int(count)])
    
    cluster_dist = collections.Counter(r["Cluster"] for r in data)
    top_clusters = sorted(cluster_dist.items(), key=lambda x: -x[1])[:10]
    cluster_table = [["Cluster", "Count"]]
    for cluster, count in top_clusters:
        cluster_table.append([str(cluster), int(count)])
    
    state_dist = collections.Counter(r["State"] for r in data)
    state_table = [["State", "Count"]]
    for state, count in sorted(state_dist.items(), key=lambda x: -x[1]):
        state_table.append([str(state), int(count)])
    
    marital_gender = collections.Counter((r["Marital_Status"], r["Sex"]) for r in data)
    marital_statuses = sorted(set(r["Marital_Status"] for r in data))
    marital_gender_table = [["Marital Status", "Female", "Male"]]
    for status in marital_statuses:
        female_count = marital_gender.get((status, "Female"), 0)
        male_count = marital_gender.get((status, "Male"), 0)
        marital_gender_table.append([str(status), int(female_count), int(male_count)])
    
    employment_stats = {}
    for emp in set(r["Employment"] for r in data):
        emp_data = [r for r in data if r["Employment"] == emp]
        birds = [r["Birds_Num"] for r in emp_data if r["Birds_Num"] > 0]
        household = [r["Household_Num"] for r in emp_data if r["Household_Num"] > 0]
        employment_stats[emp] = {
            "avg_birds": mean(birds) if birds else 0,
            "avg_household": mean(household) if household else 0
        }
    
    employment_stats_table = [["Employment Status", "Avg Birds", "Avg Household"]]
    for emp in sorted(employment_stats.keys()):
        stats = employment_stats[emp]
        employment_stats_table.append([
            str(emp),
            float(round(stats["avg_birds"], 1)),
            float(round(stats["avg_household"], 1))
        ])
    
    # Years in poultry business distribution (grouped)
    years_ranges = []
    for r in data:
        years = r["Years_Num"]
        if years == 0:
            years_ranges.append("Not Reported")
        elif years < 2:
            years_ranges.append("< 2 years")
        elif years < 5:
            years_ranges.append("2-5 years")
        elif years < 10:
            years_ranges.append("5-10 years")
        else:
            years_ranges.append("10+ years")
    
    years_dist = collections.Counter(years_ranges)
    years_table = [["Years in Business", "Count"]]
    # Sort by logical order
    order = ["< 2 years", "2-5 years", "5-10 years", "10+ years", "Not Reported"]
    for years_range in order:
        if years_range in years_dist:
            years_table.append([years_range, int(years_dist[years_range])])
    
    # LGA distribution (top 10)
    lga_dist = collections.Counter(r["LGA"] for r in data if r["LGA"] != "Not Reported")
    top_lgas = sorted(lga_dist.items(), key=lambda x: -x[1])[:10]
    lga_table = [["LGA", "Count"]]
    for lga, count in top_lgas:
        lga_table.append([str(lga), int(count)])
    
    # Farm size distribution (number of birds)
    farm_sizes = []
    for r in data:
        birds = r["Birds_Num"]
        if birds == 0:
            farm_sizes.append("Not Reported")
        elif birds < 50:
            farm_sizes.append("< 50 birds")
        elif birds < 200:
            farm_sizes.append("50-200 birds")
        elif birds < 500:
            farm_sizes.append("200-500 birds")
        else:
            farm_sizes.append("500+ birds")
    
    farm_size_dist = collections.Counter(farm_sizes)
    farm_size_table = [["Farm Size", "Count"]]
    order = ["< 50 birds", "50-200 birds", "200-500 birds", "500+ birds", "Not Reported"]
    for size_range in order:
        if size_range in farm_size_dist:
            farm_size_table.append([size_range, int(farm_size_dist[size_range])])
    
    return {
        "gender": gender_table,
        "age": age_table,
        "marital": marital_table,
        "education": education_table,
        "employment": employment_table,
        "cluster": cluster_table,
        "state": state_table,
        "marital_gender": marital_gender_table,
        "employment_stats": employment_stats_table,
        "years_business": years_table,
        "lga": lga_table,
        "farm_size": farm_size_table,
    }


def clear_sheet(service, sheet_name: str, spreadsheet_id: str):
    """Clear all content from a sheet."""
    print(f"Clearing {sheet_name}...")

    def _clear():
        return service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1:ZZ"
        ).execute()

    execute_with_retry(_clear, f"clear_sheet({sheet_name})")

    sheet_id = get_sheet_id(service, sheet_name, spreadsheet_id)

    def _get_charts():
        return service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(charts(chartId),properties(sheetId,title))"
        ).execute()

    spreadsheet = execute_with_retry(_get_charts, f"get_charts({sheet_name})")

    requests = []
    for sheet in spreadsheet.get("sheets", []):
        if sheet.get("properties", {}).get("sheetId") != sheet_id:
            continue
        for chart in sheet.get("charts", []):
            requests.append({"deleteEmbeddedObject": {"objectId": chart["chartId"]}})

    if requests:
        def _delete_charts():
            return service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests}
            ).execute()

        execute_with_retry(_delete_charts, f"delete_charts({sheet_name})")


def write_pivot_tables(service, aggregations: Dict, spreadsheet_id: str):
    """
    Write all pivot tables to Pivot_Data sheet in a structured layout.
    Uses size estimation and chunking to handle large datasets.
    """
    print("Writing pivot tables to Pivot_Data...")

    all_data = []

    row = 1
    tables_layout = {}

    tables = [
        ("gender", "Gender Distribution", aggregations["gender"]),
        ("age", "Age Range Distribution", aggregations["age"]),
        ("marital", "Marital Status Distribution", aggregations["marital"]),
        ("education", "Education Distribution", aggregations["education"]),
        ("employment", "Employment Status Distribution", aggregations["employment"]),
        ("cluster", "Top 10 Clusters", aggregations["cluster"]),
        ("state", "State Distribution", aggregations["state"]),
        ("lga", "Top 10 LGAs", aggregations["lga"]),
        ("years_business", "Years in Poultry Business", aggregations["years_business"]),
        ("farm_size", "Farm Size Distribution", aggregations["farm_size"]),
        ("marital_gender", "Marital Status by Gender", aggregations["marital_gender"]),
        ("employment_stats", "Employment Statistics", aggregations["employment_stats"]),
    ]

    for table_key, title, data in tables:
        all_data.append({
            "range": f"{PIVOT_SHEET}!A{row}",
            "values": [[f"--- {title} ---"]]
        })
        row += 1

        start_row = row
        all_data.append({
            "range": f"{PIVOT_SHEET}!A{row}",
            "values": data
        })

        # Store layout - charts will reference exact data rows only
        tables_layout[table_key] = {
            "start_row": start_row,
            "num_rows": len(data),
            "num_cols": len(data[0]) if data else 0,
            "header_row": start_row,  # Header row
            "data_start_row": start_row + 1,  # First data row after header
            "data_end_row": start_row + len(data) - 1  # Last actual data row
        }

        row += len(data) + 2

    # Estimate payload size and chunk if necessary
    payload = {"valueInputOption": "USER_ENTERED", "data": all_data}
    payload_size = estimate_request_size(payload)

    print(f"  Pivot tables payload size: {payload_size:.2f} MB")

    if payload_size > MAX_REQUEST_SIZE_MB:
        print(f"  ⚠ Payload exceeds {MAX_REQUEST_SIZE_MB}MB, splitting into chunks...")
        # Split into chunks
        chunk_size = len(all_data) // 2  # Simple split
        for i in range(0, len(all_data), chunk_size):
            chunk = all_data[i:i + chunk_size]
            chunk_payload = {"valueInputOption": "USER_ENTERED", "data": chunk}

            def _write_chunk():
                return service.spreadsheets().values().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body=chunk_payload
                ).execute()

            execute_with_retry(_write_chunk, f"write_pivot_chunk_{i // chunk_size + 1}")
            print(f"    Wrote chunk {i // chunk_size + 1} ({len(chunk)} tables)")
    else:
        def _write():
            return service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=payload
            ).execute()

        execute_with_retry(_write, "write_pivot_tables")

    print(f"  ✓ Written {len(tables)} pivot tables")
    print("\n  Pivot table layout:")
    for key, meta in tables_layout.items():
        print(f"    {key}: header_row={meta['header_row']}, data_rows={meta['data_start_row']}-{meta['data_end_row']}")

    return tables_layout


def format_pivot_sheet(service, sheet_id: int, spreadsheet_id: str):
    """Apply formatting to Pivot_Data sheet and keep it hidden."""
    print("Formatting Pivot_Data sheet...")

    requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1000,
                    "startColumnIndex": 0,
                    "endColumnIndex": 10,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": hex_to_rgb(HEX_COLORS["background"])
                    }
                },
                "fields": "userEnteredFormat.backgroundColor",
            }
        },
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "hidden": True
                },
                "fields": "hidden"
            }
        }
    ]

    def _format():
        return service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()

    execute_with_retry(_format, "format_pivot_sheet")


def create_charts(service, dashboard_sheet_id: int, pivot_sheet_id: int, layout: Dict, spreadsheet_id: str):
    """Create all charts in Dashboard sheet."""
    print("Creating charts in Dashboard...")

    requests = []
    
    def make_range(sheet_id: int, start_row: int, end_row: int, start_col: int, end_col: int):
        """Create a range object. Rows and columns are 1-indexed for input."""
        return {
            "sheetId": sheet_id,
            "startRowIndex": start_row - 1,  # Convert to 0-indexed
            "endRowIndex": end_row,  # endRowIndex is exclusive
            "startColumnIndex": start_col - 1,  # Convert to 0-indexed
            "endColumnIndex": end_col,  # endColumnIndex is exclusive
        }
    
    # Professional pixel-based positioning for clean tiling
    # Horizontal layout - spreading charts across the width
    chart_width = 380
    chart_height = 260
    charts_per_row = 4  # 4 charts per row for better horizontal spread
    
    # Pixel offsets for precise positioning
    start_x = 15  # Left margin
    start_y = 40  # Minimal gap after KPI table
    horizontal_gap = 20  # Gap between charts
    vertical_gap = 25  # Gap between rows
    
    current_chart = 0
    
    def add_chart(title: str, chart_spec: Dict, width: int = None, height: int = None):
        nonlocal current_chart
        
        w = width if width else chart_width
        h = height if height else chart_height
        
        # Calculate position based on chart number
        row_num = current_chart // charts_per_row
        col_num = current_chart % charts_per_row
        
        offset_x = start_x + col_num * (w + horizontal_gap)
        offset_y = start_y + row_num * (h + vertical_gap)
        
        requests.append({
            "addChart": {
                "chart": {
                    "spec": {
                        "title": title,
                        "titleTextFormat": {"bold": True, "fontSize": 12},
                        "fontName": "Arial",
                        "backgroundColorStyle": {"rgbColor": hex_to_rgb("FFFFFF")},
                        **chart_spec
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {
                                "sheetId": dashboard_sheet_id,
                                "rowIndex": 0,
                                "columnIndex": 0,
                            },
                            "offsetXPixels": offset_x,
                            "offsetYPixels": offset_y,
                            "widthPixels": w,
                            "heightPixels": h,
                        }
                    },
                }
            }
        })
        
        current_chart += 1
    
    if "gender" in layout:
        meta = layout["gender"]
        data_start_row = meta["data_start_row"]
        data_end_row = meta["data_end_row"]
        labels = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 1, 1)
        values = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 2, 2)
        
        add_chart("Gender Distribution", {
            "pieChart": {
                "domain": {"sourceRange": {"sources": [labels]}},
                "series": {"sourceRange": {"sources": [values]}},
                "legendPosition": "RIGHT_LEGEND",
                "pieHole": 0.4,
                "threeDimensional": False
            }
        })
    
    if "age" in layout:
        meta = layout["age"]
        # Use exact data rows only
        data_start_row = meta["data_start_row"]
        data_end_row = meta["data_end_row"]
        labels = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 1, 1)
        values = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 2, 2)

        add_chart("Age Range Distribution", {
            "basicChart": {
                "chartType": "COLUMN",
                "legendPosition": "BOTTOM_LEGEND",
                "domains": [{"domain": {"sourceRange": {"sources": [labels]}}}],
                "series": [{
                    "series": {"sourceRange": {"sources": [values]}},
                    "targetAxis": "LEFT_AXIS",
                    "color": hex_to_rgb(HEX_COLORS["accent_primary"])
                }],
                "axis": [{"position": "LEFT_AXIS", "title": "Count"}]
            }
        })
    
    if "marital_gender" in layout:
        meta = layout["marital_gender"]
        # Include header row for series names
        header_row = meta["start_row"]
        data_start_row = meta["data_start_row"]
        data_end_row = meta["data_end_row"]
        # Include header in range for proper legend labels
        labels = make_range(pivot_sheet_id, header_row, data_end_row + 1, 1, 1)
        female = make_range(pivot_sheet_id, header_row, data_end_row + 1, 2, 2)
        male = make_range(pivot_sheet_id, header_row, data_end_row + 1, 3, 3)
        
        add_chart("Marital Status by Gender", {
            "basicChart": {
                "chartType": "COLUMN",
                "legendPosition": "RIGHT_LEGEND",
                "stackedType": "STACKED",
                "headerCount": 1,
                "domains": [{"domain": {"sourceRange": {"sources": [labels]}}}],
                "series": [
                    {
                        "series": {"sourceRange": {"sources": [female]}},
                        "targetAxis": "LEFT_AXIS",
                        "color": hex_to_rgb(HEX_COLORS["accent_secondary"])
                    },
                    {
                        "series": {"sourceRange": {"sources": [male]}},
                        "targetAxis": "LEFT_AXIS",
                        "color": hex_to_rgb(HEX_COLORS["accent_primary"])
                    }
                ],
                "axis": [{"position": "LEFT_AXIS", "title": "Count"}]
            }
        })
    
    if "education" in layout:
        meta = layout["education"]
        data_start_row = meta["data_start_row"]
        data_end_row = meta["data_end_row"]
        labels = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 1, 1)
        values = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 2, 2)
        
        add_chart("Education Distribution", {
            "basicChart": {
                "chartType": "COLUMN",
                "legendPosition": "BOTTOM_LEGEND",
                "domains": [{"domain": {"sourceRange": {"sources": [labels]}}}],
                "series": [{
                    "series": {"sourceRange": {"sources": [values]}},
                    "targetAxis": "LEFT_AXIS",
                    "color": hex_to_rgb(HEX_COLORS["accent_tertiary"])
                }],
                "axis": [{"position": "LEFT_AXIS", "title": "Count"}]
            }
        })
    
    if "employment" in layout:
        meta = layout["employment"]
        data_start_row = meta["data_start_row"]
        data_end_row = meta["data_end_row"]
        labels = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 1, 1)
        values = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 2, 2)
        
        add_chart("Employment Status", {
            "basicChart": {
                "chartType": "BAR",
                "legendPosition": "RIGHT_LEGEND",
                "domains": [{"domain": {"sourceRange": {"sources": [labels]}}}],
                "series": [{
                    "series": {"sourceRange": {"sources": [values]}},
                    "targetAxis": "BOTTOM_AXIS",
                    "color": hex_to_rgb(HEX_COLORS["accent_primary"])
                }],
                "axis": [{"position": "BOTTOM_AXIS", "title": "Count"}]
            }
        })
    
    if "cluster" in layout:
        meta = layout["cluster"]
        data_start_row = meta["data_start_row"]
        data_end_row = meta["data_end_row"]
        labels = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 1, 1)
        values = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 2, 2)
        
        add_chart("Top 10 Clusters by Participants", {
            "basicChart": {
                "chartType": "COLUMN",
                "legendPosition": "BOTTOM_LEGEND",
                "domains": [{"domain": {"sourceRange": {"sources": [labels]}}}],
                "series": [{
                    "series": {"sourceRange": {"sources": [values]}},
                    "targetAxis": "LEFT_AXIS",
                    "color": hex_to_rgb(HEX_COLORS["accent_secondary"])
                }],
                "axis": [{"position": "LEFT_AXIS", "title": "Participants"}]
            }
        })
    
    if "employment_stats" in layout:
        meta = layout["employment_stats"]
        # Include header row for series names
        header_row = meta["start_row"]
        data_start_row = meta["data_start_row"]
        data_end_row = meta["data_end_row"]
        # Include header in range for proper legend labels
        labels = make_range(pivot_sheet_id, header_row, data_end_row + 1, 1, 1)
        birds = make_range(pivot_sheet_id, header_row, data_end_row + 1, 2, 2)
        household = make_range(pivot_sheet_id, header_row, data_end_row + 1, 3, 3)
        
        add_chart("Employment Statistics", {
            "basicChart": {
                "chartType": "COMBO",
                "legendPosition": "BOTTOM_LEGEND",
                "headerCount": 1,
                "domains": [{"domain": {"sourceRange": {"sources": [labels]}}}],
                "series": [
                    {
                        "series": {"sourceRange": {"sources": [birds]}},
                        "targetAxis": "LEFT_AXIS",
                        "type": "COLUMN",
                        "color": hex_to_rgb(HEX_COLORS["accent_primary"])
                    },
                    {
                        "series": {"sourceRange": {"sources": [household]}},
                        "targetAxis": "RIGHT_AXIS",
                        "type": "LINE",
                        "color": hex_to_rgb(HEX_COLORS["accent_secondary"])
                    }
                ],
                "axis": [
                    {"position": "LEFT_AXIS", "title": "Avg Birds"},
                    {"position": "RIGHT_AXIS", "title": "Avg Household"}
                ]
            }
        })
    
    if "marital" in layout:
        meta = layout["marital"]
        data_start_row = meta["data_start_row"]
        data_end_row = meta["data_end_row"]
        labels = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 1, 1)
        values = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 2, 2)
        
        add_chart("Marital Status Distribution", {
            "basicChart": {
                "chartType": "COLUMN",
                "legendPosition": "BOTTOM_LEGEND",
                "domains": [{"domain": {"sourceRange": {"sources": [labels]}}}],
                "series": [{
                    "series": {"sourceRange": {"sources": [values]}},
                    "targetAxis": "LEFT_AXIS",
                    "color": hex_to_rgb(HEX_COLORS["accent_secondary"])
                }],
                "axis": [{"position": "LEFT_AXIS", "title": "Count"}]
            }
        })
    
    if "state" in layout:
        meta = layout["state"]
        data_start_row = meta["data_start_row"]
        data_end_row = meta["data_end_row"]
        labels = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 1, 1)
        values = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 2, 2)
        
        add_chart("State Distribution", {
            "basicChart": {
                "chartType": "COLUMN",
                "legendPosition": "BOTTOM_LEGEND",
                "domains": [{"domain": {"sourceRange": {"sources": [labels]}}}],
                "series": [{
                    "series": {"sourceRange": {"sources": [values]}},
                    "targetAxis": "LEFT_AXIS",
                    "color": hex_to_rgb(HEX_COLORS["accent_tertiary"])
                }],
                "axis": [{"position": "LEFT_AXIS", "title": "Participants"}]
            }
        })
    
    if "lga" in layout:
        meta = layout["lga"]
        data_start_row = meta["data_start_row"]
        data_end_row = meta["data_end_row"]
        labels = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 1, 1)
        values = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 2, 2)
        
        add_chart("Top 10 LGAs", {
            "basicChart": {
                "chartType": "COLUMN",
                "legendPosition": "BOTTOM_LEGEND",
                "domains": [{"domain": {"sourceRange": {"sources": [labels]}}}],
                "series": [{
                    "series": {"sourceRange": {"sources": [values]}},
                    "targetAxis": "LEFT_AXIS",
                    "color": hex_to_rgb(HEX_COLORS["accent_primary"])
                }],
                "axis": [{"position": "LEFT_AXIS", "title": "Count"}]
            }
        })
    
    if "years_business" in layout:
        meta = layout["years_business"]
        data_start_row = meta["data_start_row"]
        data_end_row = meta["data_end_row"]
        labels = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 1, 1)
        values = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 2, 2)
        
        add_chart("Years in Poultry Business", {
            "basicChart": {
                "chartType": "COLUMN",
                "legendPosition": "BOTTOM_LEGEND",
                "domains": [{"domain": {"sourceRange": {"sources": [labels]}}}],
                "series": [{
                    "series": {"sourceRange": {"sources": [values]}},
                    "targetAxis": "LEFT_AXIS",
                    "color": hex_to_rgb(HEX_COLORS["accent_secondary"])
                }],
                "axis": [{"position": "LEFT_AXIS", "title": "Count"}]
            }
        })
    
    if "farm_size" in layout:
        meta = layout["farm_size"]
        data_start_row = meta["data_start_row"]
        data_end_row = meta["data_end_row"]
        labels = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 1, 1)
        values = make_range(pivot_sheet_id, data_start_row, data_end_row + 1, 2, 2)
        
        add_chart("Farm Size Distribution", {
            "basicChart": {
                "chartType": "COLUMN",
                "legendPosition": "BOTTOM_LEGEND",
                "domains": [{"domain": {"sourceRange": {"sources": [labels]}}}],
                "series": [{
                    "series": {"sourceRange": {"sources": [values]}},
                    "targetAxis": "LEFT_AXIS",
                    "color": hex_to_rgb(HEX_COLORS["accent_tertiary"])
                }],
                "axis": [{"position": "LEFT_AXIS", "title": "Count"}]
            }
        })
    
    if requests:
        # Estimate size and check if chunking is needed
        payload = {"requests": requests}
        payload_size = estimate_request_size(payload)
        print(f"  Charts payload size: {payload_size:.2f} MB")

        if payload_size > MAX_REQUEST_SIZE_MB:
            print(f"  ⚠ Payload exceeds {MAX_REQUEST_SIZE_MB}MB, splitting charts into batches...")
            # Split into batches of 6 charts each
            batch_size = 6
            for i in range(0, len(requests), batch_size):
                batch = requests[i:i + batch_size]
                batch_payload = {"requests": batch}

                def _create_batch():
                    return service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body=batch_payload
                    ).execute()

                execute_with_retry(_create_batch, f"create_charts_batch_{i // batch_size + 1}")
                print(f"    Created batch {i // batch_size + 1} ({len(batch)} charts)")
        else:
            def _create():
                return service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body=payload
                ).execute()

            execute_with_retry(_create, "create_charts")

        print(f"  ✓ Created {len(requests)} charts")


def add_dashboard_header(service, data: List[Dict], sheet_id: int, spreadsheet_id: str):
    """Add professional header with title and KPIs."""
    print("Adding dashboard header...")

    total_participants = len(data)
    distinct_clusters = len(set(r["Cluster"] for r in data))
    age_18_35 = sum(1 for r in data if r["Age_Range"] == "18-35")
    age_18_35_pct = (age_18_35 / total_participants * 100) if total_participants else 0
    female_count = sum(1 for r in data if r["Sex"] == "Female")
    female_pct = (female_count / total_participants * 100) if total_participants else 0
    avg_household = mean([r["Household_Num"] for r in data if r["Household_Num"] > 0]) if any(r["Household_Num"] > 0 for r in data) else 0

    # Get current time in WAT (UTC+1)
    wat_tz = timezone(timedelta(hours=1))
    current_time = datetime.now(wat_tz)
    timestamp = current_time.strftime("%B %d, %Y at %I:%M %p WAT")

    # GitHub raw URLs for sponsor logos
    pullus_logo_url = "https://raw.githubusercontent.com/ifyjakande/pos-derp-dashboard-automation/master/pullus-final-logo-3.svg"
    hfr_logo_url = "https://raw.githubusercontent.com/ifyjakande/pos-derp-dashboard-automation/master/hfr-logo-navy.svg"

    header_data = [
        # Row 1: Logos side by side (A1 and B1)
        {"range": f"{DASHBOARD_SHEET}!A1", "values": [[f'=IMAGE("{pullus_logo_url}", 1)']]},
        {"range": f"{DASHBOARD_SHEET}!B1", "values": [[f'=IMAGE("{hfr_logo_url}", 1)']]},
        # Row 1: Main title centered (C1) - closer to logos
        {"range": f"{DASHBOARD_SHEET}!C1", "values": [["POS-DERP 3.0 BENEFICIARIES DASHBOARD"]]},
        # Row 2: Subtitle centered (A2)
        {"range": f"{DASHBOARD_SHEET}!A2", "values": [[f"Program Overview & Demographics Analysis | Updated: {timestamp}"]]},
        # KPI headers and values
        {"range": f"{DASHBOARD_SHEET}!B3:F3", "values": [["Total Participants", "Distinct Clusters", "Age 18-35 (%)", "Female (%)", "Avg Household Size"]]},
        {"range": f"{DASHBOARD_SHEET}!B4:F4", "values": [[total_participants, distinct_clusters, age_18_35_pct / 100, female_pct / 100, f"{avg_household:.1f}"]]},
    ]

    def _write_header():
        return service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "USER_ENTERED", "data": header_data}
        ).execute()

    execute_with_retry(_write_header, "write_dashboard_header")

    requests = [
        # First unmerge any existing cells in the header area to avoid conflicts
        {
            "unmergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 4, "startColumnIndex": 0, "endColumnIndex": 21}
            }
        },
        # Merge title cells (C1:M1) for centered title over actual dashboard content
        {
            "mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 2, "endColumnIndex": 13},
                "mergeType": "MERGE_ALL"
            }
        },
        # Merge subtitle row (A2:M2) to match dashboard content width
        {
            "mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 13},
                "mergeType": "MERGE_ALL"
            }
        },
        # White background for entire header area (rows 1-2)
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 21},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": hex_to_rgb("FFFFFF"),
                        "verticalAlignment": "MIDDLE"
                    }
                },
                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.verticalAlignment"
            }
        },
        # Format main title (C1:M1) - centered, bold, blue accent color
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 2, "endColumnIndex": 13},
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"bold": True, "fontSize": 34, "foregroundColor": hex_to_rgb(HEX_COLORS["accent_primary"]), "fontFamily": "Arial"},
                        "horizontalAlignment": "CENTER"
                    }
                },
                "fields": "userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment"
            }
        },
        # Format subtitle (A2:M2) - centered, bold, italic, gray text on white background
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 13},
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"bold": True, "italic": True, "fontSize": 11, "foregroundColor": hex_to_rgb("666666"), "fontFamily": "Arial"},
                        "horizontalAlignment": "CENTER"
                    }
                },
                "fields": "userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 3, "startColumnIndex": 1, "endColumnIndex": 6},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": hex_to_rgb(HEX_COLORS["kpi_fill"]),
                        "textFormat": {"bold": True, "fontSize": 11, "fontFamily": "Arial"},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "borders": {
                            "top": {"style": "SOLID", "width": 1, "color": hex_to_rgb("CCCCCC")},
                            "bottom": {"style": "SOLID", "width": 1, "color": hex_to_rgb("CCCCCC")},
                            "left": {"style": "SOLID", "width": 1, "color": hex_to_rgb("CCCCCC")},
                            "right": {"style": "SOLID", "width": 1, "color": hex_to_rgb("CCCCCC")}
                        }
                    }
                },
                "fields": "userEnteredFormat"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": 4, "startColumnIndex": 1, "endColumnIndex": 6},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": hex_to_rgb("FFFFFF"),
                        "textFormat": {"bold": True, "fontSize": 14, "fontFamily": "Arial", "foregroundColor": hex_to_rgb(HEX_COLORS["accent_primary"])},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "borders": {
                            "top": {"style": "SOLID", "width": 1, "color": hex_to_rgb("CCCCCC")},
                            "bottom": {"style": "SOLID", "width": 2, "color": hex_to_rgb(HEX_COLORS["accent_primary"])},
                            "left": {"style": "SOLID", "width": 1, "color": hex_to_rgb("CCCCCC")},
                            "right": {"style": "SOLID", "width": 1, "color": hex_to_rgb("CCCCCC")}
                        }
                    }
                },
                "fields": "userEnteredFormat"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": 4, "startColumnIndex": 3, "endColumnIndex": 5},
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {
                            "type": "PERCENT",
                            "pattern": "0.0%"
                        }
                    }
                },
                "fields": "userEnteredFormat.numberFormat"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 90},
                "fields": "pixelSize"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 1, "endIndex": 2},
                "properties": {"pixelSize": 25},
                "fields": "pixelSize"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 2, "endIndex": 4},
                "properties": {"pixelSize": 35},
                "fields": "pixelSize"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 2},
                "properties": {"pixelSize": 100},
                "fields": "pixelSize"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 6},
                "properties": {"pixelSize": 180},
                "fields": "pixelSize"
            }
        }
    ]

    def _format_header():
        return service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()

    execute_with_retry(_format_header, "format_dashboard_header")


def format_dashboard_sheet(service, sheet_id: int, spreadsheet_id: str):
    """Format the Dashboard sheet."""
    print("Formatting Dashboard sheet...")

    requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 200,
                    "startColumnIndex": 0,
                    "endColumnIndex": 26,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": hex_to_rgb("FFFFFF")
                    }
                },
                "fields": "userEnteredFormat.backgroundColor",
            }
        },
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "frozenRowCount": 3,
                        "hideGridlines": True
                    }
                },
                "fields": "gridProperties.frozenRowCount,gridProperties.hideGridlines"
            }
        }
    ]

    def _format():
        return service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()

    execute_with_retry(_format, "format_dashboard_sheet")


def populate_spreadsheet(service, spreadsheet_id: str, spreadsheet_name: str, data: List[Dict], aggregations: Dict):
    """Populate a single spreadsheet with dashboard and pivot data."""
    print(f"\n{'='*60}")
    print(f"Populating {spreadsheet_name}...")
    print(f"{'='*60}")

    # Create sheets if they don't exist
    dashboard_id = create_sheet_if_not_exists(service, spreadsheet_id, DASHBOARD_SHEET)
    pivot_id = create_sheet_if_not_exists(service, spreadsheet_id, PIVOT_SHEET)

    # Clear existing content
    clear_sheet(service, PIVOT_SHEET, spreadsheet_id)
    clear_sheet(service, DASHBOARD_SHEET, spreadsheet_id)

    # Write pivot tables and get layout
    layout = write_pivot_tables(service, aggregations, spreadsheet_id)
    format_pivot_sheet(service, pivot_id, spreadsheet_id)

    # Create dashboard with header and charts
    format_dashboard_sheet(service, dashboard_id, spreadsheet_id)
    add_dashboard_header(service, data, dashboard_id, spreadsheet_id)
    create_charts(service, dashboard_id, pivot_id, layout, spreadsheet_id)

    print(f"✓ {spreadsheet_name} updated successfully!")


def main():
    global api_call_count
    start_time = time.time()

    print("="*60)
    print("POS-DERP 3.0 Dashboard Builder")
    print("="*60)
    print(f"Configuration:")
    print(f"  • Max retries per call: {MAX_RETRIES}")
    print(f"  • Max request size: {MAX_REQUEST_SIZE_MB}MB")
    print(f"  • Chunk size for data fetch: {CHUNK_SIZE:,} rows")
    print("="*60)

    try:
        service = get_service()
        print("✓ Connected to Google Sheets API")

        # Fetch data from Master Copy spreadsheet
        print("\nFetching data from Master Copy (Data sheet)...")
        header, rows = fetch_data(service)
        print(f"✓ Loaded {len(rows)} rows")

        # Normalize and aggregate data
        print("\nNormalizing and processing data...")
        data = normalize_data(header, rows)
        print(f"✓ Processed {len(data)} records")

        print("\nCreating pivot tables...")
        aggregations = aggregate_data(data)
        print(f"✓ Created {len(aggregations)} pivot tables")

        # Populate Master Copy spreadsheet
        populate_spreadsheet(service, MASTER_COPY_SPREADSHEET_ID, "Master Copy Spreadsheet", data, aggregations)

        # Populate Heifer spreadsheet
        populate_spreadsheet(service, HEIFER_SPREADSHEET_ID, "Heifer Spreadsheet", data, aggregations)

        elapsed_time = time.time() - start_time

        print("\n" + "="*60)
        print("✓ All spreadsheets updated successfully!")
        print("="*60)

        print("\n📈 Performance Metrics:")
        print(f"  • Total records processed: {len(data)}")
        print(f"  • Total API calls: {api_call_count}")
        print(f"  • Total execution time: {elapsed_time:.2f} seconds")

        print("\n✅ Dashboard update completed successfully!")

    except HttpError as e:
        print(f"\n✗ Google API Error: {e}")
        print(f"  Status: {e.resp.status}")
        print(f"  Reason: {e.resp.get('error', {}).get('message', 'Unknown error')}")
        import traceback
        traceback.print_exc()
        return 1
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
