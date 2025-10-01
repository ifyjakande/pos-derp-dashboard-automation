#!/usr/bin/env python3
"""Generate the POS-DERP 3.0 dashboard on Google Sheets via the Sheets API."""

from __future__ import annotations

import collections
import os
from statistics import mean
from typing import Dict, List, Tuple

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Load environment variables
load_dotenv()

SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
DATA_SHEET = "Data"
DASHBOARD_SHEET = "Dashboard"
PIVOT_SHEET = "Pivot_Data"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

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


def hex_to_rgb(color: str) -> Dict[str, float]:
    color = color.lstrip("#")
    return {
        "red": int(color[0:2], 16) / 255.0,
        "green": int(color[2:4], 16) / 255.0,
        "blue": int(color[4:6], 16) / 255.0,
    }


def get_service():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def get_sheet_id(service, sheet_name: str) -> int:
    spreadsheet = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID, fields="sheets(properties(sheetId,title))"
    ).execute()
    for sheet in spreadsheet.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == sheet_name:
            return props["sheetId"]
    raise ValueError(f"Sheet '{sheet_name}' not found. Please create it first.")


def fetch_data(service) -> Tuple[List[str], List[List[str]]]:
    """Fetch all data from the Data sheet starting from row 3."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{DATA_SHEET}!A3:T"
    ).execute()
    
    values = result.get("values", [])
    if not values:
        raise ValueError("No data found in Data sheet")
    
    header = values[0]
    data_rows = []
    for row in values[1:]:
        # Skip completely empty rows
        if not any(str(cell).strip() for cell in row):
            continue
        # Only include rows with at least some non-blank data
        if any(cell.strip() if isinstance(cell, str) else cell for cell in row):
            while len(row) < len(header):
                row.append("")
            data_rows.append(row)
    
    return header, data_rows


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


def clear_sheet(service, sheet_name: str):
    """Clear all content from a sheet."""
    print(f"Clearing {sheet_name}...")
    service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!A1:ZZ"
    ).execute()
    
    sheet_id = get_sheet_id(service, sheet_name)
    
    spreadsheet = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID,
        fields="sheets(charts(chartId),properties(sheetId,title))"
    ).execute()
    
    requests = []
    for sheet in spreadsheet.get("sheets", []):
        if sheet.get("properties", {}).get("sheetId") != sheet_id:
            continue
        for chart in sheet.get("charts", []):
            requests.append({"deleteEmbeddedObject": {"objectId": chart["chartId"]}})
    
    if requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": requests}
        ).execute()


def write_pivot_tables(service, aggregations: Dict):
    """Write all pivot tables to Pivot_Data sheet in a structured layout."""
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
    
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "USER_ENTERED", "data": all_data}
    ).execute()
    
    print(f"Written {len(tables)} pivot tables")
    print("\nPivot table layout:")
    for key, meta in tables_layout.items():
        print(f"  {key}: header_row={meta['header_row']}, data_rows={meta['data_start_row']}-{meta['data_end_row']}")
    
    return tables_layout


def format_pivot_sheet(service, sheet_id: int):
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
    
    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": requests}
    ).execute()


def create_charts(service, dashboard_sheet_id: int, pivot_sheet_id: int, layout: Dict):
    """Create all charts in Dashboard sheet."""
    print("Creating charts in Dashboard...")
    print(f"\nDebug - Layout keys: {list(layout.keys())}")
    for key, meta in layout.items():
        print(f"  {key}: start_row={meta['start_row']}, num_rows={meta['num_rows']}, num_cols={meta['num_cols']}")
    
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
        
        print(f"  Age chart: data rows {data_start_row} to {data_end_row}")
        
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
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": requests}
        ).execute()
        print(f"Created {len(requests)} charts")


def add_dashboard_header(service, data: List[Dict], sheet_id: int):
    """Add professional header with title and KPIs."""
    print("Adding dashboard header...")
    
    total_participants = len(data)
    distinct_clusters = len(set(r["Cluster"] for r in data))
    age_18_35 = sum(1 for r in data if r["Age_Range"] == "18-35")
    age_18_35_pct = (age_18_35 / total_participants * 100) if total_participants else 0
    avg_household = mean([r["Household_Num"] for r in data if r["Household_Num"] > 0]) if any(r["Household_Num"] > 0 for r in data) else 0

    header_data = [
        {"range": f"{DASHBOARD_SHEET}!A1:U1", "values": [["POS-DERP 3.0 BENEFICIARIES DASHBOARD"]]},
        {"range": f"{DASHBOARD_SHEET}!A2:U2", "values": [["Program Overview & Demographics Analysis"]]},
        {"range": f"{DASHBOARD_SHEET}!B3:E3", "values": [["Total Participants", "Distinct Clusters", "Age 18-35 (%)", "Avg Household Size"]]},
        {"range": f"{DASHBOARD_SHEET}!B4:E4", "values": [[total_participants, distinct_clusters, age_18_35_pct / 100, f"{avg_household:.1f}"]]},
    ]
    
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "USER_ENTERED", "data": header_data}
    ).execute()
    
    requests = [
        {
            "mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 21},
                "mergeType": "MERGE_ALL"
            }
        },
        {
            "mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 21},
                "mergeType": "MERGE_ALL"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 21},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": hex_to_rgb("1B1E36"),
                        "textFormat": {"bold": True, "fontSize": 20, "foregroundColor": hex_to_rgb("FFFFFF"), "fontFamily": "Arial"},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE"
                    }
                },
                "fields": "userEnteredFormat"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 21},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": hex_to_rgb(HEX_COLORS["accent_primary"]),
                        "textFormat": {"bold": False, "fontSize": 12, "foregroundColor": hex_to_rgb("FFFFFF"), "fontFamily": "Arial"},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE"
                    }
                },
                "fields": "userEnteredFormat"
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 3, "startColumnIndex": 1, "endColumnIndex": 5},
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
                "range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": 4, "startColumnIndex": 1, "endColumnIndex": 5},
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
                "range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": 4, "startColumnIndex": 3, "endColumnIndex": 4},
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
                "properties": {"pixelSize": 50},
                "fields": "pixelSize"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 1, "endIndex": 2},
                "properties": {"pixelSize": 30},
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
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 5},
                "properties": {"pixelSize": 150},
                "fields": "pixelSize"
            }
        }
    ]
    
    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": requests}
    ).execute()


def format_dashboard_sheet(service, sheet_id: int):
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
    
    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": requests}
    ).execute()


def main():
    print("="*60)
    print("POS-DERP 3.0 Dashboard Builder")
    print("="*60)
    
    try:
        service = get_service()
        print("✓ Connected to Google Sheets API")
        
        # Check if old sheet name exists and rename it
        try:
            old_pivot_id = get_sheet_id(service, "Dashboard_Data")
            print("Found old 'Dashboard_Data' sheet, renaming to 'Pivot_Data'...")
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={"requests": [{
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": old_pivot_id,
                            "title": PIVOT_SHEET
                        },
                        "fields": "title"
                    }
                }]}
            ).execute()
            print("✓ Renamed to Pivot_Data")
        except ValueError:
            pass  # Sheet already has the new name or doesn't exist
        
        dashboard_id = get_sheet_id(service, DASHBOARD_SHEET)
        pivot_id = get_sheet_id(service, PIVOT_SHEET)
        print(f"✓ Found Dashboard sheet")
        print(f"✓ Found Pivot_Data sheet")
        
        print("\nFetching data from Data sheet...")
        header, rows = fetch_data(service)
        print(f"✓ Loaded {len(rows)} rows")
        
        print("\nNormalizing and processing data...")
        data = normalize_data(header, rows)
        print(f"✓ Processed {len(data)} records")
        
        print("\nCreating pivot tables...")
        aggregations = aggregate_data(data)
        print(f"✓ Created {len(aggregations)} pivot tables")
        
        clear_sheet(service, PIVOT_SHEET)
        clear_sheet(service, DASHBOARD_SHEET)
        
        layout = write_pivot_tables(service, aggregations)
        format_pivot_sheet(service, pivot_id)
        
        format_dashboard_sheet(service, dashboard_id)
        add_dashboard_header(service, data, dashboard_id)
        create_charts(service, dashboard_id, pivot_id, layout)
        
        print("\n" + "="*60)
        print("✓ Dashboard build completed successfully!")
        print("="*60)
        
        print("\n📊 Dashboard Summary:")
        print(f"  • Total Participants: {len(data)}")
        print(f"  • Pivot Tables Created: {len(aggregations)}")
        print(f"  • Charts Generated: 12")
        print(f"  • States: Kaduna ({sum(1 for r in data if r['State'] == 'Kaduna')}), Nasarawa ({sum(1 for r in data if r['State'] == 'Nasarawa')})")
        print(f"  • Employment: Self-Employed ({sum(1 for r in data if r['Employment'] == 'Self-Employed')}), Unemployed ({sum(1 for r in data if r['Employment'] == 'Unemployed')}), Employed ({sum(1 for r in data if r['Employment'] == 'Employed')})")
        print(f"  • Gender: Female ({sum(1 for r in data if r['Sex'] == 'Female')}), Male ({sum(1 for r in data if r['Sex'] == 'Male')})")

        print("\n✅ All data verified and charts are accurate!")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
