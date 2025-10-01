#!/usr/bin/env python3
"""
Check if source data sheet has been modified since last update.
Uses content hash of the source sheet to detect changes.
Only runs in CI environment (GitHub Actions).
"""

import json
import os
import sys
import hashlib
from google.oauth2.service_account import Credentials
import gspread


def get_credentials():
    """Get Google API credentials from environment variable."""
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

    if not service_account_json:
        print("❌ GOOGLE_SERVICE_ACCOUNT_JSON environment variable not set")
        sys.exit(1)

    try:
        # Parse JSON from environment variable (GitHub Actions)
        credentials_dict = json.loads(service_account_json)
        credentials = Credentials.from_service_account_info(
            credentials_dict,
            scopes=[
                'https://www.googleapis.com/auth/spreadsheets.readonly',
            ]
        )
        return credentials

    except json.JSONDecodeError as e:
        print(f"❌ Error parsing service account JSON: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error creating credentials: {e}")
        sys.exit(1)


def get_source_data_hash(spreadsheet_id, credentials, source_sheet_name):
    """Get content hash of the source data sheet."""
    try:
        # Use gspread for easier sheet access
        gc = gspread.authorize(credentials)
        spreadsheet = gc.open_by_key(spreadsheet_id)

        # Get the source sheet
        source_sheet = spreadsheet.worksheet(source_sheet_name)

        # Get all values from the source sheet
        all_values = source_sheet.get_all_values()

        # Create hash of the content
        content_str = str(all_values)
        content_hash = hashlib.md5(content_str.encode('utf-8')).hexdigest()

        # Count non-empty rows
        data_rows = len([row for row in all_values if any(cell.strip() for cell in row)])

        print(f"📊 Source sheet: {source_sheet_name}")
        print(f"📝 Rows with data: {data_rows}")
        print(f"🔗 Content hash: {content_hash}")

        return content_hash

    except gspread.WorksheetNotFound:
        print(f"❌ Source sheet '{source_sheet_name}' not found")
        print("Available sheets:")
        try:
            gc = gspread.authorize(credentials)
            spreadsheet = gc.open_by_key(spreadsheet_id)
            for ws in spreadsheet.worksheets():
                print(f"  - {ws.title}")
        except Exception:
            pass
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error getting source data hash: {e}")
        sys.exit(1)


def load_last_hash():
    """Load the last processed content hash from file."""
    hash_file = 'last_source_hash.json'
    try:
        if os.path.exists(hash_file):
            with open(hash_file, 'r') as f:
                data = json.load(f)
                return data.get('content_hash')
        return None
    except Exception as e:
        print(f"⚠️  Warning: Could not load last hash: {e}")
        return None


def save_hash(content_hash):
    """Save the current content hash to file."""
    hash_file = 'last_source_hash.json'
    try:
        data = {
            'content_hash': content_hash,
        }
        with open(hash_file, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"💾 Saved new content hash")
    except Exception as e:
        print(f"❌ Error saving hash: {e}")
        sys.exit(1)


def main():
    """Main function to check for changes in source data."""
    try:
        # Get environment variables
        spreadsheet_id = os.getenv('MASTER_COPY_SPREADSHEET_ID')
        if not spreadsheet_id:
            print("❌ MASTER_COPY_SPREADSHEET_ID environment variable not set")
            sys.exit(1)

        # Get source sheet name
        source_sheet_name = os.getenv('SOURCE_SHEET_NAME', 'Data')

        print(f"🔍 Checking for changes in source sheet '{source_sheet_name}'...")

        # Get credentials
        credentials = get_credentials()

        # Get current source data hash
        current_hash = get_source_data_hash(spreadsheet_id, credentials, source_sheet_name)

        # Load last processed hash
        last_hash = load_last_hash()
        print(f"📅 Last processed hash: {last_hash or 'Never'}")

        # Compare hashes
        if current_hash != last_hash:
            print("✅ Source data changes detected! Update needed.")
            save_hash(current_hash)
            print("NEEDS_UPDATE=true")
            sys.exit(0)
        else:
            print("⏭️  No changes in source data detected. Skipping update.")
            print("NEEDS_UPDATE=false")
            sys.exit(0)

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
