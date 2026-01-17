import pandas as pd
import os, time, requests, logging
from datetime import datetime, timedelta, timezone
from dateutil import parser
import logging
from custom_functions.bucket.bucket_utils import upload_df_to_gcs_parquet
from airflow.models import Variable


API_BASE = os.environ.get("API_BASE_URL", "http://mock-api:8000")
API_KEY = os.environ["API_KEY"]
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

def get_watermark(name, default_iso): # name of the variable in airflow (ex: last_sync_time)
    return Variable.get(name, default_var=default_iso) # Example last_sync = get_watermark("last_sync_time", "2025-01-01T00:00:00Z"); saved value. if its the first execution uses the default

def set_watermark(name, iso): # save or update airflow variable
    Variable.set(name, iso)

def fetch_paged(endpoint, params, max_retries=5): #Endpoint (Customers, Sessions), Params(filters, dates), Max retries number if request fails
    """Iterate over pages of API results, returning data gradually."""

    page = 1 # Start in first page
    while True:
        p = params.copy() # Create a copy of original parameters to not overwrite external parameters 
        p.update({"page": page, "page_size": 500}) # Add Pagination Parameters (updates p dictionary)
        r = None

        for attempt in range(max_retries): # Tries to do the request until max retries 
            try:
                r = requests.get(f"{API_BASE}/{endpoint}",
                    headers=HEADERS,
                    params=p,
                    timeout=30,
                )
            except requests.exceptions.RequestException as e: # If any error occurs log the warning, wait 2**attemp number seconds and continue trying
                logging.warning("Request to %s failed (%s)", endpoint, e)
                time.sleep(2 ** attempt)
                continue

            #if r.status_code == 429:
                #retry = r.json().get("retry_after", 10)
                #time.sleep(int(retry))
                #continue
            #if r.status_code >= 500:
                #time.sleep(2 ** attempt)
                #continue

            r.raise_for_status() # HTTP >= 400 raises an exception
            data = r.json() # Convert JSON into Dictionary
            yield data # Returns each page of data - without the need to load everything into memory
            break

        else:
            logging.error(
                "Failed to fetch %s with params %s after %s attempts (last status %s)",
                endpoint,
                p,
                max_retries,
                getattr(r, "status_code", "unknown"),
            )
            raise RuntimeError(
                f"Failed to fetch {endpoint} after {max_retries} attempts",
            )
        


        if data.get("next_page"): # If there is a next page, update page and while loop continues. Null in JSON turns into None = False
            page = data["next_page"]
        else:
            return

def extract_and_upload_table(
    endpoint: str,
    ts_field: str,
    wm_name: str,
    bucket_name: str,
    gcs_prefix: str,
) -> str:
    """
    Extract data from API and upload to GCS as Parquet.
    """

    iso_default = (datetime.utcnow() - timedelta(days=365)).isoformat() + "Z"
    watermark = get_watermark(wm_name, iso_default)

    max_seen = parser.isoparse(watermark)
    updated_since_param = max_seen.isoformat().replace("+00:00", "Z")

    all_rows = []

    for page in fetch_paged(endpoint, {"updated_since": updated_since_param}):
        data = page.get("data") or []
        if not data:
            continue

        for d in data:
            last_seen = max(
                parser.isoparse(d["updated_at"]),
                parser.isoparse(d.get("last_seen_at") or datetime.now(timezone.utc).isoformat())
            )
            d["last_seen_at"] = last_seen.isoformat()  # convert to string # update last_seen_at for each record in the response/page, adding it to the data (dictionary)
        
        # Update max_seen  
        for d in data: # Loop through each dictionary 
            ts = parser.isoparse(d.get(ts_field)) # Get Field to use for watermark (usually updated_at) Converts text to datetime python.
            if ts > max_seen: # max_seen = old watermark, everytime something more recent appears it updates max_seen.
                max_seen = ts # max_seen will be equal to the biggest updated_at of all processed records.

        all_rows.extend(data)

    if not all_rows:
        logging.info("No new data for %s", endpoint)
        return "No rows extracted"

    # Convert to DataFrame
    df = pd.DataFrame(all_rows)

    # Build deterministic GCS path
    ingestion_date = datetime.utcnow().date().isoformat()  # 2026-01-14

    df["ingestion_ts"] = datetime.utcnow().isoformat()
    df["ingestion_date"] = ingestion_date
    df["source_endpoint"] = endpoint

    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    file_name = (
        f"{gcs_prefix}/{endpoint}/"
        f"ingestion_date={ingestion_date}/"
        f"part-{run_id}.parquet"
    )

    upload_df_to_gcs_parquet(
        df=df,
        bucket_name=bucket_name,
        file_name=file_name,
    )

    # Advance watermark AFTER successful upload
    max_seen_with_delta = max_seen + timedelta(microseconds=1) # Time delta added because of the nature of the API (which has >=)
    set_watermark(wm_name, max_seen_with_delta.isoformat())

    return f"{len(df)} rows uploaded to gs://{bucket_name}/{file_name}"
