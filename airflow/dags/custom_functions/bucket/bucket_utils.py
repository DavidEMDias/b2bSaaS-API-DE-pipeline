import pandas as pd
from google.cloud import storage
from io import BytesIO
import pyarrow  # explicit dependency check

def upload_df_to_gcs_parquet(
    df: pd.DataFrame,
    bucket_name: str,
    file_name: str
) -> None:
    
    
    # Ensure .parquet extension
    if not file_name.lower().endswith(".parquet"):
        file_name = f"{file_name}.parquet"

    buffer = BytesIO()

    df.to_parquet(
        buffer,
        engine="pyarrow",
        index=False
    )
    buffer.seek(0)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    blob.upload_from_file(
        buffer,
        content_type="application/octet-stream"
    )

    print(f"Uploaded Parquet to: gs://{bucket_name}/{file_name}")
