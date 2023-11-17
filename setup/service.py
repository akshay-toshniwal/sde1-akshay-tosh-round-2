from google.cloud import storage

def upload_to_gcs(file_path, bucket_name, destination_blob_name):
    """
    Uploads a file to Google Cloud Storage.

    Args:
        file_path (str): The path to the file to be uploaded.
        bucket_name (str): The name of the Google Cloud Storage bucket.
        destination_blob_name (str): The name of the destination blob in the bucket.

    Returns:
        str: A message indicating the upload status.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    try:
        blob.upload_from_filename(file_path)
        return True
    except Exception as e:
        return f"Error uploading file: {str(e)}"
