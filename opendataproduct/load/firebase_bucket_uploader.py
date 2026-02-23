import os

from firebase_admin import credentials, initialize_app, storage, get_app

from opendataproduct.tracking_decorator import TrackingDecorator


@TrackingDecorator.track_time
def upload_to_firebase_bucket(
    data_path,
    firebase_credentials_file,
    endings=None,
    storage_bucket="open-lifeworlds.appspot.com",
    clean=False,
    quiet=False,
):
    try:
        get_app()
    except ValueError:
        cred = credentials.Certificate(firebase_credentials_file)
        initialize_app(cred, {"storageBucket": storage_bucket})

    # Iterate over files
    for subdir, dirs, files in sorted(os.walk(data_path)):
        for file_name in sorted(files):
            _, file_extension = os.path.splitext(file_name)
            if endings and file_extension not in endings:
                continue

            bucket = storage.bucket()
            blob_name = subdir.replace(f"{data_path}/", "") + "/" + file_name
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(
                os.path.join(subdir, file_name),
                content_type=build_mime_type(file_name),
            )

            not quiet and print(f"✓ Upload {file_name} to {blob_name}")


def build_mime_type(file_name):
    _, file_extension = os.path.splitext(file_name)

    if file_extension == ".geojson":
        return "application/json"
    else:
        return None
