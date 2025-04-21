import os
import zipfile
import urllib.parse

import requests

from opendataproduct.config.data_product_manifest_loader import DataProductManifest
from opendataproduct.tracking_decorator import TrackingDecorator


@TrackingDecorator.track_time
def extract_data(
    data_product_manifest: DataProductManifest, results_path, clean=False, quiet=False
):
    # Make results path
    os.makedirs(os.path.join(results_path), exist_ok=True)

    # Iterate over input ports
    if data_product_manifest.input_ports:
        for input_port in data_product_manifest.input_ports:
            # Make results path
            os.makedirs(os.path.join(results_path, input_port.id), exist_ok=True)

            # Iterate over files
            for url in input_port.files:
                # Determine file path
                file_name = urllib.parse.unquote(url.rsplit("/", 1)[-1])
                file_path = os.path.join(results_path, input_port.id, file_name)

                # Download file
                download_file(
                    file_path=file_path,
                    file_name=file_name,
                    url=url,
                    clean=clean,
                    quiet=quiet,
                )

                # Unzip file
                if file_name.endswith(".zip"):
                    unzip_file(file_path=file_path, file_name=file_name, quiet=quiet)


def download_file(file_path, file_name, url, clean, quiet):
    # Check if result needs to be generated
    if clean or not os.path.exists(file_path):
        try:
            data = requests.get(url)
            if str(data.status_code).startswith("2"):
                with open(file_path, "wb") as file:
                    file.write(data.content)
                not quiet and print(f"✓ Download {file_name}")
            else:
                not quiet and print(f"✗️ Error: {str(data.status_code)}, url {url}")
        except Exception as e:
            print(f"✗️ Exception: {str(e)}, url {url}")

    else:
        not quiet and print(f"✓ Already exists {file_name}")


def unzip_file(file_path, file_name, quiet):
    try:
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            for member in zip_ref.namelist():
                if not member.endswith("/"):
                    filename = os.path.basename(member)
                    destination_path = os.path.join(
                        os.path.dirname(file_path), filename
                    )

                    with zip_ref.open(member) as source, open(
                        destination_path, "wb"
                    ) as target:
                        target.write(source.read())

            not quiet and print(f"✓ Unzip {file_name}")
    except Exception as e:
        print(f"✗️ Exception: {str(e)}, file {file_name}")
