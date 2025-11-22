import json
import os

import pandas as pd

from opendataproduct.tracking_decorator import TrackingDecorator


@TrackingDecorator.track_time
def convert_data_to_csv(
    source_path, results_path, year, month, clean=False, quiet=False
):
    # Iterate over files
    for subdir, dirs, files in sorted(os.walk(source_path)):
        if subdir.endswith(f"points-of-interest-{year}-{month}"):
            for file in [
                file_name
                for file_name in sorted(files)
                if file_name.endswith("-details.json")
            ]:
                filename, file_extension = os.path.splitext(file)

                source_file_path = os.path.join(
                    source_path, subdir, f"{filename}{file_extension}"
                )
                results_file_path = os.path.join(
                    results_path,
                    subdir.split(os.sep)[-1],
                    f"{filename}.csv",
                )
                convert_file_to_csv(
                    source_file_path, results_file_path, clean=clean, quiet=quiet
                )


def convert_file_to_csv(source_file_path, results_file_path, clean=False, quiet=False):
    # Make results path
    os.makedirs(os.path.dirname(results_file_path), exist_ok=True)

    # Check if result needs to be generated
    if clean or not os.path.exists(results_file_path):
        # Load json file
        json_file = read_json_file(source_file_path)

        try:
            nodes = [row for row in json_file["elements"] if row["type"] == "node"]
            if len(nodes) > 0:
                dataframe = (
                    pd.DataFrame(nodes)
                    .assign(
                        name=lambda df: df["tags"].apply(
                            lambda row: row["name"] if "name" in row else None
                        )
                    )
                    .assign(
                        street=lambda df: df["tags"].apply(
                            lambda row: f"{row['addr:street']} {row['addr:housenumber']}"
                            if "addr:street" in row and "addr:housenumber" in row
                            else None
                        )
                    )
                    .assign(
                        zip_code=lambda df: df["tags"].apply(
                            lambda row: row["addr:postcode"]
                            if "addr:postcode" in row
                            else None
                        )
                    )
                    .assign(
                        zip_code=lambda df: df["zip_code"].astype(
                            pd.Int64Dtype(), errors="ignore"
                        )
                    )
                    .assign(
                        city=lambda df: df["tags"].apply(
                            lambda row: row["addr:city"] if "addr:city" in row else None
                        )
                    )
                    .drop(
                        columns=[
                            "type",
                            "tags",
                            "bounds",
                            "nodes",
                            "geometry",
                            "members",
                        ],
                        errors="ignore",
                    )
                )

                # Write csv file
                dataframe.to_csv(results_file_path, index=False)
                if not quiet:
                    print(f"✓ Convert {os.path.basename(results_file_path)}")
        except Exception as e:
            print(f"✗️ Exception: {str(e)}")
    elif not quiet:
        print(f"✓ Already exists {os.path.basename(results_file_path)}")


def read_json_file(file_path):
    with open(file=file_path, mode="r", encoding="utf-8") as geojson_file:
        return json.load(geojson_file, strict=False)
