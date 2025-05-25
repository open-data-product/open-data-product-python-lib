import json
import os
from collections.abc import Sequence
from itertools import chain, count

from tqdm import tqdm

from opendataproduct.tracking_decorator import TrackingDecorator


@TrackingDecorator.track_time
def convert_data_geometry(
    data_transformation, source_path, results_path, clean=False, quiet=False
):
    """
    Converts inconsistent geometry
    :param data_transformation: data transformation
    :param source_path: source path
    :param results_path: results path
    :param clean: clean
    :param quiet: quiet
    :return:
    """
    already_exists, converted, exception = 0, 0, 0

    if data_transformation.input_ports:
        for input_port in data_transformation.input_ports:
            for file in input_port.files:
                source_file_path = os.path.join(
                    source_path, input_port.id, file.target_file_name
                )
                target_file_path = os.path.join(
                    results_path, input_port.id, file.target_file_name
                )

                try:
                    with open(source_file_path, "r", encoding="utf-8") as geojson_file:
                        geojson = json.load(geojson_file, strict=False)

                    geojson, changed = convert_geometry(geojson, quiet)

                    if not changed:
                        already_exists += 1
                        not quiet and print(
                            f"✓ Already converted {file.target_file_name}"
                        )
                        continue

                    with open(target_file_path, "w", encoding="utf-8") as geojson_file:
                        json.dump(geojson, geojson_file, ensure_ascii=False)

                        converted += 1
                        not quiet and print(f"✓ Clean {file.target_file_name}")
                except Exception as e:
                    exception += 1
                    print(f"✗️ Exception: {str(e)}")
    print(
        f"convert_data_geometry finished with already_exists: {already_exists}, converted: {converted}, exception: {exception}"
    )


def convert_geometry(geojson, quiet):
    changed = False

    for feature in tqdm(
        iterable=geojson["features"], desc="Clean features", unit="feature"
    ):
        polygons = feature["geometry"]["coordinates"]

        # Check if there is more than one polygon
        if len(polygons) > 1:
            polygon_max = [[]]

            for polygon in polygons:
                polygon_elements_count = len(polygon[0])
                polygon_elements_count_max = len(polygon_max[0])

                if polygon_elements_count > polygon_elements_count_max:
                    polygon_max = polygon

            feature["geometry"]["coordinates"] = [polygon_max]
            changed = True

        # Sanity-check geometry
        if get_depth(feature["geometry"]["coordinates"]) > 4 and not quiet:
            raise Exception("Invalid geometry")

    return geojson, changed


# Thanks https://stackoverflow.com/a/6040217/2992219
def get_depth(seq):
    for level in count():
        if not seq:
            return level
        seq = list(chain.from_iterable(s for s in seq if isinstance(s, Sequence)))
