import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

import itertools
import json
import os

from tqdm import tqdm

from opendataproduct.tracking_decorator import TrackingDecorator


@TrackingDecorator.track_time
def convert_bounding_box(
    data_transformation, source_path, results_path, clean=False, quiet=False
):
    """
    Adds a bounding box to each feature
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

                with open(source_file_path, "r", encoding="utf-8") as geojson_file:
                    geojson = json.load(geojson_file, strict=False)

                    if not clean and all(
                        "bounding_box" in feature["properties"]
                        for feature in geojson["features"]
                    ):
                        already_exists += 1
                        not quiet and print(
                            f"✓ Already converted {file.target_file_name}"
                        )
                        continue

                    geojson_with_bounding_box = extend_by_bounding_box(geojson)

                    with open(
                        target_file_path, "w", encoding="utf-8"
                    ) as geojson_bounding_box_file:
                        json.dump(
                            geojson_with_bounding_box,
                            geojson_bounding_box_file,
                            ensure_ascii=False,
                        )

                        converted += 1
                        not quiet and print(f"✓ Convert {file.target_file_name}")

    print(
        f"convert_bounding_box finished with already_exists: {already_exists}, converted: {converted}, exception: {exception}"
    )


def extend_by_bounding_box(geojson):
    for feature in tqdm(
        iterable=geojson["features"], desc="Convert features", unit="feature"
    ):
        xmin = None
        ymin = None
        xmax = None
        ymax = None

        geometry = feature["geometry"]
        coordinates = geometry["coordinates"]

        for coordinate in flatten_list(coordinates):
            x = coordinate[0]
            y = coordinate[1]

            if xmin is None or x < xmin:
                xmin = x
            if ymin is None or y < ymin:
                ymin = y
            if xmax is None or x > xmax:
                xmax = x
            if ymax is None or y > ymax:
                ymax = y

        feature["properties"]["bounding_box"] = [xmin, ymin, xmax, ymax]

    return geojson


def flatten_list(complex_list):
    while not (type(complex_list[0][0]) == float and type(complex_list[0][1]) == float):
        complex_list = list(itertools.chain(*complex_list))

    return complex_list
