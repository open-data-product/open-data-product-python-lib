import json
import os
import numbers
from tqdm import tqdm

from opendataproduct.tracking_decorator import TrackingDecorator


def process_coords(coords, n, decimal_places):
    """
    Recursively processes coordinate arrays.
    Rounds numbers to `decimal_places` and retains every `n`-th coordinate.
    Ensures that closed rings remain closed and have at least 4 coordinates.
    """
    if len(coords) == 0:
        return []

    # If the first element is a number, this is a single coordinate point [lon, lat, ...]
    if isinstance(coords[0], numbers.Number):
        return [round(c, decimal_places) for c in coords]

    # If the first element is a list/tuple of numbers, this is a line or ring (list of points)
    if isinstance(coords[0][0], numbers.Number):
        # Round all coordinates in the list
        rounded_coords = [[round(c, decimal_places) for c in pt] for pt in coords]

        # Select every n-th coordinate
        is_closed = len(rounded_coords) > 1 and rounded_coords[0] == rounded_coords[-1]
        reduced = []
        for i, pt in enumerate(rounded_coords):
            if i % n == 0 or i == len(rounded_coords) - 1:
                reduced.append(pt)

        # Ensure closed rings stay closed and valid
        if is_closed:
            if len(reduced) < 4:
                if len(rounded_coords) >= 4:
                    mid1 = len(rounded_coords) // 3
                    mid2 = 2 * len(rounded_coords) // 3
                    reduced = [
                        rounded_coords[0],
                        rounded_coords[mid1],
                        rounded_coords[mid2],
                        rounded_coords[-1],
                    ]
                else:
                    reduced = rounded_coords[:]
            if reduced[0] != reduced[-1]:
                reduced[-1] = reduced[0]
        else:
            if len(reduced) < 2 and len(rounded_coords) >= 2:
                reduced = [rounded_coords[0], rounded_coords[-1]]

        return reduced

    # Recursively process nested coordinate lists (e.g. list of rings or list of polygons)
    return [process_coords(c, n, decimal_places) for c in coords]


def reduce_feature_resolution(feature, n, decimal_places):
    if (
        "geometry" not in feature
        or not feature["geometry"]
        or "coordinates" not in feature["geometry"]
    ):
        return feature

    geom = feature["geometry"]
    if geom["type"] == "GeometryCollection":
        if "geometries" in geom:
            for sub_geom in geom["geometries"]:
                if "coordinates" in sub_geom:
                    sub_geom["coordinates"] = process_coords(
                        sub_geom["coordinates"], n, decimal_places
                    )
    elif "coordinates" in geom:
        geom["coordinates"] = process_coords(geom["coordinates"], n, decimal_places)

    return feature


@TrackingDecorator.track_time
def reduce_resolution(
    data_transformation,
    source_path,
    results_path,
    clean=False,
    quiet=False,
    n=5,
    decimal_places=5,
):
    """
    Reduces the resolution of coordinates in geojson files.
    :param data_transformation: data transformation configuration
    :param source_path: source path directory
    :param results_path: results path directory
    :param clean: clean flag
    :param quiet: quiet output flag
    :param n: keep every n-th coordinate
    :param decimal_places: round coordinates to this decimal place
    """
    already_exists, converted, exception = 0, 0, 0

    if data_transformation.input_ports:
        for input_port in data_transformation.input_ports:
            for file in input_port.files:
                source_file_path = os.path.join(
                    source_path, input_port.id, file.target_file_name
                )
                target_file_path = os.path.join(
                    results_path, f"{input_port.id}-low-res", file.target_file_name
                )

                if not clean and os.path.exists(target_file_path):
                    already_exists += 1
                    not quiet and print(
                        f"✓ Already reduced resolution of {file.target_file_name}"
                    )
                    continue

                try:
                    with open(source_file_path, "r", encoding="utf-8") as geojson_file:
                        geojson = json.load(geojson_file, strict=False)

                    features = geojson.get("features", [])
                    reduced_features = []
                    for feature in tqdm(
                        iterable=features,
                        desc=f"Reducing resolution of {file.target_file_name}",
                        unit="feature",
                        disable=quiet,
                    ):
                        reduced_features.append(
                            reduce_feature_resolution(feature, n, decimal_places)
                        )

                    geojson["features"] = reduced_features

                    os.makedirs(os.path.dirname(target_file_path), exist_ok=True)
                    with open(target_file_path, "w", encoding="utf-8") as out_file:
                        json.dump(geojson, out_file, ensure_ascii=False)

                    converted += 1
                    not quiet and print(
                        f"✓ Reduce resolution of {file.target_file_name}"
                    )

                except Exception as e:
                    exception += 1
                    print(f"✗ Exception in {file.target_file_name}: {str(e)}")

    print(
        f"reduce_resolution finished with already_exists: {already_exists}, converted: {converted}, exception: {exception}"
    )
