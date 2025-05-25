import json
import os

import geopandas as gpd
from geopandas import GeoDataFrame

from opendataproduct.tracking_decorator import TrackingDecorator


@TrackingDecorator.track_time
def identify_lor_area_matches(
    source_path, results_path, area_tolerance=0.01, clean=False, quiet=False
):
    """
    Identifies overlaps in LOR areas between until-2020 and from-2021 taxonomy
    :param source_path:
    :param results_path:
    :param area_tolerance:
    :param clean:
    :param quiet:
    :return:
    """
    matches = {}

    for lor_area_type in ["forecast-areas", "district-regions", "planning-areas"]:
        lor_area_type_matches = {}

        try:
            gdf_until_2020_file_path = os.path.join(
                source_path,
                f"berlin-lor-{lor_area_type}-until-2020",
                f"berlin-lor-{lor_area_type}-until-2020.geojson",
            )

            gdf_from_2021_file_path = os.path.join(
                source_path,
                f"berlin-lor-{lor_area_type}-from-2021",
                f"berlin-lor-{lor_area_type}-from-2021.geojson",
            )

            # Read geojson files
            gdf_until_2020 = gpd.read_file(gdf_until_2020_file_path)
            gdf_from_2021 = gpd.read_file(gdf_from_2021_file_path)

            # Count features
            gdf_until_2020_feature_count = gdf_until_2020.index.size
            gdf_from_2021_feature_count = gdf_from_2021.index.size

            # Set coordinate reference system
            gdf_until_2020.set_crs("EPSG:4326", inplace=True)
            gdf_from_2021.set_crs("EPSG:4326", inplace=True)

            # Identify LOR areas of until-2020 that contain LOR areas of from-2021
            lor_area_type_matches |= identify_feature_matches(
                gdf_until_2020, "until-2020", gdf_from_2021, "from-2021", area_tolerance
            )
            # Identify LOR areas of from-2021 that contain LOR areas of until-2020
            lor_area_type_matches |= identify_feature_matches(
                gdf_from_2021, "from-2021", gdf_until_2020, "until-2020", area_tolerance
            )

            print(
                f"✓ Found {len(lor_area_type_matches)} matches in {lor_area_type} (until 2020: {gdf_until_2020_feature_count}, from 2021: {gdf_from_2021_feature_count})"
            )
            matches |= lor_area_type_matches
        except Exception as e:
            print(f"✗️ Exception: {str(e)}")

    write_json_file(
        os.path.join(results_path, "berlin-lor-matches", "berlin-lor-matches.json"),
        matches,
        clean,
        quiet,
    )


def identify_feature_matches(
    outer: GeoDataFrame, outer_label, inner: GeoDataFrame, inner_label, area_tolerance
):
    matches = {}

    for _, feature_outer in outer.iterrows():
        feature_matches = []

        for _, feature_inner in inner.iterrows():
            # Calculate intersection area
            intersection_area = feature_outer.geometry.intersection(
                feature_inner.geometry
            ).area

            # Calculate the area of each polygon
            area_inner = feature_inner.geometry.area

            # Check if inner area is within outer area
            if intersection_area / area_inner > 1 - area_tolerance:
                feature_matches.append(feature_inner["id"])

        if len(feature_matches) > 0:
            matches[feature_outer["id"]] = feature_matches

    return matches


def write_json_file(file_path, json_content, clean, quiet):
    if not os.path.exists(file_path) or clean:
        # Make results path
        path_name = os.path.dirname(file_path)
        os.makedirs(os.path.join(path_name), exist_ok=True)

        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(json_content, json_file, ensure_ascii=False)

            if not quiet:
                print(f"✓ Writes LOR area matches into {os.path.basename(file_path)}")
    else:
        print(f"✓ Already exists {os.path.basename(file_path)}")
