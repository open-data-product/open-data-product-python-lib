import os
import json
import geopandas as gpd

from opendataproduct.tracking_decorator import TrackingDecorator


@TrackingDecorator.track_time
def combine_districts_into_city(source_path, results_path, clean=False, quiet=False):
    """
    Combines district geojson files into city
    :param source_path: source path
    :param results_path: results path
    :param clean: clean
    :param quiet: quiet
    :return:
    """
    source_file_path = os.path.join(
        source_path, "berlin-lor-districts", "berlin-lor-districts.geojson"
    )
    results_file_path = os.path.join(
        results_path, "berlin-lor-city", "berlin-lor-city.geojson"
    )

    gdf = gpd.read_file(source_file_path)

    # Dissolve into one feature
    combined = gdf.dissolve()

    if not os.path.exists(results_file_path) or clean:
        os.makedirs(os.path.join(os.path.dirname(results_file_path)), exist_ok=True)
        combined.to_file(results_file_path, driver="GeoJSON")

        combined_area = 0
        with open(source_file_path, "r", encoding="utf-8") as geojson_file:
            geojson = json.load(geojson_file, strict=False)
            for feature in geojson["features"]:
                combined_area += feature["properties"]["area"]

        with open(results_file_path, "r", encoding="utf-8") as geojson_file:
            geojson = json.load(geojson_file, strict=False)
            geojson["features"][0]["properties"]["id"] = "0"
            geojson["features"][0]["properties"]["name"] = "Berlin"
            geojson["features"][0]["properties"]["area"] = combined_area

        with open(results_file_path, "w", encoding="utf-8") as geojson_file:
            json.dump(geojson, geojson_file, ensure_ascii=False)
            print(f"✓ Combine berlin-lor-city.geojson")
    else:
        if not quiet:
            print(f"✓ Already combined berlin-lor-city.geojson")
