import json
import os
from urllib.parse import quote

import requests
from opendataproduct.config.data_product_manifest_loader import DataProductManifest
from opendataproduct.tracking_decorator import TrackingDecorator

points_of_interest_queries = [
    # Residential Areas
    # {"name": "apartments", "type": "way", "query": "building=apartments"},
    # {"name": "residential", "type": "way", "query": "landuse=residential"},
    # Workplaces
    {"name": "offices", "type": "nwr", "query": "office"},
    {"name": "coworking_spaces", "type": "node", "query": "amenity=coworking_space"},
    # Commercial Services
    {"name": "supermarkets", "type": "node", "query": "shop=supermarket"},
    {"name": "grocery_stores", "type": "node", "query": "shop=grocery"},
    {"name": "convenience_stores", "type": "node", "query": "shop=convenience"},
    {"name": "marketplaces", "type": "node", "query": "amenity=marketplace"},
    # {"name": "retail_areas", "type": "way", "query": "landuse=retail"},
    # Education
    {"name": "schools", "type": "node", "query": "amenity=school"},
    {"name": "kindergartens", "type": "node", "query": "amenity=kindergarten"},
    {"name": "childcare", "type": "node", "query": "amenity=childcare"},
    {"name": "libraries", "type": "node", "query": "amenity=library"},
    {"name": "universities", "type": "node", "query": "amenity=university"},
    # Healthcare
    {"name": "doctors", "type": "node", "query": "amenity=doctors"},
    {"name": "pharmacies", "type": "node", "query": "amenity=pharmacy"},
    {"name": "clinics", "type": "node", "query": "amenity=clinic"},
    {"name": "hospitals", "type": "node", "query": "amenity=hospital"},
    # Recreation and Leisure
    {"name": "sport_centers", "type": "node", "query": "leisure=sports_centre"},
    {"name": "fitness_centers", "type": "node", "query": "leisure=fitness_centre"},
    {"name": "playgrounds", "type": "node", "query": "leisure=playground"},
    # Cultural Spaces
    {"name": "art_galleries", "type": "node", "query": "tourism=artwork"},
    {"name": "theaters", "type": "node", "query": "amenity=theatre"},
    {"name": "museums", "type": "node", "query": "tourism=museum"},
    {"name": "cinemas", "type": "node", "query": "amenity=cinema"},
    # Food and Dining
    {"name": "cafes", "type": "node", "query": "amenity=cafe"},
    {"name": "restaurants", "type": "node", "query": "amenity=restaurant"},
    {"name": "bars", "type": "node", "query": "amenity=bar"},
    {"name": "pubs", "type": "node", "query": "amenity=pub"},
    {"name": "beer_gardens", "type": "node", "query": "amenity=biergarten"},
    {"name": "fast_food_restaurants", "type": "node", "query": "amenity=fast_food"},
    {"name": "food_courts", "type": "node", "query": "amenity=food_court"},
    {"name": "ice_cream_parlours", "type": "node", "query": "amenity=ice_cream"},
    {"name": "nightclubs", "type": "node", "query": "amenity=nightclub"},
    # Public Services
    {"name": "post_offices", "type": "node", "query": "amenity=post_office"},
    {"name": "police_stations", "type": "node", "query": "amenity=police"},
    {"name": "fire_stations", "type": "node", "query": "amenity=fire_station"},
    # Transportation
    {"name": "bus_stops", "type": "node", "query": "highway=bus_stop"},
    {"name": "ubahn_stops", "type": "node", "query": "railway=station][subway=yes"},
    {"name": "sbahn_stops", "type": "node", "query": "railway=station][light_rail=yes"},
    {"name": "tram_stops", "type": "node", "query": "railway=tram_stop"},
    {"name": "bicycle_rentals", "type": "node", "query": "amenity=bicycle_rental"},
    {"name": "car_sharing_stations", "type": "node", "query": "amenity=car_sharing"},
    # Community Spaces
    {"name": "community_centers", "type": "node", "query": "amenity=community_centre"},
    {"name": "places_of_worship", "type": "node", "query": "amenity=place_of_worship"},
    # Green Spaces
    {"name": "parks", "type": "nwr", "query": "leisure=park"},
    {"name": "forests", "type": "nwr", "query": "landuse=forest"},
]


@TrackingDecorator.track_time
def extract_overpass_data(
    data_product_manifest: DataProductManifest,
    bounding_box_geojson_path,
    bounding_box_feature_id,
    results_path,
    year,
    month,
    clean=False,
    quiet=False,
):
    # Make results path
    os.makedirs(os.path.join(results_path), exist_ok=True)

    # Define ID
    id = data_product_manifest.id.replace("-source-aligned", "")

    # Define bounding box
    bounding_box = build_bounding_box(
        bounding_box_geojson_path, bounding_box_feature_id
    )

    # Iterate over queries
    for points_of_interest_query in points_of_interest_queries:
        name = points_of_interest_query["name"]
        type = points_of_interest_query["type"]
        query = points_of_interest_query["query"]

        file_path = os.path.join(
            results_path,
            f"{id}-{year}-{month}",
            f"{id}-{name.replace('_', '-')}-details.json",
        )

        if not os.path.exists(file_path) or clean:
            # Query Overpass API
            overpass_json = extract_overpass_json(
                type,
                query,
                bounding_box[0],
                bounding_box[1],
                bounding_box[2],
                bounding_box[3],
            )

            # Write json file
            write_json_file(
                file_path=file_path,
                query_name=name,
                json_content=overpass_json,
                clean=clean,
                quiet=quiet,
            )
        else:
            not quiet and print(f"✓ Already exists {os.path.basename(file_path)}")


def build_bounding_box(bounding_box_geojson_path, bounding_box_feature_id):
    bounding_box = None
    geojson = read_geojson_file(bounding_box_geojson_path)
    for feature in geojson["features"]:
        if feature["properties"]["id"] == bounding_box_feature_id:
            bounding_box = feature["properties"]["bounding_box"]
            break

    if bounding_box is None:
        print(
            f"✗️ Exception: no bounding box found for feature ID {bounding_box_feature_id}"
        )
        return None

    return bounding_box


def read_geojson_file(file_path):
    with open(file=file_path, mode="r", encoding="utf-8") as geojson_file:
        return json.load(geojson_file, strict=False)


def extract_overpass_json(type, query, xmin, ymin, xmax, ymax):
    try:
        data = f"""
[out:json][timeout:25];
(
  {type}[{query}]({ymin}, {xmin}, {ymax}, {xmax});
);
out geom;
"""
        formatted_data = quote(data.lstrip("\n"))

        url = f"https://overpass-api.de/api/interpreter?data={formatted_data}"
        response = requests.get(url)
        text = response.text.replace("'", "")
        return json.loads(text)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")
        return None


def write_json_file(file_path, query_name, json_content, clean, quiet):
    # Make results path
    path_name = os.path.dirname(file_path)
    os.makedirs(os.path.join(path_name), exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as json_file:
        json.dump(json_content, json_file, ensure_ascii=False)

        not quiet and print(
            f"✓ Extract data for {query_name.replace('_', '-')} into {os.path.basename(file_path)}"
        )
