import os

import geopandas as gpd
import pandas as pd
import partridge as ptg
from opendataproduct.config.data_transformation_gold_loader import DataTransformation
from opendataproduct.tracking_decorator import TrackingDecorator
from shapely.geometry import LineString

route_type_map = {
    # --- Standard GTFS route types ---
    0: "tram",
    1: "subway-metro",
    2: "rail",
    3: "bus",
    4: "ferry",
    5: "cable-tram",
    6: "aerial-lift",
    7: "funicular",
    11: "trolleybus",
    12: "monorail",
    # --- 100s: Railway services ---
    100: "railway-service",
    101: "high-speed-rail-service",
    102: "long-distance-rail-service",
    103: "inter-regional-rail-service",
    104: "car-transport-rail-service",
    105: "sleeper-rail-service",
    106: "regional-rail-service",
    107: "tourist-railway-service",
    108: "rail-shuttle-within-complex",
    109: "suburban-railway",
    110: "replacement-rail-service",
    111: "special-rail-service",
    112: "lorry-transport-rail-service",
    113: "all-rail-services",
    114: "cross-country-rail-service",
    115: "vehicle-transport-rail-service",
    116: "rack-and-pinion-railway",
    117: "additional-rail-service",
    # --- 200s: Coach services ---
    200: "coach-service",
    201: "international-coach-service",
    202: "national-coach-service",
    203: "shuttle-coach-service",
    204: "regional-coach-service",
    205: "special-coach-service",
    206: "sightseeing-coach-service",
    207: "tourist-coach-service",
    208: "commuter-coach-service",
    209: "all-coach-services",
    # --- 400s: Urban railway services ---
    400: "urban-railway-service",
    401: "metro-service",
    402: "underground-service",
    403: "urban-railway-service",
    404: "all-urban-railway-services",
    405: "monorail",
    # --- 700s: Bus services ---
    700: "bus-service",
    701: "regional-bus-service",
    702: "express-bus-service",
    703: "stopping-bus-service",
    704: "local-bus-service",
    705: "night-bus-service",
    706: "post-bus-service",
    707: "special-needs-bus",
    708: "mobility-bus-service",
    709: "mobility-bus-for-registered-disabled",
    710: "sightseeing-bus",
    711: "shuttle-bus",
    712: "school-bus",
    713: "school-and-public-service-bus",
    714: "rail-replacement-bus-service",
    715: "demand-and-response-bus-service",
    716: "all-bus-services",
    # --- 800s: Trolleybus services ---
    800: "trolleybus-service",
    # --- 900s: Tram services ---
    900: "tram-service",
    901: "city-tram-service",
    902: "local-tram-service",
    903: "regional-tram-service",
    904: "sightseeing-tram-service",
    905: "shuttle-tram-service",
    906: "all-tram-services",
    # --- 1000s: Water transport services ---
    1000: "water-transport-service",
    1200: "ferry-service",
    # --- 1300s: Aerial lift services ---
    1300: "aerial-lift-service",
    1301: "telecabin-service",
    1302: "cable-car-service",
    1303: "elevator-service",
    1304: "chair-lift-service",
    1305: "drag-lift-service",
    1306: "small-telecabin-service",
    1307: "all-telecabin-services",
    # --- 1400s: Funicular services ---
    1400: "funicular-service",
    # --- 1500s: Taxi services ---
    1500: "taxi-service",
    1501: "communal-taxi-service",
    1502: "water-taxi-service",
    1503: "rail-taxi-service",
    1504: "bike-taxi-service",
    1505: "licensed-taxi-service",
    1506: "private-hire-service-vehicle",
    1507: "all-taxi-services",
    # --- 1700s: Miscellaneous ---
    1700: "miscellaneous-service",
    1702: "horse-drawn-carriage",
}


@TrackingDecorator.track_time
def convert_gtfs_to_geojson(
    data_transformation: DataTransformation,
    source_path,
    results_path,
    debug=False,
    clean=False,
    quiet=False,
):
    if data_transformation.input_ports:
        for input_port in data_transformation.input_ports:
            for file in input_port.files:
                source_file_path = os.path.join(
                    source_path, input_port.id, file.source_file_name
                )

                #
                # Load and filter GTFS data
                #

                feed = ptg.load_feed(source_file_path)

                routes = feed.routes
                trips = feed.trips
                stops = feed.stops
                stop_times = feed.stop_times
                shapes = feed.shapes

                if debug:
                    print(
                        f"route types: {[int(value) for value in list(set(routes["route_type"].values))]}"
                    )

                # Map route_type code to string
                routes["mode_name"] = (
                    routes["route_type"].map(route_type_map).fillna("other")
                )

                # Get unique modes
                unique_modes = routes["mode_name"].unique()

                # Iterative over modes
                for mode in unique_modes:
                    target_file_path = os.path.join(
                        results_path,
                        input_port.id,
                        file.target_file_name.replace(".geojson", f"-{mode}.geojson"),
                    )

                    # Check if result needs to be generated
                    if clean or not os.path.exists(target_file_path):
                        # Filter routes by mode
                        mode_routes = routes[routes["mode_name"] == mode]
                        mode_route_ids = mode_routes["route_id"].tolist()

                        # Filter trips that use these routes
                        mode_trips = trips[trips["route_id"].isin(mode_route_ids)]
                        mode_trip_ids = mode_trips["trip_id"].tolist()

                        if len(mode_trips) == 0:
                            continue

                        #
                        # Process shapes
                        #

                        mode_shapes_gdf = gpd.GeoDataFrame()

                        # Filter shapes used by these trips
                        mode_shape_ids = mode_trips["shape_id"].unique()

                        if not shapes.empty:
                            # Only keep shapes for this mode
                            relevant_shapes = shapes[
                                shapes["shape_id"].isin(mode_shape_ids)
                            ]

                            if not relevant_shapes.empty:
                                relevant_shapes = relevant_shapes.sort_values(
                                    by=["shape_id", "shape_pt_sequence"]
                                )
                                lines = relevant_shapes.groupby("shape_id")[
                                    ["shape_pt_lon", "shape_pt_lat"]
                                ].apply(
                                    lambda x: LineString(
                                        list(zip(x.shape_pt_lon, x.shape_pt_lat))
                                    )
                                )
                                mode_shapes_gdf = gpd.GeoDataFrame(
                                    lines, columns=["geometry"], crs="EPSG:4326"
                                )
                                mode_shapes_gdf = mode_shapes_gdf.reset_index()
                                mode_shapes_gdf.rename(
                                    columns={mode_shapes_gdf.columns[0]: "shape_id"},
                                    inplace=True,
                                )

                                # Add metadata
                                dataframe_merged = mode_trips.merge(
                                    mode_routes, on="route_id"
                                )
                                desired_cols = [
                                    "shape_id",
                                    "route_short_name",
                                    "route_color",
                                ]
                                valid_cols = [
                                    c
                                    for c in desired_cols
                                    if c in dataframe_merged.columns
                                ]
                                meta = dataframe_merged[valid_cols]
                                meta = meta.drop_duplicates(subset=["shape_id"])

                                mode_shapes_gdf = mode_shapes_gdf.merge(
                                    meta, on="shape_id", how="left"
                                )

                                # Fix color format
                                def fix_color(c):
                                    return (
                                        f"#{c}"
                                        if pd.notnull(c) and not str(c).startswith("#")
                                        else c
                                    )

                                if "route_color" in mode_shapes_gdf.columns:
                                    mode_shapes_gdf["route_color"] = mode_shapes_gdf[
                                        "route_color"
                                    ].apply(fix_color)

                                mode_shapes_gdf["feature_type"] = "route"

                        #
                        # Process stops
                        #

                        # Remove unused stops
                        relevant_stop_times = stop_times[
                            stop_times["trip_id"].isin(mode_trip_ids)
                        ]
                        active_stop_ids = relevant_stop_times["stop_id"].unique()
                        active_stops = stops[
                            stops["stop_id"].isin(active_stop_ids)
                        ].copy()

                        # Merge stops into stations
                        unique_stations = (
                            active_stops.groupby("stop_name")[["stop_lon", "stop_lat"]]
                            .mean()
                            .reset_index()
                        )

                        # Convert to geo data frame
                        mode_stops_gdf = gpd.GeoDataFrame(
                            unique_stations,
                            geometry=gpd.points_from_xy(
                                unique_stations.stop_lon, unique_stations.stop_lat
                            ),
                            crs="EPSG:4326",
                        )

                        # Cleanup columns
                        desired_cols = [
                            "stop_id",
                            "stop_name",
                            "feature_type",
                            "geometry",
                        ]
                        valid_cols = [
                            c for c in desired_cols if c in mode_stops_gdf.columns
                        ]
                        mode_stops_gdf = mode_stops_gdf[valid_cols]

                        unified_gdf = pd.concat(
                            [mode_stops_gdf, mode_shapes_gdf], ignore_index=True
                        )
                        unified_gdf = unified_gdf.fillna("")

                        if debug:
                            save_dataframe_as_geojson(
                                mode_stops_gdf,
                                target_file_path.replace(".geojson", "-stops.geojson"),
                            )
                            save_dataframe_as_geojson(
                                mode_shapes_gdf,
                                target_file_path.replace(".geojson", "-lines.geojson"),
                            )

                        save_dataframe_as_geojson(unified_gdf, target_file_path)

                        not quiet and print(
                            f"✓ Convert {os.path.basename(source_file_path)} to {os.path.basename(target_file_path)}"
                        )


def save_dataframe_as_geojson(gdf: pd.DataFrame, geojson_file_path):
    # Make results path
    os.makedirs(os.path.dirname(geojson_file_path), exist_ok=True)

    # Save as geojson
    gdf.to_file(geojson_file_path, driver="GeoJSON")
