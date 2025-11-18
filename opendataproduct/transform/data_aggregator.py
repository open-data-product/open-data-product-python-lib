import json
import os
import math
import pandas as pd
from opendataproduct.config.data_transformation_gold_loader import (
    DataTransformation,
)
from opendataproduct.tracking_decorator import TrackingDecorator
from pyproj import Transformer
from shapely.geometry.point import Point
from shapely.geometry.polygon import Polygon


@TrackingDecorator.track_time
def aggregate_data(
    data_transformation: DataTransformation,
    source_path,
    results_path,
    geojson_path=None,
    clean=False,
    quiet=False,
):
    already_exists, converted, exception = 0, 0, 0

    for input_port in data_transformation.input_ports or []:
        for file in input_port.files or []:
            file_name, _ = os.path.splitext(file.target_file_name)
            target_file_name_csv = f"{file_name}.csv"
            target_file_name_parquet = f"{file_name}.parquet"

            if geojson_path is not None:
                geojson_template_file_path = (
                    os.path.join(geojson_path, file.geojson_template_file_name)
                    if file.geojson_template_file_name is not None
                    else None
                )
                geojson_feature_cache_file_path = os.path.join(
                    geojson_path, "geojson-feature-cache.csv"
                )

            source_file_path = os.path.join(
                source_path, input_port.id, file.source_file_name
            )
            target_file_path_csv = os.path.join(
                results_path,
                f"{input_port.id.replace("-csv", "")}-csv",
                target_file_name_csv,
            )
            target_file_path_parquet = os.path.join(
                results_path,
                f"{input_port.id.replace("-csv", "")}-parquet",
                target_file_name_parquet,
            )

            if (
                not clean
                and os.path.exists(target_file_path_csv)
                and os.path.exists(target_file_path_parquet)
            ):
                already_exists += 1
                not quiet and print(
                    f"✓ Already exists {os.path.basename(target_file_path_csv)} / {os.path.basename(target_file_path_parquet)}"
                )
                continue

            try:
                with open(source_file_path, "r") as csv_file:
                    # Read csv file
                    dataframe = pd.read_csv(csv_file, dtype=str, keep_default_na=False)

                    # Apply trim
                    dataframe = dataframe.map(
                        lambda col: col.strip() if isinstance(col, str) else col
                    )

                    # Apply data type
                    dataframe = dataframe.astype(
                        {
                            name.name: name.type
                            for name in file.names
                            if name.name in dataframe.columns
                        },
                        errors="ignore",
                    )

                    # Apply value
                    for name in [name for name in file.names if name.value]:
                        dataframe[name.name] = name.value

                    # Apply coordinate transformation
                    for name in [name for name in file.names if name.transform_lon]:
                        dataframe[name.name] = dataframe.apply(
                            lambda row: transform_lon(
                                row[name.transform_lon[0]],
                                row[name.transform_lon[1]],
                                name.transform_source,
                                name.transform_target,
                            ),
                            axis=1,
                        )
                    for name in [name for name in file.names if name.transform_lat]:
                        dataframe[name.name] = dataframe.apply(
                            lambda row: transform_lat(
                                row[name.transform_lat[0]],
                                row[name.transform_lat[1]],
                                name.transform_source,
                                name.transform_target,
                            ),
                            axis=1,
                        )

                    # Apply geojson lookup
                    for name in [name for name in file.names if name.geojson_lookup]:
                        dataframe[name.name] = dataframe.apply(
                            lambda row: lookup_geojson_feature(
                                geojson_template_file_path,
                                geojson_feature_cache_file_path,
                                [
                                    row[name.geojson_lookup[0]],
                                    row[name.geojson_lookup[1]],
                                ],
                            ),
                            axis=1,
                        )
                        dataframe[name.name] = dataframe[name.name].astype(str)

                    # Apply concatenation
                    for name in [name for name in file.names if name.concat]:
                        dataframe[name.name] = dataframe[name.concat].agg(
                            name.concat_delimiter.join, axis=1
                        )
                        dataframe.insert(0, name.name, dataframe.pop(name.name))

                    # Apply split
                    for name in [name for name in file.names if name.split]:
                        dataframe[name.name] = dataframe[name.split.name].str[
                            name.split.last_n : name.split.first_n
                        ]

                    # Apply filter
                    for filter in file.filters or []:
                        if filter.operation == "starts_with":
                            dataframe = dataframe[
                                dataframe[filter.key].str.startswith(filter.value)
                            ]
                        if filter.operation == "does_not_start_with":
                            dataframe = dataframe[
                                ~dataframe[filter.key].str.startswith(filter.value)
                            ]
                        if filter.operation == "ends_with":
                            dataframe = dataframe[
                                dataframe[filter.key].str.endswith(filter.value)
                            ]
                        if filter.operation == "does_not_end_with":
                            dataframe = dataframe[
                                ~dataframe[filter.key].str.endswith(filter.value)
                            ]

                    # Apply copy
                    for name in [
                        name
                        for name in file.names
                        if name.copy and name.copy in dataframe.columns
                    ]:
                        dataframe[name.name] = dataframe[name.copy]
                        dataframe.insert(0, name.name, dataframe.pop(name.name))

                    # Apply zfill
                    for name in [
                        name
                        for name in file.names
                        if name.name in dataframe.columns and name.zfill
                    ]:
                        dataframe[name.name] = (
                            dataframe[name.name].astype(str).str.zfill(name.zfill)
                        )

                    # Apply lstrip
                    for name in [
                        name
                        for name in file.names
                        if name.name in dataframe.columns and name.lstrip
                    ]:
                        dataframe[name.name] = (
                            dataframe[name.name].astype(str).str.lstrip(name.lstrip)
                        )

                    # Apply aggregation
                    if file.aggregate_by is not None:
                        if file.aggregate_by != "total":
                            dataframe = dataframe.apply(pd.to_numeric, errors="coerce")
                            dataframe = dataframe.groupby(
                                file.aggregate_by, as_index=False
                            ).sum()
                        else:
                            dataframe = dataframe.apply(pd.to_numeric, errors="coerce")
                            dataframe = (
                                pd.DataFrame(dataframe.sum()).transpose().astype(int)
                            )
                            dataframe["id"] = 0

                    # Apply copy
                    for name in [
                        name
                        for name in file.names
                        if name.copy and name.copy in dataframe.columns
                    ]:
                        dataframe[name.name] = dataframe[name.copy]
                        dataframe.insert(0, name.name, dataframe.pop(name.name))

                    # Apply zfill
                    for name in [
                        name
                        for name in file.names
                        if name.name in dataframe.columns and name.zfill
                    ]:
                        dataframe[name.name] = (
                            dataframe[name.name].astype(str).str.zfill(name.zfill)
                        )

                    # Apply fraction
                    for name in [
                        name
                        for name in file.names
                        if name.numerator
                        and name.numerator in dataframe.columns
                        and name.denominator
                        and name.denominator in dataframe.columns
                    ]:
                        dataframe[name.name] = (
                            dataframe[name.numerator]
                            .astype(float)
                            .divide(dataframe[name.denominator].astype(float))
                            .multiply(100)
                            .fillna(0)
                        )

                    # Apply mapping
                    for name in [name for name in file.names if name.mapping]:
                        dataframe[name.name] = dataframe[name.key].map(name.mapping)
                        dataframe.insert(0, name.name, dataframe.pop(name.name))

                    # Apply remove
                    dataframe = dataframe.filter(
                        items=[
                            name.name
                            for name in file.names
                            if name.name in dataframe.columns and not name.remove
                        ]
                    )

                    # Apply rename
                    for name in [
                        name
                        for name in file.names
                        if name.name in dataframe.columns and name.rename
                    ]:
                        dataframe = dataframe.rename(columns={name.name: name.rename})

                    # Move ID column to first position
                    if "id" not in dataframe.columns.tolist():
                        dataframe["id"] = 0
                    dataframe.insert(0, "id", dataframe.pop("id"))

                    # Save csv file
                    os.makedirs(os.path.dirname(target_file_path_csv), exist_ok=True)
                    dataframe.to_csv(target_file_path_csv, index=False)

                    # Save parquet file
                    os.makedirs(
                        os.path.dirname(target_file_path_parquet), exist_ok=True
                    )
                    dataframe.to_parquet(target_file_path_parquet, index=False)

                    converted += 1

                    not quiet and print(
                        f"✓ Convert {os.path.basename(target_file_path_csv)} / {os.path.basename(target_file_path_parquet)}"
                    )
            except Exception as e:
                exception += 1
                print(f"✗️ Exception: {str(e)}")
    print(
        f"aggregate_data finished with already_exists: {already_exists}, converted: {converted}, exception: {exception}"
    )


def load_geojson_file(geojson_template_file_path):
    with open(
        file=geojson_template_file_path, mode="r", encoding="utf-8"
    ) as geojson_file:
        return json.load(geojson_file, strict=False)


def load_csv_file(source_file_path):
    with open(source_file_path, "r") as csv_file:
        return pd.read_csv(csv_file, dtype=str)


def transform_lon(source_lon, source_lat, transform_source, transform_target):
    lon, _ = Transformer.from_crs(
        transform_source, transform_target, always_xy=True
    ).transform(source_lon, source_lat)
    return lon


def transform_lat(source_lon, source_lat, transform_source, transform_target):
    _, lat = Transformer.from_crs(
        transform_source, transform_target, always_xy=True
    ).transform(source_lon, source_lat)
    return lat


def lookup_geojson_feature(
    geojson_template_file_path, geojson_feature_cache_file_path, coords: []
):
    geojson = load_geojson_file(geojson_template_file_path)

    # Read geojson feature cache
    if os.path.exists(geojson_feature_cache_file_path):
        geojson_feature_cache = load_csv_file(geojson_feature_cache_file_path)
        geojson_feature_cache.set_index("latlon", inplace=True)
    else:
        geojson_feature_cache = pd.DataFrame(columns=["latlon", "geojson_feature_id"])
        geojson_feature_cache.set_index("latlon", inplace=True)

    lat = truncate(float(coords[0]), 4)
    lon = truncate(float(coords[1]), 4)

    geojson_feature_cache_index = f"{lat}_{lon}"

    # Check if geojson feature is already in cache
    if geojson_feature_cache_index in geojson_feature_cache.index:
        return geojson_feature_cache.loc[geojson_feature_cache_index][
            "geojson_feature_id"
        ]
    else:
        point = Point(lon, lat)

        geojson_feature_id = None

        for feature in geojson["features"]:
            id = feature["properties"]["id"]
            coordinates = feature["geometry"]["coordinates"]
            polygon = build_polygon(coordinates)
            if point.within(polygon):
                geojson_feature_id = id

        # Store result in cache
        if geojson_feature_id is not None:
            geojson_feature_cache.loc[geojson_feature_cache_index] = {
                "geojson_feature_id": geojson_feature_id
            }
            geojson_feature_cache.assign(
                geojson_feature_id=lambda df: df["geojson_feature_id"]
                .astype(int)
                .astype(str)
                .str.zfill(8)
            )
            geojson_feature_cache.to_csv(geojson_feature_cache_file_path, index=True)
            return geojson_feature_id
        else:
            return 0


def truncate(value, digits):
    return math.floor(value * 10**digits) / 10**digits


def build_polygon(coordinates) -> Polygon:
    points = [tuple(point) for point in coordinates[0][0]]
    return Polygon(points)
