import os

import pandas as pd

from opendataproduct.config.data_transformation_gold_loader import (
    DataTransformation,
)
from opendataproduct.tracking_decorator import TrackingDecorator


@TrackingDecorator.track_time
def aggregate_data(
    data_transformation: DataTransformation,
    source_path,
    results_path,
    clean=False,
    quiet=False,
):
    already_exists, converted, exception = 0, 0, 0

    for input_port in data_transformation.input_ports or []:
        for file in input_port.files or []:
            source_file_path = os.path.join(
                source_path, input_port.id, file.source_file_name
            )
            target_file_path = os.path.join(
                results_path, input_port.id, file.target_file_name
            )

            if not clean and os.path.exists(target_file_path):
                already_exists += 1
                not quiet and print(f"✓ Already exists {file.target_file_name}")
                continue

            try:
                with open(source_file_path, "r") as csv_file:
                    # Read csv file
                    dataframe = pd.read_csv(csv_file, dtype=str, keep_default_na=False)

                    # Apply trim
                    dataframe = dataframe.applymap(
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
                            dataframe.insert(0, "id", dataframe.pop("id"))

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

                    os.makedirs(os.path.dirname(target_file_path), exist_ok=True)
                    dataframe.to_csv(target_file_path, index=False)
                    converted += 1

                    not quiet and print(
                        f"✓ Convert {os.path.basename(target_file_path)}"
                    )
            except Exception as e:
                exception += 1
                print(f"✗️ Exception: {str(e)}")
    print(
        f"aggregate_data finished with already_exists: {already_exists}, converted: {converted}, exception: {exception}"
    )
