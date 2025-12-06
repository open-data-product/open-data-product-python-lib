import os
import warnings

import pandas as pd

from opendataproduct.config.data_transformation_silver_loader import (
    DataTransformation,
)
from opendataproduct.tracking_decorator import TrackingDecorator

warnings.simplefilter(action="ignore", category=FutureWarning)


@TrackingDecorator.track_time
def convert_data_to_csv(
    data_transformation: DataTransformation,
    source_path,
    results_path,
    encoding="utf-8",
    delimiter=",",
    clean=False,
    quiet=False,
):
    already_exists, converted, exception = 0, 0, 0

    for input_port in data_transformation.input_ports or []:
        for file in input_port.files or []:
            for dataset in file.datasets or []:
                source_file_path = os.path.join(
                    source_path, input_port.id, file.target_file_name
                )
                target_file_path = os.path.join(
                    results_path, input_port.id, dataset.target_file_name
                )

                if not clean and os.path.exists(target_file_path):
                    already_exists += 1
                    not quiet and print(f"✓ Already exists {dataset.target_file_name}")
                    continue

                _, source_file_extension = os.path.splitext(source_file_path)
                engine = "openpyxl" if source_file_extension == ".xlsx" else None

                try:
                    _, extension = os.path.splitext(source_file_path)

                    if extension in [".xlsx", ".xls"]:
                        # Read Excel file
                        dataframe = pd.read_excel(
                            source_file_path,
                            engine=engine,
                            sheet_name=str(dataset.sheet_name),
                            header=dataset.header,
                            names=[name.name for name in dataset.names],
                            usecols=list(
                                range(
                                    dataset.skip_cols,
                                    dataset.skip_cols + len(dataset.names),
                                )
                            ),
                            skiprows=dataset.skip_rows,
                            keep_default_na=False,
                        )
                    elif extension == ".csv":
                        # Read CSV file
                        dataframe = pd.read_csv(
                            source_file_path,
                            header=dataset.header,
                            names=[name.name for name in dataset.names],
                            usecols=list(
                                range(
                                    dataset.skip_cols,
                                    dataset.skip_cols + len(dataset.names),
                                )
                            ),
                            skiprows=dataset.skip_rows,
                            keep_default_na=False,
                            encoding=encoding,
                            delimiter=delimiter,
                        )
                    else:
                        raise ValueError(
                            f"✗️ Unsupported file format: {extension}. Only .xlsx, .xls, and .csv are supported."
                        )

                    names = dataset.names

                    # Apply dropna
                    if dataset.dropna:
                        dataframe = dataframe[~(dataframe == "").any(axis=1)]

                    # Replace line breaks
                    dataframe = dataframe.applymap(
                        lambda x: x.replace("\n", "").replace("\r", "")
                        if isinstance(x, str)
                        else x
                    )

                    # Apply trim
                    dataframe = dataframe.applymap(
                        lambda col: col.strip() if isinstance(col, str) else col
                    )

                    # Apply data type
                    dataframe = dataframe.astype(
                        {name.name: name.type for name in names if not name.remove},
                        errors="ignore",
                    )

                    # Apply filter
                    dataframe = dataframe.filter(
                        items=[name.name for name in names if not name.remove]
                    )

                    # Apply zfill
                    dataframe = (
                        dataframe[[name.name for name in names if not name.remove]]
                        .astype(str)
                        .apply(
                            lambda col: col.str.zfill(
                                next(
                                    name.zfill if name.zfill is not None else 0
                                    for name in names
                                    if name.name == col.name
                                )
                            )
                        )
                    )

                    # Apply lstrip
                    dataframe = (
                        dataframe[[name.name for name in names if not name.remove]]
                        .astype(str)
                        .apply(
                            lambda col: col.str.lstrip(
                                next(
                                    name.lstrip if name.lstrip is not None else ""
                                    for name in names
                                    if name.name == col.name
                                )
                            )
                        )
                    )

                    # Apply value mapping
                    for name in [
                        name for name in names if name.value_mapping is not None
                    ]:
                        dataframe[name.name] = dataframe[name.name].map(
                            name.value_mapping
                        )

                    # Apply format
                    for name in [
                        name for name in names if name.format == "phone_number"
                    ]:
                        dataframe[name.name] = dataframe[name.name].apply(
                            lambda row: build_phone_number(row),
                        )
                    for name in [
                        name for name in names if name.format == "coordinate"
                    ]:
                        dataframe[name.name] = dataframe[name.name].apply(
                            lambda row: row.replace('\"', '').replace(",", "."),
                        )

                    # Apply head
                    if dataset.head:
                        dataframe = dataframe.head(dataset.head)

                    # Apply removal of empty rows
                    dataframe = dataframe.replace("davon", "")
                    dataframe = dataframe[~(dataframe == "").all(axis=1)]
                    dataframe = dataframe.dropna(how="all")

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
        f"convert_data_to_csv finished with already_exists: {already_exists}, converted: {converted}, exception: {exception}"
    )


def build_phone_number(row):
    phone_number = f"{row.replace(' ', '').replace('/', '').replace('-', '').lstrip('‭').rstrip('‬').lstrip('030').lstrip('(030)').replace('------', '')}"
    return f"+4930{phone_number}" if len(phone_number) > 0 else ""
