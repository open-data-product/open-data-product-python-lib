import os

import nbformat
import nbformat as nbf
from nbconvert.preprocessors import ExecutePreprocessor

from opendataproduct.config.data_product_manifest_loader import DataProductManifest
from opendataproduct.tracking_decorator import TrackingDecorator


@TrackingDecorator.track_time
def create_jupyter_notebook_for_csv(
    data_product_manifest: DataProductManifest,
    data_path,
    results_path,
    clean=False,
    quiet=False,
):
    # Make results path
    os.makedirs(os.path.join(results_path), exist_ok=True)

    target_file_path = os.path.join(results_path, "main.ipynb")

    if not clean and os.path.exists(target_file_path):
        not quiet and print(f"✓ Already exists {os.path.basename(target_file_path)}")
        return

    #
    # Create
    #

    # Create a new notebook object
    nb = nbf.v4.new_notebook()

    # Identify latest csv file
    csv_file_path_relative = get_latest_csv_file(os.walk(data_path)).replace(
        results_path, "."
    )

    if csv_file_path_relative is None:
        print(
            f"✗️ Warning: no csv file found in {os.path.join(results_path, "data", "03-gold")}"
        )
        return

    # Add the cells to the notebook
    nb["cells"] = [
        nbf.v4.new_markdown_cell(
            f"# {data_product_manifest.metadata.name} - Sample Notebook"
        ),
        nbf.v4.new_markdown_cell("## Explore data"),
        nbf.v4.new_code_cell(f"""
import pandas as pd        

# Read csv file
dataframe = pd.read_csv("{csv_file_path_relative}")
            """),
        nbf.v4.new_code_cell("""
# Display 5 first rows
dataframe.head()
            """),
        nbf.v4.new_code_cell("""
# Display dataframe info
dataframe.info()
            """),
        nbf.v4.new_code_cell("""
# Describe dataframe
dataframe.describe()
            """),
    ]

    not quiet and print(f"✓ Create {os.path.basename(target_file_path)}")

    # Write the notebook to a file
    with open(target_file_path, "w", encoding="utf-8") as file:
        nbf.write(nb, file)

    #
    # Run
    #

    with open(target_file_path) as f:
        nb_in = nbformat.read(f, nbformat.NO_CONVERT)

    # Run notebook
    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    nb_out, resources = ep.preprocess(nb_in, {"metadata": {"path": "./"}})

    # Write the notebook to a file
    with open(target_file_path, "w", encoding="utf-8") as f:
        nbformat.write(nb_out, f)

    not quiet and print(f"✓ Run {os.path.basename(target_file_path)}")


def get_latest_csv_file(search_path):
    for subdir, dirs, files in sorted(search_path, reverse=True):
        for file in [
            file_name
            for file_name in sorted(files, reverse=True)
            if file_name.endswith(".csv")
        ]:
            return os.path.join(subdir, file)


@TrackingDecorator.track_time
def create_jupyter_notebook_for_geojson(
    data_product_manifest: DataProductManifest,
    data_path,
    results_path,
    map_location=[52.516388888889, 13.377777777778],
    map_zoom_start=10,
    clean=False,
    quiet=False,
):
    # Make results path
    os.makedirs(os.path.join(results_path), exist_ok=True)

    target_file_path = os.path.join(results_path, "main.ipynb")

    if not clean and os.path.exists(target_file_path):
        not quiet and print(f"✓ Already exists {os.path.basename(target_file_path)}")
        return

    #
    # Create
    #

    # Create a new notebook object
    nb = nbf.v4.new_notebook()

    # Identify latest csv file
    geojson_file_path_relative = get_latest_geojson_file(os.walk(data_path)).replace(
        results_path, "."
    )

    if geojson_file_path_relative is None:
        print(
            f"✗️ Warning: no csv file found in {os.path.join(results_path, "data", "03-gold")}"
        )
        return

    # Add the cells to the notebook
    nb["cells"] = [
        nbf.v4.new_markdown_cell(
            f"# {data_product_manifest.metadata.name} - Sample Notebook"
        ),
        nbf.v4.new_markdown_cell("## Explore data"),
        nbf.v4.new_code_cell(f"""
import folium
import json
from IPython.display import display

# Read geojson file
with open(
    file="{geojson_file_path_relative}", mode="r", encoding="utf-8"
) as geojson_file:
    geojson = json.load(geojson_file, strict=False)
            """),
        nbf.v4.new_code_cell(f"""
# Create a folium map
m = folium.Map(location={map_location}, zoom_start={map_zoom_start})
            """),
        nbf.v4.new_code_cell("""
# Add the GeoJSON layer to the map
folium.GeoJson(
    geojson,
    name='GeoJSON Layer',
    style_function=lambda x: {
        'fillColor': 'blue',
        'color': 'black',
        'weight': 1,
        'fillOpacity': 0.5
    }
).add_to(m)
            """),
        nbf.v4.new_code_cell("""
display(m)
            """),
    ]

    not quiet and print(f"✓ Create {os.path.basename(target_file_path)}")

    # Write the notebook to a file
    with open(target_file_path, "w", encoding="utf-8") as file:
        nbf.write(nb, file)

    #
    # Run
    #

    with open(target_file_path) as f:
        nb_in = nbformat.read(f, nbformat.NO_CONVERT)

    # Run notebook
    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    nb_out, resources = ep.preprocess(nb_in, {"metadata": {"path": "./"}})

    # Write the notebook to a file
    with open(target_file_path, "w", encoding="utf-8") as f:
        nbformat.write(nb_out, f)

    not quiet and print(f"✓ Run {os.path.basename(target_file_path)}")


def get_latest_geojson_file(search_path):
    for subdir, dirs, files in sorted(search_path, reverse=True):
        for file in [
            file_name
            for file_name in sorted(files, reverse=True)
            if file_name.endswith(".geojson")
        ]:
            return os.path.join(subdir, file)
