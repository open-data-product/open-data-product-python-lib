import os
from dataclasses import asdict

import yaml
from opendataproduct.config.odps_loader import ODPS, DataHolder, LocalizedInfo
from opendataproduct.config.data_product_manifest_loader import DataProductManifest
from opendataproduct.config.odps_loader import Product
from opendataproduct.tracking_decorator import TrackingDecorator


class IndentDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super(IndentDumper, self).increase_indent(flow, False)


@TrackingDecorator.track_time
def update_odps(
    data_product_manifest: DataProductManifest,
    odps: ODPS,
    config_path,
    output_file_formats: list[str] = [],
):
    odps_path = os.path.join(config_path, "odps.yml")

    odps.schema = "https://opendataproducts.org/v3.1/schema/odps.yaml"
    odps.version = "3.1"

    if odps.product is None:
        odps.product = Product()

    if odps.product.dataHolder is None:
        odps.product.dataHolder = DataHolder()
    odps.product.dataHolder.URL = "https://opendataproduct.de"

    if odps.product.en is None:
        odps.product.en = LocalizedInfo()

    odps.product.en.name = data_product_manifest.metadata.name
    odps.product.en.productID = data_product_manifest.id
    odps.product.en.description = data_product_manifest.metadata.description
    odps.product.en.version = "1.0"
    odps.product.en.tags = data_product_manifest.tags
    odps.product.en.type = "source-aligned"
    odps.product.en.logoURL = f"https://raw.githubusercontent.com/open-data-product/open-data-product-{data_product_manifest.id}/refs/heads/main/logo-with-text.png"
    odps.product.en.OutputFileFormats = (
        output_file_formats if output_file_formats is not None else []
    )

    with open(odps_path, "w") as file:
        yaml.dump(
            asdict(odps),
            file,
            sort_keys=False,
            default_flow_style=False,
            Dumper=IndentDumper,
            allow_unicode=True,
            width=float("inf"),
            explicit_start=True,
        )
