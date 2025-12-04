import os
from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional, Union

import yaml
from dacite import from_dict, Config
from jinja2 import Template

from opendataproduct.tracking_decorator import TrackingDecorator


@dataclass
class SchemaItem:
    name: str
    description: Optional[str]


@dataclass
class Metadata:
    name: str
    owner: str
    description: Optional[str]
    url: Optional[str]
    license: Optional[str]
    updated: Optional[date]
    schema: Optional[List[SchemaItem]] = field(default_factory=list)


@dataclass
class TransformationStep:
    name: str
    path: str
    description: Optional[str]


@dataclass
class MetricFile:
    name: str
    value: float


@dataclass
class QualityMetric:
    name: str
    description: str
    files: Optional[List[MetricFile]] = field(default_factory=list)


@dataclass
class Observability:
    quality: Optional[List[QualityMetric]] = field(default_factory=list)
    operational: Optional[List[str]] = field(default_factory=list)
    slas: Optional[List[str]] = field(default_factory=list)
    security: Optional[List[str]] = field(default_factory=list)


@dataclass
class Term:
    name: str
    description: Optional[str]


@dataclass
class Port:
    id: str


@dataclass
class ExtendedPort(Port):
    metadata: Metadata
    files: Optional[List[str]]


@dataclass
class SimplePort(Port):
    manifest_url: str


@dataclass
class DataProductManifest:
    id: str
    metadata: Metadata
    input_ports: Optional[List[SimplePort | ExtendedPort]] = field(default_factory=list)
    transformation_steps: Optional[List[TransformationStep]] = field(
        default_factory=list
    )
    output_ports: Optional[List[SimplePort | ExtendedPort]] = field(
        default_factory=list
    )
    observability: Optional[Observability] = None
    consumers: Optional[List[str]] = field(default_factory=list)
    use_cases: Optional[List[str]] = field(default_factory=list)
    classification: Optional[str] = None
    ubiquitous_language: Optional[List[Term]] = field(default_factory=list)
    tags: Optional[List[str]] = field(default_factory=list)


def resolve_metadata(data: dict) -> Metadata:
    return from_dict(data_class=Metadata, data=data)


def resolve_port_union(data: dict) -> Union[ExtendedPort, SimplePort]:
    if "manifest_url" in data:
        return from_dict(data_class=SimplePort, data=data)
    if "files" in data:
        return from_dict(data_class=ExtendedPort, data=data)


@TrackingDecorator.track_time
def load_data_product_manifest(
    config_path, file_name="data-product-manifest.yml", context=None
) -> DataProductManifest:
    data_product_manifest_path = os.path.join(config_path, file_name)

    if os.path.exists(data_product_manifest_path):
        with open(data_product_manifest_path, "r") as file:
            config = Config(
                type_hooks={
                    Union[SimplePort, ExtendedPort]: resolve_port_union,
                    Metadata: resolve_metadata,
                }
            )

            if context is None:
                data = yaml.load(file, Loader=yaml.SafeLoader)
            else:
                template = Template(file.read()).render(context)
                data = yaml.load(template, Loader=yaml.SafeLoader)
        return from_dict(data_class=DataProductManifest, data=data, config=config)
    else:
        print(f"✗️ Config file {data_product_manifest_path} does not exist")
