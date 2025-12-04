import os
from dataclasses import asdict

import yaml
from attr.validators import instance_of
from opendataproduct.config.data_product_manifest_loader import ExtendedPort
from opendataproduct.tracking_decorator import TrackingDecorator

from opendataproduct.config.dpds_loader import (
    Info,
    Owner,
    InterfaceComponents,
    OutputPort,
    InputPort,
)


class IndentDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super(IndentDumper, self).increase_indent(flow, False)


@TrackingDecorator.track_time
def update_dpds(data_product_manifest, dpds, config_path):
    dpds_path = os.path.join(config_path, "dpds.yml")

    if dpds.info is None:
        dpds.info = Info()
    if dpds.info.owner is None:
        dpds.info.owner = Owner()

    dpds.info.id = data_product_manifest.id
    dpds.info.fullyQualifiedName = (
        f"urn:dpds:openlifeworlds:dataproducts:{data_product_manifest.id}:1.0"
    )
    dpds.info.name = data_product_manifest.metadata.name
    dpds.info.version = "1.0"
    dpds.info.owner.id = "Open Lifeworlds"
    dpds.info.owner.name = "Open Lifeworlds"
    dpds.info.displayName = data_product_manifest.metadata.name
    dpds.info.description = data_product_manifest.metadata.description

    if dpds.interfaceComponents is None:
        dpds.interfaceComponents = InterfaceComponents()

    dpds.interfaceComponents.outputPorts = []
    dpds.interfaceComponents.inputPorts = []

    for port in data_product_manifest.output_ports:
        output_port = OutputPort()
        output_port.id = port.id
        output_port.fullyQualifiedName = port.id
        output_port.description = port.metadata.description

        dpds.interfaceComponents.outputPorts.append(output_port)

    for port in data_product_manifest.input_ports:
        input_port = InputPort()
        input_port.id = port.id
        input_port.fullyQualifiedName = port.id
        if isinstance(port, ExtendedPort):
            input_port.description = port.metadata.description

        dpds.interfaceComponents.inputPorts.append(input_port)

    with open(dpds_path, "w") as file:
        yaml.dump(
            asdict(dpds),
            file,
            sort_keys=False,
            default_flow_style=False,
            Dumper=IndentDumper,
            allow_unicode=True,
            width=float("inf"),
            explicit_start=True,
        )
