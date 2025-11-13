from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, List, Dict, Union, Any

import yaml
from dacite import from_dict
from yaml import MappingNode
from yaml.constructor import ConstructorError

from opendataproduct.tracking_decorator import TrackingDecorator


@dataclass
class Reference:
    ref: str
    mediaType: Optional[str] = None
    description: Optional[str] = None


@dataclass
class ExternalResource:
    href: str
    description: Optional[str] = None
    mediaType: Optional[str] = None


@dataclass
class EntityMixin:
    id: Optional[str] = None
    fullyQualifiedName: Optional[str] = None
    entityType: Optional[str] = None
    name: str = ""
    version: Optional[str] = None
    displayName: Optional[str] = None
    description: Optional[str] = None
    componentGroup: Optional[str] = None
    tags: Optional[List[str]] = None
    externalDocs: Optional[ExternalResource] = None


@dataclass
class DefinitionMixin:
    specification: str
    specificationVersion: Optional[str] = None
    definition: Optional[Union[Dict[str, Any], str, Reference]] = None


@dataclass
class StandardDefinition(EntityMixin, DefinitionMixin):
    pass


@dataclass
class Owner:
    id: str
    name: Optional[str] = None


@dataclass
class ContactPoint:
    name: Optional[str] = None
    description: Optional[str] = None
    channel: Optional[str] = None
    address: Optional[str] = None


@dataclass
class Info:
    fullyQualifiedName: str
    name: str
    version: str
    domain: str
    owner: Owner
    id: Optional[str] = None
    entityType: Optional[str] = None
    displayName: Optional[str] = None
    description: Optional[str] = None
    contactPoints: Optional[List[ContactPoint]] = None


@dataclass
class Promises:
    platform: Optional[str] = None
    servicesType: Optional[str] = None
    api: Optional[StandardDefinition] = None
    deprecationPolicy: Optional[StandardDefinition] = None
    slo: Optional[StandardDefinition] = None


@dataclass
class Expectations:
    audience: Optional[StandardDefinition] = None
    usage: Optional[StandardDefinition] = None


@dataclass
class Obligations:
    termsAndConditions: Optional[StandardDefinition] = None
    billingPolicy: Optional[StandardDefinition] = None
    sla: Optional[StandardDefinition] = None


@dataclass
class PortMixin:
    promises: Optional[Union[Promises, Reference]] = None
    expectations: Optional[Union[Expectations, Reference]] = None
    obligations: Optional[Union[Obligations, Reference]] = None


@dataclass
class InputPort(EntityMixin, PortMixin):
    pass


@dataclass
class OutputPort(EntityMixin, PortMixin):
    pass


@dataclass
class DiscoveryPort(EntityMixin, PortMixin):
    pass


@dataclass
class ObservabilityPort(EntityMixin, PortMixin):
    pass


@dataclass
class ControlPort(EntityMixin, PortMixin):
    pass


@dataclass
class ApplicationComponent(EntityMixin):
    platform: Optional[str] = None
    applicationType: Optional[str] = None
    consumesFrom: Optional[List[str]] = None
    providesTo: Optional[List[str]] = None
    dependsOn: Optional[List[str]] = None


@dataclass
class InfrastructuralComponent(EntityMixin):
    platform: Optional[str] = None
    infrastructureType: Optional[str] = None
    dependsOn: Optional[List[str]] = None


@dataclass
class LifecycleTaskInfo:
    name: Optional[str] = None
    order: Optional[float] = None
    service: Optional[ExternalResource] = None
    template: Optional[Union[StandardDefinition, Reference]] = None
    configurations: Optional[Union[Dict[str, Any], Reference]] = None


@dataclass
class InterfaceComponents:
    outputPorts: List[Union[OutputPort, Reference]]
    inputPorts: Optional[List[Union[InputPort, Reference]]] = None
    discoveryPorts: Optional[List[Union[DiscoveryPort, Reference]]] = None
    observabilityPorts: Optional[List[Union[ObservabilityPort, Reference]]] = None
    controlPorts: Optional[List[Union[ControlPort, Reference]]] = None


@dataclass
class InternalComponents:
    lifecycleInfo: Optional[Dict[str, List[LifecycleTaskInfo]]] = None
    applicationComponents: Optional[List[Union[ApplicationComponent, Reference]]] = None
    infrastructuralComponents: Optional[
        List[Union[InfrastructuralComponent, Reference]]
    ] = None


@dataclass
class Components:
    inputPorts: Optional[Dict[str, Union[InputPort, Reference]]] = None
    outputPorts: Optional[Dict[str, Union[OutputPort, Reference]]] = None
    discoveryPorts: Optional[Dict[str, Union[DiscoveryPort, Reference]]] = None
    observabilityPorts: Optional[Dict[str, Union[ObservabilityPort, Reference]]] = None
    controlPorts: Optional[Dict[str, Union[ControlPort, Reference]]] = None
    applicationComponents: Optional[
        Dict[str, Union[ApplicationComponent, Reference]]
    ] = None
    infrastructuralComponents: Optional[
        Dict[str, Union[InfrastructuralComponent, Reference]]
    ] = None


# The top-level Data Product Descriptor
@dataclass
class DataProductDescriptor:
    dataProductDescriptor: str
    info: Info
    interfaceComponents: InterfaceComponents
    internalComponents: Optional[InternalComponents] = None
    components: Optional[Components] = None
    tags: Optional[List[str]] = None
    externalDocs: Optional[ExternalResource] = None


class Loader(yaml.SafeLoader):
    """
    Customer loader that makes sure that some fields are read in a raw format
    """

    raw_fields = []

    def construct_mapping(self, node, deep=False):
        if not isinstance(node, MappingNode):
            raise ConstructorError(
                None,
                None,
                f"expected a mapping node, but found {node.id}",
                node.start_mark,
            )
        mapping = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)

            if key in self.raw_fields:
                value = value_node.value
                mapping[key] = value
                continue
            else:
                value = self.construct_object(value_node, deep=deep)
                mapping[key] = value

        return mapping


@TrackingDecorator.track_time
def load_dpds(config_path) -> DataProductDescriptor:
    dpds = os.path.join(config_path, "dpds.yml")

    if os.path.exists(dpds):
        with open(dpds, "r") as file:
            data = yaml.load(file, Loader=Loader)
        return from_dict(data_class=DataProductDescriptor, data=data)
    else:
        print(f"✗️ Config file {dpds} does not exist")
        return DataProductDescriptor(
            dataProductDescriptor="data-product-descriptor",
            info=Info(
                fullyQualifiedName="fully-qualified-name",
                name="name",
                version="1.0",
                domain="domain",
                owner=Owner(id="id", name="name"),
            ),
            interfaceComponents=InterfaceComponents(outputPorts=[]),
        )
