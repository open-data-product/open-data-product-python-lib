import os

from opendataproduct.config.dpds_loader import DataProductDescriptor
from opendataproduct.tracking_decorator import TrackingDecorator

SCHEMA_AS_TABLE = True


@TrackingDecorator.track_time
def generate_dpds_canvas(
    dpds: DataProductDescriptor,
    docs_path,
):
    dpds_canvas_path = os.path.join(docs_path, "dpds-canvas.md")

    content = (
        f"\n# Data Product Descriptor Specification (DPDS) Canvas - {dpds.info.name}"
    )
    content += "\n"
    content += f"\n* data product descriptor: {dpds.dataProductDescriptor}"
    content += "\n"

    if dpds.info:
        content += "\n## Info"
        content += "\n"

        if dpds.info.id:
            content += f"\n* ID: {dpds.info.id}"
        if dpds.info.fullyQualifiedName:
            content += f"\n* fully qualified name: {dpds.info.fullyQualifiedName}"
        if dpds.info.entityType:
            content += f"\n* entity type: {dpds.info.entityType}"
        if dpds.info.name:
            content += f"\n* name: {dpds.info.name}"
        if dpds.info.version:
            content += f"\n* version: {dpds.info.version}"
        if dpds.info.displayName:
            content += f"\n* display name: {dpds.info.displayName}"
        if dpds.info.description:
            content += f"\n* description: {dpds.info.description}"
        if dpds.info.domain:
            content += f"\n* domain: {dpds.info.domain}"
        if dpds.info.owner:
            content += "\n### Owner"
            content += "\n"
            content += f"\n* id: {dpds.info.owner.id}"
            content += f"\n* name: {dpds.info.owner.name}"
        content += "\n"

        for contact_point in dpds.info.contactPoints or []:
            content += "\n## Contact Point"
            content += "\n"

            if contact_point.name:
                content += f"\n* name: {contact_point.name}"
            if contact_point.description:
                content += f"\n* description: {contact_point.description}"
            if contact_point.channel:
                content += f"\n* channel: {contact_point.channel}"
            if contact_point.address:
                content += f"\n* address: {contact_point.address}"
            content += "\n"

    if dpds.interfaceComponents:
        content += "\n## Interface Components"
        content += "\n"

        if dpds.interfaceComponents.inputPorts:
            content += "\n### Input Ports"
            content += "\n"

            for input_port in dpds.interfaceComponents.inputPorts or []:
                content += build_entity(input_port)
                content += build_port(input_port)

        if dpds.interfaceComponents.outputPorts:
            content += "\n### Output Ports"
            content += "\n"

            for output_port in dpds.interfaceComponents.outputPorts or []:
                content += build_entity(output_port)
                content += build_port(output_port)

        if dpds.interfaceComponents.discoveryPorts:
            content += "\n### Discovery Ports"
            content += "\n"

            for discovery_port in dpds.interfaceComponents.discoveryPorts or []:
                content += build_entity(discovery_port)
                content += build_port(discovery_port)

        if dpds.interfaceComponents.observabilityPorts:
            content += "\n### Observability Ports"
            content += "\n"

            for observability_port in dpds.interfaceComponents.observabilityPorts or []:
                content += build_entity(observability_port)
                content += build_port(observability_port)

        if dpds.interfaceComponents.controlPorts:
            content += "\n### Control Ports"
            content += "\n"

            for control_port in dpds.interfaceComponents.controlPorts or []:
                content += build_entity(control_port)
                content += build_port(control_port)

    if dpds.internalComponents:
        content += "\n## Internal Components"
        content += "\n"

        if dpds.internalComponents.lifecycleInfo:
            content += "\n### Lifecycle Info"
            content += "\n"

            for _, lifecycle_info in (
                dpds.internalComponents.lifecycleInfo.items() or {}
            ):
                content += build_additional_properties(lifecycle_info)

        if dpds.internalComponents.applicationComponents:
            content += "\n### Application Components"
            content += "\n"

            for application_component in (
                dpds.internalComponents.applicationComponents or []
            ):
                content += build_entity(application_component)

                if application_component.platform:
                    content += f"\n* platform: {application_component.platform}"
                if application_component.applicationType:
                    content += (
                        f"\n* application type: {application_component.applicationType}"
                    )
                if application_component.consumesFrom:
                    content += f"\n* consumes from: {', '.join(application_component.consumesFrom)}"
                if application_component.providesTo:
                    content += f"\n* provides to: {', '.join(application_component.providesTo)}"
                if application_component.dependsOn:
                    content += (
                        f"\n* depends on: {', '.join(application_component.dependsOn)}"
                    )
                content += "\n"

        if dpds.internalComponents.infrastructuralComponents:
            content += "\n### Infrastructure Components"
            content += "\n"

            for infrastructure_component in (
                dpds.internalComponents.infrastructuralComponents or []
            ):
                content += build_entity(infrastructure_component)

                if infrastructure_component.platform:
                    content += f"\n* platform: {infrastructure_component.platform}"
                if infrastructure_component.infrastructureType:
                    content += f"\n* infrastructure type: {infrastructure_component.infrastructureType}"
                if infrastructure_component.dependsOn:
                    content += f"\n* depends on: {', '.join(infrastructure_component.dependsOn)}"
                content += "\n"

    if dpds.components:
        content += "\n## Components"
        content += "\n"

        if dpds.components.inputPorts:
            content += "\n### Input Ports"
            content += "\n"

            for _, input_port in dpds.components.inputPorts.items() or {}:
                content += build_entity(input_port)
                content += build_port(input_port)

        if dpds.components.inputPorts:
            content += "\n### Input Ports"
            content += "\n"

            for _, input_port in dpds.components.inputPorts.items() or {}:
                content += build_entity(input_port)
                content += build_port(input_port)

        if dpds.components.outputPorts:
            content += "\n### Output Ports"
            content += "\n"

            for _, output_port in dpds.components.outputPorts.items() or {}:
                content += build_entity(output_port)
                content += build_port(output_port)

        if dpds.components.discoveryPorts:
            content += "\n### Discovery Ports"
            content += "\n"

            for _, discovery_port in dpds.components.discoveryPorts.items() or {}:
                content += build_entity(discovery_port)
                content += build_port(discovery_port)

        if dpds.components.observabilityPorts:
            content += "\n### Observability Ports"
            content += "\n"

            for _, observability_port in (
                dpds.components.observabilityPorts.items() or {}
            ):
                content += build_entity(observability_port)
                content += build_port(observability_port)

        if dpds.components.controlPorts:
            content += "\n### Control Ports"
            content += "\n"

            for _, control_port in dpds.components.controlPorts.items() or {}:
                content += build_entity(control_port)
                content += build_port(control_port)

        if dpds.internalComponents.applicationComponents:
            content += "\n### Application Components"
            content += "\n"

            for _, application_component in (
                dpds.components.applicationComponents.items() or {}
            ):
                content += build_entity(application_component)

                if application_component.platform:
                    content += f"\n* platform: {application_component.platform}"
                if application_component.applicationType:
                    content += (
                        f"\n* application type: {application_component.applicationType}"
                    )
                if application_component.consumesFrom:
                    content += f"\n* consumes from: {', '.join(application_component.consumesFrom)}"
                if application_component.providesTo:
                    content += f"\n* provides to: {', '.join(application_component.providesTo)}"
                if application_component.dependsOn:
                    content += (
                        f"\n* depends on: {', '.join(application_component.dependsOn)}"
                    )
                content += "\n"

        if dpds.internalComponents.infrastructuralComponents:
            content += "\n### Infrastructure Components"
            content += "\n"

            for _, infrastructure_component in (
                dpds.components.infrastructuralComponents.items() or []
            ):
                content += build_entity(infrastructure_component)

                if infrastructure_component.platform:
                    content += f"\n* platform: {infrastructure_component.platform}"
                if infrastructure_component.infrastructureType:
                    content += f"\n* infrastructure type: {infrastructure_component.infrastructureType}"
                if infrastructure_component.dependsOn:
                    content += f"\n* depends on: {', '.join(infrastructure_component.dependsOn)}"
                content += "\n"

    if dpds.tags:
        content += "\n## Tags"
        content += "\n"

        for tag in dpds.tags or []:
            content += f"\n* {tag}"

    if dpds.externalDocs:
        content += "\n## External Docs"
        content += "\n"

        content += build_external_docs(dpds.externalDocs)

    content += "\n"
    content += "\n---"
    content += "\nThis Data Product Descriptor Specification canvas is based on [Data Product Descriptor Specification 1.0.0](https://dpds.opendatamesh.org/specifications/dpds/1.0.0/)."

    with open(dpds_canvas_path, "w") as file:
        file.write(content)


def build_entity(entity):
    content = ""

    if entity.id:
        content += f"#### {entity.id}"
    if entity.fullyQualifiedName:
        content += f"\n* fully qualified name: {entity.fullyQualifiedName}"
    if entity.entityType:
        content += f"\n* entity type: {entity.entityType}"
    if entity.name:
        content += f"\n* name: {entity.name}"
    if entity.version:
        content += f"\n* version: {entity.version}"
    if entity.displayName:
        content += f"\n* display name: {entity.displayName}"
    if entity.description:
        content += f"\n* description: {entity.description}"
    if entity.componentGroup:
        content += f"\n* component group: {entity.componentGroup}"
    if entity.tags:
        content += f"\n* tags: {", ".join(entity.tags)}"
    if entity.externalDocs:
        content += "\n#### External Docs"
        content += "\n"

        content += build_external_docs(entity.externalDocs)
    content += "\n"

    return content


def build_external_docs(external_docs):
    content = ""

    if external_docs.description:
        content += f"\n* description: {external_docs.description}"
    if external_docs.mediaType:
        content += f"\n* media type: {external_docs.mediaType}"
    if external_docs.href:
        content += f"\n* href: <a href='{external_docs.href} target='_blank'>{external_docs.href}</a>"
    content += "\n"

    return content


def build_port(port):
    content = ""

    if port.promises:
        content += "\n#### Promises"
        content += "\n"

        if port.promises.platform:
            content += f"\n* platform: {port.promises.platform}"
        if port.promises.servicesType:
            content += f"\n* service type: {port.promises.servicesType}"

        if port.promises.api:
            content += "\n##### API"
            content += "\n"

            content += build_entity(port.promises.api)
            content += build_definition(port.promises.api)
        content += "\n"

    if port.expectations:
        content += "\n#### Expectations"
        content += "\n"

        if port.expectations.audience:
            content += "\n##### Audience"
            content += "\n"

            content += build_entity(port.expectations.audience)
            content += build_definition(port.expectations.audience)

        if port.expectations.usage:
            content += "\n##### Usage"
            content += "\n"

            content += build_entity(port.expectations.usage)
            content += build_definition(port.expectations.usage)
        content += "\n"

    if port.obligations:
        content += "\n#### Obligations"
        content += "\n"

        if port.obligations.termsAndConditions:
            content += "\n##### Terms and Conditions"
            content += "\n"

            content += build_entity(port.obligations.termsAndConditions)
            content += build_definition(port.obligations.termsAndConditions)

        if port.obligations.billingPolicy:
            content += "\n##### Billing Policy"
            content += "\n"

            content += build_entity(port.obligations.billingPolicy)
            content += build_definition(port.obligations.billingPolicy)

        if port.obligations.sla:
            content += "\n##### SLA"
            content += "\n"

            content += build_entity(port.obligations.sla)
            content += build_definition(port.obligations.sla)
        content += "\n"

    return content


def build_definition(definition):
    content = ""

    if definition.specification:
        content += f"\n* specification: {definition.specification}"
    if definition.specificationVersion:
        content += f"\n* specification version: {definition.specificationVersion}"
    if definition.definition:
        content += f"\n* definition: {definition.definition}"
    content += "\n"

    return content


def build_additional_properties(additional_properties):
    content = ""

    if additional_properties.name:
        content += f"\n* name: {additional_properties.name}"
    if additional_properties.order:
        content += f"\n* order: {additional_properties.order}"
    if additional_properties.service:
        content += f"\n* service: {additional_properties.service}"
    if additional_properties.template:
        content += f"\n* template: {additional_properties.template}"
    if additional_properties.configurations:
        content += f"\n* configurations: {additional_properties.configurations}"
    content += "\n"

    return content
