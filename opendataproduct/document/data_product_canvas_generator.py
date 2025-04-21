import os

from opendataproduct.config.data_product_manifest_loader import DataProductManifest
from opendataproduct.tracking_decorator import TrackingDecorator

SCHEMA_AS_TABLE = True


@TrackingDecorator.track_time
def generate_data_product_canvas(
    data_product_manifest: DataProductManifest,
    docs_path,
):
    data_product_canvas_path = os.path.join(docs_path, "data-product-canvas.md")

    content = f"\n# Data Product Canvas - {data_product_manifest.metadata.name}"
    content += "\n"

    if data_product_manifest.metadata:
        content += "\n## Metadata"
        content += "\n"
        content += f"\n* owner: {data_product_manifest.metadata.owner}"

        if data_product_manifest.metadata.description:
            content += f"\n* description: {data_product_manifest.metadata.description}"
        if data_product_manifest.metadata.url:
            content += f"\n* url: {data_product_manifest.metadata.url}"
        if data_product_manifest.metadata.license:
            content += f"\n* license: {data_product_manifest.metadata.license}"
        if data_product_manifest.metadata.updated:
            content += f"\n* updated: {data_product_manifest.metadata.updated}"
        content += "\n"

    if data_product_manifest.input_ports:
        content += "\n## Input Ports"
        content += "\n"

        for port in data_product_manifest.input_ports:
            content += f"\n### {port.metadata.name}"
            content += "\n"
            if port.metadata.owner:
                content += f"\n* owner: {port.metadata.owner}"
            if port.metadata.url:
                content += f"\n* url: {port.metadata.url}"
            if port.metadata.license:
                content += f"\n* license: {port.metadata.license}"
            if port.metadata.updated:
                content += f"\n* updated: {port.metadata.updated}"
            content += "\n"

            if port.metadata.schema:
                content += "\n**Schema**"
                content += "\n"

                if SCHEMA_AS_TABLE:
                    content += "\n| Name | Description |"
                    content += "\n| --- | --- |"

                    for item in port.metadata.schema:
                        content += f"\n| {item.name} | {item.description} |"
                else:
                    for item in port.metadata.schema:
                        content += f"\n* {item.name}: {item.description}"
                content += "\n"

            if port.files:
                content += "\n**Files**"
                content += "\n"

                for file in port.files:
                    content += f"\n* [{file.rsplit('/', 1)[-1]}]({file})"
                content += "\n"

    if data_product_manifest.transformation_steps:
        content += "\n## Transformation Steps"
        content += "\n"

        for transformation_step in data_product_manifest.transformation_steps:
            content += f"\n* [{transformation_step.name}](../{transformation_step.path}) {transformation_step.description}"
        content += "\n"

    if data_product_manifest.output_ports:
        content += "\n## Output Ports"
        content += "\n"

        for port in data_product_manifest.output_ports:
            content += f"\n### {port.metadata.name}"
            content += "\n"
            if port.metadata.owner:
                content += f"\n* owner: {port.metadata.owner}"
            if port.metadata.url:
                content += f"\n* url: {port.metadata.url}"
            if port.metadata.license:
                content += f"\n* license: {port.metadata.license}"
            if port.metadata.updated:
                content += f"\n* updated: {port.metadata.updated}"
            content += "\n"

            if port.metadata.schema:
                content += "\n**Schema**"
                content += "\n"

                if SCHEMA_AS_TABLE:
                    content += "\n| Name | Description |"
                    content += "\n| --- | --- |"

                    for item in port.metadata.schema:
                        content += f"\n| {item.name} | {item.description} |"
                else:
                    for item in port.metadata.schema:
                        content += f"\n* {item.name}: {item.description}"
                content += "\n"

            if port.files:
                content += "\n**Files**"
                content += "\n"

                for file in port.files:
                    content += f"\n* [{file.rsplit('/', 1)[-1]}]({file})"
                content += "\n"

    if data_product_manifest.observability and (
        data_product_manifest.observability.quality
        or data_product_manifest.observability.operational
        or data_product_manifest.observability.slas
        or data_product_manifest.observability.security
    ):
        content += "\n## Observability"
        content += "\n"

        if data_product_manifest.observability.quality:
            content += "\n### Quality metrics"
            content += "\n"

            for quality in data_product_manifest.observability.quality:
                content += f"\n * {quality}"
            content += "\n"

        if data_product_manifest.observability.operational:
            content += "\n### Operational metrics"
            content += "\n"

            for operational in data_product_manifest.observability.operational:
                content += f"\n * {operational}"
            content += "\n"

        if data_product_manifest.observability.slas:
            content += "\n### SLAs"
            content += "\n"

            for sla in data_product_manifest.observability.slas:
                content += f"\n * {sla}"
            content += "\n"

        if data_product_manifest.observability.security:
            content += "\n"
            content += "\n### Security"
            content += "\n"

            for security in data_product_manifest.observability.security:
                content += f"\n * {security}"
            content += "\n"

    if data_product_manifest.consumers:
        content += "\n## Consumers"
        content += "\n"

        content += "\n**Who is the consumer of the Data Product?**"
        content += "\n"

        content += "\nConsumers of this data product may include"
        content += "\n"

        for consumer in data_product_manifest.consumers:
            content += f"\n* {consumer}"
        content += "\n"

    if data_product_manifest.use_cases:
        content += "\n## Use Cases"
        content += "\n"

        content += "\n**We believe that ...**"
        content += "\n**We help achieving ...**"
        content += "\n**We know, we are getting there based on ..., ..., ...**"
        content += "\n"

        content += "\nWe believe that this data product can be used"
        content += "\n"

        for use_case in data_product_manifest.use_cases:
            content += f"\n* {use_case}"
        content += "\n"

    if data_product_manifest.classification:
        content += "\n## Classification"
        content += "\n"

        content += "\n**The nature of the exposed data (source-aligned, aggregate, consumer-aligned)**"
        content += "\n"

        content += f"\n{data_product_manifest.classification}"
        content += "\n"

    if data_product_manifest.ubiquitous_language:
        content += "\n## Ubiquitous Language"
        content += "\n"

        content += "\n**Context-specific domain terminology (relevant for Data Product), Data Product polysemes which are used to create the current Data Product**"
        content += "\n"

        if SCHEMA_AS_TABLE:
            content += "\n| Name | Description |"
            content += "\n| --- | --- |"

            for term in data_product_manifest.ubiquitous_language:
                content += f"\n| {term.name} | {term.description} |"
        else:
            for term in data_product_manifest.ubiquitous_language:
                content += f"\n* **{term.name}** {term.description}"

    content += "\n"
    content += "\n---"
    content += "\nThis data product canvas uses the template of [datamesh-architecture.com](https://www.datamesh-architecture.com/data-product-canvas)."

    with open(data_product_canvas_path, "w") as file:
        file.write(content)
