import os

from opendataproduct.config.odps_loader import ODPS
from opendataproduct.tracking_decorator import TrackingDecorator

SCHEMA_AS_TABLE = True


@TrackingDecorator.track_time
def generate_odps_canvas(
    odps: ODPS,
    docs_path,
):
    odps_canvas_path = os.path.join(docs_path, "odps-canvas.md")

    content = (
        f"\n# Open Data Product Specification (ODPS) Canvas - {odps.product.en.name}"
    )
    content += "\n"
    content += f"\n* schema version: {odps.version}"

    if odps.details:
        content += "\n## Details"
        content += "\n"

        if odps.details.summary:
            content += f"\n* summary: {odps.details.summary}"
        if odps.details.description:
            content += f"\n* description: {odps.details.description}"
        if odps.details.language:
            content += f"\n* language: {odps.details.language}"
        if odps.details.metadata and len(odps.details.metadata) > 0:
            content += "\n* metadata:"
            for key, value in odps.details.metadata.items():
                content += f"\n  * {key}: {value}"
        content += "\n"

    if odps.product:
        content += "\n## Product"
        content += "\n"

        if odps.product.en:
            content += "\n### Basic Information"
            content += "\n"

            if (
                odps.product.en.OutputFileFormats
                and len(odps.product.en.OutputFileFormats) > 0
            ):
                content += f"\n* output file formats: {', '.join(odps.product.en.OutputFileFormats)}"
            if odps.product.en.brandSlogan:
                content += f"\n* brand slogan: {odps.product.en.brandSlogan}"
            if odps.product.en.categories and len(odps.product.en.categories) > 0:
                content += f"\n* categories: {', '.join(odps.product.en.categories)}"
            if odps.product.en.description:
                content += f"\n* description: {odps.product.en.description}"
            if odps.product.en.logoURL:
                content += f"\n* logo URL: {odps.product.en.logoURL}"
            if odps.product.en.productID:
                content += f"\n* product ID: {odps.product.en.productID}"
            if odps.product.en.productSeries:
                content += f"\n* product series: {odps.product.en.productSeries}"
            if odps.product.en.standards:
                content += f"\n* standards: {', '.join(odps.product.en.standards)}"
            if odps.product.en.status:
                content += f"\n* status: {odps.product.en.status}"
            if odps.product.en.tags:
                content += f"\n* tags: {', '.join(odps.product.en.tags)}"
            if odps.product.en.type:
                content += f"\n* type: {odps.product.en.type}"
            if odps.product.en.useCases:
                content += "\n* use cases:"
                for use_case in odps.product.en.useCases:
                    content += f"\n  * title: {use_case.useCase.useCaseTitle}"
                    content += (
                        f"\n    * description: {use_case.useCase.useCaseDescription}"
                    )
                    content += f"\n    * URL: {use_case.useCase.useCaseURL}"
            if odps.product.en.valueProposition:
                content += f"\n* value proposition: {odps.product.en.valueProposition}"
            if odps.product.en.version:
                content += f"\n* version: {odps.product.en.version}"
            if odps.product.en.visibility:
                content += f"\n* visibility: {odps.product.en.visibility}"
            content += "\n"

        if odps.product.sla and len(odps.product.sla) > 0:
            content += "\n### Service Level Agreements (SLA)"
            content += "\n"
            for sla in odps.product.sla:
                if sla.displaytitle and len(sla.displaytitle) > 0:
                    content += f"\n#### {sla.displaytitle[0].en}"
                    content += "\n"
                    if sla.dimension:
                        content += f"\n* dimension: {sla.dimension}"
                    if sla.monitoring:
                        content += "\n* monitoring:"
                        if sla.monitoring.reference:
                            content += f"\n  * reference: {sla.monitoring.reference}"
                        if sla.monitoring.spec:
                            content += f"\n  * spec: {sla.monitoring.spec}"
                        if sla.monitoring.type:
                            content += f"\n  * type: {sla.monitoring.type}"
                    if sla.objective:
                        content += f"\n* objective: {sla.objective}"
                    if sla.unit:
                        content += f"\n* unit: {sla.unit}"
                content += "\n"

        if odps.product.dataAccess:
            content += "\n### Data Access"
            content += "\n"
            if odps.product.dataAccess.authenticationMethod:
                content += f"\n* authentication method: {odps.product.dataAccess.authenticationMethod}"
            if odps.product.dataAccess.documentationURL:
                content += (
                    f"\n* documentation URL: {odps.product.dataAccess.documentationURL}"
                )
            if odps.product.dataAccess.format:
                content += f"\n* format: {odps.product.dataAccess.format}"
            if odps.product.dataAccess.specification:
                content += f"\n* specification: {odps.product.dataAccess.specification}"
            if odps.product.dataAccess.type:
                content += f"\n* type: {odps.product.dataAccess.type}"
            content += "\n"

        if odps.product.dataHolder:
            content += "\n### Data Holder"
            content += "\n"
            if odps.product.dataHolder.URL:
                content += f"\n* URL: {odps.product.dataHolder.URL}"
            if odps.product.dataHolder.addressCountry:
                content += (
                    f"\n* address country: {odps.product.dataHolder.addressCountry}"
                )
            if odps.product.dataHolder.addressLocality:
                content += (
                    f"\n* address locality: {odps.product.dataHolder.addressLocality}"
                )
            if odps.product.dataHolder.addressRegion:
                content += (
                    f"\n* address region: {odps.product.dataHolder.addressRegion}"
                )
            if odps.product.dataHolder.streetAddress:
                content += (
                    f"\n* street address: {odps.product.dataHolder.streetAddress}"
                )
            if odps.product.dataHolder.postalCode:
                content += f"\n* postal code: {odps.product.dataHolder.postalCode}"
            if odps.product.dataHolder.telephone:
                content += f"\n* telephone: {odps.product.dataHolder.telephone}"
            if odps.product.dataHolder.aggregateRating:
                content += (
                    f"\n* aggregate rating: {odps.product.dataHolder.aggregateRating}"
                )
            if odps.product.dataHolder.businessDomain:
                content += (
                    f"\n* business domain: {odps.product.dataHolder.businessDomain}"
                )
            if odps.product.dataHolder.description:
                content += f"\n* description: {odps.product.dataHolder.description}"
            if odps.product.dataHolder.logoURL:
                content += f"\n* logo URL: {odps.product.dataHolder.logoURL}"
            if odps.product.dataHolder.parentOrganization:
                content += f"\n* parent organization: {odps.product.dataHolder.parentOrganization}"
            if odps.product.dataHolder.ratingCount:
                content += f"\n* rating count: {odps.product.dataHolder.ratingCount}"
            if odps.product.dataHolder.slogan:
                content += f"\n* slogan: {odps.product.dataHolder.slogan}"
            if odps.product.dataHolder.taxID:
                content += f"\n* tax ID: {odps.product.dataHolder.taxID}"
            if odps.product.dataHolder.vatID:
                content += f"\n* vat ID: {odps.product.dataHolder.vatID}"
            content += "\n"

        if odps.product.dataOps:
            content += "\n### Data Operations (DataOps)"
            content += "\n"
            if odps.product.dataOps.build:
                content += "\n#### Build"
                content += "\n"
                if odps.product.dataOps.build.checksum:
                    content += f"\n* checksum: {odps.product.dataOps.build.checksum}"
                if odps.product.dataOps.build.deploymentDocumentationURL:
                    content += f"\n* deployment documentation URL: {odps.product.dataOps.build.deploymentDocumentationURL}"
                if odps.product.dataOps.build.format:
                    content += f"\n* format: {odps.product.dataOps.build.format}"
                if odps.product.dataOps.build.hashType:
                    content += f"\n* hash type: {odps.product.dataOps.build.hashType}"
                if odps.product.dataOps.build.scriptURL:
                    content += f"\n* script URL: {odps.product.dataOps.build.scriptURL}"
                if odps.product.dataOps.build.signatureType:
                    content += f"\n* signature type: {odps.product.dataOps.build.signatureType}"
                content += "\n"

            if odps.product.dataOps.data:
                content += "\n#### Data"
                content += "\n"
                if odps.product.dataOps.data.schemaLocationURL:
                    content += f"\n* schema location URL: {odps.product.dataOps.data.schemaLocationURL}"
                content += "\n"

            if odps.product.dataOps.infrastructure:
                content += "\n#### Infrastructure"
                content += "\n"
                if odps.product.dataOps.infrastructure.containerTool:
                    content += f"\n* container tool: {odps.product.dataOps.infrastructure.containerTool}"
                if odps.product.dataOps.infrastructure.platform:
                    content += (
                        f"\n* platform: {odps.product.dataOps.infrastructure.platform}"
                    )
                if odps.product.dataOps.infrastructure.region:
                    content += (
                        f"\n* region: {odps.product.dataOps.infrastructure.region}"
                    )
                if odps.product.dataOps.infrastructure.storageTechnology:
                    content += f"\n* storage technology: {odps.product.dataOps.infrastructure.storageTechnology}"
                if odps.product.dataOps.infrastructure.storageType:
                    content += f"\n* storage type: {odps.product.dataOps.infrastructure.storageType}"
                content += "\n"

            if odps.product.dataOps.lineage:
                content += "\n#### Lineage"
                content += "\n"
                if odps.product.dataOps.lineage.dataLineageOutput:
                    content += f"\n* data lineage output: {odps.product.dataOps.lineage.dataLineageOutput}"
                if odps.product.dataOps.lineage.dataLineageTool:
                    content += f"\n* data lineage tool: {odps.product.dataOps.lineage.dataLineageTool}"
                content += "\n"

        if odps.product.dataQuality and len(odps.product.dataQuality) > 0:
            for data_quality in odps.product.dataQuality:
                content += "\n### Data Quality"
                content += "\n"
                if data_quality.displaytitle and len(data_quality.displaytitle) > 0:
                    content += f"\n#### {data_quality.displaytitle[0].en}"
                    content += "\n"
                    if data_quality.dimension:
                        content += f"\n* dimension: {data_quality.dimension}"
                    if data_quality.monitoring:
                        content += "\n* monitoring:"
                        if data_quality.monitoring.reference:
                            content += (
                                f"\n  * reference: {data_quality.monitoring.reference}"
                            )
                        if data_quality.monitoring.spec:
                            content += f"\n  * spec: {data_quality.monitoring.spec}"
                        if data_quality.monitoring.type:
                            content += f"\n  * type: {data_quality.monitoring.type}"
                    if data_quality.objective:
                        content += f"\n* objective: {data_quality.objective}"
                    if data_quality.unit:
                        content += f"\n* unit: {data_quality.unit}"
            content += "\n"

        if odps.product.license:
            content += "\n### License"
            content += "\n"

            if odps.product.license.governance:
                content += "\n#### Governance"
                content += "\n"
                if odps.product.license.governance.applicableLaws:
                    content += f"\n* applicable laws: {odps.product.license.governance.applicableLaws}"
                if odps.product.license.governance.audit:
                    content += f"\n* audit: {odps.product.license.governance.audit}"
                if odps.product.license.governance.confidentiality:
                    content += f"\n* confidentiality: {odps.product.license.governance.confidentiality}"
                if odps.product.license.governance.damages:
                    content += f"\n* damages: {odps.product.license.governance.damages}"
                if odps.product.license.governance.forceMajeure:
                    content += f"\n* force majeure: {odps.product.license.governance.forceMajeure}"
                if odps.product.license.governance.ownership:
                    content += (
                        f"\n* ownership: {odps.product.license.governance.ownership}"
                    )
                if odps.product.license.governance.warranties:
                    content += (
                        f"\n* warranties: {odps.product.license.governance.warranties}"
                    )
                content += "\n"

            if odps.product.license.scope:
                content += "\n#### Scope"
                content += "\n"
                if odps.product.license.scope.definition:
                    content += (
                        f"\n* definition: {odps.product.license.scope.definition}"
                    )
                if odps.product.license.scope.exclusive:
                    content += f"\n* exclusive: {odps.product.license.scope.exclusive}"
                if odps.product.license.scope.language:
                    content += f"\n* language: {odps.product.license.scope.language}"
                if odps.product.license.scope.permanent:
                    content += f"\n* permanent: {odps.product.license.scope.permanent}"
                if odps.product.license.scope.restrictions:
                    content += (
                        f"\n* restrictions: {odps.product.license.scope.restrictions}"
                    )
                if (
                    odps.product.license.scope.geographicalArea
                    and len(odps.product.license.scope.geographicalArea) > 0
                ):
                    content += f"\n* geographical area: {', '.join(odps.product.license.scope.geographicalArea)}"
                if odps.product.license.scope.rights:
                    content += "\n* rights:"
                    for rights in odps.product.license.scope.rights:
                        content += f"\n  * {rights}"
                content += "\n"

            if odps.product.license.termination:
                content += "\n#### Termination"
                content += "\n"
                if odps.product.license.termination.continuityConditions:
                    content += f"\n* continuityConditions: {odps.product.license.termination.continuityConditions}"
                if odps.product.license.termination.terminationConditions:
                    content += f"\n* terminationConditions: {odps.product.license.termination.terminationConditions}"
                content += "\n"

        if odps.product.pricingPlans and len(odps.product.pricingPlans.en) > 0:
            content += "\n### Pricing Plans"
            content += "\n"
            for pricing_plan in odps.product.pricingPlans.en:
                content += f"\n##### {pricing_plan.name}"
                content += "\n"
                if pricing_plan.billingDuration:
                    content += f"\n* billing duration: {pricing_plan.billingDuration}"
                if pricing_plan.maxTransactionQuantity:
                    content += f"\n* max transaction quantity: {pricing_plan.maxTransactionQuantity}"
                if pricing_plan.offering and len(pricing_plan.offering) > 0:
                    content += f"\n* offering: {', '.join(pricing_plan.offering)}"
                if pricing_plan.price:
                    content += f"\n* price: {pricing_plan.price}"
                if pricing_plan.priceCurrency:
                    content += f"\n* price currency: {pricing_plan.priceCurrency}"
                if pricing_plan.unit:
                    content += f"\n* unit: {pricing_plan.unit}"
            content += "\n"

        if odps.product.support:
            content += "\n### Support"
            content += "\n"
            if odps.product.support.documentationURL:
                content += (
                    f"\n* documentation URL: {odps.product.support.documentationURL}"
                )
            if odps.product.support.email:
                content += f"\n* email: {odps.product.support.email}"
            if odps.product.support.emailServiceHours:
                content += (
                    f"\n* email service hours: {odps.product.support.emailServiceHours}"
                )
            if odps.product.support.phoneNumber:
                content += f"\n* phone number: {odps.product.support.phoneNumber}"
            if odps.product.support.phoneServiceHours:
                content += (
                    f"\n* phone service hours: {odps.product.support.phoneServiceHours}"
                )
            content += "\n"

        if (
            odps.product.recommendedDataProducts
            and len(odps.product.recommendedDataProducts) > 0
        ):
            content += "\n### Recommended Data Products"
            content += "\n"
            for recommended_data_product in odps.product.recommendedDataProducts:
                content += f"\n* {recommended_data_product}"
            content += "\n"

    content += "\n"
    content += "\n---"
    content += "\nThis Open Data Product Specification canvas is based on [OPEN DATA PRODUCT SPECIFICATION 3.1](https://opendataproducts.org/v3.1/#open-data-product-specification-3-1)."

    with open(odps_canvas_path, "w") as file:
        file.write(content)
