import os
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import List, Optional, Dict, Any

import yaml
from dacite import from_dict
from yaml import MappingNode
from yaml.constructor import ConstructorError

from opendataproduct.tracking_decorator import TrackingDecorator


class SlaDimension(Enum):
    AVAILABILITY = "Availability"
    PERFORMANCE = "Performance"
    RELIABILITY = "Reliability"
    SCALABILITY = "Scalability"
    SECURITY = "Security"
    COMPLIANCE = "Compliance"
    SUPPORT = "Support"
    DATA_FRESHNESS = "Data Freshness"
    DATA_ACCURACY = "Data Accuracy"
    DATA_COMPLETENESS = "Data Completeness"
    DATA_CONSISTENCY = "Data Consistency"


class DataQualityDimenion(Enum):
    ACCURACY = "Accuracy"
    COMPLETENESS = "Completeness"
    CONSISTENCY = "Consistency"
    TIMELINESS = "Timeliness"
    UNIQUENESS = "Uniqueness"
    VALIDITY = "Validity"
    INTEGRITY = "Integrity"
    AVAILABILITY = "Availability"


class PricingPlanName(Enum):
    FREE = "Free"
    FREEMIUM = "Freemium"
    SUBSCRIPTION = "Subscription"
    PAY_PER_USE = "Pay-per-use"
    TIERED = "Tiered"
    PERPETUAL_LICENSE = "Perpetual License"
    METERED = "Metered"
    ONE_TIME_PURCHASE = "One-time Purchase"
    ENTERPRISE_LICENSE = "Enterprise License"
    OPEN_SOURCE = "Open Source"
    DONATION_BASED = "Donation-based"
    AD_SUPPORTED = "Ad-supported"


@dataclass
class Monitoring:
    reference: Optional[str] = None
    spec: Optional[str] = None
    type: Optional[str] = None


@dataclass
class DisplayTitle:
    en: Optional[str] = None


@dataclass
class SLA:
    dimension: Optional[SlaDimension] = None
    displaytitle: Optional[List[DisplayTitle]] = None
    monitoring: Optional[Monitoring] = None
    objective: Optional[float] = None
    unit: Optional[str] = None


@dataclass
class DataAccess:
    authenticationMethod: str
    documentationURL: str
    format: str
    specification: str
    type: str


@dataclass
class DataHolder:
    URL: Optional[str] = None
    addressCountry: Optional[str] = None
    addressLocality: Optional[str] = None
    addressRegion: Optional[str] = None
    streetAddress: Optional[str] = None
    postalCode: Optional[str] = None
    telephone: Optional[str] = None
    aggregateRating: Optional[str] = None
    businessDomain: Optional[str] = None
    description: Optional[str] = None
    logoURL: Optional[str] = None
    parentOrganization: Optional[str] = None
    ratingCount: Optional[int] = None
    slogan: Optional[str] = None
    taxID: Optional[str] = None
    vatID: Optional[str] = None


@dataclass
class Build:
    checksum: Optional[str] = None
    deploymentDocumentationURL: Optional[str] = None
    format: Optional[str] = None
    hashType: Optional[str] = None
    scriptURL: Optional[str] = None
    signatureType: Optional[str] = None


@dataclass
class Data:
    schemaLocationURL: str


@dataclass
class Infrastructure:
    containerTool: Optional[str] = None
    platform: Optional[str] = None
    region: Optional[str] = None
    storageTechnology: Optional[str] = None
    storageType: Optional[str] = None


@dataclass
class Lineage:
    dataLineageOutput: Optional[str] = None
    dataLineageTool: Optional[str] = None


@dataclass
class DataOps:
    build: Optional[Build] = None
    data: Optional[Data] = None
    infrastructure: Optional[Infrastructure] = None
    lineage: Optional[Lineage] = None


@dataclass
class DataQuality:
    dimension: Optional[DataQualityDimenion] = None
    displaytitle: Optional[List[DisplayTitle]] = None
    monitoring: Optional[Monitoring] = None
    objective: Optional[float] = None
    unit: Optional[str] = None


@dataclass
class UseCase:
    useCaseDescription: Optional[str] = None
    useCaseTitle: Optional[str] = None
    useCaseURL: Optional[str] = None


@dataclass
class UseCaseWrapper:
    useCase: UseCase = None


@dataclass
class LocalizedInfo:
    name: str
    productID: str
    status: str
    type: str
    visibility: str
    OutputFileFormats: Optional[List[str]] = field(default_factory=list)
    brandSlogan: Optional[str] = None
    categories: Optional[List[str]] = field(default_factory=list)
    description: Optional[str] = None
    logoURL: Optional[str] = None
    productSeries: Optional[str] = None
    standards: Optional[List[str]] = field(default_factory=list)
    tags: Optional[List[str]] = field(default_factory=list)
    useCases: Optional[List[UseCaseWrapper]] = field(default_factory=list)
    valueProposition: Optional[str] = None
    version: Optional[str] = None


@dataclass
class Governance:
    applicableLaws: Optional[str] = None
    audit: Optional[str] = None
    confidentiality: Optional[str] = None
    damages: Optional[str] = None
    forceMajeure: Optional[str] = None
    ownership: Optional[str] = None
    warranties: Optional[str] = None


@dataclass
class Scope:
    definition: Optional[str] = None
    exclusive: Optional[bool] = None
    language: Optional[str] = None
    permanent: Optional[bool] = None
    restrictions: Optional[str] = None
    geographicalArea: Optional[List[str]] = field(default_factory=list)
    rights: Optional[List[str]] = field(default_factory=list)


@dataclass
class Termination:
    continuityConditions: Optional[str] = None
    terminationConditions: Optional[str] = None


@dataclass
class License:
    governance: Optional[Governance] = None
    scope: Optional[Scope] = None
    termination: Optional[Termination] = None


@dataclass
class PricingPlan:
    billingDuration: Optional[str] = None
    maxTransactionQuantity: Optional[str] = None
    name: Optional[PricingPlanName] = None
    offering: Optional[List[str]] = field(default_factory=list)
    price: Optional[str] = None
    priceCurrency: Optional[str] = None
    unit: Optional[str] = None


@dataclass
class PricingPlans:
    en: List[PricingPlan] = field(default_factory=list)


@dataclass
class Support:
    documentationURL: Optional[str] = None
    email: Optional[str] = None
    emailServiceHours: Optional[str] = None
    phoneNumber: Optional[str] = None
    phoneServiceHours: Optional[str] = None


@dataclass
class Product:
    en: LocalizedInfo
    sla: Optional[List[SLA]] = field(default_factory=list)
    dataAccess: Optional[DataAccess] = None
    dataHolder: Optional[DataHolder] = None
    dataOps: Optional[DataOps] = None
    dataQuality: Optional[List[DataQuality]] = field(default_factory=list)
    license: Optional[License] = None
    pricingPlans: Optional[PricingPlans] = None
    support: Optional[Support] = None
    recommendedDataProducts: Optional[List[str]] = field(default_factory=list)


@dataclass
class Details:
    summary: str
    description: str
    language: str
    metadata: Optional[Dict[str, Any]] = field(default_factory=dict)


@dataclass
class ODPS:
    schema: str
    version: str
    product: Product
    details: Optional[Details] = None


class Loader(yaml.SafeLoader):
    """
    Customer loader that makes sure that some fields are read in a raw format
    """

    raw_fields = ["version"]

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
def load_odps(config_path) -> ODPS:
    data_product_manifest_path = os.path.join(config_path, "odps.yml")

    if os.path.exists(data_product_manifest_path):
        with open(data_product_manifest_path, "r") as file:
            data = yaml.load(file, Loader=Loader)
        return from_dict(data_class=ODPS, data=data)
    else:
        print(f"✗️ Config file {data_product_manifest_path} does not exist")
        return ODPS(
            schema="schema",
            version="version",
            product=Product(
                en=LocalizedInfo(
                    name="product-name",
                    productID="product-id",
                    status="active",
                    type="data-product",
                    visibility="public",
                )
            ),
        )
