from twisted.application.service import ServiceMaker

SpreadFlowCoreService = ServiceMaker(
    "SpreadFlow Core Service",
    "spreadflow_core.service",
    "Metadata extraction and processing engine",
    "spreadflow_service")
