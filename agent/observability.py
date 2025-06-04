# Observability module - now using modular dashboard structure
# This file maintains backward compatibility while delegating to the dashboard module

from .dashboard.routes import (
    ObservabilityCollector,
    TTLInfo,
    QueueMetrics,
    AgentPerformanceMetrics,
    SystemMetrics,
    TaskDistribution,
    ActivityBucket,
    ActivityChartData,
    SystemActivityCharts,
    add_observability_routes,
)

# Re-export all the classes and functions for backward compatibility
__all__ = [
    "ObservabilityCollector",
    "TTLInfo",
    "QueueMetrics",
    "AgentPerformanceMetrics",
    "SystemMetrics",
    "TaskDistribution",
    "ActivityBucket",
    "ActivityChartData",
    "SystemActivityCharts",
    "add_observability_routes",
]
