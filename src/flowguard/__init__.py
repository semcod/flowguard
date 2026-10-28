"""flowguard - Autonomous bounded concurrent dispatch and flow governor."""

from flowguard.governor import FlowGovernor, ResourceSnapshot, ResourceLimits
from flowguard.dag import TaskDAG, TaskNode, TaskStatus
from flowguard.quarantine import QuarantineManager, GateFailureClassification
from flowguard.engine import FlowEngine, RateLimited, TaskExecutionReceipt

__version__ = "0.2.0"
__all__ = [
    "FlowGovernor",
    "ResourceSnapshot",
    "ResourceLimits",
    "TaskDAG",
    "TaskNode",
    "TaskStatus",
    "QuarantineManager",
    "GateFailureClassification",
    "FlowEngine",
    "RateLimited",
    "TaskExecutionReceipt",
]
