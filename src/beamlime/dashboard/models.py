"""Data models for dashboard configuration."""

from pydantic import BaseModel


class ProcessingConfig(BaseModel):
    """Configuration for data processing."""

    threshold: float = 1.0
    smoothing_factor: float = 0.5
    enable_filtering: bool = True
    roi_x_min: int = 0
    roi_x_max: int = 100
    roi_y_min: int = 0
    roi_y_max: int = 100


class DashboardConfig(BaseModel):
    """Configuration for dashboard itself."""

    update_interval_ms: int = 1000
    buffer_size: int = 1000
    debug_mode: bool = False
