"""Figure preparation service."""

import logging
from typing import Any

import numpy as np
import plotly.express as px
import plotly.graph_objects as go

logger = logging.getLogger(__name__)


class FigureService:
    """Creates Plotly figures from data."""

    def __init__(self):
        logger.info('Figure service initialized')

    def create_1d_plot(self) -> go.Figure:
        """Create a 1D line plot with mock data."""
        x = np.linspace(0, 10, 100)
        y = np.sin(x) + 0.1 * np.random.randn(100)

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=x, y=y, mode='lines', name='Signal'))
        fig.update_layout(
            title='1D Detector Signal',
            xaxis_title='Position',
            yaxis_title='Intensity',
            template='plotly_white',
        )
        return fig

    def create_2d_heatmap(self) -> go.Figure:
        """Create a 2D heatmap with mock data."""
        data = np.random.exponential(2, (50, 50))

        fig = px.imshow(
            data,
            title='2D Detector Heatmap',
            color_continuous_scale='viridis',
            labels={'x': 'X Position', 'y': 'Y Position', 'color': 'Intensity'},
        )
        fig.update_layout(template='plotly_white')
        return fig

    def create_statistics_table(self) -> go.Figure:
        """Create a statistics table."""
        stats_data = {
            'Metric': ['Mean', 'Std Dev', 'Max', 'Min', 'Count'],
            'Value': [125.3, 45.7, 892.1, 12.4, 2500],
        }

        fig = go.Figure(
            data=[
                go.Table(
                    header=dict(values=list(stats_data.keys()), fill_color='lightblue'),
                    cells=dict(values=list(stats_data.values()), fill_color='white'),
                )
            ]
        )
        fig.update_layout(title='Data Statistics', template='plotly_white')
        return fig

    def create_all_figures(self) -> dict[str, Any]:
        """Create all dashboard figures."""
        return {
            '1d_plot': self.create_1d_plot(),
            '2d_heatmap': self.create_2d_heatmap(),
            'statistics': self.create_statistics_table(),
        }
