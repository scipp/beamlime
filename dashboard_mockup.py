import numpy as np
import panel as pn
import param
from bokeh.plotting import figure

pn.extension('bokeh', template='material')


class DashboardApp(param.Parameterized):
    """Main dashboard application with tab-dependent sidebar controls."""

    # Common controls (always visible)
    sample_name = param.String(default="Sample_001", doc="Sample name")
    run_number = param.Integer(default=12345, doc="Run number")

    # Active tab tracker
    active_tab = param.String(default="Detectors")

    # Tab-specific parameters
    detector_view_mode = param.Selector(
        default="Current", objects=["Current", "Cumulative"], doc="Detector view mode"
    )
    detector_threshold = param.Number(
        default=0.5, bounds=(0, 1), doc="Detection threshold"
    )
    detector_binning = param.Integer(
        default=100, bounds=(10, 1000), doc="Number of bins"
    )

    monitor_integration_time = param.Number(
        default=10.0, bounds=(0.1, 100), doc="Integration time (s)"
    )
    monitor_normalization = param.Boolean(default=True, doc="Apply normalization")

    reduction_algorithm = param.Selector(
        default="Standard",
        objects=["Standard", "Advanced", "Custom"],
        doc="Reduction algorithm",
    )
    background_subtraction = param.Boolean(default=False, doc="Subtract background")

    def __init__(self, **params):
        super().__init__(**params)
        self.active_tab = "Detectors"

    @pn.depends('detector_view_mode')
    def create_detector_plot_content(self):
        """Create reactive plot content that updates with view mode changes."""
        # Plot 1: 2D detector image
        x = np.linspace(0, 10, 50)
        y = np.linspace(0, 10, 50)
        X, Y = np.meshgrid(x, y)

        if self.detector_view_mode == "Cumulative":
            Z = 2 * np.sin(X) * np.cos(Y) + np.random.normal(0, 0.05, X.shape)
            title_suffix = " (Cumulative)"
        else:
            Z = np.sin(X) * np.cos(Y) + np.random.normal(0, 0.1, X.shape)
            title_suffix = " (Current)"

        p1 = figure(
            title=f"Detector Image{title_suffix}",
            width=400,
            height=300,
            x_axis_label="X (mm)",
            y_axis_label="Y (mm)",
        )
        p1.image(image=[Z], x=0, y=0, dw=10, dh=10, palette="Viridis256")

        # Plot 2: Intensity histogram
        if self.detector_view_mode == "Cumulative":
            counts = np.random.poisson(200, 1000)
        else:
            counts = np.random.poisson(100, 1000)

        hist, edges = np.histogram(counts, bins=50)

        p2 = figure(
            title=f"Intensity Distribution{title_suffix}",
            width=400,
            height=300,
            x_axis_label="Counts",
            y_axis_label="Frequency",
        )
        p2.quad(
            top=hist,
            bottom=0,
            left=edges[:-1],
            right=edges[1:],
            fill_color="navy",
            line_color="white",
            alpha=0.7,
        )

        return pn.FlexBox(pn.pane.Bokeh(p1), pn.pane.Bokeh(p2))

    def create_detector_plots(self):
        """Create plots for the Detectors tab with reactive content."""
        view_toggle = pn.widgets.RadioBoxGroup(
            name="View Mode",
            value=self.detector_view_mode,
            options=["Current", "Cumulative"],
            inline=True,
            margin=(10, 0),
        )

        view_toggle.link(self, value='detector_view_mode')

        return pn.Column(view_toggle, self.create_detector_plot_content)

    def create_monitor_plots(self) -> list:
        """Create plots for the Monitors tab."""
        # Plot 1: Time series
        time = np.linspace(0, 100, 1000)
        signal = np.sin(0.1 * time) + 0.1 * np.random.randn(1000)

        p1 = figure(
            title="Monitor Signal vs Time",
            width=400,
            height=300,
            x_axis_label="Time (s)",
            y_axis_label="Signal",
        )
        p1.line(time, signal, line_width=2, color="blue")

        # Plot 2: Beam profile
        x = np.linspace(-5, 5, 100)
        profile = np.exp(-(x**2) / 2) + 0.05 * np.random.randn(100)

        p2 = figure(
            title="Beam Profile",
            width=400,
            height=300,
            x_axis_label="Position (mm)",
            y_axis_label="Intensity",
        )
        p2.line(x, profile, line_width=2, color="red")

        return [pn.pane.Bokeh(p1), pn.pane.Bokeh(p2)]

    def create_reduction_plots(self) -> list:
        """Create plots for the Data Reduction tab."""
        # Plot 1: Before/after comparison
        x = np.linspace(0, 20, 200)
        raw_data = np.sin(x) + 0.3 * np.random.randn(200)
        processed_data = np.convolve(raw_data, np.ones(5) / 5, mode='same')

        p1 = figure(
            title="Data Processing Comparison",
            width=400,
            height=300,
            x_axis_label="Q (Å⁻¹)",
            y_axis_label="Intensity",
        )
        p1.line(x, raw_data, legend_label="Raw", line_width=2, color="gray", alpha=0.7)
        p1.line(
            x, processed_data, legend_label="Processed", line_width=2, color="green"
        )
        p1.legend.location = "top_right"

        # Plot 2: Residuals
        residuals = raw_data - processed_data

        p2 = figure(
            title="Processing Residuals",
            width=400,
            height=300,
            x_axis_label="Q (Å⁻¹)",
            y_axis_label="Residual",
        )
        p2.line(x, residuals, line_width=2, color="orange")
        p2.line(x, np.zeros_like(x), line_width=1, color="black", line_dash="dashed")

        return [pn.pane.Bokeh(p1), pn.pane.Bokeh(p2)]

    @pn.depends('active_tab')
    def get_dynamic_sidebar(self):
        """Create dynamic sidebar that updates based on active tab."""
        common_controls = pn.Column(
            pn.pane.Markdown("## Status"),
            pn.pane.Markdown("## Controls"),
            pn.Param(
                self,
                parameters=['sample_name', 'run_number'],
                show_name=False,
                width=250,
            ),
            pn.layout.Spacer(height=20),
        )

        if self.active_tab == "Detectors":
            specific_controls = pn.Column(
                pn.pane.Markdown("## Detector Settings"),
                pn.Param(
                    self,
                    parameters=['detector_threshold', 'detector_binning'],
                    show_name=False,
                    width=250,
                ),
            )
        elif self.active_tab == "Monitors":
            specific_controls = pn.Column(
                pn.pane.Markdown("## Monitor Settings"),
                pn.Param(
                    self,
                    parameters=['monitor_integration_time', 'monitor_normalization'],
                    show_name=False,
                    width=250,
                ),
            )
        elif self.active_tab == "Data Reduction":
            specific_controls = pn.Column(
                pn.pane.Markdown("## Reduction Settings"),
                pn.Param(
                    self,
                    parameters=['reduction_algorithm', 'background_subtraction'],
                    show_name=False,
                    width=250,
                ),
            )
        else:
            specific_controls = pn.Column()

        return pn.Column(common_controls, specific_controls)


def create_dashboard():
    """Create and configure the main dashboard."""
    app = DashboardApp()

    detector_content = app.create_detector_plots()
    monitor_plots = pn.FlexBox(*app.create_monitor_plots())
    reduction_plots = pn.FlexBox(*app.create_reduction_plots())

    # Create tabs
    tabs = pn.Tabs(
        ("Detectors", detector_content),
        ("Monitors", monitor_plots),
        ("Data Reduction", reduction_plots),
        dynamic=True,
    )

    # Callback to update active tab parameter
    def update_active_tab(event):
        tab_names = ["Detectors", "Monitors", "Data Reduction"]
        new_tab_name = tab_names[event.new]
        print(f"Switching to tab: {new_tab_name}")
        app.active_tab = new_tab_name

    tabs.param.watch(update_active_tab, 'active')

    # Configure template with dynamic sidebar
    template = pn.template.MaterialTemplate(
        title="DREAM — Live Data",
        sidebar=app.get_dynamic_sidebar,
        main=tabs,
        header_background='#2596be',
    )

    return template


def main():
    """Main function to serve the Panel dashboard."""
    dashboard = create_dashboard()
    dashboard.servable()
    return dashboard


if __name__ == "__main__":
    app = main()
    pn.serve(app, port=5007, show=False, autoreload=True, dev=True)
