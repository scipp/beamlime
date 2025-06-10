import holoviews as hv
import hvplot.pandas  # noqa
import panel as pn
from bokeh.sampledata.iris import flowers

pn.extension(sizing_mode="stretch_both")
hv.extension("bokeh")

# Create widgets for different tabs
tab_selector = pn.widgets.RadioButtonGroup(
    name="Tabs",
    options=["Overview", "Analysis", "Settings"],
    value="Overview",
    button_type="primary",
)
chbox = pn.widgets.Checkbox(name="Show Combined View")
slider = pn.widgets.FloatSlider(name="Threshold", start=0, end=10, value=5)


@pn.depends(tab_selector, chbox, slider)
def get_main_content(selected_tab: str, combine_plots: bool, threshold: float):
    if selected_tab == "Overview":
        # Tab 1: Overview with grid layout
        scatter = flowers.hvplot.scatter(
            x="sepal_length", y="sepal_width", responsive=True, title="Sepal Dimensions"
        )
        hist = flowers.hvplot.hist(
            "petal_width", responsive=True, title="Petal Width Distribution"
        )

        if combine_plots:
            # Single column layout
            return pn.Column(
                pn.pane.Markdown("## Overview Dashboard"),
                (scatter + hist).cols(1),
                height_policy="max",
            )
        else:
            # Grid layout using template positioning
            overview_grid = pn.GridSpec(sizing_mode="stretch_both", height=600)
            overview_grid[0, :] = pn.pane.Markdown("## Overview Dashboard")
            overview_grid[1:3, 0] = scatter
            overview_grid[1:3, 1] = hist
            return overview_grid

    elif selected_tab == "Analysis":
        # Tab 2: Analysis with different layout
        filtered_data = flowers[flowers.petal_width > threshold]
        box_plot = filtered_data.hvplot.box(
            y="sepal_length",
            by="species",
            responsive=True,
            title=f"Sepal Length by Species (threshold: {threshold})",
        )
        violin_plot = filtered_data.hvplot.violin(
            y="petal_length",
            by="species",
            responsive=True,
            title="Petal Length Distribution",
        )
        scatter_analysis = filtered_data.hvplot.scatter(
            x="petal_length",
            y="petal_width",
            color="species",
            responsive=True,
            title="Petal Dimensions by Species",
        )

        # Grid layout for analysis tab
        analysis_grid = pn.GridSpec(sizing_mode="stretch_both", height=600)
        analysis_grid[0, :] = pn.pane.Markdown(
            f"## Analysis Dashboard\nData filtered: {len(filtered_data)} rows"
        )
        analysis_grid[1, 0] = box_plot
        analysis_grid[1, 1] = violin_plot
        analysis_grid[2, :] = scatter_analysis
        return analysis_grid

    else:  # Settings tab
        # Tab 3: Settings with form layout
        settings_grid = pn.GridSpec(sizing_mode="stretch_both", height=600)
        settings_grid[0, :] = pn.pane.Markdown("## Settings")
        settings_grid[1, 0] = pn.Column(
            pn.pane.Markdown("### Data Configuration"),
            chbox,
            slider,
            sizing_mode="stretch_width",
        )
        settings_grid[1, 1] = pn.Column(
            pn.pane.Markdown("### About"),
            pn.pane.Markdown(
                "This dashboard demonstrates Panel templates with tabs and grid layouts."
            ),
            pn.pane.Markdown("### Statistics"),
            pn.pane.HTML(f"<p>Total flowers: {len(flowers)}</p>"),
            sizing_mode="stretch_width",
        )
        return settings_grid


# Create template
template = pn.template.FastGridTemplate(
    title="Multi-Tab Dashboard", sidebar_width=250, prevent_collision=True
)

# Add navigation to sidebar
template.sidebar.append(pn.pane.Markdown("## Navigation"))
template.sidebar.append(tab_selector)
template.sidebar.append(pn.layout.Divider())
template.sidebar.append(pn.pane.Markdown("## Controls"))
template.sidebar.append(chbox)
template.sidebar.append(slider)

# Add main content (this will be the grid layouts inside tabs)
template.main[:, :] = get_main_content

template.servable()
