import holoviews as hv
import numpy as np
import panel as pn
from holoviews import opts, streams

# Enable Panel extensions
pn.extension('bokeh')
hv.extension('bokeh')


def create_polydraw_demo() -> pn.Column:
    """
    Create a Panel dashboard demonstrating the PolyDraw tool.

    Returns
    -------
    Panel Column containing the dashboard components.
    """
    # Create a sample image as background
    x = np.linspace(0, 10, 100)
    y = np.linspace(0, 10, 100)
    X, Y = np.meshgrid(x, y)
    Z = np.sin(X) * np.cos(Y) + 0.1 * np.random.randn(100, 100)

    # Create image element
    background_image = hv.Image(Z, bounds=(0, 0, 10, 10)).opts(
        opts.Image(
            cmap='viridis',
            colorbar=True,
            width=600,
            height=400,
            title='Interactive Polygon Drawing on Image',
        )
    )

    # Initial polygon data (scaled to image bounds)
    poly = hv.Polygons([[]])

    # Create PolyDraw stream
    poly_stream = streams.PolyDraw(
        source=poly,
        drag=True,
        num_objects=4,
        show_vertices=True,
        styles={'fill_color': ['red', 'green', 'blue', 'orange']},
    )

    # Create PolyEdit stream for editing existing polygons
    poly_edit_stream = streams.PolyEdit(
        source=poly, vertex_style={'color': 'red'}, shared=True, show_vertices=True
    )

    # Configure polygon styling for visibility over image
    interactive_poly = poly.opts(
        opts.Polygons(
            fill_alpha=0.4,
            line_color='white',
            line_width=3,
            active_tools=['poly_draw', 'poly_edit'],
        )
    )

    # Overlay polygons on the image
    overlay = (background_image * interactive_poly).opts(
        opts.Overlay(width=600, height=400)
    )

    # Create info panel
    info_text = pn.pane.Markdown("""
    ## PolyDraw Tool Demo

    **Instructions:**
    - Click to add vertices and create polygons
    - Drag existing vertices to modify shapes
    - Double-click to complete a polygon
    - Use the polygon draw tool to create up to 4 polygons

    **Features:**
    - Interactive vertex editing
    - Multiple polygon support
    - Hover tooltips
    - Customizable styling
    """)

    # Create polygon count indicator
    count_indicator = pn.pane.HTML("<b>Polygons created: 1</b>", width=200)

    def update_count(data):
        count = len(data) if data else 0
        count_indicator.object = f"<b>Polygons created: {count}</b>"

    poly_stream.param.watch(lambda event: update_count(event.new), 'data')
    poly_edit_stream.param.watch(lambda event: update_count(event.new), 'data')

    # Create reset button
    def reset_polygons(event):
        poly_stream.source.data = {'xs': [[2, 5, 8]], 'ys': [[2, 8, 2]]}

    reset_button = pn.widgets.Button(name="Reset Polygons", button_type="primary")
    reset_button.on_click(reset_polygons)

    # Layout the dashboard
    controls = pn.Row(count_indicator, reset_button, margin=(10, 0))

    dashboard = pn.Column(
        "# PolyDraw Tool Dashboard",
        pn.Row(
            pn.Column(info_text, width=300),
            pn.Column(overlay, controls, width=650),
        ),
        margin=20,
    )

    return dashboard


def main():
    """Main function to serve the Panel dashboard."""
    dashboard = create_polydraw_demo()
    dashboard.servable()
    return dashboard


if __name__ == "__main__":
    # Create and serve the dashboard
    app = main()
    pn.serve(app, port=5007, show=True, autoreload=True)
