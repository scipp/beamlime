import threading
import time
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import panel as pn
from matplotlib.figure import Figure

pn.extension('ipywidgets')


class InteractivePlotApp:
    def __init__(self):
        self.fig, self.ax = plt.subplots(figsize=(10, 6))
        (self.line,) = self.ax.plot([], [], 'b-', linewidth=2)
        self.ax.set_xlim(0, 100)
        self.ax.set_ylim(-3, 3)
        self.ax.set_xlabel('Index')
        self.ax.set_ylabel('Value')
        self.ax.set_title('Real-time Random Data')
        self.ax.grid(True, alpha=0.3)

        # Initialize data
        self.x_data = np.arange(100)
        self.y_data = np.random.randn(100)
        self.line.set_data(self.x_data, self.y_data)

        # Create Panel matplotlib pane
        self.plot_pane = pn.pane.Matplotlib(
            self.fig,
            tight=True,
            # sizing_mode='stretch_width',
            interactive=True,
            format="svg",
        )

        # Status info
        self.status_text = pn.pane.HTML(
            f"<b>Last updated:</b> {datetime.now().strftime('%H:%M:%S')}"
        )

        # Start update thread
        self.running = True
        self.update_thread = threading.Thread(target=self._update_loop, daemon=True)
        self.update_thread.start()

    def _update_loop(self):
        """Background thread to update data every second."""
        while self.running:
            self.update_data()
            time.sleep(1.0)

    def update_data(self):
        """Update the plot with new random data."""
        # Generate new random data
        self.y_data = np.random.randn(100)

        # Update the line data
        self.line.set_data(self.x_data, self.y_data)

        # Update y-axis limits based on data range
        y_min, y_max = np.min(self.y_data), np.max(self.y_data)
        margin = 0.1 * (y_max - y_min)
        self.ax.set_ylim(y_min - margin, y_max + margin)

        # Update status
        current_time = datetime.now().strftime('%H:%M:%S')
        self.status_text.object = f"<b>Last updated:</b> {current_time}"

        # Trigger plot update
        self.plot_pane.param.trigger('object')

    def create_layout(self):
        """Create the Panel layout."""
        return pn.Column(
            "# Interactive Real-time Plot",
            self.status_text,
            self.plot_pane,
            sizing_mode='stretch_width',
        )

    def stop(self):
        """Stop the update thread."""
        self.running = False


# Create the app
app = InteractivePlotApp()
layout = app.create_layout()

# Serve the app
if __name__ == "__main__":
    pn.serve(layout, show=True, port=5007, autoreload=True)
