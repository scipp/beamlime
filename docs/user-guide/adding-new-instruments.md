# Adding New Instruments

This guide explains how to add support for a new instrument in Beamlime.

## Required Steps

1. Create a new configuration file in `src/beamlime/config/raw_detectors/<instrument>.py`
   - The filename will be used as the instrument identifier
   - Beamlime automatically detects and loads all Python files in this directory
2. Add detector configuration including:
   - Detector names and pixel ID ranges
   - View configurations (resolution, projection type)
   - Optional pixel noise settings

## Detector Configuration Example

Example for a new instrument with two detector panels:

```python
from ess.reduce.live import raw

detectors_config = {  # Must use this exact variable name
    'detectors': {
        'Panel A': {
            'detector_name': 'panel_a',  # Name in NeXus file as well as source_name in Kafka
            'projection': raw.LogicalView(  # Standard choice for 2D detectors
                fold={
                    'y': 128,  # Number of pixels in y direction
                    'x': 128,  # Number of pixels in x direction
                },
            ),
        },
        'Panel B': {
            'detector_name': 'panel_b',
            'projection': raw.LogicalView(
                fold={
                    'y': 128,
                    'x': 128,
                },
            ),
        },
    },
}
```

Note that it is valid to configure multiple views for the same detector, e.g., for different resolutions or projections.

## View Types

Beamlime supports different view types for detectors:

- `raw.LogicalView`: Go-to solution for regular 2-D detectors, or for viewing individual layers or slices of 3-D detectors.
- `xy_plane`: 2D projection onto XY plane, used when geometric information is needed.
- `cylinder_mantle_z`: Projection onto cylinder, for cylindrical detectors like DREAM's mantle detector.

Geometric projections (`xy_plane`, `cylinder_mantle_z`) are used when detector panels have a complex geometry such as a 3-D structure.
Projections are towards the sample position (assumed at the origin).
For more details see [ess.reduce.live.raw](https://scipp.github.io/essreduce/generated/modules/ess.reduce.live.raw.html).

## Geometry Files

If your instrument is configured to use `raw.LogicalView` *and* defined `detector_number` in the configuration, you do not need to provide a geometry file.
Otherwise, you need to provide a NeXus geometry file for the instrument:

1. Create a NeXus geometry file following the naming convention: `geometry-<instrument>-<date>.nxs`
2. Add the file's MD5 hash to the `_registry` in `detector_data_handler.py`
3. Upload the file to the Scipp HTML server so it is available in https://public.esss.dk/groups/scipp/beamlime/geometry/.

The date should be the first date the geometry file is used in production.
There can be more than one geometry file for an instrument, but only one can be active at a time.

## Development Support

### Setting up Fake Detector Data

To enable development without real detector data, you can add your instrument to the fake detectors service:

1. Add detector pixel ID ranges in `fake_detectors.py`:

```python
detector_config = {
    'your_instrument': {
        'panel_a': (1, 16384),       # (first_id, last_id)
        'panel_b': (16385, 32768),
    },
    # ...existing instruments...
}
```

2. The pixel IDs must match your detector configuration and should not overlap
3. The fake detector service will generate random events within these ID ranges

## Examples

See existing instrument configurations for reference implementations:

- DREAM: Complex setup with multiple detector types including cylindrical projection
- LOKI: Multiple detector panels with standard XY projections
- NMX: Example of logical view configuration

For more details about specific detector configurations, refer to the corresponding files in `src/beamlime/config/raw_detectors/`.
