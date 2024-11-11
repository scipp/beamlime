# Order in 'resolution' matters so plots have X as horizontal axis and Y as vertical.
# The other DREAM detectors have non-consecutive detector numbers. This is not
# supported currently

from ess.reduce.live import raw

_res_scale = 8

_dream = {
    'dashboard': {'nrow': 3, 'ncol': 2},
    'detectors': {
        'endcap_backward': {
            'detector_name': 'endcap_backward_detector',
            'resolution': {'y': 30 * _res_scale, 'x': 20 * _res_scale},
            'gridspec': (0, 0),
        },
        'endcap_forward': {
            'detector_name': 'endcap_forward_detector',
            'resolution': {'y': 20 * _res_scale, 'x': 20 * _res_scale},
            'gridspec': (0, 1),
        },
        # We use the arc length instead of phi as it makes it easier to get a correct
        # aspect ratio for the plot if both axes have the same unit.
        'mantle_projection': {
            'detector_name': 'mantle_detector',
            'resolution': {'arclength': 10 * _res_scale, 'z': 40 * _res_scale},
            'projection': 'cylinder_mantle_z',
            'gridspec': (1, slice(None, 2)),
        },
        # Different view of the same detector, showing just the front layer instead of
        # a projection.
        'mantle_front_layer': {
            'detector_name': 'mantle_detector',
            'projection': raw.LogicalView(
                fold={
                    'wire': 32,
                    'module': 5,
                    'segment': 6,
                    'strip': 256,
                    'counter': 2,
                },
                transpose=('wire', 'module', 'segment', 'counter', 'strip'),
                select={'wire': 0},
                flatten={'z_id': ('module', 'segment', 'counter')},
            ),
            'gridspec': (2, slice(None, 2)),
        },
    },
}
