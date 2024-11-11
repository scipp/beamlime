from ess.reduce.live import raw

_nmx = {
    'dashboard': {'nrow': 1, 'ncol': 3},
    'detectors': {
        'Panel 0': {
            'detector_name': 'detector_panel_0',
            'gridspec': (0, 0),
            'projection': raw.LogicalView(),
        },
        'Panel 1': {
            'detector_name': 'detector_panel_1',
            'gridspec': (0, 1),
            'projection': raw.LogicalView(),
        },
        'Panel 2': {
            'detector_name': 'detector_panel_2',
            'gridspec': (0, 2),
            'projection': raw.LogicalView(),
        },
    },
}
# These banks currently have out-of-range event_id values, so we ignore them.
del _nmx['detectors']['Panel 1']
del _nmx['detectors']['Panel 2']
