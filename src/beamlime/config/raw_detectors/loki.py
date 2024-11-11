_res_scale = 8

_loki = {
    'dashboard': {'nrow': 3, 'ncol': 9, 'figsize_scale': 3},
    'detectors': {
        'Rear-detector': {
            'detector_name': 'loki_detector_0',
            'resolution': {'y': 12 * _res_scale, 'x': 12 * _res_scale},
            'gridspec': (slice(0, 3), slice(0, 3)),
            'pixel_noise': 'cylindrical',
        },
        # First window frame
        'loki_detector_1': {
            'detector_name': 'loki_detector_1',
            'resolution': {'y': 3 * _res_scale, 'x': 9 * _res_scale},
            'gridspec': (2, 4),
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_2': {
            'detector_name': 'loki_detector_2',
            'resolution': {'y': 9 * _res_scale, 'x': 3 * _res_scale},
            'gridspec': (1, 3),
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_3': {
            'detector_name': 'loki_detector_3',
            'resolution': {'y': 3 * _res_scale, 'x': 9 * _res_scale},
            'gridspec': (0, 4),
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_4': {
            'detector_name': 'loki_detector_4',
            'resolution': {'y': 9 * _res_scale, 'x': 3 * _res_scale},
            'gridspec': (1, 5),
            'pixel_noise': 'cylindrical',
        },
        # Second window frame
        'loki_detector_5': {
            'detector_name': 'loki_detector_5',
            'resolution': {'y': 3 * _res_scale, 'x': 9 * _res_scale},
            'gridspec': (2, 7),
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_6': {
            'detector_name': 'loki_detector_6',
            'resolution': {'y': 9 * _res_scale, 'x': 3 * _res_scale},
            'gridspec': (1, 6),
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_7': {
            'detector_name': 'loki_detector_7',
            'resolution': {'y': 3 * _res_scale, 'x': 9 * _res_scale},
            'gridspec': (0, 7),
            'pixel_noise': 'cylindrical',
        },
        'loki_detector_8': {
            'detector_name': 'loki_detector_8',
            'resolution': {'y': 9 * _res_scale, 'x': 3 * _res_scale},
            'gridspec': (1, 8),
            'pixel_noise': 'cylindrical',
        },
    },
}
