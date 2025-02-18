import numpy as np
import scipp as sc
from ess.reduce.live import raw

sizes = {'x': 100, 'y': 3}
dim = 'detector_number'
start = 123
detector_start = 100
detectors_config = {}
detectors_config['detectors'] = {}
for i in range(1, 10):
    for j in range(1, 6):
        detector_number_start = detector_start * i + ((j - 1) * 2700)
        array = np.concatenate(
            [
                np.arange(detector_number_start + 1 - 100, detector_number_start + 1),
                np.arange(
                    detector_number_start + 1 - 100 + 900,
                    detector_number_start + 1 + 900,
                ),
                np.arange(
                    detector_number_start + 1 - 100 + 1800,
                    detector_number_start + 1 + 1800,
                ),
            ]
        )
        detectors_config['detectors'][f'Channel ({i}, {j})'] = {
            'detector_name': f'{start}_channel_{i}_{j}_triplet',
            'projection': raw.LogicalView(),
            'detector_number': sc.Variable(dims=[dim], values=array).fold(
                dim=dim, sizes=sizes
            ),
        }
        start += 4
    start += 1
