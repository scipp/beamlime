import scipp as sc
from ess.reduce.live import raw

detector_number = sc.arange('detector_number', 1, 5 * 3 * 9 * 100 + 1, unit=None).fold(
    dim='detector_number', sizes={'analyzer': 5, 'tube': 3, 'sector': 9, 'pixel': 100}
)
start = 123
detectors_config = {}
detectors_config['detectors'] = {}
# Each NXdetetor is a He3 tube triplet with shape=(3, 100). Detector numbers in triplet
# are *not* consecutive:
# 1...900 with increasing angle (across all sectors)
# 901 is back to first sector and detector, second tube
for sector in range(1, 10):
    for analyzer in range(1, 6):
        # The slice is non-contiguous, so we make a copy
        det_num = detector_number['sector', sector - 1]['analyzer', analyzer - 1].copy()
        detectors_config['detectors'][f'Channel ({sector}, {analyzer})'] = {
            'detector_name': f'{start}_channel_{sector}_{analyzer}_triplet',
            'projection': raw.LogicalView(),
            'detector_number': det_num,
        }
        start += 4
    start += 1
