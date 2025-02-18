from ess.reduce.live import raw

start = 123
detectors_config = {}
detectors_config['detectors'] = {}
for i in range(1, 10):
    for j in range(1, 6):
        detectors_config['detectors'][f'Channel ({i}, {j})'] = {
            'detector_name': f'{start}_channel_{i}_{j}_triplet',
            'projection': raw.LogicalView(),
        }
        start += 4
    start += 1
