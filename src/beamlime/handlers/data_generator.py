# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from logging import Logger

import h5py
import numpy as np
from numpy.random import Generator
from scipp import DataGroup

from beamlime.communication.broker import CommunicationBroker
from beamlime.config.preset_options import DEFAULT_TIMEOUT

from ..applications.interfaces import BeamlimeApplicationInterface


class FakeDreamDataGenerator(BeamlimeApplicationInterface):
    def __init__(
        self,
        /,
        app_name: str,
        broker: CommunicationBroker = None,
        logger: Logger = None,
        timeout: float = DEFAULT_TIMEOUT,
        pulse_rate: int = 100_000,
        original_file_name: str = "DREAM_baseline_all_dets.nxs",
        keep_tmp_file: bool = False,
        num_frame: int = 1_000_000,
        max_events=10e8,
        random_seed=0,
        **kwargs,
    ) -> None:
        super().__init__(app_name, broker, logger, timeout, 1 / pulse_rate, **kwargs)
        self.original_file_name = original_file_name
        self.keep_tmp_file = keep_tmp_file
        self.max_events = max_events
        self.random_seed = random_seed
        self.pulse_rate = pulse_rate
        self.num_frame = num_frame
        self.det_profiles = {
            "endcap_backward_detector": 1978368 // 8,
            "endcap_forward_detector": 1978368 // 8,
            "mantel_detector": 8257536 // 8,  # sic
            "high_resolution_detector": 1769472 // 8,
            "sans_detector": 1769472 // 8,
        }

    def __del__(self) -> None:
        # remove temporary file
        self.stop()

    def _create_fake_events(
        self, detector_gr: h5py.Group, n_pixel: int, i_frame: int, rng: Generator
    ) -> None:
        """Return events from a single pulse"""
        nevent = rng.integers(int(self.max_events / 10), self.max_events)

        if "detector_number" in detector_gr:
            del detector_gr["detector_number"]

        detector_gr["detector_number"] = np.arange(n_pixel)

        events = detector_gr.create_group("events")
        events.attrs["NX_class"] = "NXevent_data"

        events["event_index"] = np.linspace(0, nevent, num=1, dtype=np.int64)
        events["event_id"] = rng.integers(0, n_pixel, size=(nevent,))
        events["event_time_offset"] = rng.uniform(0.0, 71.0, size=(nevent,))
        events["event_time_offset"].attrs["units"] = "ms"
        events["event_time_zero"] = self.wait_interval * i_frame
        events["event_time_zero"].attrs["units"] = "ns"
        events["event_time_zero"].attrs["offset"] = "2023-01-01T00:00:00"

    def _create_tmp_nexus(self, tmp_file_name: str, max_events: int) -> None:
        rng = np.random.default_rng(self.random_seed)

        with h5py.File(tmp_file_name, mode="r+") as f:
            for i_frame in range(self.num_frame):
                group = f["entry/instrument"]
                for detname, n_pixels in self.det_profiles.items():
                    detector_gr = group[detname]
                    del detector_gr[f'{detname[:-len("_detector")]}_event_data']
                    self._create_fake_events(
                        detector_gr,
                        n_pixel=n_pixels,
                        i_frame=i_frame,
                        max_events=max_events,
                        rng=rng,
                    )
                    for dim in "xyz":
                        # These have a broken incompatible shape right now
                        key = f"{dim}_pixel_offset"
                        detector_gr.pop(key, None)

    def create_fake_events(self, original_file_name: str) -> DataGroup:
        import shutil
        from uuid import uuid4

        import scippnexus.v2 as snx

        tmp_file_name = uuid4().hex + original_file_name
        shutil.copyfile(original_file_name, tmp_file_name)
        self._create_tmp_nexus(tmp_file_name)

        with snx.File(tmp_file_name) as f:
            dg = f["/entry/instrument"][()]
        if not self.keep_tmp_file:
            shutil.rmtree(tmp_file_name)

        return dg

    async def _run(self) -> None:
        from uuid import uuid4

        from streaming_data_types.run_start_pl72 import serialise_pl72

        self.info("Start data generating...")

        run_start_msg = serialise_pl72(
            job_id=uuid4().hex,
            filename="not-saving-into-file",
            run_name="beamlime test run",
            nexus_structure="{}",
            instrument_name="DDREAM",
            broker="localhost:9092",
            metadata="{}",
            detector_spectrum_map=None,
            control_topic="",
        )

        self.produce(run_start_msg, channel="log")
        # dream_dg = self.create_fake_events()
        self.info("Finishing data feeding...")
