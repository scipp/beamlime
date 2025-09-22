"""
Bifrost with all banks merged into a single one.
"""

from collections.abc import Generator
from enum import Enum
from typing import NewType

import pydantic
import scipp as sc
from scippnexus import NXdetector

from ess.livedata.parameter_models import EdgesModel, QUnit, QEdges, EnergyEdges
from ess.livedata.config import Instrument, instrument_registry
from ess.livedata.config.env import StreamingEnv
from ess.livedata.config.workflows import register_monitor_timeseries_workflows
from ess.livedata.handlers.detector_data_handler import (
    DetectorLogicalView,
    LogicalViewConfig,
    get_nexus_geometry_filename,
)
from ess.livedata.handlers.stream_processor_workflow import StreamProcessorWorkflow
from ess.reduce.nexus.types import (
    CalibratedBeamline,
    DetectorData,
    Filename,
    NeXusData,
    NeXusName,
    SampleRun,
)
from ess.spectroscopy.indirect.time_of_flight import TofWorkflow
from ess.spectroscopy.types import InstrumentAngle, SampleAngle
from ess.bifrost import BifrostWorkflow
from ess.bifrost.live import CutAxis, CutAxis1, CutAxis2, CutData, BifrostQCutWorkflow

from ._ess import make_common_stream_mapping_inputs, make_dev_stream_mapping
import scipp as sc
import sciline
import time
from ess.spectroscopy.types import *
import scippnexus as snx

from ess.bifrost.data import (
    simulated_elastic_incoherent_with_phonon,
    tof_lookup_table_simulation,
)

fname = simulated_elastic_incoherent_with_phonon()
with snx.File(fname) as f:
    detector_names = list(f['entry/instrument'][snx.NXdetector])
detector_names = detector_names[:2]
print(f'{fname=}')

workflow = BifrostQCutWorkflow(detector_names)
workflow[Filename[SampleRun]] = simulated_elastic_incoherent_with_phonon()
workflow[TimeOfFlightLookupTableFilename] = tof_lookup_table_simulation()
workflow[PreopenNeXusFile] = PreopenNeXusFile(True)

q_vectors = {
    'Qx': sc.vector([1, 0, 0]),
    'Qy': sc.vector([0, 1, 0]),
    'Qz': sc.vector([0, 0, 1]),
}


class CutAxisOption(str, Enum):
    Q = '|Q|'
    Qx = 'Qx'
    Qy = 'Qy'
    Qz = 'Qz'
    dE = 'ΔE'


class CustomQAxisOption(str, Enum):
    CUSTOM = 'custom'
    Q = '|Q|'
    Qx = 'Qx'
    Qy = 'Qy'
    Qz = 'Qz'


class CustomQAxisParams(pydantic.BaseModel):
    axis: CutAxisOption = pydantic.Field(
        default=CutAxisOption.Q,
        description="Cut axis.",
    )
    qx: int = pydantic.Field(
        default=0, title='Qx', description="Custom x component of the cut axis."
    )
    qy: int = pydantic.Field(
        default=0, title='Qy', description="Custom y component of the cut axis."
    )
    qz: int = pydantic.Field(
        default=0, title='Qz', description="Custom z component of the cut axis."
    )


class BifrostQMapParams(pydantic.BaseModel):
    q_edges: QEdges = pydantic.Field(
        default=QEdges(start=0.5, stop=1.5, num_bins=100),
        description="Q bin edges.",
    )
    energy_edges: EnergyEdges = pydantic.Field(
        default=EnergyEdges(start=-0.1, stop=0.1, num_bins=100),
        description="Energy transfer bin edges.",
    )

    def get_q_cut(self) -> CutAxis:
        bins = self.q_edges.get_edges()
        return CutAxis(
            output=bins.dim,
            fn=lambda sample_table_momentum_transfer: sc.norm(
                x=sample_table_momentum_transfer
            ),
            bins=bins,
        )

    def get_energy_cut(self) -> CutAxis:
        bins = self.energy_edges.get_edges()
        return CutAxis(
            output=bins.dim, fn=lambda energy_transfer: energy_transfer, bins=bins
        )


class QAxisOption(str, Enum):
    Qx = 'Qx'
    Qy = 'Qy'
    Qz = 'Qz'


class QAxisSelection(pydantic.BaseModel):
    axis: QAxisOption = pydantic.Field(description="Cut axis.")


class QAxisParams(QEdges, QAxisSelection):
    def to_cut_axis(self) -> CutAxis:
        vec = q_vectors[self.axis.value]
        dim = self.axis.value
        edges = self.get_edges().rename(Q=dim)
        return CutAxis.from_q_vector(output=dim, vec=vec, bins=edges)


class BifrostElasticQMapParams(pydantic.BaseModel):
    axis1: QAxisParams = pydantic.Field(
        default=QAxisParams(axis=QAxisOption.Qx, start=-2.0, stop=2.0, num_bins=100),
        description="First cut axis.",
    )
    axis2: QAxisParams = pydantic.Field(
        default=QAxisParams(axis=QAxisOption.Qz, start=-2.0, stop=2.0, num_bins=100),
        description="Second cut axis.",
    )


def register_qcut_workflows(
    instrument: Instrument,
) -> None:
    @instrument.register_workflow(
        name='qmap',
        version=1,
        title='Q map',
        description='',
        source_names=['unified_detector'],
        aux_source_names=['detector_rotation', 'sample_rotation'],
    )
    def _qmap_workflow(params: BifrostQMapParams) -> StreamProcessorWorkflow:
        wf = workflow.copy()
        wf[CutAxis1] = params.get_q_cut()
        wf[CutAxis2] = params.get_energy_cut()
        return _make_cut_stream_processor(wf)

    @instrument.register_workflow(
        name='elastic_qmap',
        version=1,
        title='Elastic Q map',
        description='',
        source_names=['unified_detector'],
        aux_source_names=['detector_rotation', 'sample_rotation'],
    )
    def _elastic_qmap_workflow(
        params: BifrostElasticQMapParams,
    ) -> StreamProcessorWorkflow:
        wf = workflow.copy()
        wf[CutAxis1] = params.axis1.to_cut_axis()
        wf[CutAxis2] = params.axis2.to_cut_axis()
        return _make_cut_stream_processor(wf)


def _make_cut_stream_processor(workflow: sciline.Pipeline) -> StreamProcessorWorkflow:
    return StreamProcessorWorkflow(
        workflow,
        dynamic_keys={'unified_detector': NeXusData[NXdetector, SampleRun]},
        context_keys={
            'detector_rotation': InstrumentAngle[SampleRun],
            'sample_rotation': SampleAngle[SampleRun],
        },
        target_keys=(CutData[SampleRun],),
        accumulators=(CutData[SampleRun],),
    )
