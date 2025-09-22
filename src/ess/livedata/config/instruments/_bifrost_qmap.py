# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Bifrost Q-map workflows."""

from enum import Enum

import pydantic
import sciline
import scipp as sc
import scippnexus as snx
from scippnexus import NXdetector

from ess.bifrost.data import (
    simulated_elastic_incoherent_with_phonon,
    tof_lookup_table_simulation,
)
from ess.bifrost.live import BifrostQCutWorkflow, CutAxis, CutAxis1, CutAxis2, CutData
from ess.livedata.config import Instrument
from ess.livedata.handlers.stream_processor_workflow import StreamProcessorWorkflow
from ess.livedata.parameter_models import EnergyEdges, QEdges
from ess.reduce.nexus.types import Filename, NeXusData, SampleRun
from ess.spectroscopy.types import (
    InstrumentAngle,
    PreopenNeXusFile,
    SampleAngle,
    TimeOfFlightLookupTableFilename,
)

fname = simulated_elastic_incoherent_with_phonon()
with snx.File(fname) as f:
    detector_names = list(f['entry/instrument'][snx.NXdetector])

workflow = BifrostQCutWorkflow(detector_names)
workflow[Filename[SampleRun]] = fname
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


def register_qmap_workflows(
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
