# BeamLine Status and Planning

## General

- Where do we put the ScippNexus bits?
  Is the idea to use ScippNeutron for the parsing of each message?
  - The workflow should deal with this, invisible to BeamLime?
- How does cancellation work?
  Or stopping on user request?
- prototype_mini.py BasePrototype has too many workflow details?
- Workflow, DataReductionApp and related?
- DataReductionApp stop conditions?
- DataReductionApp self.workflow is a factory for the output type, how does this interact with input/output pipes?
  Or is it a factory for the function that can make the output from the input? does workflow.constant_provider add to factory?
  Line 228!
  Can all this be avoided by making the workflow a callable that takes a single input?
- What is the point of splitting the workflow into apps, based on function?
  The chunk merging should be quite static, workflow configurable with single input and output?
  SingletonProvider not needed?
- Concat and load instrument specific?
  Do we only need to concat events and logs, merge with run-start and hand off to workflow (which can use scippnexus)?
  That is, concat can be generic, "loader" part of workflow (black box)?
- tests/prototypes/parameters.py params/config embedded in code, what is the plan here?
- How should workflow handle things such as frame unwrapping, which may require longer-term info, but also history from previous chunks?
  Do it internally, not exposed to BeamLime!?
- We must concat raw data, before changing event layout or anything!
  We want to feed concat result to ScippNexus.
- Do we need a way to avoid timing out when there are now events for a long time, e.g., when beam is down, but there is no need to restart dashboard.
- #95 redundant if we do not handle workflow pieces via Beamline's DI framework?

## Kafka

- tests/prototypes/prototype_kafka.py: only single partition supported, do we need to change that? What is the plan?
- run-start message pl72. See scippneutron.data_steaming._consumer.py
- Is the plan to copy concrete Kafka bits from scippneutron into BeamLime?