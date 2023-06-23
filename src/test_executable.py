from logging import DEBUG

from beamlime.applications.controller import Controller, app_factory
from beamlime.constructors import Factory
from beamlime.logging import BeamlimeLogger
from beamlime.ready_factory import log_factory

factory = Factory(log_factory, app_factory)
factory[BeamlimeLogger].setLevel(DEBUG)
ctrl = factory[Controller]
ctrl.run()
