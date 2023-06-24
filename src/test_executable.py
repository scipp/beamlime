from logging import DEBUG

from beamlime.applications.controller import Controller
from beamlime.constructors import Factory
from beamlime.logging import BeamlimeLogger
from beamlime.ready_factory import app_factory, log_factory

factory = Factory(log_factory, app_factory)
factory[BeamlimeLogger].setLevel(DEBUG)
ctrl = factory[Controller]
ctrl.run()
