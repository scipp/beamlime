# Virtual Environments

There are integration tests and unit tests of ``beamlime`` that are depending on other frameworks, i.e. ``kafka``.

Those tests are marked to be skipped by default.

Since ``beamlime`` is not bound to specific real-time data streaming or GUI frameworks,
they are not included in the dependency of the package.

Therefore users or developers may need to deploy or install the frameworks locally.

This section is for setting up virtual environments or services to enable the full tests.

```{include} kafka.md
```
