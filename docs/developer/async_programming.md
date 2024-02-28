# Asynchronous Programming

As a real-time data handling framework with user-interface functionalities,
``beamlime`` needs to handle asynchronous processes.

This page explains asynchronous characteristic of ``beamlime`` and debugging logs.

## Debugging Logs

### `asyncio.get_event_loop` vs `asyncio.new_event_loop`

1. `asyncio.get_event_loop`
    ``get_event_loop`` will always return the current event loop.
    If there is no event loop set in the thread, it will create a new one
    and set it as a current event loop of the thread, and return the loop.
    Many of ``asyncio`` free functions internally use ``get_event_loop``,
    i.e. `asyncio.create_task`.

    **Things to consider while using `asyncio.get_event_loop`.**

    * ``asyncio.create_task`` does not guarantee
        whether the current loop is/will be alive until the task is done.
        You may use ``run_until_complete`` to make sure the loop is not closed
        until the task is finished.
        When you need to throw multiple async calls to the loop,
        use ``asyncio.gather`` to merge all the tasks like in this method.
    * ``close`` or ``stop`` might accidentally destroy/interrupt
        other tasks running in the same event loop.
        i.e. You can accidentally destroy the event loop of a jupyter kernel.
    * *1* :class:`RuntimeError` if there has been an event loop set in the
        thread object before but it is now removed.

2. `asyncio.new_event_loop`
    ``asyncio.new_event_loop`` will always return the new event loop,
    but it is not set it as a current loop of the thread automatically.

    However, sometimes it is automatically handled within the thread,
    and it caused errors which was hard to debug under ``pytest`` session.
    For example,

    * The new event loop was not closed properly as it is destroyed.
    * The new event loop was never started until it is destroyed.


    ``Traceback`` of ``pytest`` did not show
    where exactly the error is from in those cases.
    It was resolved by using `asyncio.get_event_loop`,
    or manually closing the event loop at the end of the test.

    **When to use** `asyncio.new_event_loop`.

    * `asyncio.get_event_loop` raises `RuntimeError` *1*.
    * Multi-threads.

Please note that the loop object might need to be **closed** manually.
