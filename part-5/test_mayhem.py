# contents of test_mayhem.py
#!/usr/bin/env python3.7
"""
Notice! This requires pytest==4.3.1
"""

import asyncio
import pytest
import os
import signal
import time
import threading
import mayhem


@pytest.fixture
def create_mock_coro(mocker, monkeypatch):
    def _create_mock_patch_coro(to_patch=None):
        mock = mocker.Mock()

        async def _coro(*args, **kwargs):
            return mock(*args, **kwargs)

        if to_patch:  # <-- may not need/want to patch anything
            monkeypatch.setattr(to_patch, _coro)
        return mock, _coro

    return _create_mock_patch_coro


@pytest.fixture
def message():
    return mayhem.PubSubMessage(message_id="1234", instance_name="mayhem_test")


@pytest.fixture
def mock_sleep(create_mock_coro):
    # won't need the returned coroutine here
    mock, _ = create_mock_coro(to_patch="mayhem.asyncio.sleep")
    return mock


@pytest.fixture
def mock_queue(mocker, monkeypatch):
    queue = mocker.Mock()
    monkeypatch.setattr(mayhem.asyncio, "Queue", queue)
    return queue.return_value


@pytest.fixture
def mock_get(mock_queue, create_mock_coro):
    mock_get, coro_get = create_mock_coro()
    mock_queue.get = coro_get
    return mock_get


@pytest.mark.asyncio
async def test_save(mock_sleep, message):
    assert not message.saved  # sanity check
    await mayhem.save(message)
    assert message.saved
    assert 1 == mock_sleep.call_count


@pytest.fixture
def event_loop(event_loop, mocker):
    new_loop = asyncio.get_event_loop_policy().new_event_loop()
    asyncio.set_event_loop(new_loop)
    new_loop._close = new_loop.close
    new_loop.close = mocker.Mock()

    yield new_loop

    new_loop._close()


@pytest.mark.asyncio
async def test_consume(mock_get, mock_queue, message, create_mock_coro):
    mock_get.side_effect = [
        message,
        Exception("break while loop"),
    ]
    mock_handle_message, _ = create_mock_coro("mayhem.handle_message")

    with pytest.raises(Exception, match="break while loop"):
        await mayhem.consume(mock_queue)

    ret_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    # should be 1 per side effect minus the Exception (i.e. messages consumed)
    assert 1 == len(ret_tasks)
    mock_handle_message.assert_not_called()  # <-- sanity check

    # explicitly await tasks scheduled by `asyncio.create_task`
    await asyncio.gather(*ret_tasks)

    mock_handle_message.assert_called_once_with(message)


@pytest.mark.parametrize("tested_signal", ("SIGINT", "SIGTERM", "SIGHUP", "SIGUSR1"))
def test_main(tested_signal, create_mock_coro, event_loop, mock_queue, mocker):
    tested_signal = getattr(signal, tested_signal)
    mock_consume, _ = create_mock_coro("mayhem.consume")
    mock_publish, _ = create_mock_coro("mayhem.publish")
    # mock out `asyncio.gather` that `shutdown` calls instead
    # of `shutdown` itself
    mock_asyncio_gather, _ = create_mock_coro("mayhem.asyncio.gather")

    mock_shutdown = mocker.Mock()

    def _shutdown():
        mock_shutdown()
        event_loop.stop()

    event_loop.add_signal_handler(signal.SIGUSR1, _shutdown)

    def _send_signal():
        time.sleep(0.1)
        os.kill(os.getpid(), tested_signal)

    thread = threading.Thread(target=_send_signal, daemon=True)
    thread.start()

    mayhem.main()

    assert tested_signal in event_loop._signal_handlers
    assert mayhem.handle_exception == event_loop.get_exception_handler()

    mock_consume.assert_called_once_with(mock_queue)
    mock_publish.assert_called_once_with(mock_queue)

    if tested_signal is not signal.SIGUSR1:
        mock_asyncio_gather.assert_called_once_with(return_exceptions=True)
        mock_shutdown.assert_not_called()
    else:
        mock_asyncio_gather.assert_not_called()
        mock_shutdown.assert_called_once_with()

    # asserting the loop is stopped but not closed
    assert not event_loop.is_running()
    assert not event_loop.is_closed()
    event_loop.close.assert_called_once_with()
