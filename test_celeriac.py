import time
from unittest.mock import patch

from celeriac_queue import Celeriac

# Import the functions from main.py
celeriac = Celeriac("test")


@celeriac.register
def some_log_running_task():
    time.sleep(100)


@celeriac.register
def some_task_that_is_called_en_masse(x: int, y: int):
    print(f"x: {x}, y: {y}")


# Tests
def test_celeriac_project():
    """Test that the celeriac project is properly set up."""
    assert True


def test_pytest_working():
    """Test that pytest is working correctly."""
    expected = 2 + 2
    assert expected == 4


def test_celeriac_instance():
    """Test that Celeriac instance is created correctly."""
    assert celeriac is not None
    assert isinstance(celeriac, Celeriac)


def test_some_task_that_is_called_en_masse():
    """Test the some_task_that_is_called_en_masse function."""
    # Mock the executor's execute_task method
    with patch.object(celeriac.client, "receive_batch") as mock_receive_batch:
        # Verify execute_task was called when delay() is invoked
        some_task_that_is_called_en_masse.delay(5, 10)
        celeriac.flush()
        mock_receive_batch.assert_called_once()
        assert celeriac.processing_complete()


def test_some_log_running_task():
    """Test the some_log_running_task function."""
    # Mock the executor's execute_task method
    with patch.object(celeriac.client, "receive_batch") as mock_receive_batch:
        # Verify execute_task was called when delay() is invoked
        some_log_running_task.delay()
        celeriac.flush()
        mock_receive_batch.assert_called_once()
        assert celeriac.processing_complete()


# Buffering Tests - These should fail initially
def test_single_task_sent_immediately():
    """Test that a single task is sent immediately without buffering."""
    with patch.object(celeriac.client, "receive_batch") as mock_receive_batch:
        some_task_that_is_called_en_masse.delay(1, 2)

        # Should be called immediately with a single task
        celeriac.flush()
        mock_receive_batch.assert_called_once()
        args, kwargs = mock_receive_batch.call_args
        assert len(args[0]) == 1  # Single task in batch
        assert celeriac.processing_complete()


def test_multiple_tasks_buffered_until_batch_size():
    """Test that multiple tasks are buffered until batch size is reached."""
    with patch.object(celeriac.client, "receive_batch") as mock_receive_batch:
        # Send 19 tasks (should be buffered, not sent yet)
        for i in range(19):
            some_task_that_is_called_en_masse.delay(i, i + 1)

        # Should not be called yet (still buffering)
        assert mock_receive_batch.call_count == 0

        # Wait 200ms - tasks should be sent after this delay
        time.sleep(0.2)

        # Should be called once with exactly 19 tasks after 200ms delay
        mock_receive_batch.assert_called_once()
        args, kwargs = mock_receive_batch.call_args
        assert len(args[0]) == 19

        # Send the 20th task (should trigger immediate batch send)
        some_task_that_is_called_en_masse.delay(19, 20)

        # Should be called twice: once with 19 tasks (after delay), once with 1 task (immediate)
        celeriac.flush()  # Wait for tasks to be processed
        assert mock_receive_batch.call_count == 2
        calls = mock_receive_batch.call_args_list
        assert len(calls[0][0][0]) == 19  # First batch: 19 tasks (after delay)
        assert len(calls[1][0][0]) == 1  # Second batch: 1 task (immediate)
        assert celeriac.processing_complete()


def test_batch_size_limit_enforced():
    """Test that batches never exceed 20 tasks."""
    with patch.object(celeriac.client, "receive_batch") as mock_receive_batch:
        # Send 25 tasks
        for i in range(25):
            some_task_that_is_called_en_masse.delay(i, i + 1)

        # Should be called twice: once with 20 tasks, once with 5 tasks
        celeriac.flush()  # Wait for all tasks to be processed
        assert mock_receive_batch.call_count == 2

        calls = mock_receive_batch.call_args_list
        assert len(calls[0][0][0]) == 20  # First batch: 20 tasks
        assert len(calls[1][0][0]) == 5  # Second batch: 5 tasks
        assert celeriac.processing_complete()


def test_tasks_sent_in_order():
    """Test that tasks are sent in the order they were invoked."""
    with patch.object(celeriac.client, "receive_batch") as mock_receive_batch:
        # Send tasks with specific values
        some_task_that_is_called_en_masse.delay(1, 1)
        some_task_that_is_called_en_masse.delay(2, 2)
        some_task_that_is_called_en_masse.delay(3, 3)

        # Should not be called immediately (buffering)
        assert mock_receive_batch.call_count == 0

        # Wait 200ms - tasks should be sent after this delay
        time.sleep(0.2)

        # Should be called once with 3 tasks in order
        mock_receive_batch.assert_called_once()
        args, kwargs = mock_receive_batch.call_args
        batch = args[0]

        # Check that tasks are in the correct order
        assert batch[0]["args"] == (1, 1)
        assert batch[1]["args"] == (2, 2)
        assert batch[2]["args"] == (3, 3)
        assert celeriac.processing_complete()


def test_partial_batch_sent_after_delay():
    """Test that partial batches are sent after a reasonable delay."""
    with patch.object(celeriac.client, "receive_batch") as mock_receive_batch:
        # Send 5 tasks
        for i in range(5):
            some_task_that_is_called_en_masse.delay(i, i + 1)

        # Should not be called immediately
        assert mock_receive_batch.call_count == 0

        # Wait 200ms (simulating time passing)
        time.sleep(0.2)

        # Should be called once with 5 tasks after delay
        mock_receive_batch.assert_called_once()
        args, kwargs = mock_receive_batch.call_args
        assert len(args[0]) == 5
        assert celeriac.processing_complete()


def test_mixed_task_types_in_batch():
    """Test that different task types can be mixed in the same batch."""
    with patch.object(celeriac.client, "receive_batch") as mock_receive_batch:
        # Send mixed task types
        some_task_that_is_called_en_masse.delay(1, 2)
        some_log_running_task.delay()
        some_task_that_is_called_en_masse.delay(3, 4)

        # Should not be called immediately (buffering)
        assert mock_receive_batch.call_count == 0

        # Wait 200ms - tasks should be sent after this delay
        time.sleep(0.2)

        # Should be called once with 3 tasks
        mock_receive_batch.assert_called_once()
        args, kwargs = mock_receive_batch.call_args
        batch = args[0]

        assert len(batch) == 3
        # Check that different task types are preserved
        assert batch[0]["task"] == "test_celeriac$some_task_that_is_called_en_masse"
        assert batch[1]["task"] == "test_celeriac$some_log_running_task"
        assert batch[2]["task"] == "test_celeriac$some_task_that_is_called_en_masse"
        assert celeriac.processing_complete()
