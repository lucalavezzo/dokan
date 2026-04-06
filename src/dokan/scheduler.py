import os

from luigi import rpc, scheduler, worker
from luigi.event import Event


def _patch_retry_logging() -> None:
    """Downgrade expected retry conditions from traceback noise to info logs."""
    if getattr(worker.TaskProcess, "_dokan_retry_logging_patched", False):
        return

    original = worker.TaskProcess._handle_run_exception

    def _handle_run_exception(self, ex):
        if getattr(ex, "dokan_retry_no_trace", False):
            worker.logger.info(
                "[pid %s] Worker %s retrying  %s: %s",
                os.getpid(),
                self.worker_id,
                self.task,
                ex,
            )
            self.task.trigger_event(Event.FAILURE, self.task, ex)
            return self.task.on_failure(ex)
        return original(self, ex)

    worker.TaskProcess._handle_run_exception = _handle_run_exception
    worker.TaskProcess._dokan_retry_logging_patched = True


_patch_retry_logging()


class WorkerSchedulerFactory:
    """The dokan scheduler factory

    This scheduler factory is almost identical to the one within luigi.
      luigi/interface.py: _WorkerSchedulerFactory
      luigi/worker.py: worker
    It's minimally adapted to allow for additional options to be passed to the scheduler.
    We do this since we want to avoid the use of a `luigi.cfg` file
    and want to use the `luigi.build` function to start the workflow.
    """

    def __init__(self, **kwargs):
        self.resources = kwargs.pop("resources", None)
        self.cache_task_completion = kwargs.pop("cache_task_completion", False)
        self.check_complete_on_run = kwargs.pop("check_complete_on_run", False)
        self.check_unfulfilled_deps = kwargs.pop("check_unfulfilled_deps", True)
        self.retry_external_tasks = kwargs.pop("retry_external_tasks", False)
        self.retry_delay = kwargs.pop("retry_delay", None)
        self.wait_interval = kwargs.pop("wait_interval", 0.1)  # luigi default: 1.0
        self.wait_jitter = kwargs.pop("wait_jitter", 0.5)  # luigi default: 5.0
        self.ping_interval = kwargs.pop("ping_interval", 0.1)  # luigi default: 1.0

        if kwargs:
            raise RuntimeError(f"WorkerSchedulerFactory: left-over options {kwargs}")

    def create_local_scheduler(self):
        scheduler_kwargs = {
            "prune_on_get_work": True,
            "record_task_history": False,
            "resources": self.resources,
        }
        if self.retry_delay is not None:
            scheduler_kwargs["retry_delay"] = self.retry_delay
        return scheduler.Scheduler(**scheduler_kwargs)

    def create_remote_scheduler(self, url):
        return rpc.RemoteScheduler(url)

    def create_worker(self, scheduler, worker_processes, assistant=False):
        return worker.Worker(
            scheduler=scheduler,
            worker_processes=worker_processes,
            assistant=assistant,
            cache_task_completion=self.cache_task_completion,
            check_complete_on_run=self.check_complete_on_run,
            check_unfulfilled_deps=self.check_unfulfilled_deps,
            retry_external_tasks=self.retry_external_tasks,
            wait_interval=self.wait_interval,
            wait_jitter=self.wait_jitter,
            ping_interval=self.ping_interval,
        )
