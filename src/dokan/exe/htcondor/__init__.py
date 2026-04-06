"""NNLOJET execution on HTCondor.

The HTCondor backend used to keep one long-lived local Luigi worker per remote
job by blocking in a polling loop after submission.  That scales poorly for the
batch-system use case, so submission and completion tracking are split:

* ``HTCondorExec`` submits the job and returns immediately.
* ``HTCondorTracker`` is a lightweight external task that periodically checks
  the cluster id and finalises ``job.json`` once the job leaves the queue.
"""

import json
import os
import re
import string
import subprocess
import time
from pathlib import Path

import luigi

from ..._types import GenericPath
from ...db._loglevel import LogLevel
from .._executor import Executor
from .._exe_data import ExeData


class HTCondorRetry(RuntimeError):
    """Expected retry condition for non-blocking HTCondor tracking."""

    dokan_retry_no_trace: bool = True


class HTCondorExec(Executor):
    """Task to execute batch jobs on HTCondor

    Attributes
    ----------
    _file_sub : str
        name of the HTCondor submisison file
    """

    _file_sub: str = "job.sub"
    _file_submit_state: str = "htcondor_submit.json"

    # @todo consider using `concurrency_limits` instead?
    @property
    def resources(self):
        return {"jobs_concurrent": self.njobs}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.htcondor_template: Path = Path(
            self.exe_data["policy_settings"]["htcondor_template"]
            if "htcondor_template" in self.exe_data["policy_settings"]
            else self.templates()[0]  # default to first template
        )
        self.file_sub: Path = Path(self.path) / self._file_sub
        self.file_submit_state: Path = Path(self.path) / self._file_submit_state
        self.njobs: int = len(self.exe_data["jobs"])

    def output(self):
        """Submission completes once a durable submit marker is written."""
        return [luigi.LocalTarget(self.file_submit_state)]

    @staticmethod
    def templates() -> list[GenericPath]:
        template_list: list[str] = ["htcondor.template", "lxplus.template"]
        return [Path(__file__).parent.resolve() / t for t in template_list]

    @staticmethod
    def _query_job(job_id: int) -> tuple[bool | None, list[dict]]:
        """Inspect a cluster id via condor_q.

        Returns
        -------
        tuple
            ``(is_active, ads)`` where ``is_active`` is:
            * ``True`` when at least one proc is idle/running,
            * ``False`` when the cluster is terminal / no longer in queue,
            * ``None`` when the query failed and should be retried later.
        """

        condor_q = subprocess.run(["condor_q", "-json", str(job_id)], capture_output=True, text=True)
        if condor_q.returncode != 0:
            return None, []
        if condor_q.stdout.strip() == "":
            return False, []

        ads: list[dict] = json.loads(condor_q.stdout)
        if not ads:
            return False, []

        # HTCondor JobStatus:
        # 0 Unexpanded, 1 Idle, 2 Running, 3 Removed, 4 Completed, 5 Held, 6 Submission_err
        active_status = {0, 1, 2, 5}  # include Held
        is_active = any(entry.get("JobStatus") in active_status for entry in ads)
        return is_active, ads

    def complete(self) -> bool:
        """Treat submission as the completion condition for the executor task."""
        self.exe_data.load()
        if self.exe_data.is_final:
            return True
        return self.exe_data["policy_settings"].get("htcondor_id", -1) > 0

    def run(self):
        """Submit the HTCondor job and persist the cluster id.

        Unlike the generic executor path, we intentionally do not wait for the
        remote job to finish here.  Completion is handled by ``HTCondorTracker``.
        """
        self.exe_data.scan_dir([self._file_log])
        if "timestamp" not in self.exe_data:
            self.exe_data["timestamp"] = time.time()
        self.exe_data.write()
        self._write_submit_state()
        if self.complete():
            return
        self.exe()

    def _write_submit_state(self) -> None:
        """Persist a stable submission marker for Luigi dependency checks."""
        cluster_id = self.exe_data["policy_settings"].get("htcondor_id", -1)
        if cluster_id <= 0:
            return
        tmp = self.file_submit_state.with_suffix(".json.tmp")
        with open(tmp, "w") as f:
            json.dump({"job_id": cluster_id}, f, indent=2)
        os.replace(tmp, self.file_submit_state)

    def exe(self):
        if self.complete():
            return

        # > populate the submission template file
        condor_settings: dict = {
            "exe": self.exe_data["exe"],
            "job_path": str(self.exe_data.path.absolute()),
            "ncores": self.exe_data["policy_settings"]["htcondor_ncores"]
            if "htcondor_ncores" in self.exe_data["policy_settings"]
            else 1,
            "start_seed": min(job["seed"] for job in self.exe_data["jobs"].values()),
            "nseed": self.njobs,
            "input_files": ", ".join(self.exe_data["input_files"]),
            "max_runtime": int(self.exe_data["policy_settings"]["max_runtime"]),
        }
        with open(self.htcondor_template) as t, open(self.file_sub, "w") as f:
            f.write(string.Template(t.read()).substitute(condor_settings))

        job_env = os.environ.copy()
        job_env["OMP_NUM_THREADS"] = "{}".format(condor_settings["ncores"])
        job_env["OMP_STACKSIZE"] = "1024M"

        cluster_id: int = -1  # init failed state
        re_cluster_id = re.compile(r".*job\(s\) submitted to cluster\s+(\d+).*", re.DOTALL)

        for _ in range(self.exe_data["policy_settings"]["htcondor_nretry"]):
            condor_submit = subprocess.run(
                ["condor_submit", HTCondorExec._file_sub],
                env=job_env,
                cwd=self.exe_data.path,
                capture_output=True,
                text=True,
            )
            if condor_submit.returncode == 0 and (match_id := re.match(re_cluster_id, condor_submit.stdout)):
                cluster_id = int(match_id.group(1))
                self.exe_data["policy_settings"]["htcondor_id"] = cluster_id
                self.exe_data.write()
                self._write_submit_state()
                break
            else:
                self._logger(
                    f"HTCondorExec failed to submit job {self.exe_data.path}:\n"
                    + f"{condor_submit.stdout}\n"
                    + f"{condor_submit.stderr}",
                    LogLevel.INFO,
                )
                time.sleep(self.exe_data["policy_settings"]["htcondor_retry_delay"])

        if cluster_id < 0:
            self._logger(
                f"HTCondorExec failed to submit job (exhausted max retries) {self.exe_data.path}",
                LogLevel.WARN,
            )
            return  # failed job


class HTCondorTracker(luigi.Task):
    """Wait for a submitted HTCondor job to leave the queue.

    This is a lightweight active task.  Each run does a single ``condor_q``
    check and either finalises ``job.json`` or raises to be retried by Luigi
    after the scheduler retry delay.  That avoids both a long-lived sleeping
    worker and the limitations of ``ExternalTask`` for dynamic dependencies.
    """

    path: str = luigi.Parameter()

    _file_state: str = "htcondor_tracker.json"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exe_data = ExeData(Path(self.path))
        self.state_file = Path(self.path) / self._file_state

    def output(self):
        return luigi.LocalTarget(self.exe_data.file_fin)

    def requires(self):
        return Executor.factory(
            policy=self.exe_data["policy"],
            path=str(self.exe_data.path.absolute()),
        )

    def _read_state(self) -> dict:
        if not self.state_file.exists():
            return {}
        try:
            with open(self.state_file) as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return {}

    def _write_state(self, **state) -> None:
        tmp = self.state_file.with_suffix(".json.tmp")
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, self.state_file)

    def complete(self) -> bool:
        self.exe_data.load()
        return self.exe_data.is_final

    def on_failure(self, exception):
        if isinstance(exception, HTCondorRetry):
            return str(exception)
        return super().on_failure(exception)

    def run(self):
        self.exe_data.load()
        if self.exe_data.is_final:
            return

        job_id = self.exe_data["policy_settings"].get("htcondor_id", -1)
        if job_id <= 0:
            raise RuntimeError(f"HTCondorTracker missing cluster id at {self.path}")

        poll_time = self.exe_data["policy_settings"]["htcondor_poll_time"]
        state = self._read_state()
        now = time.time()
        if state.get("job_id") == job_id and (now - state.get("timestamp", 0.0)) < poll_time:
            raise HTCondorRetry(f"HTCondorTracker polling too soon for cluster {job_id}")

        is_active, _ads = HTCondorExec._query_job(job_id)
        self._write_state(job_id=job_id, timestamp=now, active=is_active)
        if is_active is None:
            raise HTCondorRetry(f"HTCondorTracker failed to query cluster {job_id}")
        if is_active:
            raise HTCondorRetry(f"HTCondorTracker cluster {job_id} still active")

        self.exe_data.scan_dir([self._file_state, HTCondorExec._file_submit_state, Executor._file_log])
        self.exe_data.finalize()
