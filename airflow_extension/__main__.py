"""Dynatrace Extension 2.0 entrypoint for Apache Airflow 2.10+/3.x."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from dynatrace_extension import DtEventType, Extension, Status, StatusValue
from dynatrace_extension.sdk.metric import MetricType, SummaryStat

from .client import AirflowAPIError, AirflowClient, BoundedSet, EndpointConfig, parse_iso


@dataclass
class SummaryAccumulator:
    count: int = 0
    total: float = 0.0
    minimum: float = float("inf")
    maximum: float = float("-inf")

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
        if value < self.minimum:
            self.minimum = value
        if value > self.maximum:
            self.maximum = value

    def to_summary_stat(self) -> SummaryStat:
        return SummaryStat(
            value_min=self.minimum,
            value_max=self.maximum,
            value_sum=self.total,
            value_count=self.count,
        )


@dataclass
class EndpointRuntimeState:
    cfg: EndpointConfig
    client: AirflowClient
    airflow_version: str = ""
    initialized: bool = False
    run_watermark: datetime = field(default_factory=lambda: datetime.now(UTC))
    ti_watermark: datetime = field(default_factory=lambda: datetime.now(UTC))
    audit_watermark: datetime = field(default_factory=lambda: datetime.now(UTC))
    seen_runs: BoundedSet = field(default_factory=BoundedSet)
    seen_tis: BoundedSet = field(default_factory=BoundedSet)
    seen_audits: BoundedSet = field(default_factory=BoundedSet)


class AirflowExtension(Extension):
    """Monitors Apache Airflow 2.10+/2.11 and 3.x clusters via REST API v1/v2."""

    def initialize(self) -> Status | None:
        self._endpoints: list[EndpointRuntimeState] = []
        raw_endpoints = self.activation_config.get("endpoints") or []
        for raw in raw_endpoints:
            cfg = self._parse_endpoint(raw)
            state = EndpointRuntimeState(cfg=cfg, client=AirflowClient(cfg))
            self._endpoints.append(state)
            interval = max(int(cfg.collection_interval_seconds or 30), 10)
            self.schedule(self._scrape_endpoint, timedelta(seconds=interval), args=(state,))
        return Status(StatusValue.OK, f"Initialized {len(self._endpoints)} Airflow endpoint(s)")

    def on_shutdown(self) -> None:
        for state in getattr(self, "_endpoints", []):
            state.client.close()

    def fastcheck(self) -> Status:
        raw_endpoints = self.activation_config.get("endpoints") or []
        if not raw_endpoints:
            return Status(StatusValue.INVALID_CONFIG_ERROR, "No Airflow endpoints configured")

        for raw in raw_endpoints:
            cfg = self._parse_endpoint(raw)
            client = AirflowClient(cfg)
            try:
                api_ver = client.api_version()
                af_ver = client.get_version()
                self.logger.info(
                    "Fastcheck succeeded for %s (%s, API %s, Airflow %s)",
                    cfg.instance_name,
                    cfg.url,
                    api_ver,
                    af_ver,
                )
            except AirflowAPIError as exc:
                if exc.status_code in (401, 403):
                    return Status(StatusValue.AUTHENTICATION_ERROR, str(exc))
                return Status(StatusValue.DEVICE_CONNECTION_ERROR, str(exc))
            except Exception as exc:
                return Status(StatusValue.DEVICE_CONNECTION_ERROR, str(exc))
            finally:
                client.close()

        return Status(StatusValue.OK, "All Airflow endpoints reachable and authenticated")

    def query(self) -> None:
        # Used when `dt-sdk run` executes a single-shot query cycle without custom schedules.
        if not hasattr(self, "_endpoints"):
            self.initialize()

    @staticmethod
    def _parse_endpoint(raw: dict[str, Any]) -> EndpointConfig:
        url = str(raw.get("url") or "http://localhost:8080").strip()
        inst = str(raw.get("instance_name") or "").strip()
        if not inst:
            inst = url.split("://")[-1].split("/")[0]
        return EndpointConfig(
            instance_name=inst,
            url=url,
            api_version=str(raw.get("api_version") or "auto").strip().lower(),
            auth_mode=str(raw.get("auth_mode") or "auto").strip().lower(),
            username=str(raw.get("username") or ""),
            password=str(raw.get("password") or ""),
            verify_ssl=bool(raw.get("verify_ssl", True)),
            collection_interval_seconds=int(raw.get("collection_interval_seconds") or 30),
            initial_lookback_minutes=int(raw.get("initial_lookback_minutes") or 15),
            emit_completion_logs=bool(raw.get("emit_completion_logs", True)),
            emit_audit_logs=bool(raw.get("emit_audit_logs", False)),
            emit_davis_events=bool(raw.get("emit_davis_events", True)),
        )

    def _base_dims(self, state: EndpointRuntimeState) -> dict[str, str]:
        host, port = state.cfg.host_and_port
        dims = {
            "airflow.instance.name": state.cfg.instance_name,
            "server.address": host,
            "server.port": port,
        }
        if state.airflow_version:
            dims["airflow.version"] = state.airflow_version
        return dims

    def _scrape_endpoint(self, state: EndpointRuntimeState) -> None:
        now = datetime.now(UTC)
        if not state.initialized:
            lookback = timedelta(minutes=max(state.cfg.initial_lookback_minutes, 1))
            state.run_watermark = now - lookback
            state.ti_watermark = now - lookback
            state.audit_watermark = now - timedelta(minutes=2)
            state.initialized = True

        try:
            if not state.airflow_version:
                state.airflow_version = state.client.get_version()
            health = state.client.get_health()
            dags = state.client.list_dags()
        except Exception as exc:
            self.logger.warning("Airflow scrape failed for %s: %s", state.cfg.instance_name, exc)
            base_dims = self._base_dims(state)
            self.report_metric("airflow.api.up", 0, dimensions=base_dims, metric_type=MetricType.GAUGE)
            if state.cfg.emit_davis_events:
                self.report_dt_event(
                    event_type=DtEventType.AVAILABILITY_EVENT,
                    title=f"Airflow API unreachable ({state.cfg.instance_name})",
                    properties={
                        "airflow.instance.name": state.cfg.instance_name,
                        "dt.event.description": str(exc),
                    },
                )
            return

        base_dims = self._base_dims(state)
        self.report_metric("airflow.api.up", 1, dimensions=base_dims, metric_type=MetricType.GAUGE)

        self._record_health(state, base_dims, now, health)
        self._record_dags(base_dims, dags)
        self._record_import_errors(state, base_dims)
        self._record_pools(base_dims, state)
        self._record_jobs(base_dims, state)

        log_events: list[dict[str, Any]] = []
        self._record_dag_runs(state, base_dims, now, log_events)
        self._record_task_instances(state, base_dims, now, log_events)
        if state.cfg.emit_audit_logs:
            self._record_audit_logs(state, log_events)

        if log_events:
            # Batch in chunks of 200 to stay well within ActiveGate payload limits
            for idx in range(0, len(log_events), 200):
                self.report_log_events(log_events[idx : idx + 200])

    def _record_health(
        self,
        state: EndpointRuntimeState,
        base_dims: dict[str, str],
        now: datetime,
        health: dict[str, Any],
    ) -> None:
        components = ("metadatabase", "scheduler", "triggerer", "dag_processor")
        heartbeat_keys = {
            "scheduler": "latest_scheduler_heartbeat",
            "triggerer": "latest_triggerer_heartbeat",
            "dag_processor": "latest_dag_processor_heartbeat",
        }
        for comp in components:
            block = health.get(comp)
            if not isinstance(block, dict) or not block.get("status"):
                continue
            is_healthy = 1 if str(block.get("status")).lower() == "healthy" else 0
            dims = {**base_dims, "airflow.component": comp}
            self.report_metric("airflow.component.health", is_healthy, dimensions=dims, metric_type=MetricType.GAUGE)

            hb_field = heartbeat_keys.get(comp)
            if hb_field and block.get(hb_field):
                hb_dt = parse_iso(str(block[hb_field]))
                if hb_dt is not None:
                    age = max((now - hb_dt).total_seconds(), 0.0)
                    self.report_metric(
                        "airflow.component.heartbeat.age", age, dimensions=dims, metric_type=MetricType.GAUGE
                    )

            if is_healthy == 0 and state.cfg.emit_davis_events:
                self.report_dt_event(
                    event_type=DtEventType.AVAILABILITY_EVENT,
                    title=f"Airflow {comp} unhealthy on {state.cfg.instance_name}",
                    properties={
                        "airflow.instance.name": state.cfg.instance_name,
                        "airflow.component": comp,
                    },
                )

    def _record_dags(self, base_dims: dict[str, str], dags: list[dict[str, Any]]) -> None:
        counts: dict[tuple[str, str], int] = {
            ("true", "false"): 0,
            ("true", "true"): 0,
            ("false", "false"): 0,
            ("false", "true"): 0,
        }
        for dag in dags:
            if dag.get("is_stale") is True:
                continue
            # Airflow 3 omits is_active on non-stale DAGs; default True
            is_active = "false" if dag.get("is_active") is False else "true"
            is_paused = "true" if dag.get("is_paused") is True else "false"
            counts[(is_active, is_paused)] += 1

            parse_dur = dag.get("last_parse_duration")
            dag_id = str(dag.get("dag_id") or "")
            if isinstance(parse_dur, int | float) and dag_id:
                dims = {
                    **base_dims,
                    "airflow.dag.id": dag_id,
                    "airflow.dag.file_token": str(dag.get("file_token") or ""),
                }
                self.report_metric(
                    "airflow.dag_processing.total_parse_time",
                    float(parse_dur),
                    dimensions=dims,
                    metric_type=MetricType.GAUGE,
                )

        for (active_s, paused_s), cnt in counts.items():
            dims = {
                **base_dims,
                "airflow.dag.is_active": active_s,
                "airflow.dag.is_paused": paused_s,
            }
            self.report_metric("airflow.dag.count", cnt, dimensions=dims, metric_type=MetricType.GAUGE)

    def _record_import_errors(self, state: EndpointRuntimeState, base_dims: dict[str, str]) -> None:
        try:
            err_cnt = state.client.count_import_errors()
        except Exception as exc:
            self.logger.debug("importErrors check failed on %s: %s", state.cfg.instance_name, exc)
            return

        self.report_metric("airflow.dag.import_errors", err_cnt, dimensions=base_dims, metric_type=MetricType.GAUGE)
        if err_cnt > 0 and state.cfg.emit_davis_events:
            self.report_dt_event(
                event_type=DtEventType.ERROR_EVENT,
                title=f"Airflow DAG import errors detected ({err_cnt}) on {state.cfg.instance_name}",
                properties={
                    "airflow.instance.name": state.cfg.instance_name,
                    "airflow.dag.import_errors": str(err_cnt),
                },
            )

    def _record_pools(self, base_dims: dict[str, str], state: EndpointRuntimeState) -> None:
        try:
            pools = state.client.list_pools()
        except Exception as exc:
            self.logger.debug("pools scrape failed on %s: %s", state.cfg.instance_name, exc)
            return

        slot_fields = {
            "running": "running_slots",
            "queued": "queued_slots",
            "scheduled": "scheduled_slots",
            "deferred": "deferred_slots",
            "open": "open_slots",
        }
        for pool in pools:
            pool_name = str(pool.get("name") or "")
            if not pool_name:
                continue
            pdims = {**base_dims, "airflow.pool.name": pool_name}
            total = int(pool.get("slots") or 0)
            open_slots = int(pool.get("open_slots") or 0)
            self.report_metric("airflow.pool.slots.limit", total, dimensions=pdims, metric_type=MetricType.GAUGE)
            for slot_state, field_name in slot_fields.items():
                val = int(pool.get(field_name) or 0)
                self.report_metric(
                    "airflow.pool.slots",
                    val,
                    dimensions={**pdims, "airflow.slot.state": slot_state},
                    metric_type=MetricType.GAUGE,
                )
            if total > 0:
                util = min(max(float(total - open_slots) / float(total), 0.0), 1.0)
            else:
                util = 0.0
            self.report_metric("airflow.pool.utilization", util, dimensions=pdims, metric_type=MetricType.GAUGE)

    def _record_jobs(self, base_dims: dict[str, str], state: EndpointRuntimeState) -> None:
        try:
            jobs = state.client.list_alive_jobs()
        except Exception as exc:
            self.logger.debug("jobs scrape failed on %s: %s", state.cfg.instance_name, exc)
            return
        if jobs is None:
            return
        counts = {"scheduler": 0, "triggerer": 0, "dag_processor": 0}
        mapping = {
            "SchedulerJob": "scheduler",
            "TriggererJob": "triggerer",
            "DagProcessorJob": "dag_processor",
        }
        for job in jobs:
            comp = mapping.get(str(job.get("job_type") or ""))
            if comp:
                counts[comp] += 1
        for comp, cnt in counts.items():
            self.report_metric(
                "airflow.component.instances.alive",
                cnt,
                dimensions={**base_dims, "airflow.component": comp},
                metric_type=MetricType.GAUGE,
            )

    def _record_dag_runs(
        self,
        state: EndpointRuntimeState,
        base_dims: dict[str, str],
        now: datetime,
        log_events: list[dict[str, Any]],
    ) -> None:
        try:
            active_runs = state.client.list_dag_runs(states=["queued", "running"])
        except Exception as exc:
            self.logger.warning("active dagRuns scrape failed on %s: %s", state.cfg.instance_name, exc)
            active_runs = []

        active_counts: dict[tuple[str, str], int] = defaultdict(int)
        max_running_age: dict[str, float] = {}
        for run in active_runs:
            dag_id = str(run.get("dag_id") or "")
            rstate = str(run.get("state") or "").lower()
            if not dag_id or not rstate:
                continue
            active_counts[(dag_id, rstate)] += 1
            if rstate == "running":
                start_dt = parse_iso(run.get("start_date"))
                if start_dt is not None:
                    age = max((now - start_dt).total_seconds(), 0.0)
                    if age > max_running_age.get(dag_id, -1.0):
                        max_running_age[dag_id] = age

        for (dag_id, rstate), cnt in active_counts.items():
            self.report_metric(
                "airflow.dag_run.active",
                cnt,
                dimensions={**base_dims, "airflow.dag.id": dag_id, "airflow.dag_run.state": rstate},
                metric_type=MetricType.GAUGE,
            )
        for dag_id, max_age in max_running_age.items():
            self.report_metric(
                "airflow.dag_run.running.duration.max",
                max_age,
                dimensions={**base_dims, "airflow.dag.id": dag_id},
                metric_type=MetricType.GAUGE,
            )

        try:
            completed_runs = state.client.list_dag_runs(states=["success", "failed"], end_date_gte=state.run_watermark)
        except Exception as exc:
            self.logger.warning("completed dagRuns scrape failed on %s: %s", state.cfg.instance_name, exc)
            return

        comp_counts: dict[tuple[str, str, str], int] = defaultdict(int)
        dur_stats: dict[tuple[str, str, str], SummaryAccumulator] = defaultdict(SummaryAccumulator)
        delay_stats: dict[str, SummaryAccumulator] = defaultdict(SummaryAccumulator)
        max_end = state.run_watermark

        for run in completed_runs:
            dag_id = str(run.get("dag_id") or "")
            run_id = str(run.get("dag_run_id") or run.get("run_id") or "")
            rstate = str(run.get("state") or "").lower()
            run_type = str(run.get("run_type") or "unknown").lower()
            end_dt = parse_iso(run.get("end_date"))
            if not dag_id or not run_id or end_dt is None:
                continue
            dedup_key = f"{dag_id}\x00{run_id}"
            if state.seen_runs.seen(dedup_key):
                continue
            state.seen_runs.add(dedup_key)
            if end_dt > max_end:
                max_end = end_dt

            key = (dag_id, rstate, run_type)
            comp_counts[key] += 1

            start_dt = parse_iso(run.get("start_date"))
            dur_s: float | None = None
            if start_dt is not None and end_dt >= start_dt:
                dur_s = (end_dt - start_dt).total_seconds()
                dur_stats[key].add(dur_s)

            sched_end = parse_iso(run.get("data_interval_end"))
            if run_type == "scheduled" and start_dt is not None and sched_end is not None and start_dt >= sched_end:
                delay_stats[dag_id].add((start_dt - sched_end).total_seconds())

            if state.cfg.emit_completion_logs:
                log_events.append(
                    {
                        "content": f"Airflow DAG run {dag_id}/{run_id} finished with state={rstate}",
                        "loglevel": "ERROR" if rstate == "failed" else "INFO",
                        "event.name": "airflow.dag_run.completed",
                        "airflow.instance.name": state.cfg.instance_name,
                        "airflow.dag.id": dag_id,
                        "airflow.dag_run.id": run_id,
                        "airflow.dag_run.state": rstate,
                        "airflow.dag_run.type": run_type,
                        "event.duration": f"{dur_s:.3f}" if dur_s is not None else "",
                    }
                )

        state.run_watermark = max_end

        for (dag_id, rstate, run_type), cnt in comp_counts.items():
            dims = {
                **base_dims,
                "airflow.dag.id": dag_id,
                "airflow.dag_run.state": rstate,
                "airflow.dag_run.type": run_type,
            }
            self.report_metric("airflow.dag_run.completed", cnt, dimensions=dims, metric_type=MetricType.DELTA)
            if (dag_id, rstate, run_type) in dur_stats:
                self.report_metric(
                    "airflow.dag_run.duration",
                    dur_stats[(dag_id, rstate, run_type)].to_summary_stat(),
                    dimensions=dims,
                    metric_type=MetricType.GAUGE,
                )

        for dag_id, acc in delay_stats.items():
            self.report_metric(
                "airflow.dag_run.schedule_delay",
                acc.to_summary_stat(),
                dimensions={**base_dims, "airflow.dag.id": dag_id},
                metric_type=MetricType.GAUGE,
            )

    def _record_task_instances(
        self,
        state: EndpointRuntimeState,
        base_dims: dict[str, str],
        now: datetime,
        log_events: list[dict[str, Any]],
    ) -> None:
        active_states = [
            "scheduled",
            "queued",
            "running",
            "restarting",
            "up_for_retry",
            "up_for_reschedule",
            "deferred",
        ]
        try:
            active_tis = state.client.list_task_instances(states=active_states)
        except Exception as exc:
            self.logger.warning("active taskInstances scrape failed on %s: %s", state.cfg.instance_name, exc)
            active_tis = []

        active_counts: dict[tuple[str, str], int] = defaultdict(int)
        max_queued_age: dict[str, float] = {}
        for ti in active_tis:
            dag_id = str(ti.get("dag_id") or "")
            tstate = str(ti.get("state") or "").lower()
            if not dag_id or not tstate:
                continue
            active_counts[(dag_id, tstate)] += 1
            if tstate == "queued":
                q_dt = parse_iso(ti.get("queued_when"))
                if q_dt is not None:
                    age = max((now - q_dt).total_seconds(), 0.0)
                    if age > max_queued_age.get(dag_id, -1.0):
                        max_queued_age[dag_id] = age

        for (dag_id, tstate), cnt in active_counts.items():
            self.report_metric(
                "airflow.task_instance.active",
                cnt,
                dimensions={**base_dims, "airflow.dag.id": dag_id, "airflow.task.state": tstate},
                metric_type=MetricType.GAUGE,
            )
        for dag_id, max_age in max_queued_age.items():
            self.report_metric(
                "airflow.task_instance.queued.duration.max",
                max_age,
                dimensions={**base_dims, "airflow.dag.id": dag_id},
                metric_type=MetricType.GAUGE,
            )

        term_states = ["success", "failed", "skipped", "upstream_failed"]
        try:
            completed_tis = state.client.list_task_instances(states=term_states, end_date_gte=state.ti_watermark)
        except Exception as exc:
            self.logger.warning("completed taskInstances scrape failed on %s: %s", state.cfg.instance_name, exc)
            return

        comp_counts: dict[tuple[str, str, str, str], int] = defaultdict(int)
        dur_stats: dict[tuple[str, str, str, str], SummaryAccumulator] = defaultdict(SummaryAccumulator)
        max_end = state.ti_watermark

        for ti in completed_tis:
            dag_id = str(ti.get("dag_id") or "")
            task_id = str(ti.get("task_id") or "")
            run_id = str(ti.get("dag_run_id") or ti.get("run_id") or "")
            tstate = str(ti.get("state") or "").lower()
            operator = str(ti.get("operator") or "unknown")
            try_num = int(ti.get("try_number") or 1)
            map_idx = int(ti.get("map_index") if ti.get("map_index") is not None else -1)
            end_dt = parse_iso(ti.get("end_date"))
            if not dag_id or not task_id or end_dt is None:
                continue

            dedup_key = f"{dag_id}\x00{run_id}\x00{task_id}\x00{map_idx}\x00{try_num}\x00{tstate}"
            if state.seen_tis.seen(dedup_key):
                continue
            state.seen_tis.add(dedup_key)
            if end_dt > max_end:
                max_end = end_dt

            key = (dag_id, task_id, tstate, operator)
            comp_counts[key] += 1

            dur_val = ti.get("duration")
            if isinstance(dur_val, int | float) and dur_val >= 0:
                dur_stats[key].add(float(dur_val))

            if state.cfg.emit_completion_logs and tstate in ("failed", "upstream_failed"):
                msg = f"Airflow task {dag_id}.{task_id} ({run_id}) ended in state={tstate} (try={try_num})"
                log_events.append(
                    {
                        "content": msg,
                        "loglevel": "ERROR",
                        "event.name": "airflow.task_instance.completed",
                        "airflow.instance.name": state.cfg.instance_name,
                        "airflow.dag.id": dag_id,
                        "airflow.task.id": task_id,
                        "airflow.dag_run.id": run_id,
                        "airflow.task.state": tstate,
                        "airflow.task.operator": operator,
                        "event.try_number": str(try_num),
                        "event.pool": str(ti.get("pool") or ""),
                    }
                )

        state.ti_watermark = max_end

        for (dag_id, task_id, tstate, operator), cnt in comp_counts.items():
            dims = {
                **base_dims,
                "airflow.dag.id": dag_id,
                "airflow.task.id": task_id,
                "airflow.task.state": tstate,
                "airflow.task.operator": operator,
            }
            self.report_metric("airflow.task_instance.completed", cnt, dimensions=dims, metric_type=MetricType.DELTA)
            if (dag_id, task_id, tstate, operator) in dur_stats:
                self.report_metric(
                    "airflow.task_instance.duration",
                    dur_stats[(dag_id, task_id, tstate, operator)].to_summary_stat(),
                    dimensions=dims,
                    metric_type=MetricType.GAUGE,
                )

    def _record_audit_logs(self, state: EndpointRuntimeState, log_events: list[dict[str, Any]]) -> None:
        try:
            events = state.client.list_event_logs(after=state.audit_watermark)
        except Exception as exc:
            self.logger.debug("eventLogs scrape skipped on %s: %s", state.cfg.instance_name, exc)
            return

        max_when = state.audit_watermark
        for ev in events:
            ev_id = str(ev.get("event_log_id") or "")
            if not ev_id or state.seen_audits.seen(ev_id):
                continue
            state.seen_audits.add(ev_id)
            when_dt = parse_iso(ev.get("when"))
            if when_dt and when_dt > max_when:
                max_when = when_dt
            ev_name = str(ev.get("event") or "")
            dag_id = str(ev.get("dag_id") or "")
            log_events.append(
                {
                    "content": f"Airflow audit event '{ev_name}' on dag='{dag_id}'",
                    "loglevel": "INFO",
                    "event.name": "airflow.audit_log",
                    "airflow.instance.name": state.cfg.instance_name,
                    "airflow.audit.event": ev_name,
                    "airflow.dag.id": dag_id,
                    "airflow.task.id": str(ev.get("task_id") or ""),
                    "airflow.dag_run.id": str(ev.get("run_id") or ""),
                    "airflow.audit.owner": str(ev.get("owner") or ""),
                }
            )
        state.audit_watermark = max_when


def main() -> None:
    AirflowExtension().run()


if __name__ == "__main__":
    main()
