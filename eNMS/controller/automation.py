from apscheduler.jobstores.base import JobLookupError
from collections import defaultdict
from datetime import datetime
from flask import request, session
from flask_login import current_user
from napalm._SUPPORTED_DRIVERS import SUPPORTED_DRIVERS
from netmiko.ssh_dispatcher import CLASS_MAPPER, FILE_TRANSFER_MAP
from pathlib import Path
from re import search, sub
from uuid import uuid4

from eNMS.controller.base import BaseController
from eNMS.database import Session
from eNMS.database.functions import delete, factory, fetch, fetch_all, objectify


class AutomationController(BaseController):

    NETMIKO_DRIVERS = sorted((driver, driver) for driver in CLASS_MAPPER)
    NETMIKO_SCP_DRIVERS = sorted((driver, driver) for driver in FILE_TRANSFER_MAP)
    NAPALM_DRIVERS = sorted((driver, driver) for driver in SUPPORTED_DRIVERS[1:])
    connections_cache = {"napalm": defaultdict(dict), "netmiko": defaultdict(dict)}
    service_db = defaultdict(lambda: {"runs": 0})
    run_db = defaultdict(dict)
    run_logs = defaultdict(list)

    def stop_workflow(self, runtime):
        if runtime in self.run_db:
            self.run_db[runtime]["stop"] = True
            return True
        else:
            return None

    def add_edge(self, workflow_id, subtype, source, destination):
        workflow_edge = factory(
            "workflow_edge",
            **{
                "name": f"{workflow_id}-{subtype}:{source}->{destination}",
                "workflow": workflow_id,
                "subtype": subtype,
                "source": source,
                "destination": destination,
            },
        )
        Session.commit()
        now = self.get_time()
        fetch("workflow", id=workflow_id).last_modified = now
        return {"edge": workflow_edge.serialized, "update_time": now}

    def add_services_to_workflow(self, workflow_id, service_ids):
        workflow = fetch("workflow", id=workflow_id)
        services = objectify(
            "service", [int(service_id) for service_id in service_ids.split("-")]
        )
        for service in services:
            service.workflows.append(workflow)
        now = self.get_time()
        workflow.last_modified = now
        return {
            "services": [service.serialized for service in services],
            "update_time": now,
        }

    def clear_results(self, service_id):
        for result in fetch(
            "run", all_matches=True, allow_none=True, service_id=service_id
        ):
            Session.delete(result)

    def create_label(self, workflow_id, x, y, **kwargs):
        workflow, label_id = fetch("workflow", id=workflow_id), str(uuid4())
        label = {
            "positions": [x, y],
            "content": kwargs["content"],
            "alignment": kwargs["alignment"],
        }
        workflow.labels[label_id] = label
        return {"id": label_id, **label}

    def delete_edge(self, workflow_id, edge_id):
        delete("workflow_edge", id=edge_id)
        now = self.get_time()
        fetch("workflow", id=workflow_id).last_modified = now
        return now

    def delete_node(self, workflow_id, service_id):
        workflow, service = (
            fetch("workflow", id=workflow_id),
            fetch("service", id=service_id),
        )
        workflow.services.remove(service)
        now = self.get_time()
        workflow.last_modified = now
        return {"service": service.serialized, "update_time": now}

    def delete_label(self, workflow_id, label):
        workflow = fetch("workflow", id=workflow_id)
        workflow.labels.pop(label)
        now = self.get_time()
        workflow.last_modified = now
        return now

    def duplicate_workflow(self, workflow_id, **kwargs):
        parent_workflow = fetch("workflow", id=workflow_id)
        new_workflow = factory("workflow", **kwargs)
        Session.commit()
        for service in parent_workflow.services:
            new_workflow.services.append(service)
            service.positions[new_workflow.name] = service.positions[
                parent_workflow.name
            ]
        Session.commit()
        for edge in parent_workflow.edges:
            subtype, src, destination = edge.subtype, edge.source, edge.destination
            new_workflow.edges.append(
                factory(
                    "workflow_edge",
                    **{
                        "name": (
                            f"{new_workflow.id}-{subtype}:"
                            f"{src.id}->{destination.id}"
                        ),
                        "workflow": new_workflow.id,
                        "subtype": subtype,
                        "source": src.id,
                        "destination": destination.id,
                    },
                )
            )
        return new_workflow.serialized

    def get_service_logs(self, **kwargs):
        run = fetch("run", allow_none=True, runtime=kwargs["runtime"])
        result = run.result() if run else None
        logs = result["logs"] if result else self.run_logs.get(kwargs["runtime"], [])
        filtered_logs = (log for log in logs if kwargs["filter"] in log)
        return {"logs": "\n".join(filtered_logs), "refresh": not bool(result)}

    def get_runtimes(self, type, id):
        if type == "device":
            results = fetch("result", allow_none=True, all_matches=True, device_id=id)
            runs = [result.run for result in results]
        else:
            runs = fetch("run", allow_none=True, all_matches=True, service_id=id)
        return sorted(set((run.runtime, run.name) for run in runs))

    def get_result(self, id):
        return fetch("result", id=id).result

    @staticmethod
    def run(service, **kwargs):
        run_kwargs = {
            key: kwargs.pop(key)
            for key in ("creator", "runtime", "task")
            if kwargs.get(key)
        }
        restart_run = fetch(
            "run",
            allow_none=True,
            service_id=service,
            runtime=kwargs.get("restart_runtime"),
        )
        if restart_run:
            run_kwargs["restart_run"] = restart_run
        run = factory("run", service=service, **run_kwargs)
        run.properties = kwargs
        return run.run(kwargs.get("payload"))

    def run_service(self, id=None, **kwargs):
        for property in ("user", "csrf_token", "form_type"):
            kwargs.pop(property, None)
        kwargs["creator"] = getattr(current_user, "name", "admin")
        service = fetch("service", id=id)
        kwargs["runtime"] = runtime = self.get_time()
        if kwargs.get("asynchronous", True):
            self.scheduler.add_job(
                id=self.get_time(),
                func=self.run,
                run_date=datetime.now(),
                args=[id],
                kwargs=kwargs,
                trigger="date",
            )
        else:
            service.run(runtime=runtime)
        return {**service.serialized, "runtime": runtime}

    def save_positions(self, workflow_id):
        now = self.get_time()
        workflow = fetch("workflow", allow_none=True, id=workflow_id)
        session["workflow"] = workflow.id
        for id, position in request.json.items():
            new_position = [position["x"], position["y"]]
            if "-" in id:
                old_position = workflow.labels[id]["positions"]
                workflow.labels[id] = {
                    "positions": new_position,
                    "content": workflow.labels[id]["content"],
                }
            else:
                service = fetch("service", id=id)
                old_position = service.positions.get(workflow.name)
                service.positions[workflow.name] = new_position
            if new_position != old_position:
                workflow.last_modified = now
        return now

    def skip_services(self, skip, service_ids):
        for service_id in service_ids.split("-"):
            fetch("service", id=service_id).skip = skip == "skip"

    def get_workflow_state(self, workflow_id, runtime=None):
        workflow = fetch("workflow", id=workflow_id)
        runtimes = [
            (r.runtime, r.creator)
            for r in fetch(
                "run", allow_none=True, all_matches=True, service_id=workflow_id
            )
        ]
        state = None
        if runtimes and runtime not in ("normal", None):
            if runtime == "latest":
                runtime = runtimes[-1][0]
            state = self.run_db.get(runtime) or fetch("run", runtime=runtime).state
        return {
            "workflow": workflow.to_dict(include=["services", "edges"]),
            "runtimes": runtimes,
            "state": state,
            "runtime": runtime,
        }

    def convert_date(self, date):
        python_month = search(r".*-(\d{2})-.*", date).group(1)
        month = "{:02}".format((int(python_month) - 1) % 12)
        return [
            int(i)
            for i in sub(
                r"(\d+)-(\d+)-(\d+) (\d+):(\d+).*", r"\1," + month + r",\3,\4,\5", date
            ).split(",")
        ]

    def calendar_init(self, type):
        results = {}
        for instance in fetch_all(type):
            if getattr(instance, "workflow", None):
                continue
            date = getattr(instance, "next_run_time" if type == "task" else "runtime")
            if date:
                results[instance.name] = {
                    "start": self.convert_date(date),
                    **instance.serialized,
                }
        return results

    def scheduler_action(self, action):
        getattr(self.scheduler, action)()

    def task_action(self, action, task_id):
        try:
            return getattr(fetch("task", id=task_id), action)()
        except JobLookupError:
            return {"error": "This task no longer exists."}

    def scan_playbook_folder(self):
        path = Path(self.playbook_path or self.path / "playbooks")
        playbooks = [[str(f) for f in path.glob(e)] for e in ("*.yaml", "*.yml")]
        return sorted(sum(playbooks, []))
