from apscheduler.jobstores.base import JobLookupError
from collections import defaultdict
from datetime import datetime
from flask import request
from flask_login import current_user
from napalm._SUPPORTED_DRIVERS import SUPPORTED_DRIVERS
from netmiko.ssh_dispatcher import CLASS_MAPPER, FILE_TRANSFER_MAP
from operator import itemgetter
from pathlib import Path
from queue import Queue
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
    run_frontend = defaultdict(dict)
    run_logs = defaultdict(lambda: defaultdict(list))
    run_backend = defaultdict(
        lambda: {"threads": None, "queue": Queue(), "blocking_queue": Queue()}
    )

    def stop_workflow(self, runtime):
        run = fetch("run", allow_none=True, runtime=runtime)
        if run and run.run_state["status"] == "Running":
            run.run_state["status"] = "stop"
            return True

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

    def add_service_to_workflow(self, workflow, service):
        workflow = fetch("workflow", id=workflow)
        workflow.services.append(fetch("service", id=service))

    def copy_service_in_workflow(self, workflow_id, **kwargs):
        service_sets = list(set(kwargs["services"].split(",")))
        service_instances = objectify("service", service_sets)
        workflow = fetch("workflow", id=workflow_id)
        services, errors = [], []
        if kwargs["mode"] == "shallow":
            for service in service_instances:
                if not service.shared:
                    errors.append(f"'{service.name}' is not a shared service.")
                elif service in workflow.services:
                    errors.append(f"This workflow already contains '{service.name}'.")
        if errors:
            return {"alert": errors}
        for service in service_instances:
            if kwargs["mode"] == "deep":
                service = service.duplicate(workflow)
            else:
                workflow.services.append(service)
            services.append(service)
        workflow.last_modified = self.get_time()
        Session.commit()
        return {
            "services": [service.serialized for service in services],
            "update_time": workflow.last_modified,
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
            "content": kwargs["text"],
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
        if not service.shared:
            delete("service", id=service.id)
        now = self.get_time()
        workflow.last_modified = now
        return {"service": service.serialized, "update_time": now}

    def delete_label(self, workflow_id, label):
        workflow = fetch("workflow", id=workflow_id)
        workflow.labels.pop(label)
        now = self.get_time()
        workflow.last_modified = now
        return now

    def duplicate_workflow(self, workflow_id):
        workflow = fetch("workflow", id=workflow_id)
        return workflow.duplicate().serialized

    def get_service_logs(self, service, runtime):
        log_instance = fetch(
            "service_log", allow_none=True, runtime=runtime, service_id=service
        )
        if log_instance:
            log = log_instance.content
        else:
            log = "\n".join(self.run_logs[runtime].get(int(service), []))
        return {"logs": log, "refresh": not log_instance}

    def get_runtimes(self, type, id):
        runs = fetch("run", allow_none=True, all_matches=True, service_id=id)
        return sorted(
            set(
                (run.runtime, f"{run.runtime} (run by '{run.creator}')") for run in runs
            )
        )

    def get_result(self, id):
        return fetch("result", id=id).result

    def get_top_level_workflows(self):
        return [
            workflow.base_properties
            for workflow in fetch_all("workflow")
            if not workflow.workflows
        ]

    def get_parent_workflows(self, workflow=None):
        yield workflow
        for parent_workflow in workflow.workflows:
            yield from self.get_parent_workflows(parent_workflow)

    def get_workflow_services(self, id, node):
        parents = list(self.get_parent_workflows(fetch("workflow", id=id)))
        if node == "all":
            return (
                [
                    {
                        "data": {"id": "standalone"},
                        "id": "standalone",
                        "text": "Standalone services",
                        "children": True,
                        "state": {"disabled": True},
                        "a_attr": {
                            "class": "no_checkbox",
                            "style": "color: #000000; width: 100%",
                        },
                        "type": "category",
                    }
                ]
                + [
                    {
                        "data": {"id": "shared"},
                        "id": "shared",
                        "text": "Shared services",
                        "children": True,
                        "state": {"disabled": True},
                        "a_attr": {
                            "class": "no_checkbox",
                            "style": "color: #FF1694; width: 100%",
                        },
                        "type": "category",
                    }
                ]
                + sorted(
                    (
                        {
                            "id": workflow.name,
                            "data": {"id": workflow.id},
                            "text": workflow.name,
                            "children": True,
                            "type": "workflow",
                            "state": {"disabled": workflow in parents},
                            "a_attr": {
                                "class": "no_checkbox" if workflow in parents else "",
                                "style": "color: #6666FF; width: 100%",
                            },
                        }
                        for workflow in fetch_all("workflow")
                        if not workflow.workflows
                    ),
                    key=itemgetter("text"),
                )
            )
        elif node == "standalone":
            return sorted(
                (
                    {
                        "id": service.name,
                        "data": {"id": service.id},
                        "text": service.scoped_name,
                        "a_attr": {"style": ("color: #6666FF;" "width: 100%")},
                    }
                    for service in fetch_all("service")
                    if not service.workflows and service.type != "workflow"
                ),
                key=itemgetter("text"),
            )
        elif node == "shared":
            return sorted(
                (
                    {
                        "id": service.name,
                        "data": {"id": service.id},
                        "text": service.scoped_name,
                        "a_attr": {"style": ("color: #FF1694;" "width: 100%")},
                    }
                    for service in fetch_all("service")
                    if service.shared
                ),
                key=itemgetter("text"),
            )
        else:
            return sorted(
                (
                    {
                        "id": service.name,
                        "data": {"id": service.id},
                        "text": service.scoped_name,
                        "children": service.type == "workflow",
                        "type": "workflow" if service.type == "workflow" else "service",
                        "state": {"disabled": service in parents},
                        "a_attr": {
                            "class": "no_checkbox" if service in parents else "",
                            "style": (
                                f"color: #{'FF1694' if service.shared else '6666FF'};"
                                "width: 100%"
                            ),
                        },
                    }
                    for service in fetch("workflow", id=node).services
                    if service.scoped_name not in ("Start", "End")
                ),
                key=itemgetter("text"),
            )

    def search_workflow_services(self, *args, **kwargs):
        return [
            "standalone",
            "shared",
            *[
                workflow.name
                for workflow in fetch_all("workflow")
                if any(
                    kwargs["str"].lower() in service.scoped_name.lower()
                    for service in workflow.services
                )
            ],
        ]

    def get_workflow_results(self, workflow, runtime):
        state = fetch("run", runtime=runtime).result().result["state"]

        def rec(service):
            results = fetch(
                "result",
                run_runtime=runtime,
                allow_none=True,
                all_matches=True,
                service_id=service.id,
            )
            print(len(results))
            if service.scoped_name in ("Start", "End") or not results:
                return
            progress = state["services"][service.id].get("progress")
            track_progress = progress and progress["device"]["total"]
            data = {"progress": progress["device"]} if track_progress else {}
            color = "32CD32" if all(result.success for result in results) else "FF6666"
            result = {
                "runtime": min(result.runtime for result in results),
                "data": {"properties": service.base_properties, **data},
                "text": service.scoped_name,
                "a_attr": {"style": f"color: #{color};width: 100%"},
            }
            if service.type == "workflow":
                children = sorted(
                    filter(None, (rec(child) for child in service.services)),
                    key=itemgetter("runtime"),
                )
                return {"children": children, **result}
            else:
                return result

        return rec(fetch("workflow", id=workflow))

    @staticmethod
    def run(service, **kwargs):
        run_kwargs = {
            key: kwargs.pop(key)
            for key in (
                "trigger",
                "creator",
                "start_services",
                "runtime",
                "task",
                "devices",
                "pools",
            )
            if kwargs.get(key)
        }
        restart_run = fetch(
            "run", allow_none=True, runtime=kwargs.get("restart_runtime"),
        )
        if restart_run:
            run_kwargs["restart_run"] = restart_run
        initial_payload = fetch("service", id=service).initial_payload
        run = factory("run", service=service, **run_kwargs)
        run.properties = kwargs
        payload = {**initial_payload, **kwargs}
        return run.run(payload)

    def run_service(self, path, **kwargs):
        service_id = str(path).split(">")[-1]
        for property in ("user", "csrf_token", "form_type"):
            kwargs.pop(property, None)
        kwargs["creator"] = getattr(current_user, "name", "admin")
        service = fetch("service", id=service_id)
        kwargs["runtime"] = runtime = self.get_time()
        if kwargs.get("asynchronous", True):
            self.scheduler.add_job(
                id=runtime,
                func=self.run,
                run_date=datetime.now(),
                args=[service_id],
                kwargs=kwargs,
                trigger="date",
            )
        else:
            service.run(runtime=runtime)
        return {
            "service": service.serialized,
            "runtime": runtime,
            "user": current_user.name,
        }

    def save_positions(self, workflow_id):
        now, old_position = self.get_time(), None
        workflow = fetch("workflow", allow_none=True, id=workflow_id)
        for id, position in request.json.items():
            new_position = [position["x"], position["y"]]
            if "-" not in id:
                service = fetch("service", id=id)
                old_position = service.positions.get(workflow.name)
                service.positions[workflow.name] = new_position
            elif id in workflow.labels:
                old_position = workflow.labels[id].pop("positions")
                workflow.labels[id] = {"positions": new_position, **workflow.labels[id]}
            if new_position != old_position:
                workflow.last_modified = now
        return now

    def skip_services(self, workflow_id, service_ids):
        services = [fetch("service", id=id) for id in service_ids.split("-")]
        workflow = fetch("workflow", id=workflow_id)
        skip = not all(service.skip for service in services)
        for service in services:
            service.skip = skip
        workflow.last_modified = self.get_time()
        return {
            "skip": "skip" if skip else "unskip",
            "update_time": workflow.last_modified,
        }

    def get_service_state(self, path, runtime=None):
        state, service = None, fetch("service", id=path.split(">")[-1])
        runs = fetch_all("run", service_id=path.split(">")[0])
        if not runtime:
            runtime = "latest"
        if runs and runtime != "normal":
            if runtime == "latest":
                runtime = runs[-1].runtime
            state = (
                self.run_frontend.get(runtime) or fetch("run", runtime=runtime).state
            )
        return {
            "service": service.to_dict(include=["services", "edges"]),
            "runtimes": sorted(set((r.runtime, r.creator) for r in runs)),
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
            return {"alert": "This task no longer exists."}

    def scan_playbook_folder(self):
        path = Path(
            self.settings["paths"]["playbooks"] or self.path / "files" / "playbooks"
        )
        playbooks = [[str(f) for f in path.glob(e)] for e in ("*.yaml", "*.yml")]
        return sorted(sum(playbooks, []))
