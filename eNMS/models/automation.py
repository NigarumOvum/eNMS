from builtins import __dict__ as builtins
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from functools import partial
from io import BytesIO
from json import dumps, loads
from json.decoder import JSONDecodeError
from multiprocessing.pool import ThreadPool
from napalm import get_network_driver
from netmiko import ConnectHandler
from os import environ
from paramiko import SFTPClient
from queue import Empty
from ruamel import yaml
from re import compile, search
from requests import post
from scp import SCPClient
from slackclient import SlackClient
from sqlalchemy import Boolean, ForeignKey, Integer
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship
from threading import Thread
from time import sleep
from traceback import format_exc
from xmltodict import parse
from xml.parsers.expat import ExpatError

from eNMS import app
from eNMS.database import Session
from eNMS.database.associations import (
    run_pool_table,
    run_device_table,
    service_device_table,
    service_pool_table,
    service_workflow_table,
)
from eNMS.database.base import AbstractBase
from eNMS.database.dialect import (
    Column,
    LargeString,
    MutableDict,
    MutableList,
    SmallString,
)
from eNMS.database.functions import factory, fetch, set_custom_properties
from eNMS.models import models
from eNMS.models.inventory import Device  # noqa: F401
from eNMS.models.scheduling import Task  # noqa: F401
from eNMS.models.administration import User  # noqa: F401


@set_custom_properties
class Service(AbstractBase):

    __tablename__ = "service"
    type = Column(SmallString)
    __mapper_args__ = {"polymorphic_identity": "service", "polymorphic_on": type}
    id = Column(Integer, primary_key=True)
    name = Column(SmallString, unique=True)
    shared = Column(Boolean, default=False)
    scoped_name = Column(SmallString)
    last_modified = Column(SmallString, info={"dont_track_changes": True})
    description = Column(SmallString)
    number_of_retries = Column(Integer, default=0)
    time_between_retries = Column(Integer, default=10)
    max_number_of_retries = Column(Integer, default=100)
    positions = Column(MutableDict, info={"dont_track_changes": True})
    tasks = relationship("Task", back_populates="service", cascade="all,delete")
    events = relationship("Event", back_populates="service", cascade="all,delete")
    vendor = Column(SmallString)
    operating_system = Column(SmallString)
    waiting_time = Column(Integer, default=0)
    creator = Column(SmallString, default="admin")
    workflows = relationship(
        "Workflow", secondary=service_workflow_table, back_populates="services"
    )
    device_query = Column(LargeString)
    device_query_property = Column(SmallString, default="ip_address")
    devices = relationship(
        "Device", secondary=service_device_table, back_populates="services"
    )
    pools = relationship(
        "Pool", secondary=service_pool_table, back_populates="services"
    )
    update_pools = Column(Boolean, default=False)
    send_notification = Column(Boolean, default=False)
    send_notification_method = Column(SmallString, default="mail")
    notification_header = Column(LargeString, default="")
    display_only_failed_nodes = Column(Boolean, default=True)
    include_device_results = Column(Boolean, default=True)
    include_link_in_summary = Column(Boolean, default=True)
    mail_recipient = Column(SmallString)
    initial_payload = Column(MutableDict)
    skip = Column(Boolean, default=False)
    skip_query = Column(LargeString)
    skip_value = Column(SmallString, default="True")
    iteration_values = Column(LargeString)
    iteration_variable_name = Column(SmallString, default="iteration_value")
    iteration_devices = Column(LargeString)
    iteration_devices_property = Column(SmallString, default="ip_address")
    result_postprocessing = Column(LargeString)
    log_level = Column(Integer, default=1)
    runs = relationship("Run", back_populates="service", cascade="all, delete-orphan")
    maximum_runs = Column(Integer, default=1)
    max_processes = Column(Integer, default=5)
    conversion_method = Column(SmallString, default="none")
    validation_method = Column(SmallString, default="none")
    content_match = Column(LargeString, default="")
    content_match_regex = Column(Boolean, default=False)
    dict_match = Column(MutableDict)
    negative_logic = Column(Boolean, default=False)
    delete_spaces_before_matching = Column(Boolean, default=False)
    run_method = Column(SmallString, default="per_device")
    status = Column(SmallString, default="Idle")

    def __init__(self, **kwargs):
        kwargs.pop("status", None)
        super().__init__(**kwargs)
        if "name" not in kwargs:
            self.set_name()

    def convert_result(self, result):
        if self.conversion_method == "none" or "result" not in result:
            return result
        try:
            if self.conversion_method == "text":
                result["result"] = str(result["result"])
            elif self.conversion_method == "json":
                result["result"] = loads(result["result"])
            elif self.conversion_method == "xml":
                result["result"] = parse(result["result"])
        except (ExpatError, JSONDecodeError) as exc:
            result = {
                "success": False,
                "text_response": result,
                "error": f"Conversion to {self.conversion_method} failed",
                "exception": str(exc),
            }
        return result

    def update(self, **kwargs):
        if kwargs["scoped_name"] != self.scoped_name:
            self.set_name(kwargs["scoped_name"])
        super().update(**kwargs)

    def duplicate(self, workflow=None):
        for i in range(10):
            number = f" ({i})" if i else ""
            scoped_name = f"{self.scoped_name}{number}"
            name = f"[{workflow.name}] {scoped_name}" if workflow else scoped_name
            if not fetch("service", allow_none=True, name=name):
                service = super().duplicate(
                    name=name, scoped_name=scoped_name, shared=False
                )
                break
        if workflow:
            workflow.services.append(service)
        return service

    @property
    def filename(self):
        return app.strip_all(self.name)

    def match_dictionary(self, result, match, first=True):
        if self.validation_method == "dict_equal":
            return result == self.dict_match
        else:
            match_copy = deepcopy(match) if first else match
            if isinstance(result, dict):
                for k, v in result.items():
                    if k in match_copy and match_copy[k] == v:
                        match_copy.pop(k)
                    else:
                        self.match_dictionary(v, match_copy, False)
            elif isinstance(result, list):
                for item in result:
                    self.match_dictionary(item, match_copy, False)
            return not match_copy

    def set_name(self, name=None):
        if self.shared:
            workflow = "[Shared] "
        elif not self.workflows:
            workflow = ""
        else:
            workflow = f"[{self.workflows[0].name}] "
        self.name = f"{workflow}{name or self.scoped_name}"

    def transfer_file(self, ssh_client, files):
        if self.protocol == "sftp":
            with SFTPClient.from_transport(
                ssh_client.get_transport(),
                window_size=self.window_size,
                max_packet_size=self.max_transfer_size,
            ) as sftp:
                for source, destination in files:
                    getattr(sftp, self.direction)(source, destination)
        else:
            with SCPClient(ssh_client.get_transport()) as scp:
                for source, destination in files:
                    getattr(scp, self.direction)(source, destination)

    def neighbors(self, workflow, direction, subtype):
        for edge in getattr(self, f"{direction}s"):
            if edge.subtype == subtype and edge.workflow == workflow:
                yield getattr(edge, direction), edge


class ConnectionService(Service):

    __tablename__ = "connection_service"
    id = Column(Integer, ForeignKey("service.id"), primary_key=True)
    parent_type = "service"
    credentials = Column(SmallString, default="device")
    custom_username = Column(SmallString)
    custom_password = Column(SmallString)
    start_new_connection = Column(Boolean, default=False)
    close_connection = Column(Boolean, default=False)
    __mapper_args__ = {"polymorphic_identity": "connection_service"}


class Result(AbstractBase):

    __tablename__ = type = "result"
    private = True
    dont_track_changes = True
    id = Column(Integer, primary_key=True)
    success = Column(Boolean, default=False)
    runtime = Column(SmallString)
    duration = Column(SmallString)
    result = Column(MutableDict)
    run_id = Column(Integer, ForeignKey("run.id"))
    run = relationship("Run", back_populates="results", foreign_keys="Result.run_id")
    parent_runtime = Column(SmallString)
    parent_device_id = Column(Integer, ForeignKey("device.id"))
    parent_device = relationship("Device", uselist=False, foreign_keys=parent_device_id)
    parent_device_name = association_proxy("parent_device", "name")
    device_id = Column(Integer, ForeignKey("device.id"))
    device = relationship("Device", uselist=False, foreign_keys=device_id)
    device_name = association_proxy("device", "name")
    service_id = Column(Integer, ForeignKey("service.id"))
    service = relationship("Service", foreign_keys="Result.service_id")
    service_name = association_proxy(
        "service", "scoped_name", info={"name": "service_name"}
    )
    workflow_id = Column(Integer, ForeignKey("workflow.id", ondelete="cascade"))
    workflow = relationship("Workflow", foreign_keys="Result.workflow_id")
    workflow_name = association_proxy(
        "workflow", "scoped_name", info={"name": "workflow_name"}
    )

    def __getitem__(self, key):
        return self.result[key]

    def __init__(self, **kwargs):
        self.success = kwargs["result"]["success"]
        self.runtime = kwargs["result"]["runtime"]
        self.duration = kwargs["result"]["duration"]
        super().__init__(**kwargs)
        self.parent_runtime = self.run.runtime


class ServiceLog(AbstractBase):

    __tablename__ = type = "service_log"
    private = True
    dont_track_changes = True
    id = Column(Integer, primary_key=True)
    content = Column(LargeString)
    runtime = Column(SmallString)
    service_id = Column(Integer, ForeignKey("service.id"))
    service = relationship("Service", foreign_keys="ServiceLog.service_id")


class Run(AbstractBase):

    __tablename__ = type = "run"
    private = True
    id = Column(Integer, primary_key=True)
    restart_run_id = Column(Integer, ForeignKey("run.id"))
    restart_run = relationship("Run", uselist=False, foreign_keys=restart_run_id)
    start_services = Column(MutableList)
    creator = Column(SmallString, default="admin")
    properties = Column(MutableDict)
    success = Column(Boolean, default=False)
    status = Column(SmallString, default="Running")
    runtime = Column(SmallString)
    duration = Column(SmallString)
    trigger = Column(SmallString, default="UI")
    parent_id = Column(Integer, ForeignKey("run.id"))
    parent = relationship(
        "Run", remote_side=[id], foreign_keys="Run.parent_id", back_populates="children"
    )
    children = relationship("Run", foreign_keys="Run.parent_id")
    parent_device_id = Column(Integer, ForeignKey("device.id"))
    parent_device = relationship("Device", foreign_keys="Run.parent_device_id")
    devices = relationship("Device", secondary=run_device_table, back_populates="runs")
    pools = relationship("Pool", secondary=run_pool_table, back_populates="runs")
    service_id = Column(Integer, ForeignKey("service.id"))
    service = relationship(
        "Service", back_populates="runs", foreign_keys="Run.service_id"
    )
    service_name = association_proxy(
        "service", "scoped_name", info={"name": "service_name"}
    )
    workflow_id = Column(Integer, ForeignKey("workflow.id", ondelete="cascade"))
    workflow = relationship("Workflow", foreign_keys="Run.workflow_id")
    workflow_name = association_proxy(
        "workflow", "scoped_name", info={"name": "workflow_name"}
    )
    task_id = Column(Integer, ForeignKey("task.id", ondelete="SET NULL"))
    task = relationship("Task", foreign_keys="Run.task_id")
    state = Column(MutableDict, info={"dont_track_changes": True})
    results = relationship("Result", back_populates="run", cascade="all, delete-orphan")
    model_properties = ["progress", "service_properties"]

    def __init__(self, **kwargs):
        self.runtime = app.get_time()
        super().__init__(**kwargs)
        if not self.start_services:
            self.start_services = [fetch("service", scoped_name="Start").id]

    @property
    def name(self):
        return repr(self)

    @property
    def queue(self):
        return app.run_queue[self.runtime]

    def __repr__(self):
        return f"{self.runtime}: SERVICE '{self.service}' run by USER '{self.creator}'"

    def result(self, device=None):
        result = [r for r in self.results if r.device_name == device]
        return result.pop() if result else None

    @property
    def service_properties(self):
        return {k: getattr(self.service, k) for k in ("id", "type", "name")}

    @property
    def run_state(self):
        return self.state or app.run_db[self.runtime]

    @property
    def edge_state(self):
        return app.run_db[self.runtime]["edges"]

    @property
    def stop(self):
        return self.run_state["status"] == "stop"

    @property
    def progress(self):
        if self.status == "Running" and self.run_state.get("progress"):
            progress = self.run_state["progress"]["device"]
            try:
                return (
                    f"{progress['success'] + progress['failure']}/{progress['total']}"
                    f" ({progress['failure']} failed)"
                )
            except KeyError:
                return "N/A"
        else:
            return "N/A"

    def compute_devices_from_query(self, query, property, **locals):
        values = self.eval(query, **locals)[0]
        devices, not_found = set(), []
        if isinstance(values, str):
            values = [values]
        for value in values:
            device = fetch("device", allow_none=True, **{property: value})
            if device:
                devices.add(device)
            else:
                not_found.append(value)
        if not_found:
            raise Exception(f"Device query invalid targets: {', '.join(not_found)}")
        return devices

    def compute_devices(self):
        devices = set(self.devices)
        for pool in self.pools:
            devices |= set(pool.devices)
        if not devices:
            if self.service.device_query:
                devices |= self.compute_devices_from_query(
                    self.service.device_query, self.service.device_query_property,
                )
            devices |= set(self.service.devices)
            for pool in self.service.pools:
                if self.service.update_pools:
                    pool.compute_pool()
                devices |= set(pool.devices)
        return list(devices)

    def get_state(self, payload=None, service=None, path=None):
        if not service:
            service = self.service
        if self.runtime in app.run_db:
            if not path:
                return app.run_db[self.runtime]
            elif path in app.run_db[self.runtime]["services"]:
                return app.run_db[self.runtime]["services"][path]
        state = {
            "payload": payload,
            "status": "Idle",
            "success": None,
            "progress": {
                "device": {"total": 0, "success": 0, "failure": 0, "skipped": 0}
            },
            "attempt": 0,
            "waiting_time": {
                "total": service.waiting_time,
                "left": service.waiting_time,
            },
            "summary": {"success": [], "failure": []},
        }
        if service.type == "workflow":
            state.update(
                {
                    "edges": defaultdict(int),
                    "services": defaultdict(dict),
                    "runs": defaultdict(int),
                }
            )
            state["progress"]["service"] = {
                "total": len(service.services),
                "success": 0,
                "failure": 0,
                "skipped": 0,
            }
            state["progress"]["visited"] = defaultdict(int)
        if path:
            app.run_db[self.runtime]["services"][path] = state
        else:
            app.run_db[self.runtime] = state
        return state

    def run(self, payload):
        self.get_state(payload)
        self.run_state["status"] = "Running"
        start = datetime.now().replace(microsecond=0)
        try:
            app.service_db[self.service.id]["runs"] += 1
            self.service.status = "Running"
            Session.commit()
            results = {"runtime": self.runtime, **self.device_run()}
        except Exception:
            result = (
                f"Running {self.service.type} '{self.service.name}'"
                " raised the following exception:\n"
                f"{chr(10).join(format_exc().splitlines())}\n\n"
                "Run aborted..."
            )
            self.log("error", result)
            results = {"success": False, "runtime": self.runtime, "result": result}
        finally:
            self.close_remaining_connections()
            Session.commit()
            results["summary"] = self.run_state.get("summary", None)
            self.status = "Aborted" if self.stop else "Completed"
            self.run_state["status"] = self.status
            if self.run_state["success"] is not False:
                self.success = self.run_state["success"] = results["success"]
            app.service_db[self.service.id]["runs"] -= 1
            if not app.service_db[self.id]["runs"]:
                self.service.status = "Idle"
            results["duration"] = self.duration = str(
                datetime.now().replace(microsecond=0) - start
            )
            self.state = results["state"] = app.run_db.pop(self.runtime)

            if self.task and not self.task.frequency:
                self.task.is_active = False
            self.create_result(results)
            Session.commit()
        return results

    def device_iteration(self, device):
        derived_devices = self.compute_devices_from_query(
            self.service.iteration_devices,
            self.service.iteration_devices_property,
            **locals(),
        )
        derived_run = factory(
            "run",
            **{
                "service": self.service.id,
                "devices": [derived_device.id for derived_device in derived_devices],
                "workflow": self.workflow.id,
                "parent_device": device.id,
                "restart_run": self.restart_run,
                "parent": self,
                "runtime": self.runtime,
            },
        )
        success = derived_run.run()["success"]
        key = "success" if success else "failure"
        self.run_state["summary"][key].append(device.name)
        return success

    def queue_worker(self):
        try:
            while True:
                data = self.queue.get_nowait()
                device = fetch("device", id=data["device"])
                run = fetch("run", runtime=data["runtime"])
                run.get_results(device, data["path"])
                self.queue.task_done()
        except Empty:
            pass

    def device_run(self):
        self.devices = self.compute_devices()
        if self.service.iteration_devices and not self.parent_device:
            if not self.workflow:
                return {
                    "success": False,
                    "result": "Device iteration not allowed outside of a workflow",
                    "runtime": self.runtime,
                }
            results = [self.device_iteration(device) for device in self.devices]
            return {"success": all(results), "runtime": self.runtime}
        elif self.service.run_method != "per_device":
            return self.get_results(path=str(self.service_id))
        else:
            threads = []
            thread_number = min(self.service.max_processes, len(self.devices))
            for device in self.devices:
                self.queue.put(
                    {
                        "runtime": self.runtime,
                        "path": str(self.service_id),
                        "device": device.id,
                    }
                )
            for _ in range(thread_number):
                t = Thread(target=self.queue_worker)
                threads.append(t)
                t.start()
            for t in threads:
                t.join()
            result = self.run_state["progress"]["device"]
            success = result["total"] == result["success"] + result["skipped"]
            return {"success": success}

    def create_result(self, results, service=None, device=None):
        if not service:
            service = self.service
        self.success = results["success"]
        result_kw = {
            "run": self,
            "result": results,
            "service": service.id,
            "runtime": self.runtime,
        }
        if self.workflow_id:
            result_kw["workflow"] = self.workflow_id
        if self.parent_device_id:
            result_kw["parent_device"] = self.parent_device_id
        if device:
            result_kw["device"] = device.id
        if not device:
            for service_id, log in app.run_logs.pop(self.runtime, {}).items():
                factory(
                    "service_log",
                    runtime=self.runtime,
                    service=service_id,
                    content="\n".join(log),
                )
        factory("result", **result_kw)

    def run_service_job(self, device, service):
        args = (device,) if device else ()
        retries, total_retries = service.number_of_retries + 1, 0
        while retries and total_retries < service.max_number_of_retries:
            retries -= 1
            total_retries += 1
            try:
                if service.number_of_retries - retries:
                    retry = service.number_of_retries - retries
                    self.log("error", f"RETRY n°{retry}", device)
                results = service.job(self, *args)
                if device and getattr(self, "close_connection", False):
                    self.close_device_connection(device.name)
                service.convert_result(results)
                if "success" not in results:
                    results["success"] = True
                try:
                    _, exec_variables = self.eval(
                        service.result_postprocessing, function="exec", **locals()
                    )
                    if isinstance(exec_variables.get("retries"), int):
                        retries = exec_variables["retries"]
                except SystemExit:
                    pass
                if results["success"] and service.validation_method != "none":
                    self.validate_result(results, service, device)
                if results["success"]:
                    return results
                elif retries:
                    sleep(service.time_between_retries)
            except Exception as exc:
                self.log("error", str(exc), device)
                result = chr(10).join(format_exc().splitlines())
                results = {"success": False, "result": result}
        return results

    def get_results(self, device=None, path=None):
        service_path = path.split(">")
        service = fetch("service", id=service_path[-1])
        if len(service_path) > 1:
            workflow = fetch("service", id=service_path[-2])
            workflow_state = self.get_state(path="".join(service_path[:-1]), service=workflow)
        else:
            workflow = None
        if len(service_path) > 2:
            preworkflow = fetch("service", id=service_path[-3])
        else:
            preworkflow = None
        state = self.get_state(path=path, service=service)
        state["progress"]["device"]["total"] += 1
        self.log("info", "STARTING", device)
        if service.type == "workflow":
            start = fetch("service", scoped_name="Start")
            self.queue.put(
                {
                    "runtime": self.runtime,
                    "path": f"{path}>{start.id}",
                    "device": device.id if device else None,
                }
            )
        start = datetime.now().replace(microsecond=0)
        skip_service = False
        if service.skip_query:
            skip_service = self.eval(service.skip_query, **locals())[0]
        if skip_service or service.skip:
            if device:
                state["progress"]["device"]["skipped"] += 1
                key = "success" if self.skip_value == "True" else "failure"
                state["summary"][key].append(device.name)
            return {
                "result": "skipped",
                "success": service.skip_value == "True",
            }
        results = {"logs": []}
        try:
            if service.iteration_values:
                targets_results = {}
                targets = self.eval(service.iteration_values, **locals())[0]
                if not isinstance(targets, dict):
                    targets = dict(zip(map(str, targets), targets))
                for target_name, target_value in targets.items():
                    self.payload_helper(
                        self.iteration_variable_name,
                        target_value,
                        device=getattr(device, "name", None),
                    )
                    targets_results[target_name] = self.run_service_job(device, service)
                results.update(
                    {
                        "result": targets_results,
                        "success": all(r["success"] for r in targets_results.values()),
                    }
                )
            else:
                results.update(self.run_service_job(device, service))
        except Exception:
            results.update(
                {"success": False, "result": chr(10).join(format_exc().splitlines())}
            )
            self.log("error", chr(10).join(format_exc().splitlines()), device)
        results["duration"] = str(datetime.now().replace(microsecond=0) - start)
        status = "success" if results["success"] else "failure"
        if device:
            state["progress"]["device"][status] += 1
            state["summary"][status].append(device.name)
            self.create_result({"runtime": app.get_time(), **results}, service, device)
        if service.scoped_name == "End" and preworkflow and results["success"]:
            #workflow_state["progress"]["device"]["success"] += 1
            for neighbor, edge in workflow.neighbors(
                preworkflow, "destination", "success"
            ):
                prepath = ">".join(service_path[:-2])
                self.queue.put(
                    {
                        "runtime": self.runtime,
                        "path": f"{prepath}>{neighbor.id}",
                        "device": device.id if device else None,
                    }
                )
        elif workflow:
            for neighbor, edge in service.neighbors(
                workflow, "destination", status
            ):
                self.edge_state[edge.id] += 1
                prepath = ">".join(service_path[:-1])
                self.queue.put(
                    {
                        "runtime": self.runtime,
                        "path": f"{prepath}>{neighbor.id}",
                        "device": device.id if device else None,
                    }
                )
        self.log("info", "FINISHED", device)
        if service.send_notification:
            results = service.notify(results)
        if service.waiting_time:
            self.log("info", f"SLEEP {service.waiting_time} seconds...", device)
            sleep(service.waiting_time)
        Session.commit()
        return results

    def log(self, severity, content, device=None, service=None):
        if not service:
            service = self.service
        log_level = int(self.service.log_level)
        if not log_level or severity not in app.log_levels[log_level - 1 :]:
            return
        log = f"{app.get_time()} - {severity} - SERVICE {service.scoped_name}"
        if device:
            log += f" - DEVICE {device if isinstance(device, str) else device.name}"
        log += f" : {content}"
        app.run_logs[self.runtime][service.id].append(log)

    def build_notification(self, service, results):
        notification = {
            "Service": f"{service.name} ({service.type})",
            "Runtime": self.runtime,
            "Status": "PASS" if results["success"] else "FAILED",
        }
        if service.notification_header:
            notification["Header"] = service.notification_header
        if service.include_link_in_summary:
            address = app.settings["app"]["address"]
            notification["Link"] = f"{address}/view_service_results/{service.id}"
        summary = results["summary"]
        if summary:
            if summary["failure"]:
                notification["FAILED"] = summary["failure"]
            if summary["success"] and not service.display_only_failed_nodes:
                notification["PASSED"] = summary["success"]
        return notification

    def notify(self, service, results):
        self.log("info", f"Sending {service.send_notification_method} notification...")
        notification = self.build_notification(service, results)
        file_content = deepcopy(notification)
        if service.include_device_results:
            file_content["Device Results"] = {}
            for device in self.devices:
                device_result = fetch(
                    "result",
                    service_id=self.service_id,
                    runtime=self.runtime,
                    device_id=device.id,
                    allow_none=True,
                )
                if device_result:
                    file_content["Device Results"][device.name] = device_result.result
        try:
            if service.send_notification_method == "mail":
                filename = self.runtime.replace(".", "").replace(":", "")
                status = "PASS" if results["success"] else "FAILED"
                result = app.send_email(
                    f"{status}: {service.name} run by {self.creator}",
                    app.str_dict(notification),
                    recipients=service.mail_recipient,
                    filename=f"results-{filename}.txt",
                    file_content=app.str_dict(file_content),
                )
            elif service.send_notification_method == "slack":
                result = SlackClient(environ.get("SLACK_TOKEN")).api_call(
                    "chat.postMessage",
                    channel=app.settings["slack"]["channel"],
                    text=notification,
                )
            else:
                result = post(
                    app.settings["mattermost"]["url"],
                    verify=app.settings["mattermost"]["verify_certificate"],
                    data=dumps(
                        {
                            "channel": app.settings["mattermost"]["channel"],
                            "text": notification,
                        }
                    ),
                ).text
            results["notification"] = {"success": True, "result": result}
        except Exception:
            results["notification"] = {
                "success": False,
                "error": "\n".join(format_exc().splitlines()),
            }
        return results

    def get_credentials(self, service, device):
        if service.credentials == "device":
            return device.username, device.password
        elif service.credentials == "user":
            user = fetch("user", name=self.creator)
            return user.name, user.password
        else:
            return (
                self.sub(service.custom_username, locals()),
                self.sub(service.custom_password, locals()),
            )

    def validate_result(self, results, service, device):
        if service.validation_method == "text":
            match = self.sub(service.content_match, locals())
            str_result = str(results["result"])
            if service.delete_spaces_before_matching:
                match, str_result = map(self.space_deleter, (match, str_result))
            success = (
                service.content_match_regex
                and bool(search(match, str_result))
                or match in str_result
                and not service.content_match_regex
            )
        else:
            match = self.sub(service.dict_match, locals())
            success = service.match_dictionary(results["result"], match)
        results["success"] = not success if service.negative_logic else success
        results.update({"match": match, "negative_logic": service.negative_logic})

    def payload_helper(
        self,
        name,
        value=None,
        device=None,
        section=None,
        operation="set",
        allow_none=False,
    ):
        payload = self.run_state["payload"].setdefault("variables", {})
        if device:
            payload = payload.setdefault("devices", {})
            payload = payload.setdefault(device, {})
        if section:
            payload = payload.setdefault(section, {})
        if value is not None:
            if operation == "set":
                payload[name] = value
            else:
                getattr(payload[name], operation)(value)
        else:
            if name not in payload and not allow_none:
                raise Exception(f"Payload Editor: {name} not found in {payload}.")
            return payload.get(name)

    def get_var(self, name, device=None, **kwargs):
        return self.payload_helper(name, device=device, **kwargs)

    def get_result(self, service_name, device=None, workflow=None):
        def filter_run(query, property):
            query = query.filter(
                models["run"].service.has(
                    getattr(models["service"], property) == service_name
                )
            )
            return query.all()

        def recursive_search(run: "Run"):
            if not run:
                return None
            query = Session.query(models["run"]).filter(
                models["run"].parent_runtime == run.parent_runtime
            )
            if workflow or self.workflow:
                name = workflow or self.workflow.name
                query.filter(
                    models["run"].workflow.has(models["workflow"].name == name)
                )
            runs = filter_run(query, "scoped_name") or filter_run(query, "name")
            results = list(filter(None, [run.result(device) for run in runs]))
            if not results:
                return recursive_search(run.restart_run)
            else:
                return results.pop().result

        return recursive_search(self)

    def global_variables(_self, **locals):  # noqa: N805
        payload, device = _self.run_state["payload"], locals.get("device")
        variables = {
            "settings": app.settings,
            "devices": _self.devices,
            "get_var": partial(_self.get_var),
            "get_result": _self.get_result,
            "log": _self.log,
            "workflow": _self.workflow,
            "set_var": partial(_self.payload_helper),
            "parent_device": _self.parent_device or device,
            **locals,
        }
        if "variables" not in payload:
            return variables
        variables.update(
            {k: v for k, v in payload["variables"].items() if k != "devices"}
        )
        if "devices" in payload["variables"] and device:
            variables.update(payload["variables"]["devices"].get(device.name, {}))
        return variables

    def eval(_self, query, function="eval", **locals):  # noqa: N805
        exec_variables = _self.global_variables(**locals)
        results = builtins[function](query, exec_variables)
        return results, exec_variables

    def sub(self, input, variables):
        r = compile("{{(.*?)}}")

        def replace(match):
            return str(self.eval(match.group()[2:-2], **variables)[0])

        def rec(input):
            if isinstance(input, str):
                return r.sub(replace, input)
            elif isinstance(input, list):
                return [rec(x) for x in input]
            elif isinstance(input, dict):
                return {rec(k): rec(v) for k, v in input.items()}
            else:
                return input

        return rec(input)

    def space_deleter(self, input):
        return "".join(input.split())

    def update_netmiko_connection(self, service, connection):
        for property in ("fast_cli", "timeout", "global_delay_factor"):
            service_value = getattr(service, property)
            if service_value:
                setattr(connection, property, service_value)
        try:
            if not hasattr(connection, "check_config_mode"):
                self.log("error", f"Netmiko 'check_config_mode' method is missing.")
                return
            mode = connection.check_config_mode()
            if mode and not service.config_mode:
                connection.exit_config_mode()
            elif service.config_mode and not mode:
                connection.config_mode()
        except Exception as exc:
            self.log("error", f"Failed to honor the config mode {exc}")
        return connection

    def netmiko_connection(self, service, device):
        connection = self.get_or_close_connection(service, "netmiko", device.name)
        if connection:
            self.log("info", "Using cached Netmiko connection", device)
            return self.update_netmiko_connection(service, connection)
        self.log("info", "Opening new Netmiko connection", device)
        username, password = self.get_credentials(service, device)
        driver = device.netmiko_driver if service.use_device_driver else service.driver
        netmiko_connection = ConnectHandler(
            device_type=driver,
            ip=device.ip_address,
            port=device.port,
            username=username,
            password=password,
            secret=device.enable_password,
            fast_cli=service.fast_cli,
            timeout=service.timeout,
            global_delay_factor=service.global_delay_factor,
            session_log=BytesIO(),
        )
        if service.enable_mode:
            netmiko_connection.enable()
        if service.config_mode:
            netmiko_connection.config_mode()
        app.connections_cache["netmiko"][self.runtime][device.name] = netmiko_connection
        return netmiko_connection

    def napalm_connection(self, service, device):
        connection = self.get_or_close_connection(service, "napalm", device.name)
        if connection:
            self.log("info", "Using cached NAPALM connection", device)
            return connection
        self.log("info", "Opening new NAPALM connection", device)
        username, password = self.get_credentials(service, device)
        optional_args = self.service.optional_args
        if not optional_args:
            optional_args = {}
        if "secret" not in optional_args:
            optional_args["secret"] = device.enable_password
        driver = get_network_driver(
            device.napalm_driver if service.use_device_driver else service.driver
        )
        napalm_connection = driver(
            hostname=device.ip_address,
            username=username,
            password=password,
            timeout=service.timeout,
            optional_args=optional_args,
        )
        napalm_connection.open()
        app.connections_cache["napalm"][self.runtime][device.name] = napalm_connection
        return napalm_connection

    def get_or_close_connection(self, service, library, device):
        connection = self.get_connection(library, device)
        if not connection:
            return
        if service.start_new_connection:
            return self.disconnect(library, device, connection)
        if library == "napalm":
            if connection.is_alive():
                return connection
            else:
                self.disconnect(library, device, connection)
        else:
            try:
                connection.find_prompt()
                return connection
            except Exception:
                self.disconnect(library, device, connection)

    def get_connection(self, library, device):
        cache = app.connections_cache[library].get(self.runtime, {})
        return cache.get(device)

    def close_device_connection(self, device):
        for library in ("netmiko", "napalm"):
            connection = self.get_connection(library, device)
            if connection:
                self.disconnect(library, device, connection)

    def close_remaining_connections(self):
        threads = []
        for library in ("netmiko", "napalm"):
            for connection in app.connections_cache[library][self.runtime].items():
                thread = Thread(target=self.disconnect, args=(library, *connection))
                thread.start()
                threads.append(thread)
        for thread in threads:
            thread.join()

    def disconnect(self, library, device, connection):
        try:
            connection.disconnect() if library == "netmiko" else connection.close()
            app.connections_cache[library][self.runtime].pop(device)
            self.log("info", f"Closed {library} connection", device)
        except Exception as exc:
            self.log(
                "error", f"Error while closing {library} connection ({exc})", device
            )

    def generate_yaml_file(self, path, device):
        data = {
            "last_failure": device.last_failure,
            "last_runtime": device.last_runtime,
            "last_update": device.last_update,
            "last_status": device.last_status,
        }
        with open(path / "data.yml", "w") as file:
            yaml.dump(data, file, default_flow_style=False)
