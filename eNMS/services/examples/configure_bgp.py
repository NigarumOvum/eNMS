from sqlalchemy import ForeignKey, Integer

from eNMS.database.dialect import Column, SmallString
from eNMS.forms.automation import NapalmForm
from eNMS.forms.fields import HiddenField, IntegerField, StringField

from eNMS.models.automation import ConnectionService


class ConfigureBgpService(ConnectionService):

    __tablename__ = "configure_bgp_service"
    pretty_name = "Configure BGP"
    parent_type = "connection_service"
    id = Column(Integer, ForeignKey("connection_service.id"), primary_key=True)
    local_as = Column(Integer, default=0)
    loopback = Column(SmallString)
    loopback_ip = Column(SmallString)
    neighbor_ip = Column(SmallString)
    remote_as = Column(Integer, default=0)
    vrf_name = Column(SmallString)
    driver = "ios"

    __mapper_args__ = {"polymorphic_identity": "configure_bgp_service"}

    def job(self, run, device):
        napalm_connection = run.napalm_connection(self, device)
        config = f"""
            ip vrf {self.vrf_name}
            rd {self.local_as}:235
            route-target import {self.local_as}:410
            route-target export {self.local_as}:400
            maximum route 10000 80
            interface {self.loopback}
            ip vrf forwarding {self.vrf_name}
            ip address {self.loopback_ip} 255.255.255.255
            router bgp {self.local_as}
            address-family ipv4 vrf {self.vrf_name}
            network {self.loopback_ip} mask 255.255.255.255
            neighbor {self.neighbor_ip} remote-as {self.remote_as}
            neighbor {self.neighbor_ip} activate
            neighbor {self.neighbor_ip} send-community both
            neighbor {self.neighbor_ip} as-override
            exit-address-family
        """
        config = "\n".join(config.splitlines())
        run.log("info", "Pushing BGP configuration with Napalm", device, self)
        napalm_connection.load_merge_candidate(config=config)
        napalm_connection.commit_config()
        return {"success": True, "result": f"Config push ({config})"}


class ConfigureBgpForm(NapalmForm):
    form_type = HiddenField(default="configure_bgp_service")
    local_as = IntegerField("Local AS", default=0)
    loopback = StringField("Loopback", default="Lo42")
    loopback_ip = StringField("Loopback IP")
    neighbor_ip = StringField("Neighbor IP")
    remote_as = IntegerField("Remote AS")
    vrf_name = StringField("VRF Name")
    groups = {
        "Main Parameters": {
            "commands": [
                "local_as",
                "loopback",
                "loopback_ip",
                "neighbor_ip",
                "remote_as",
                "vrf_name",
            ],
            "default": "expanded",
        },
        **NapalmForm.groups,
    }
