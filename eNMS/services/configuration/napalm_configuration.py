from sqlalchemy import Boolean, ForeignKey, Integer
from wtforms.widgets import TextArea

from eNMS.database.dialect import Column, LargeString, MutableDict, SmallString
from eNMS.forms.fields import HiddenField, SelectField, StringField
from eNMS.forms.automation import NapalmForm
from eNMS.models.automation import ConnectionService


class NapalmConfigurationService(ConnectionService):

    __tablename__ = "napalm_configuration_service"
    pretty_name = "NAPALM Configuration"
    parent_type = "connection_service"
    id = Column(Integer, ForeignKey("connection_service.id"), primary_key=True)
    action = Column(SmallString)
    content = Column(LargeString, default="")
    driver = Column(SmallString)
    use_device_driver = Column(Boolean, default=True)
    timeout = Column(Integer, default=60)
    optional_args = Column(MutableDict)

    __mapper_args__ = {"polymorphic_identity": "napalm_configuration_service"}

    def job(self, run, device, **kwargs):
        napalm_connection = run.napalm_connection(self, device)
        run.log("info", "Pushing Configuration with NAPALM", device, self)
        config = "\n".join(run.sub(self.content, locals()).splitlines())
        getattr(napalm_connection, self.action)(config=config)
        napalm_connection.commit_config()
        return {"success": True, "result": f"Config push ({config})"}


class NapalmConfigurationForm(NapalmForm):
    form_type = HiddenField(default="napalm_configuration_service")
    action = SelectField(
        choices=(
            ("load_merge_candidate", "Load merge"),
            ("load_replace_candidate", "Load replace"),
        )
    )
    content = StringField(widget=TextArea(), render_kw={"rows": 5}, substitution=True)
    groups = {
        "Main Parameters": {"commands": ["action", "content"], "default": "expanded"},
        **NapalmForm.groups,
    }
