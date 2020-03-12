from subprocess import check_output
from sqlalchemy import ForeignKey, Integer

from eNMS.database.dialect import Column, SmallString
from eNMS.forms.automation import ServiceForm
from eNMS.forms.fields import HiddenField, StringField
from eNMS.models.automation import Service


class UnixCommandService(Service):

    __tablename__ = "unix_command_service"
    pretty_name = "Unix Command"
    id = Column(Integer, ForeignKey("service.id"), primary_key=True)
    command = Column(SmallString)

    __mapper_args__ = {"polymorphic_identity": "unix_command_service"}

    def job(self, run, device):
        command = run.sub(self.command, locals())
        run.log("info", f"Running UNIX command: {command}", device, self)
        return {"command": command, "result": check_output(command.split()).decode()}


class UnixCommandForm(ServiceForm):
    form_type = HiddenField(default="unix_command_service")
    command = StringField(substitution=True)
