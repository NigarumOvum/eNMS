from sqlalchemy import Boolean, Float, ForeignKey, Integer
from traceback import format_exc

from eNMS.database.dialect import Column, LargeString, SmallString
from eNMS.forms.fields import HiddenField, StringField
from eNMS.forms.automation import NetmikoForm
from eNMS.models.automation import ConnectionService


class NetmikoPromptsService(ConnectionService):

    __tablename__ = "netmiko_prompts_service"
    pretty_name = "Netmiko Prompts"
    parent_type = "connection_service"
    id = Column(Integer, ForeignKey("connection_service.id"), primary_key=True)
    enable_mode = Column(Boolean, default=True)
    config_mode = Column(Boolean, default=False)
    command = Column(SmallString)
    confirmation1 = Column(LargeString, default="")
    response1 = Column(SmallString)
    confirmation2 = Column(LargeString, default="")
    response2 = Column(SmallString)
    confirmation3 = Column(LargeString, default="")
    response3 = Column(SmallString)
    driver = Column(SmallString)
    use_device_driver = Column(Boolean, default=True)
    fast_cli = Column(Boolean, default=False)
    timeout = Column(Integer, default=10.0)
    delay_factor = Column(Float, default=1.0)
    global_delay_factor = Column(Float, default=1.0)

    __mapper_args__ = {"polymorphic_identity": "netmiko_prompts_service"}

    def job(self, run, device):
        netmiko_connection = run.netmiko_connection(device)
        send_strings = (run.command, run.response1, run.response2, run.response3)
        expect_strings = (run.confirmation1, run.confirmation2, run.confirmation3, None)
        commands = []
        results = {"commands": commands}
        for send_string, expect_string in zip(send_strings, expect_strings):
            if not send_string:
                break
            command = run.sub(send_string, locals())
            commands.append(command)
            run.log("info", f"Sending '{command}' with Netmiko", device)
            confirmation = run.sub(expect_string, locals())
            try:
                result = netmiko_connection.send_command_timing(
                    command, delay_factor=run.delay_factor
                )
            except Exception:
                return {
                    **results,
                    **{
                        "error": format_exc(),
                        "result": netmiko_connection.session_log.getvalue().decode(),
                        "match": confirmation,
                        "success": False,
                    },
                }
            results[command] = {"result": result, "match": confirmation}
            if confirmation and confirmation not in result:
                results.update(
                    {"success": False, "result": result, "match": confirmation}
                )
                return results
        return {"commands": commands, "result": result}


class NetmikoPromptsForm(NetmikoForm):
    form_type = HiddenField(default="netmiko_prompts_service")
    command = StringField()
    confirmation1 = StringField(substitution=True)
    response1 = StringField(substitution=True)
    confirmation2 = StringField(substitution=True)
    response2 = StringField(substitution=True)
    confirmation3 = StringField(substitution=True)
    response3 = StringField(substitution=True)
    groups = {
        "Main Parameters": {
            "commands": [
                "command",
                "confirmation1",
                "response1",
                "confirmation2",
                "response2",
                "confirmation3",
                "response3",
            ],
            "default": "expanded",
        },
        **NetmikoForm.groups,
    }
