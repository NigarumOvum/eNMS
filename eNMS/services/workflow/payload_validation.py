from sqlalchemy import ForeignKey, Integer

from eNMS.database.dialect import Column, SmallString
from eNMS.forms.automation import ServiceForm
from eNMS.forms.fields import HiddenField, StringField
from eNMS.models.automation import Service


class PayloadValidationService(Service):

    __tablename__ = "payload_validation_service"
    pretty_name = "Payload Validation"
    id = Column(Integer, ForeignKey("service.id"), primary_key=True)
    query = Column(SmallString)

    __mapper_args__ = {"polymorphic_identity": "payload_validation_service"}

    def job(self, run, device=None):
        return {"query": self.query, "result": run.eval(self.query, **locals())[0]}


class PayloadValidationForm(ServiceForm):
    form_type = HiddenField(default="payload_validation_service")
    query = StringField("Python Query")
    query_fields = ServiceForm.query_fields + ["query"]
