from json import dumps, loads
from requests import (
    get as rest_get,
    post as rest_post,
    put as rest_put,
    delete as rest_delete,
)
from requests.auth import HTTPBasicAuth
from sqlalchemy import Boolean, Column, ForeignKey, Integer, PickleType, String, Text
from sqlalchemy.ext.mutable import MutableDict
from typing import Optional
from wtforms import BooleanField, HiddenField, IntegerField, SelectField, StringField

from eNMS.database import LARGE_STRING_LENGTH, SMALL_STRING_LENGTH
from eNMS.forms.automation import ServiceForm
from eNMS.forms.fields import DictField
from eNMS.forms.services import ValidationForm
from eNMS.models.automation import Service
from eNMS.models.inventory import Device


class RestCallService(Service):

    __tablename__ = "RestCallService"

    id = Column(Integer, ForeignKey("Service.id"), primary_key=True)
    has_targets = Column(Boolean, default=False)
    call_type = Column(String(SMALL_STRING_LENGTH), default="")
    url = Column(String(SMALL_STRING_LENGTH), default="")
    payload = Column(MutableDict.as_mutable(PickleType), default={})
    params = Column(MutableDict.as_mutable(PickleType), default={})
    headers = Column(MutableDict.as_mutable(PickleType), default={})
    timeout = Column(Integer, default=15)
    validation_method = Column(String(SMALL_STRING_LENGTH), default="")
    content_match = Column(Text(LARGE_STRING_LENGTH), default="")
    content_match_regex = Column(Boolean, default=False)
    dict_match = Column(MutableDict.as_mutable(PickleType), default={})
    negative_logic = Column(Boolean, default=False)
    delete_spaces_before_matching = Column(Boolean, default=False)
    username = Column(String(SMALL_STRING_LENGTH), default="")
    password = Column(String(SMALL_STRING_LENGTH), default="")

    request_dict = {
        "GET": rest_get,
        "POST": rest_post,
        "PUT": rest_put,
        "DELETE": rest_delete,
    }

    __mapper_args__ = {"polymorphic_identity": "RestCallService"}

    def job(self, payload: dict, device: Optional[Device] = None) -> dict:
        rest_url = self.sub(self.url, locals())
        self.logs.append(f"Sending REST call to {rest_url}")
        kwargs = {p: getattr(self, p) for p in ("headers", "params", "timeout")}
        if self.call_type in ("GET", "DELETE"):
            response = self.request_dict[self.call_type](
                rest_url, auth=HTTPBasicAuth(self.username, self.password), **kwargs
            )
        else:
            response = self.request_dict[self.call_type](
                rest_url,
                data=dumps(self.sub_dict(self.payload, locals())),
                auth=HTTPBasicAuth(self.username, self.password),
                **kwargs,
            )
        if response.status_code != 200:
            return {"success": False, "response_code": response.status_code}
        result = (
            response.json() if self.call_type in ("GET", "DELETE") else response.content
        )
        match = self.sub(self.content_match, locals())
        return {
            "url": rest_url,
            "match": match if self.validation_method == "text" else self.dict_match,
            "negative_logic": self.negative_logic,
            "result": result,
            "success": self.match_content(result, match),
        }


class RestCallForm(ServiceForm, ValidationForm):
    form_type = HiddenField(default="RestCallService")
    has_targets = BooleanField()
    call_type = SelectField(
        choices=(("GET", "GET"), ("POST", "POST"), ("PUT", "PUT"), ("DELETE", "DELETE"))
    )
    url = StringField()
    payload = DictField()
    params = DictField()
    headers = DictField()
    timeout = IntegerField(default=15)
    username = StringField()
    password = StringField()
    pass_device_properties = BooleanField()
    options = DictField()
