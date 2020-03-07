from io import StringIO
from re import findall
from sqlalchemy import ForeignKey, Integer
from textfsm import TextFSM
from wtforms.widgets import TextArea

from eNMS.database.dialect import Column, LargeString, SmallString
from eNMS.forms.automation import ServiceForm
from eNMS.forms.fields import HiddenField, SelectField, StringField
from eNMS.models.automation import Service


class PayloadExtractionService(Service):

    __tablename__ = "payload_extraction_service"
    pretty_name = "Payload Extraction"
    id = Column(Integer, ForeignKey("service.id"), primary_key=True)
    variable1 = Column(SmallString)
    query1 = Column(SmallString)
    match_type1 = Column(SmallString, default="none")
    match1 = Column(LargeString, default="")
    operation1 = Column(SmallString, default="set")
    variable2 = Column(SmallString)
    query2 = Column(SmallString)
    match_type2 = Column(SmallString, default="none")
    match2 = Column(LargeString, default="")
    operation2 = Column(SmallString, default="set")
    variable3 = Column(SmallString)
    query3 = Column(SmallString)
    match_type3 = Column(SmallString, default="none")
    match3 = Column(LargeString, default="")
    operation3 = Column(SmallString, default="set")

    __mapper_args__ = {"polymorphic_identity": "payload_extraction_service"}

    def job(self, run, device=None):
        result, success = {}, True
        for i in range(1, 4):
            variable = getattr(self, f"variable{i}")
            if not variable:
                continue
            query = getattr(self, f"query{i}")
            try:
                variables = locals()
                variables.pop("query")
                value = run.eval(query, **variables)[0]
            except Exception as exc:
                success = False
                result[variable] = f"Wrong Python query for {variable} ({exc})"
                continue
            match_type = getattr(self, f"match_type{i}")
            match = getattr(self, f"match{i}")
            operation = getattr(self, f"operation{i}")
            value = (
                value
                if match_type == "none"
                else findall(match, value)
                if match_type == "regex"
                else TextFSM(StringIO(match)).ParseText(value)
            )
            run.payload_helper(variable, value, device=device.name, operation=operation)
            result[variable] = {
                "query": query,
                "match_type": match_type,
                "match": match,
                "value": value,
            }
        return {"result": result, "success": success}


match_choices = (
    ("none", "Use Value as Extracted"),
    ("regex", "Apply Regular Expression (findall)"),
    ("textfsm", "Apply TextFSM Template"),
)

operation_choices = (
    ("set", "Set / Replace"),
    ("append", "Append to a list"),
    ("extend", "Extend list"),
    ("update", "Update dictionary"),
)


class PayloadExtractionForm(ServiceForm):
    form_type = HiddenField(default="payload_extraction_service")
    variable1 = StringField("Variable Name")
    query1 = StringField("Python Extraction Query")
    match_type1 = SelectField("Post Processing", choices=match_choices)
    match1 = StringField(
        "Regular Expression / TextFSM Template Text",
        widget=TextArea(),
        render_kw={"rows": 5},
    )
    operation1 = SelectField("Operation", choices=operation_choices)
    variable2 = StringField("Variable Name")
    query2 = StringField("Python Extraction Query")
    match_type2 = SelectField("Post Processing", choices=match_choices)
    match2 = StringField(
        "Regular Expression / TextFSM Template Text",
        widget=TextArea(),
        render_kw={"rows": 5},
    )
    operation2 = SelectField("Operation", choices=operation_choices)
    variable3 = StringField("Variable Name")
    query3 = StringField("Python Extraction Query")
    match_type3 = SelectField("Post Processing", choices=match_choices)
    match3 = StringField(
        "Regular Expression / TextFSM Template Text",
        widget=TextArea(),
        render_kw={"rows": 5},
    )
    operation3 = SelectField("Operation", choices=operation_choices)
    groups = {
        "Extraction 1": {
            "commands": ["variable1", "query1", "match_type1", "match1", "operation1"],
            "default": "expanded",
        },
        "Extraction 2": {
            "commands": ["variable2", "query2", "match_type2", "match2", "operation2"],
            "default": "expanded",
        },
        "Extraction 3": {
            "commands": ["variable3", "query3", "match_type3", "match3", "operation3"],
            "default": "expanded",
        },
    }
    query_fields = ServiceForm.query_fields + [f"query{i}" for i in range(1, 4)]
