from flask import current_app, jsonify, request, send_file, session
from flask_wtf import FlaskForm
from flask.wrappers import Response
from json.decoder import JSONDecodeError
from sqlalchemy import and_
from sqlalchemy.exc import IntegrityError
from typing import List

from eNMS.database.functions import delete, factory, fetch, fetch_all, Session
from eNMS.forms import form_actions, form_classes, form_templates
from eNMS.forms.administration import LoginForm
from eNMS.forms.automation import ServiceTableForm
from eNMS.models import models
from eNMS.properties.diagram import type_to_diagram_properties
from eNMS.properties.table import (
    filtering_properties,
    table_fixed_columns,
    table_properties,
)


class Dispatcher:
    def filtering(self, table: str) -> dict:
        model = models.get(table, models["Device"])
        properties = table_properties[table]
        try:
            order_property = properties[int(request.args["order[0][column]"])]
        except IndexError:
            order_property = "name"
        order = getattr(getattr(model, order_property), request.args["order[0][dir]"])()
        constraints = []
        for property in filtering_properties[table]:
            value = request.args.get(f"form[{property}]")
            if value:
                constraints.append(getattr(model, property).contains(value))
        result = Session.query(model).filter(and_(*constraints)).order_by(order)
        if table in ("device", "link", "configuration"):
            pools = [int(id) for id in request.args.getlist("form[pools][]")]
            if pools:
                result = result.filter(model.pools.any(models["pool"].id.in_(pools)))
        return {
            "jsonify": True,
            "draw": int(request.args["draw"]),
            "recordsTotal": len(Session.query(model).all()),
            "recordsFiltered": len(result.all()),
            "data": [
                [getattr(obj, property) for property in properties]
                + obj.generate_row(table)
                for obj in result.limit(int(request.args["length"]))
                .offset(int(request.args["start"]))
                .all()
            ],
        }

    def form(self, form_type: str) -> dict:
        return dict(
            action=form_actions.get(form_type),
            form=form_classes.get(form_type, FlaskForm)(request.form),
            form_type=form_type,
            template=f"forms/{form_templates.get(form_type, 'base')}_form",
        )

    def table(self, table_type: str) -> dict:
        table_dict = dict(
            properties=table_properties[table_type],
            fixed_columns=table_fixed_columns[table_type],
            type=table_type,
            template="pages/table",
        )
        if table_type == "service":
            service_table_form = ServiceTableForm(request.form)
            service_table_form.services.choices = [
                (service, service)
                for service in models
                if service != "Service" and service.endswith("Service")
            ]
            table_dict["service_table_form"] = service_table_form
        return table_dict

    def dashboard(self) -> dict:
        return dict(properties=type_to_diagram_properties)

    def workflow_builder(self) -> dict:
        workflow = fetch("Workflow", id=session.get("workflow", None))
        return dict(workflow=workflow.serialized if workflow else None)

    def calendar(self) -> dict:
        return {}

    def download_configuration(self, name: str) -> Response:
        try:
            return send_file(
                filename_or_fp=str(
                    current_app.path / "git" / "configurations" / name / name
                ),
                as_attachment=True,
                attachment_filename=f"configuration_{name}.txt",
            )
        except FileNotFoundError:
            return jsonify("No configuration stored")

    def administration(self) -> dict:
        return dict(folders=listdir(current_app.path / "migrations"))

    def login(self) -> Response:
        if request.method == "POST":
            name, password = request.form["name"], request.form["password"]
            try:
                if request.form["authentication_method"] == "Local User":
                    user = fetch("User", name=name)
                    if user and password == user.password:
                        login_user(user)
                        return redirect(url_for("bp.route", page="dashboard"))
                elif request.form["authentication_method"] == "LDAP Domain":
                    with Connection(
                        controller.ldap_client,
                        user=f"{controller.LDAP_USERDN}\\{name}",
                        password=password,
                        auto_bind=True,
                        authentication=NTLM,
                    ) as connection:
                        connection.search(
                            controller.LDAP_BASEDN,
                            f"(&(objectClass=person)(samaccountname={name}))",
                            search_scope=SUBTREE,
                            get_operational_attributes=True,
                            attributes=["cn", "memberOf", "mail"],
                        )
                        json_response = loads(connection.response_to_json())["entries"][
                            0
                        ]
                        if json_response:
                            user = {
                                "name": name,
                                "password": password,
                                "email": json_response["attributes"].get("mail", ""),
                            }
                            if any(
                                group in s
                                for group in controller.LDAP_ADMIN_GROUP
                                for s in json_response["attributes"]["memberOf"]
                            ):
                                user["permissions"] = ["Admin"]
                            new_user = factory("User", **user)
                            login_user(new_user)
                            return redirect(url_for("bp.route", page="dashboard"))
                elif request.form["authentication_method"] == "TACACS":
                    if controller.tacacs_client.authenticate(name, password).valid:
                        user = factory("User", **{"name": name, "password": password})
                        login_user(user)
                        return redirect(url_for("bp.route", page="dashboard"))
                abort(403)
            except Exception as e:
                info(f"Authentication failed ({str(e)})")
                abort(403)
        if not current_user.is_authenticated:
            login_form = LoginForm(request.form)
            authentication_methods = [("Local User",) * 2]
            if controller.USE_LDAP:
                authentication_methods.append(("LDAP Domain",) * 2)
            if controller.USE_TACACS:
                authentication_methods.append(("TACACS",) * 2)
            login_form.authentication_method.choices = authentication_methods
            return render_template("login.html", login_form=login_form)
        return redirect(url_for("bp.route", page="dashboard"))

    def logout(self) -> Response:
        logout_user()
        return redirect(url_for("bp.route", page="login"))


dispatcher = Dispatcher()
