class Base {
  constructor(properties) {
    Object.assign(this, properties);
  }
}

class Device extends Base {
  static get columns() {
    return [
      { data: "name", title: "Name", search: "text" },
      { data: "description", title: "Description", search: "text" },
      { data: "subtype", title: "Subtype", search: "text" },
      { data: "model", title: "Model", search: "text" },
      { data: "location", title: "Location", search: "text" },
      { data: "vendor", title: "Vendor", search: "text" },
      { data: "operating_system", title: "Operating System", search: "text" },
      { data: "os_version", title: "OS Version", search: "text" },
      { data: "ip_address", title: "IP Address", search: "text" },
      { data: "port", title: "Port", search: "text" },
      { data: "buttons" },
    ];
  }

  static get controls() {
    return `
      <button
        class="btn btn-primary"
        onclick="eNMS.showTypePanel('device')"
        data-tooltip="New"
        type="button"
      >
        <span class="glyphicon glyphicon-plus"></span>
      </button>
      <button
        class="btn btn-info btn-file"
        onclick="eNMS.showPanel('device_filtering')"
        data-tooltip="Advanced Search"
        type="button"
      >
        <span class="glyphicon glyphicon-search"></span>
      </button>
      <button
        class="btn btn-info btn-file"
        onclick="eNMS.refreshTable('device', true)"
        data-tooltip="Refresh"
        type="button"
      >
        <span class="glyphicon glyphicon-refresh"></span>
      </button>
      `;
  }

  get buttons() {
    const instance = JSON.stringify(this);
    return `
      <ul class="pagination pagination-lg" style="margin: 0px; width: 230px">
        <li>
          <button type="button" class="btn btn-sm btn-info"
          onclick='eNMS.showDeviceNetworkData(${instance})'
          data-tooltip="Network Data"
            ><span class="glyphicon glyphicon-cog"></span
          ></button>
        </li>
        <li>
          <button type="button" class="btn btn-sm btn-info"
          onclick="showDeviceResultsPanel(${instance})"
          data-tooltip="Results"
            ><span class="glyphicon glyphicon-list-alt"></span
          ></button>
        </li>
        <li>
          <button type="button" class="btn btn-sm btn-success"
          onclick="eNMS.showPanel('device_connection', '${this.id}')"
          data-tooltip="Connection"
            ><span class="glyphicon glyphicon-console"></span
          ></button>
        </li>
        <li>
          <button type="button" class="btn btn-sm btn-primary"
          onclick="eNMS.showTypePanel('device', '${
            this.id
          }')" data-tooltip="Edit"
            ><span class="glyphicon glyphicon-edit"></span
          ></button>
        </li>
        <li>
          <button type="button" class="btn btn-sm btn-primary"
          onclick="eNMS.showTypePanel('device', '${this.id}', 'duplicate')"
          data-tooltip="Duplicate"
            ><span class="glyphicon glyphicon-duplicate"></span
          ></button>
        </li>
        <li>
          <button type="button" class="btn btn-sm btn-danger"
          onclick="showDeletionPanel(${instance})" data-tooltip="Delete"
            ><span class="glyphicon glyphicon-trash"></span
          ></button>
        </li>
      </ul>`;
  }
}

class Link extends Base {
  static get columns() {
    return [
      { data: "name", title: "Name", search: "text" },
      { data: "description", title: "Description", search: "text" },
      { data: "subtype", title: "Subtype", search: "text" },
      { data: "model", title: "Model", search: "text" },
      { data: "location", title: "Location", search: "text" },
      { data: "vendor", title: "Vendor", search: "text" },
      { data: "source_name", title: "Source", search: "text" },
      { data: "destination_name", title: "Destination", search: "text" },
      { data: "buttons" },
    ];
  }

  get buttons() {
    const instance = JSON.stringify(this);
    return `
      <ul class="pagination pagination-lg" style="margin: 0px; width: 120px">
        <li>
          <button type="button" class="btn btn-sm btn-primary"
          onclick="eNMS.showTypePanel('link', '${this.id}')" data-tooltip="Edit"
            ><span class="glyphicon glyphicon-edit"></span
          ></button>
        </li>
        <li>
          <button type="button" class="btn btn-sm btn-primary"
          onclick="eNMS.showTypePanel('link', '${this.id}', 'duplicate')"
          data-tooltip="Duplicate"
            ><span class="glyphicon glyphicon-duplicate"></span
          ></button>
        </li>
        <li>
          <button type="button" class="btn btn-sm btn-danger"
          onclick='showDeletionPanel(${instance})' data-tooltip="Delete"
            ><span class="glyphicon glyphicon-trash"></span
          ></button>
        </li>
      </ul>`;
  }
}

class Pool extends Base {
  get object_number() {
    return `${this.device_number} devices - ${this.link_number} links`;
  }

  static get columns() {
    return [
      { data: "name", title: "Name", search: "text" },
      { data: "last_modified", title: "Last modified", search: "text" },
      { data: "description", title: "Description", search: "text" },
      { data: "never_update", title: "Never update", search: "bool" },
      { data: "longitude", title: "Longitude", search: "text" },
      { data: "latitude", title: "Latitude", search: "text" },
      { data: "object_number", title: "Object Count" },
      { data: "buttons" },
    ];
  }

  get buttons() {
    const instance = JSON.stringify(this);
    return `
      <ul class="pagination pagination-lg" style="margin: 0px; width: 230px">
        <li>
          <button type="button" class="btn btn-sm btn-info"
          onclick="showPoolView('${this.id}')" data-tooltip="Internal View"
            ><span class="glyphicon glyphicon-eye-open"></span
          ></button>
        </li>
        <li>
          <button type="button" class="btn btn-sm btn-primary"
          onclick="showPoolObjectsPanel('${
            this.id
          }')" data-tooltip="Pool Objects"
            ><span class="glyphicon glyphicon-wrench"></span
          ></button>
        </li>
        <li>
          <button type="button" class="btn btn-sm btn-primary"
          onclick="updatePools('${this.id}')" data-tooltip="Update"
            ><span class="glyphicon glyphicon-refresh"></span
          ></button>
        </li>
        <li>
          <button type="button" class="btn btn-sm btn-primary"
          onclick="eNMS.showTypePanel('pool', '${this.id}')" data-tooltip="Edit"
            ><span class="glyphicon glyphicon-edit"></span
          ></button>
        </li>
        <li>
          <button type="button" class="btn btn-sm btn-primary"
          onclick="eNMS.showTypePanel('pool', '${this.id}', 'duplicate')"
          data-tooltip="Duplicate"
            ><span class="glyphicon glyphicon-duplicate"></span
          ></button>
        </li>
        <li>
          <button type="button" class="btn btn-sm btn-danger"
          onclick="showDeletionPanel(${instance})" data-tooltip="Delete"
            ><span class="glyphicon glyphicon-trash"></span
          ></button>
        </li>
      </ul>
    `;
  }
}

class Service extends Base {

  static get columns() {
    return [
      { data: "name", title: "Name", search: "text" },
      { data: "last_modified", title: "Last modified", search: "text" },
      { data: "type", title: "Type", search: "text" },
      { data: "description", title: "Description", search: "text" },
      { data: "vendor", title: "Vendor", search: "text" },
      { data: "operating_system", title: "Operating System", search: "text" },
      { data: "creator", title: "Creator", search: "text" },
      { data: "buttons" },
    ];
  }

  get buttons() {
    const instance = JSON.stringify(this);
    return `
      <ul class="pagination pagination-lg" style="margin: 0px; width: 230px">
        <li>
          <button type="button" class="btn btn-sm btn-info"
          onclick="showPoolView('${this.id}')" data-tooltip="Internal View"
            ><span class="glyphicon glyphicon-eye-open"></span
          ></button>
        </li>
        <li>
          <button type="button" class="btn btn-sm btn-primary"
          onclick="showPoolObjectsPanel('${
            this.id
          }')" data-tooltip="Pool Objects"
            ><span class="glyphicon glyphicon-wrench"></span
          ></button>
        </li>
        <li>
          <button type="button" class="btn btn-sm btn-primary"
          onclick="updatePools('${this.id}')" data-tooltip="Update"
            ><span class="glyphicon glyphicon-refresh"></span
          ></button>
        </li>
        <li>
          <button type="button" class="btn btn-sm btn-primary"
          onclick="eNMS.showTypePanel('pool', '${this.id}')" data-tooltip="Edit"
            ><span class="glyphicon glyphicon-edit"></span
          ></button>
        </li>
        <li>
          <button type="button" class="btn btn-sm btn-primary"
          onclick="eNMS.showTypePanel('pool', '${this.id}', 'duplicate')"
          data-tooltip="Duplicate"
            ><span class="glyphicon glyphicon-duplicate"></span
          ></button>
        </li>
        <li>
          <button type="button" class="btn btn-sm btn-danger"
          onclick="showDeletionPanel(${instance})" data-tooltip="Delete"
            ><span class="glyphicon glyphicon-trash"></span
          ></button>
        </li>
      </ul>
    `;
  }
}

const models = {
  device: Device,
  link: Link,
  pool: Pool,
};

export default models;
