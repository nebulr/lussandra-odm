var Client      = require('cassandra-prom')(require('cassandra-driver').Client);
var types       = require('./types');
var modelObj    = require('./model');
var comparer    = require('./comparer');
var util        = require('./utility');
var instance    = require('./instance');
var query       = require('./query');
var sync        = require('./sync');
var Promise     = require('bluebird');

function Lussandra() {
  this._connected = false;
  this._errorState = false;
  this._connectionOptions = null;
  this._connection = null;
};

Lussandra.prototype.createConnection = function(options) {
  this.options = options;
  this._connectionOptions = this.options;//fetchSize:
  this._connectionOptions.cassandra || (this._connectionOptions.cassandra = {});
  this._connectionOptions.cassandra.fetchSize || (this._connectionOptions.cassandra.fetchSize = 5000);
  return this.connect()
}

Lussandra.prototype.model = function(schema, opts) {
  if(!schema)
    throw "No Schema Provided";

  var toRet = modelObj.createModel(schema, this, opts);
  return toRet;
};

Lussandra.prototype.sync = function (Instance) {
    var model = new Instance();
    var describeTablesQuery = "select * from system_schema.columns where keyspace_name=? and table_name=?;";
    var describeTablesArguments = [this._connectionOptions.keyspace.toLowerCase(), model._internalState.m_tableName];
    var self = this;
    this._connection.execute(describeTablesQuery, describeTablesArguments)
    .then(function(result) {
        // The table doesn't exist, so we need to create it
        if (result.rows.length === 0) {
            sync.createTable(model, self);
        } else {
            sync.updateTable(model, self);
        }
    });
};

Lussandra.prototype.rawQuery = function(sql,params, done) {
  if(!this._connection)
    throw "Must call cassandra.createConnection(options)";
  var queryObj = new query({},{});
  queryObj.rawQuery(sql, params, this._connection, done);
};

Lussandra.prototype.connect  = function() {
  var self = this;

  return new Promise (function (resolve, reject) {
    if(!self._connectionOptions) {
        return reject(new Error("Must specify cassandra connection options, please call cassandra.createConnection(options)"));
    }
    // Check to see if the keyspace exists first
    //
    var client = new Client ({ contactPoints : self._connectionOptions.contactPoints });
    sync.checkKeyspaceExists (client, self._connectionOptions.keyspace, self._connectionOptions)
    .then (function () {
      self._connection = new Client (self._connectionOptions);
      self._connection.connect(function(err, result) {
        if(err) {
          self._errorState = true;
          reject (err);
        } else {
          self._connected = true;
        }
        resolve (self._connection);
      });
    });
  });
};

Lussandra.prototype.connection  = function() {
  if(!this._connection) {
    this.connect();
  }
  return this._connection;
}

Object.defineProperty(Lussandra.prototype, "instance", {
  get: function myProperty() {
    return instance;
  }
});

Object.defineProperty(Lussandra.prototype, "types", {
  get: function myProperty() {
    return types;
  }
});

Object.defineProperty(Lussandra.prototype, "comparer", {
  get: function myProperty() {
    return comparer;
  }
});

Object.defineProperty(Lussandra.prototype, "util", {
  get: function myProperty() {
    return util;
  }
});


Object.defineProperty(Lussandra.prototype, "client", {
  get: function myProperty() {
    return this.connection();
  }
});

module.exports = new Lussandra;
