'use strict';

var cassandra   = require('cassandra-driver');
var Client      = require('cassandra-prom')(cassandra.Client);
var types       = require('./types');
var Model       = require ('./model');
var modelObj    = new Model();
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

    var self = this;

    var describeTablesArguments = [self._connectionOptions.keyspace.toLowerCase(), model._internalState.m_tableName];
    self._connection.execute(self.describeTablesQuery, describeTablesArguments)
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
    var strippedOptions = { contactPoints : self._connectionOptions.contactPoints };
    if (self._connectionOptions.username != null && self._connectionOptions.password != null) {
      strippedOptions.authProvider = new cassandra.auth.PlainTextAuthProvider(self._connectionOptions.username, self._connectionOptions.password);
    }

    var client = new Client (strippedOptions);
    sync.checkKeyspaceExists (client, self._connectionOptions.keyspace, self._connectionOptions)
    .then (function () {
      var connectionOptions = self._connectionOptions;

      if (self._connectionOptions.username != null && self._connectionOptions.password != null) {
        connectionOptions.authProvider = new cassandra.auth.PlainTextAuthProvider(self._connectionOptions.username, self._connectionOptions.password);
      }

      self._connection = new Client (self._connectionOptions);
      self._connection.connect(function(err, result) {
        if(err) {
          self._errorState = true;
          reject (err);
        } else {
          self._connected = true;

          var describeVersion = 'select release_version from system.local;';
          self._connection.execute(describeVersion, [])
          .then(function(result) {
            var version = result.rows[0].release_version.replace('-SNAPSHOT', '').split('.');
            self.describeTablesQuery = "select * from system_schema.columns where keyspace_name=? and table_name=?;";
            self.version = version[0];
            if (version[0] === '2') {
              self.describeTablesQuery = "SELECT * FROM system.schema_columnfamilies WHERE keyspace_name=? AND columnfamily_name =?";
            }
            resolve (self._connection);
          });
        }
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
