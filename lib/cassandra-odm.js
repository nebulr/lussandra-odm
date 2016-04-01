var Client      = require('cassandra-prom')(require('cassandra-driver').Client);
var types       = require('./types');
var modelObj    = require('./model');
var comparer    = require('./comparer');
var util        = require('./utility');
var instance    = require('./instance');
var query       = require('./query');
var sync        = require('./sync');

function cassandra() {
  this._connected = false;
  this._errorState = false;
  this._connectionOptions = null;
  this._connection = null;
};

cassandra.prototype.createConnection = function(options, done) {
  this._connectionOptions = options;//fetchSize:
  this._connectionOptions.cassandra || (this._connectionOptions.cassandra = {});
  this._connectionOptions.cassandra.fetchSize || (this._connectionOptions.cassandra.fetchSize = 5000);
  this.connect(done);
}

cassandra.prototype.model = function(schema, opts) {
  if(!schema)
    throw "No Schema Provided";

  var toRet = modelObj.createModel(schema, this, opts);
  return toRet;
};

cassandra.prototype.sync = function (Instance) {
    var model = new Instance();
    var describeTablesQuery = "SELECT * FROM system.schema_columnfamilies WHERE keyspace_name=? AND columnfamily_name =?";
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

cassandra.prototype.rawQuery = function(sql,params, done) {
  if(!this._connection)
    throw "Must call cassandra.createConnection(options)";
  var queryObj = new query({},{});
  queryObj.rawQuery(sql, params, this._connection, done);
};

cassandra.prototype.connect  = function(done) {
  var self = this;
  if(!this._connectionOptions) {
    if (done) {
      done(new Error("Must specify cassandra connection options, please call cassandra.createConnection(options)"));
      return;
    }
    else {
      throw(new Error("Must specify cassandra connection options, please call cassandra.createConnection(options)"));
    }
  }

  this._connection = new Client(this._connectionOptions);
  this._connection.connect(function(err, result) {
    if(err) {
      self._errorState = true;
    }
    else {
      self._connected = true;
    }
    if ( done ) {
      done(err);
    }
  });
};

cassandra.prototype.connection  = function() {
  if(!this._connection) {
    this.connect();
  }
  return this._connection;
}

Object.defineProperty(cassandra.prototype, "instance", {
  get: function myProperty() {
    return instance;
  }
});

Object.defineProperty(cassandra.prototype, "types", {
  get: function myProperty() {
    return types;
  }
});

Object.defineProperty(cassandra.prototype, "comparer", {
  get: function myProperty() {
    return comparer;
  }
});

Object.defineProperty(cassandra.prototype, "util", {
  get: function myProperty() {
    return util;
  }
});


Object.defineProperty(cassandra.prototype, "client", {
  get: function myProperty() {
    return this.connection();
  }
});

module.exports = new cassandra;
