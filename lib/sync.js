'use strict';

var _       = require ('lodash');
var async   = require ('async');

/**
 * createTable - creating the table if the schema does not currently exist.
 * @param {Model} - model the model object that contains the schema information related to the table.
 * @param {Cassandra} - cql the connection object that we will use to execute queries to cassandra.
 */
exports.createTable = function (model, cql) {

    var schema = model._internalState.m_schema;
    var createTableQuery = 'CREATE TABLE ' + model._internalState.m_tableName + ' (';
    var primaryKey = [];
    var indices = [];
    var clusterKey = [];
    var clusterOrder = [];
    
    for (var key in schema) {
      if (schema[key].hasOwnProperty('key_type') && schema[key].key_type.type_name === 'Primary Key') {
        primaryKey.push(key);
      } else if (schema[key].hasOwnProperty('key_type') && schema[key].key_type.type_name === 'Index') {
        indices.push(key);
      } else if (schema[key].hasOwnProperty('key_type') && schema[key].key_type.type_name === 'Cluster Key') {
        primaryKey.push(key);
      }
      createTableQuery += key + ' ' + schema[key].type.cql_name + ',';
    }

    if (primaryKey.length === 0) {
        createTableQuery = createTableQuery.slice(0, -1);
    } else {
        createTableQuery += 'PRIMARY KEY (' + primaryKey.join() + ')';
    }
    createTableQuery += ")";
    
    if (clusterKey.length > 0) {
       createTableQuery += 'WITH CLUSTERING ORDER BY (' + clusterKey[0] + ' ' + clusterOrder[0] + ')';
    }
    
    cql._connection.execute(createTableQuery)
    .then(function(result) {
        for (var i = 0; i < indices.length; i++) {
            cql._connection.execute('CREATE INDEX ON ' + model._internalState.m_tableName + ' (' + indices[i] + ')');
        }
    });
};

/**
 * updateTable - updating the table if the schema is inconsistent with the schema in the localfile. Nothing gets deleted
 * @param {Model} - model the model object that contains the schema information related to the table
 * @param {Cassandra} - cql the connection object that we will use to execute queries to cassandra.
 * @return {Null} - does not return anything. It is simply a call function. This is an entirely optional function. 
 */
exports.updateTable = function (model, cql) {

    var describeTableQuery = 'select * from system.schema_columns where keyspace_name=? and columnfamily_name=?';
    var describeTableArguments = [cql._connectionOptions.keyspace.toLowerCase(), model._internalState.m_tableName];

    cql._connection.execute(describeTableQuery, describeTableArguments)
    .then(function(result) {

        var toAdd = {};
        var needToAdd = false;
        var schema = model._internalState.m_schema;
        for (var key in schema) {
            var found = false;
            for (var i = 0; i < result.rows.length; i++) {
                if (result.rows[i].column_name.toLowerCase() === key.toLowerCase()) {
                    found = true;
                    break;
                }
            }
            if(!found) {
                needToAdd = true;
                toAdd[key] = schema[key];
            }
        }

        if (needToAdd) {
            for (var key in toAdd) {
                var alterTableQuery = 'ALTER TABLE ' + model._internalState.m_tableName + ' ADD ' + key + ' ' + toAdd[key].type.cql_name;
                cql._connection.execute(alterTableQuery);
            }
        }
    });
};
