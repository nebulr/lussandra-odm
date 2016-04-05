'use strict';

var _       = require ('lodash');
var async   = require ('async');
var Promise = require ('bluebird');

/**
 * checkKeyspaceExists - check to see if the keyspace exists if it doesn't then create it
 * @param  {[type]}   client      [description]
 * @param  {[type]}   keyspace    [description]
 * @param  {[type]}   options     [description]
 * @param  {[type]}   execOptions [description]
 * @param  {Function} callback    [description]
 * @return {[type]}               [description]
 */
exports.checkKeyspaceExists = function (cql, keyspace, options) {

  return new Promise (function (resolve, reject) {

    //TODO: Write Replication Strategy Notes in Readme / documentation
    //Notes on Replication Strategy and Topology:

    //Default is Simple strategy with a default replication_factor of 1

    //Notes on NetworkTopologyStrategy:
    //RackInferringSnitch or PropertyFileSnitch - configured in cassandra.yml (located )

    // Notes on strategy_options (from http://www.datastax.com/docs/1.0/configuration/storage_configuration#strategy-options)
    //
    // Specifies configuration options for the chosen replication strategy.
    //
    // For SimpleStrategy, it specifies replication_factor in the format of replication_factor:number_of_replicas.
    //
    // For NetworkTopologyStrategy, it specifies the number of replicas per data center in a comma separated list of datacenter_name:number_of_replicas. Note that what you specify for datacenter_name depends on the cluster-configured snitch you are using. There is a correlation between the data center name defined in the keyspace strategy_options and the data center name as recognized by the snitch you are using. The nodetool ring command prints out data center names and rack locations of your nodes if you are not sure what they are.
    //
    // See Choosing Keyspace Replication Options for guidance on how to best configure replication strategy and strategy options for your cluster.
    //
    // Setting and updating strategy options with the Cassandra CLI requires a slightly different command syntax than other attributes; note the brackets and curly braces in this example:
    //
    // CREATE KEYSPACE test
    // WITH placement_strategy = 'NetworkTopologyStrategy'
    // AND strategy_options={us-east:6,us-west:3};

    //NetworkTopologyStrategy using RackInferringSnitch for production default - assumes 10.ddd.rrr.nnn IP network (default of 10.0.0.0/16 vpc network when setting up EC2 VPC in a region, 10.0.x.0/24 for EC2 VPC subnet in a region), replication of 3 - can give property object containing name:DC1, name:DC2, etc.
    //NetworkTopologyStrategy using PropertyFileSnitch if topology is specified in JSON (ie, pass in JSON to use) - NOT IMPLEMENTED YET

    //NetworkTopologyStrategy requires that strategy_options is specified (and ignores replication_factor)

    var replicationStrategy = "";
    if (options.replication) {
        if (options.replication.strategy === 'SimpleStrategy') {
            if (options.replication.replication_factor) {
                // replicationStrategy = "placement_strategy = 'SimpleStrategy' AND strategy_options:replication_factor = " options.replication.replication_factor.toString();

                replicationStrategy = "{ 'class' : 'SimpleStrategy', 'replication_factor': " + options.replication.replication_factor.toString() + "}";
            } else {
                // replicationStrategy = "placement_strategy = 'SimpleStrategy' AND strategy_options:replication_factor = 1";

                replicationStrategy = "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";
            }
        } else if (options.replication.strategy === 'NetworkTopologyStrategy') {
            if (options.replication.strategy_options) {
                replicationStrategy = {'class': 'NetworkTopologyStrategy'};
                for (var key in options.replication.strategy_options) {
                    replicationStrategy[key] = options.replication.strategy_options[key];
                }
                replicationStrategy = JSON.stringify(replicationStrategy);
            } else {
                throw "Replication Strategy 'NetworkTopologyStrategy' requires strategy_options object /dictionary containing <datacenter_name>:<replication_number> fields. See Cassandra docs on CREATE KEYSPACE / Keyspace Replication Options for more information.";
            }
        } else {
            throw "Config.options.strategy must be either 'SimpleStrategy' (recommended for development) or 'NetworkTopologyStrategy' (recommended for production). See Cassandra docs for CREATE KEYSPACE / Keyspace Replication Options more information.";
        }

    } else {
        replicationStrategy = "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";
        // replicationStrategy = "placement_strategy = 'SimpleStrategy' AND strategy_options:replication_factor = 1";
    }

    var queryString = "CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH REPLICATION = " + replicationStrategy;

    cql._connection.execute(createTableQuery)
    .then(function(results) {
        client.shutdown(function() {
            resolve(results);
        });
    })
    .catch (reject);
  });
};

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
