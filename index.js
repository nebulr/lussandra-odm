'use strict';

var cassandraOdm   = require('./lib/cassandra-odm');
var cassandra = null;

exports.init = function (config) {
  cassandra = cassandraOdm.createConnection(config);
  return cassandra;
};

exports.client = cassandra;
