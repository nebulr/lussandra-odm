'use strict';

var lussandraOdm  = require('./lib/lussandra-odm');
var Promise       = require ('bluebird')
var client     = null;

exports.init = function (config) {
  var self = this;
  return new Promise (function (resolve, reject) {
    lussandraOdm.createConnection(config).then (function (connection) {
      self.client = lussandraOdm;
      resolve (connection);
    }).catch(reject);
  });
};

exports.client = client;
