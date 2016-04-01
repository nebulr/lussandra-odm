var Instance = require('./instance');
var _ = require('lodash');
var states = require('./states');
var ChildContainer = require('./child_container');
var types = require('./types');

var Model = function () {};

Model.prototype.hydrateModel = function (schema, cassandra, opts, fromObject, state) {
  opts || (opts = {});
  opts.fromObject = fromObject;
  var NewModel = this.createModel(schema, cassandra, opts, states.UPDATE);
  var model = new NewModel();

  return model;
};

Model.prototype.createChildModel = function(schema, parent, parentSchema) {
  var childModel = new ChildContainer(schema, states.NEW, this,parent, parentSchema);
  return childModel;
};

Model.prototype.createModel = function(schema, cassandra, opts, state) {
  var modelObj = this;
  var instanceObj = new Instance();
  opts || (opts = {});
  var newInstance = instanceObj._compile(schema, cassandra, opts, state || states.NEW, modelObj);
  return newInstance;
};

Model.prototype.toObject = function(prop, schema) {
  if(schema.type.toObject) {
    return schema.type.toObject(prop, schema.type, schema);
  }
  else
    return prop;
};


module.exports = new Model();