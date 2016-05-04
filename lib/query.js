var utility     = require ('./utility');
var _           = require ('lodash');
var comparers   = require ('./comparer');
var types       = require ('./types');
var states      = require ('./states');
var Promise     = require ('bluebird');
var uuid        = require ('node-uuid');

module.exports = function(instance, opts) {
  var m_opts = opts;

  function getInsertParameters(obj, schema) {
    var keys = [];
    var values = [];
    for (var property in schema) {
      if (typeof obj[property] !== 'undefined') {
        var schemaItem = schema[property];
        if(schemaItem.type === types.LIST || schemaItem.type === types.MAP) {
          var container = obj._doc[property];
          if(container.itemCount && container.itemCount > 0) {
            keys.push(property);
            values.push(_writeCqlType(container, schemaItem, property));
          }
        }
        else if(schemaItem.type === types.COUNTER) {
          throw "Cannot insert counters, use update!";
        }
        else {
          keys.push(property);
          values.push(_writeCqlType(obj._doc[property], schemaItem, property));
        }
      }
    }
    return {
      keys: keys.join(','),
      values: values
    };
  }

  function getSelectParameters(obj, schema, criteria) {
    var keys = [];
    var values = [];
    // TODO : In this area we need to determine the equals, name and value of the attribute we're going to check against.
    //        there could be some more additional functionality like greater than or less than or like etc.
    //        might need to do some research into the mongo query functionality to see if there is something I Can
    //        take from that to implement here. Would be easier and add more functionality to cassandra odm.

    for (var key in criteria) {
      if (typeof criteria[key] === 'object') {
        // More complicated set of instructions
        // should handle greater than, greater than equal, less than, less than equal, in
      } else {
        var property = { name : key, value : criteria[key], comparer : comparers.EQUALS };

        writeSelectKey (property, schema, keys, values);
      }
    }

    return {
      keys: keys.join(' '),
      values: values
    };
  }

  function checkValid(obj) {
    var val = obj._validate();
    if(!val)
      throw "Could not validate!";
    else {
      if(!val.isValid) {
        return false;
      }
      else {
        return true;
      }
    }
  }

  function getUpdateParameters(obj, schema) {
    var keys = [];
    var values = [];

    for (var i = 0, len = obj._internalState.m_dirtyFields.length; i < len; i++) {
      var property = obj._internalState.m_dirtyFields[i];
      if(schema[property]) {
        writeUpdateKey(obj, property, schema, keys, values);
      }
    }

    return {
      keys: keys.join(' '),
      values: values
    };
  }

  function writeUpdateKey(obj, property, schema, keys, values) {
    var key = false;
    if (keys.length > 0)
      keys.push(', ');

    var value = obj[property];
    var schemaValue = schema[property];
    if (!schemaValue)
      return;
    if (schemaValue.type === types.COUNTER) {
      var key = property + "=" + _writeCqlType(value,schemaValue,property);
      keys.push(key);
    } else {
      if (keys.indexOf(property  + "=?") < 0) {
        keys.push(property + "=?");
        values.push(_writeCqlType(value, schemaValue, property));
      } else {
        keys.splice(keys.length -1,1);
      }
    }
  }

  function writeSelectKey (property, schema, keys, values) {
    if (keys.length > 0)
      keys.push(' AND ');

    var comparer = property.comparer;
    var value = property.value;
    var prop = property.name;

    if (!schema[prop])
      return;
    if (!comparer)
      comparer = comparers.EQUALS;
    if (typeof property.value === 'undefined')
      throw "Value not specified";
    if (comparer === comparers.IN) {
      keys.push(prop + comparer.toCQLString(value.length));
      for (var i = 0, len = value.length; i < len; i++) {
        values.push(_writeCqlType(value[i], schema[prop], property));
      }
    } else {
      keys.push(prop + comparer.toCQLString() + "?");
      values.push(_writeCqlType(value, schema[prop], property));
    }

  }

  function _writeCqlType (val, schema, property) {
    if (!val && schema.default) {
      return schema.type.toCQLString(schema.default, schema, property);
    } else  {
      return schema.type.toCQLString(val, schema, property);
    }
  }

  function update (instance, updateParams) {
    var self = this;
    self.instance = instance;
    self.updateParams = updateParams;

    return new Promise (function (resolve, reject) {

        self.instance._setState(states.UPDATE);
        self.instance._runPre();

        var query = {};
        if (self.instance._internalState.m_dirtyFields.length === 0){
            return resolve(self.instance);
        }
        var errors = buildAndValidateQueryObj(self.params, query, self.instance);
        if(errors) {
            return reject(errors);
        }

        if (!checkValid(self.instance)) {
            return reject("Object is not valid!", null);
        }

        var sql = 'update [[table_name]] set [[keys]] where [[updateKeys]];';

        var params = getUpdateParameters(self.instance, self.instance._schema);
        var allParams = params.values;
        sql = sql.replace('[[table_name]]', query.tableName);
        sql = sql.replace('[[keys]]', params.keys);

        try {
            var updateKeys = getSelectParameters(self.instance, self.instance._schema, self.updateParams);
        } catch (err) {
            return reject(new Error(err));
        }

        allParams = allParams.concat(updateKeys.values);
        sql = sql.replace('[[updateKeys]]', updateKeys.keys);

        return self.instance._client.execute(sql, allParams, query.options)
        .then(function(result) {
            self.instance._markClean();
            self.instance._runPost();
            return resolve(self.instance);
        })
        .catch(reject);
     });
  }

  function insert (instance, params) {
      var self = this;
      self.instance = instance;
      self.params = params;
      return new Promise (function( resolve, reject ) {
        var query = {};

        self.instance._setState(states.NEW);

        self.instance._runPre();

        if (self.instance.id == null) {
            self.instance.id = uuid.v4();
        }

        var errors = buildAndValidateQueryObj(self.params, query, self.instance);

        if(errors) {
            return reject(errors);
        }

        if(!checkValid(self.instance)) {
            return reject("Object is not valid!", null)
        }

        var sql = 'insert into [[table_name]] ([[keys]]) VALUES([[values]]);';
        self.params = getInsertParameters(self.instance, self.instance._schema);
        sql = sql.replace('[[table_name]]', query.tableName);
        sql = sql.replace('[[keys]]', self.params.keys);

        var emptyArray = new Array(self.params.values.length);
        sql = sql.replace('[[values]]', emptyArray.join("?,")+"?");

        return instance._client.execute(sql, self.params.values, query.options)
        .then (function(data) {
            self.instance._markClean();
            self.instance._runPost();
            return resolve(self.instance);
        })
        .catch(reject);
     });
  }

  function buildAndValidateQueryObj(params, query, instance) {
    if(!query)
      throw "Query not provided.";
    params ||  (params = []); //should this throw?
    query.options || (query.options = {});
    query.options.pageState || (query.options.pageState = null);
    //query.options.fetchSize || (query.options.fetchSize = getOption(instance, "fetchSize"));
    query.options.prepare = true;
    query.tableName || (query.tableName = instance._options.tableName)

    return null;
  }

  function findAndPage(instance, params, cb, done) {
    var query = {};
    var errors = buildAndValidateQueryObj(params, query, instance);
    if(errors) {
      cb(errors);
      return;
    }

    instance._runPre();

    var sql = 'select * from [[table_name]] where [[keys]]';

    var params = getSelectParameters(instance, instance._schema, params);

    sql = sql.replace('[[table_name]]', query.tableName);
    sql = sql.replace('[[keys]]', params.keys);

    instance._client.eachRow(sql, params.values, query.options, function (n, row) {
        if (utility.isFunction(cb)) {
          var model = createModel(instance, row, {}, states.UPDATE);
          model._markClean();
          model._runPost();
          cb(null, model, row);
        }
      }, function (err, result) {
        if (err) {
          if (utility.isFunction(cb))
            cb(err, null);
        }
       var pageState = result.pageState;
        if (utility.isFunction(done))
          done(err, result, pageState);
      });
  }

  function find (instance, params, criteria) {
    var self = this;
    self.instance = instance;
    self.params = params;
    self.criteria = criteria;

    return new Promise (function (resolve, reject) {
        var query = {};
        var errors = buildAndValidateQueryObj(self.params, query, self.instance);
        if(errors) {
            return reject(errors);
        }
        self.instance._setState(states.UNKNOWN);
        self.instance._runPre();

        if (!checkValid(self.instance)) {
            return reject("Object is not valid!", null)
        }

        var sql = 'select * from [[table_name]] where [[keys]];';

        if(self.criteria.hasOwnProperty('limit')) {
          sql.replace (';', ' limit ' + self.criteria.limit + ';');
        }

        if(self.criteria.hasOwnProperty('fields')) {
          sql.replace ('*', self.criteria.fields.join(','));
        }

        try {
            var params = getSelectParameters(self.instance, self.instance._schema, self.params);
        } catch (err) {
            return reject (new Error(err));
        }

        if (params.values.length === 0) {
          sql = 'select * from [[table_name]];'
        } else {
          sql = sql.replace('[[keys]]', params.keys);
        }

        sql = sql.replace('[[table_name]]', query.tableName);

        return self.instance._client.execute(sql, self.params, query.options)
        .then(function (results) {
            var objArray = [];
            _.each(results.rows, function(row) {
                var model = createModel(self.instance, row, {}, states.UPDATE);
                model._markClean();
                model._runPost();
                objArray.push(model);
            });
            return resolve(objArray, results);
        })
        .catch(reject);
     });
  }

  function findOne (instance, params) {
      return new Promise (function (resolve, reject) {
         find (instance, params, { limit : 1 })
         .then (function (results) {
             return resolve((results.length > 0) ? results[0] : {});
         })
         .catch(reject);
      });
  }

  function findLucene (instance, params, criteria) {
    return new Promise (function (resolve, reject) {
      find (instance, { lucene : JSON.stringify(params) }, criteria)
      .then (resolve).catch (reject);
    });
  }

  function remove(instance, params) {
    var self = this;
    self.instance = instance;
    self.params = params;

    return new Promise (function( resolve, reject ) {
      var query = {};
      var errors = buildAndValidateQueryObj(self.params, query, self.instance);

      if(errors) {
        return reject(errors);
      }

      self.instance._runPre();

      if (!checkValid(self.instance)) {
          return reject("Object is not valid!", null);
      }

      var sql = 'delete from [[table_name]] where [[keys]];';
      try {
        var params = getSelectParameters(self.instance, self.instance._schema, self.params);
      } catch (err) {
        return reject (new Error(err));
      }

      sql = sql.replace('[[table_name]]', query.tableName);
      sql = sql.replace('[[keys]]', params.keys);

      self.instance._client.execute(sql, params.values, query.options)
      .then(function (results) {
        return resolve();
      })
      .catch(reject);
    });
  }

  function getOption(instance, option) {
    return instance._internalState.m_cqlify._connectionOptions.cqlify[option];
  }

  function createModel(instance, obj) {
    var modelObj = require('./model');
    var toRet = modelObj.hydrateModel(instance._schema, instance._internalState.m_cqlify, instance._options, obj);
    return toRet;
  }

  function rawQuery(sql,params,client) {
    if(sql.constructor === Array) {
      return client.batch(sql, {prepare: true});
    } else {
      return client.execute(sql, params, {prepare: true});
    }
  }

  return {
    insert: insert,
    find: find,
    findOne: findOne,
    findLucene: findLucene,
    update: update,
    remove: remove,
    rawQuery: rawQuery,
    findAndPage: findAndPage
  }
};
