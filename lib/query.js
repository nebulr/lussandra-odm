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
      } else if (schema[property].default != null) {
        keys.push(property);
        values.push(_writeCqlType(schema[property].default, schemaItem, property));
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
      // More complicated set of instructions
      // should handle greater than, greater than equal, less than, less than equal, in
      var property = null;

      if (typeof schema[key] === 'object' && schema[key].type.type_name !== 'JSON to Text'
          && criteria[key].hasOwnProperty('value') && criteria[key].hasOwnProperty('comparer')) {
        property = { name : key, value : schema[key].type.toCQLString(criteria[key].value), comparer : criteria[key].comparer };
      } else  {
        // Default equals
        property = { name : key, value : schema[key].type.toCQLString(criteria[key]), comparer : comparers.EQUALS };
      }

      writeSelectKey (property, schema, keys, values);
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
      value = value.split(',');
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
    return schema.type.toCQLString(val, schema, property);
  }

  function update (instance, updateParams) {
    var self = this;
    self.updateParams = updateParams;
    return new Promise (function (resolve, reject) {

        instance._setState(states.UPDATE);
        instance._runPre();

        var query = {};
        if (instance._internalState.m_dirtyFields.length === 0){
            return resolve(instance);
        }
        var errors = buildAndValidateQueryObj(self.updateParams, query, instance);
        if(errors) {
            return reject(errors);
        }

        if (!checkValid(instance)) {
            return reject("Object is not valid!", null);
        }

        var sql = 'update [[table_name]] set [[keys]] where [[updateKeys]];';

        var params = getUpdateParameters(instance, instance._schema);
        var allParams = params.values;
        sql = sql.replace('[[table_name]]', query.tableName);
        sql = sql.replace('[[keys]]', params.keys);

        try {
            var updateKeys = getSelectParameters(instance, instance._schema, self.updateParams);
        } catch (err) {
            return reject(new Error(err));
        }

        allParams = allParams.concat(updateKeys.values);
        sql = sql.replace('[[updateKeys]]', updateKeys.keys);

        return instance._client.execute(sql, allParams, query.options)
        .then(function(result) {
            instance._markClean();
            instance._runPost();
            return resolve(instance);
        })
        .catch(reject);
     });
  }

  function insert (instance, params) {
      var self = this;
      self.params = params;
      return new Promise (function( resolve, reject ) {
        var query = {};

        instance._setState(states.NEW);

        instance._runPre();

        if (instance._schema.hasOwnProperty('id') && instance.id == null) {
            instance.id = uuid.v4();
        }

        var errors = buildAndValidateQueryObj(self.params, query, instance);

        if(errors) {
            return reject(errors);
        }

        if(!checkValid(instance)) {
            return reject("Object is not valid!", null)
        }

        var sql = 'insert into [[table_name]] ([[keys]]) VALUES([[values]]);';
        var params = getInsertParameters(instance, instance._schema);
        sql = sql.replace('[[table_name]]', query.tableName);
        sql = sql.replace('[[keys]]', params.keys);

        var emptyArray = new Array(params.values.length);
        sql = sql.replace('[[values]]', emptyArray.join("?,")+"?");

        return instance._client.execute(sql, params.values, query.options)
        .then (function(data) {
            instance._markClean();
            instance._runPost();
            return resolve(instance);
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
          var model = createModel(instance, row);
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

  function findDetailed (instance, params, criteria) {
    var self = this;
    self.criteria = criteria;
    self.params = params;
    return new Promise (function (resolve, reject) {
        var query = {};
        var raw = false;
        if (self.criteria.raw) {
          raw = self.criteria.raw;
          delete self.criteria.raw;
        }

        var errors = buildAndValidateQueryObj(self.params, query, instance);
        if(errors) {
            return reject(errors);
        }
        instance._setState(states.UNKNOWN);
        instance._runPre();

        if (!checkValid(instance)) {
            return reject("Object is not valid!", null)
        }

        var sql = 'select * from [[table_name]]';

        if(self.criteria.limit != null) {
          sql = sql.replace (';', ' limit ' + self.criteria.limit + ';');
        }
        // Aggregate is priority
        if(self.criteria.aggregate != null) {
          if (self.criteria.aggregate === 'count') {
            sql = sql.replace ('*', 'count(*)');
          } else if (self.criteria.aggregate === 'sum') {
            sql = sql.replace ('*', 'sum(*)');
          }
        // Omit is secondary
        } else if (self.criteria.omit != null && self.criteria.omit.length > 0) {
          // Turn the array of omitted fields into a hashmap
          var omissions = {};
          for (var i = 0; i < self.criteria.omit.length; i++) {
            omissions[self.criteria.omit[i]] = true;
          }

          // Get all the columns and skip the ones in the omission list
          var fields = [];
          for (var key in instance._internalState.m_schema) {
            if (!omissions[key]) {
              fields.push (key);
            }
          }
          sql = sql.replace ('*', fields.join());
        // Fields takes last
        } else if (self.criteria.fields != null && self.criteria.fields.length > 0) {
          sql = sql.replace ('*', self.criteria.fields.join());
        }

        if (self.criteria.pageState) {
            query.options.pageState = self.criteria.pageState;
        }

        if (self.criteria.fetchSize) {
          query.options.fetchSize = self.criteria.fetchSize;
        }

        try {
            var params = getSelectParameters(instance, instance._schema, self.params);
        } catch (err) {
            return reject (new Error(err));
        }

        if (params.values.length === 0) {
          sql += ';';
        } else {
          sql += ' where ' + params.keys + ' allow filtering;'
        }

        sql = sql.replace('[[table_name]]', query.tableName);
        return instance._client.execute(sql, params.values, query.options)
        .then(function (results) {

            if (!raw) {
              var objArray = [];
              _.each(results.rows, function(row) {
                  var model = createModel(instance, row);
                  model._markClean();
                  model._runPost();
                  objArray.push(model);
              });
              results.rows = objArray;
            }

            return resolve(results);
        })
        .catch(reject);
     });
  }

  function find (instance, params, criteria) {
    return new Promise (function (resolve, reject) {
       findDetailed (instance, params, criteria)
       .then (function (results) {
           return resolve(results.rows);
       })
       .catch(reject);
    });
  };

  function findOne (instance, params, criteria) {
    criteria.limit = 1;
    return new Promise (function (resolve, reject) {
       findDetailed (instance, params, criteria)
       .then (function (results) {
           return resolve((results.rows.length > 0) ? results.rows[0] : {});
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
    self.params = params;
    return new Promise (function( resolve, reject ) {
      var query = {};
      var errors = buildAndValidateQueryObj(self.params, query, instance);

      if(errors) {
        return reject(errors);
      }

      instance._runPre();

      if (!checkValid(instance)) {
          return reject("Object is not valid!", null);
      }

      var sql = 'delete from [[table_name]] where [[keys]];';
      try {
        var params = getSelectParameters(instance, instance._schema, self.params);
      } catch (err) {
        return reject (new Error(err));
      }

      sql = sql.replace('[[table_name]]', query.tableName);
      sql = sql.replace('[[keys]]', params.keys);

      instance._client.execute(sql, params.values, query.options)
      .then(function (results) {
        return resolve();
      })
      .catch(reject);
    });
  }

  function getOption(instance, option) {
    return instance._internalState.m_lussandra._connectionOptions.lussandra[option];
  }

  function createModel(instance, obj) {
    var Model    = require ('./model');
    var modelObj = new Model();
    var toRet = modelObj.hydrateModel(instance._schema, instance._internalState.m_lussandra, instance._options, obj);
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
    findDetailed: findDetailed,
    find: find,
    findOne: findOne,
    findLucene: findLucene,
    update: update,
    remove: remove,
    rawQuery: rawQuery,
    findAndPage: findAndPage
  }
};
