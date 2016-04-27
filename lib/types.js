var moment = require('moment');
var validator = require('./validator');
var util = require('./utility');
var cassandra = require('cassandra-driver');

function Types(){


}

Types.prototype.getTimeUuid = function() {
  return cassandra.types.TimeUuid;
}

Types.prototype.TIMEUUID = function() {
  var validators = [
    validator.isTimeUuid
  ];

  function validate(obj, userValidators) {
    var vArr = validators.concat(userValidators);
    var result = util.validateAll(obj, vArr);
    return result;
  }
  function toCQLString(obj) {
    return obj;
  }
  return {
    toCQLString: toCQLString,
    validate: validate,
    type_name:'Time UUID',
    cql_name: 'timeuuid'
  };
}();

Types.prototype.COUNTER = function() {
  var validators = [

  ];
  function validate(obj, userValidators) {
    var vArr = validators.concat(userValidators);
    var result = util.validateAll(obj, vArr);
    return result;
  }
  function toObject(obj) {
    if(obj && obj.toInt) {
      return obj.toInt();
    }
    return obj;
  }
  function toCQLString(obj, schema, prop) {
    if(typeof obj == 'string') {
     if(obj[0] === '-' || obj[0] === '+') {
       var prefix = obj[0];
       var val =  prop +' '+ obj;
       return val;
      }else {
       throw "Counter must be a string type in the format '+N' or '-N' ";
     }
    }
    else {
      throw "Counter must be a string type in the format '+N' or '-N' ";
    }
  }
  return {
    toCQLString: toCQLString,
    validate: validate,
    toObject:toObject,
    type_name:'Counter',
    cql_name: 'counter'
  };
}();

Types.prototype.TEXT = function() {
  var validators = [
    validator.isString
  ];
  function toCQLString(obj) {
    return obj;
  }
  function validate(obj, userValidators) {
    var vArr = validators.concat(userValidators);
    var result = util.validateAll(obj, vArr);
    return result;
  }
  return {
    toCQLString: toCQLString,
    validate: validate,
    type_name:'Text',
    cql_name: 'text'
  };
}();

Types.prototype.FLOAT = function () {
  var validators = [
  validator.isFloat
  ];
  function toCQLString(obj) {
    return obj;
  }
  function validate(obj, userValidators) {
    var vArr = validators.concat(userValidators);
    var result = util.validateAll(obj, vArr);
    return result;
  }
  return {
    toCQLString: toCQLString,
    validate: validate,
    type_name:'Float',
    cql_name: 'float'
  };
}();

Types.prototype.BLOB = function () {
  var validators = [
    validator.isObject
  ];
  function toCQLString (obj) {
    return obj;
  }
  function validate(obj, userValidators) {
    var vArr = validators.concat(userValidators);
    var result = util.validateAll(obj, vArr);
    return result;
  }
  return {
    toCQLString: toCQLString,
    validate: validate,
    type_name:'Blob',
    cql_name: 'blob'
  };
}();

Types.prototype.TIMESTAMP = function() {
  var validators = [
    validator.isDate
  ];

  function toCQLString(obj, insert) {
    return moment(obj).utc().format(); // This clones the underlying moment object, puts it in utc mode, and the returns date as UTC string
  }
  function toObject(obj) {
    return moment.utc(obj);
  }
  function validate(obj, userValidators) {
    var vArr = validators.concat(userValidators);
    var result = util.validateAll(obj, vArr);
    return result;
  }
  return {
    toCQLString: toCQLString,
    validate: validate,
    toObject: toObject,
    type_name:'Time Stamp',
    cql_name: 'timestamp'
  };
}();

Types.prototype.INT = function() {
  var validators = [
    validator.isInt32
  ];
  function toCQLString(obj) {
    return obj;
  }
  function validate(obj, userValidators) {
    var vArr = validators.concat(userValidators);
    var result = util.validateAll(obj, vArr);
    return result;
  }
  return {
    toCQLString: toCQLString,
    validate: validate,
    type_name:'Integer32',
    cql_name: 'int'
  };
}();

Types.prototype.BOOLEAN = function() {
  var validators = [
    validator.isBoolean
  ];

  function toCQLString(obj) {
    return obj;
  }


  function validate(obj, userValidators) {
    var vArr = validators.concat(userValidators);
    var result = util.validateAll(obj, vArr);
    return result;
  }
  return {
    toCQLString: toCQLString,
    validate: validate,
    type_name:'Boolean',
    cql_name: 'boolean'
  };
}();

Types.prototype.BIGINT = function() {
  var validators = [
    validator.isInt64
  ];

  function toCQLString(obj) {
    return obj;
  }
  function validate(obj, userValidators) {
    var vArr = validators.concat(userValidators);
    var result = util.validateAll(obj, vArr);
    return result;
  }
  return {
    toCQLString: toCQLString,
    validate: validate,
    type_name:'Big Int',
    cql_name: 'bigint'
  };
}();

Types.prototype.UUID = function() {
  var validators = [

  ];

  function toCQLString(obj) {
    return obj.toString();
  }
  function validate(obj, userValidators) {
    var vArr = validators.concat(userValidators);
    var result = util.validateAll(obj, vArr);
    return result;
  }
  return {
    toCQLString: toCQLString,
    validate: validate,
    type_name:'UUID',
    cql_name: 'uuid'
  };
}();

Types.prototype.JSONTOTEXT = function() {
  var validators = [

  ];

  function toCQLString(obj) {
    return JSON.stringify(obj);
  }
  function validate(obj, userValidators) {
    var vArr = validators.concat(userValidators);
    var result = util.validateAll(obj, vArr);
    return result;
  }
  function toObject(obj) {
    try {
      if(typeof obj === 'string') {
        return JSON.parse(obj);
      }
      else return obj;
    }
    catch(err){
      throw "Failed to convert to json: "+ err;
    }
  }
  return {
    toCQLString: toCQLString,
    toObject: toObject,
    validate: validate,
    type_name:'JSON to Text',
    cql_name: 'text'
  };
}();

Types.prototype.MAP = function() {
  var validators = [

  ];

  function toCQLString(obj) {
    return obj.toJSON();
  }
  function validate(obj, userValidators) {
    var vArr = validators.concat(userValidators);
    var result = util.validateAll(obj, vArr);
    return result;
  }
  function toObject(obj) {
    try {
      return obj.toObject();
    }
    catch(err){
      throw "Failed to convert to json: "+ err;
    }
  }
  return {
    toCQLString: toCQLString,
    toObject: toObject,
    validate: validate,
    type_name:'MAP',
    cql_name: 'map'
  };
}();

Types.prototype.LIST= function() {
  var validators = [

  ];

  function toCQLString(obj) {
    return obj.toObject();
  }
  function validate(obj, userValidators) {
    var vArr = validators.concat(userValidators);
    var result = util.validateAll(obj, vArr);
    return result;
  }
  function toObject(obj) {
    try {
      return obj.toObject();
    }
    catch(err){
      throw "Failed to convert to json: "+ err;
    }
  }
  return {
    toCQLString: toCQLString,
    toObject: toObject,
    validate: validate,
    type_name:'List',
    cql_name: 'list'
  };
}();

Types.prototype.ENUMTOTEXT = function() {
  var validators = [

  ];

  function toCQLString(obj) {
    return obj.value;
  }
  function validate(obj, userValidators) {
    var vArr = validators.concat(userValidators);
    var result = util.validateAll(obj, vArr);
    return result;
  }
  function toObject(obj, type, schema) {
    if (typeof obj === 'string') {
      return schema.enumerator.FromValue(obj);
    }
    return obj;
  }
  return {
    toCQLString: toCQLString,
    toObject: toObject,
    validate: validate
  };
}();

Types.prototype.PRIMARY_KEY = function() {
  return {
    value: 1,
    type_name:'Primary Key'
  }
}();

Types.prototype.CLUSTER_KEY = function() {
  return {
    value: 2,
    type_name:'Cluster Key'
  }
}();

Types.prototype.INDEX = function() {
  return {
    value: 3,
    type_name:'Index'
  }
}();
module.exports = new Types();
