#Object Document Modeling for Lussandra
###Lucene + Cassandra = Lussandra
_____
___Take note this module is in Beta. Use at your own risk___

Instantiate the cassandra client by calling :

`require ('lussandra-odm').init(config);`

Where your config follows the following format :

`var config = {
    contactPoints : [ '127.0.0.1:9042' ],
    keyspace : 'nana',
    replication : { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }
};`

An example of the usage :

`'use strict';

var cassandra   = require ('lussandra-odm').client;
var uuid        = require ('node-uuid');

var UserModel = cassandra.model ({
  email : {
    type : cassandra.types.TEXT,
    key_type : cassandra.types.PRIMARY_KEY,
    key_order : 1
  },
  id : {
    type : cassandra.types.UUID,
    key_type : cassandra.types.INDEX,
    key_order : 2
  },
  password : { type : cassandra.types.TEXT },
  salt : { type : cassandra.types.TEXT },
  account : { type : cassandra.types.TEXT },
  created : { type : cassandra.types.TIMESTAMP },
  latitude : { type : cassandra.types.FLOAT },
  longitude : { type : cassandra.types.FLOAT },
  accessed : { type : cassandra.types.TIMESTAMP },
  settings :  { type : cassandra.types.JSONTOTEXT },
  role : { type : cassandra.types.TEXT },
  subRole : { type : cassandra.types.TEXT },
  setup : { type : cassandra.types.BOOLEAN },
  lucene : { type : cassandra.types.TEXT }
}, {
  tableName: 'user',
  pre: function(obj) {
    if(obj.isNew) {
      obj.id = uuid.v4();
      obj.created = cassandra.types.getTimeUuid().now();
    }
    obj.accessed = cassandra.types.getTimeUuid().now();
  },
  post:function(obj) {

  }
});

cassandra.sync(UserModel);

module.exports = UserModel;`
