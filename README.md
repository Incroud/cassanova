# Cassanova

Cassanova is an object modeler for Cassandra CQL.

## Features
- Ability to create models that are mapped to and validated against table schemas
- The ability to create queries using chained methods
- Almost all Cassandra data types are supported
- Unit tested against Cassandra
- CQL utility for executing cql files


## Installation

First install [node.js](http://nodejs.org/) and [cassandra](http://cassandra.apache.org/). Then:

```sh
git clone https://github.com/incroud/cassanova.git
cd cassanova
npm install
```

## Overview

### Basic Usage

```javascript
var Cassanova = require('cassanova'),
    table = Cassanova.Table("users", Cassanova.Schema({
        id : Cassanova.SchemaType.UUID().PRIMARY_KEY(),
        username : Cassanova.SchemaType.TEXT(),
    })),
    UserModel = Cassanova.Model("userModel", table),
    myUserModel = new UserModel();

myUserModel.save({id:"80398220-e461-11e3-ac10-0800200c9a66", username:"jeboothjr"}, function(err, result){

});

```

### Creating a Cassanova Client

`Cassanova.createClient(options)`

A client is created as follows. Internally, the driver manages connections to Cassandra.  `options.hosts`, `options.port` and `options.keyspace` are required. For additional options, please refer to the driver documentation : [nodejs-driver](https://github.com/datastax/nodejs-driver)

```javascript
var Cassanova = require('cassanova'),
    options = {
        hosts: ['localhost'], /* An array of hosts */
        port:9042,
        keyspace: "cassanova_ks", /* The keyspace to use. */
        username: "", /* optional, the username for cassandra */
        password: "", /* optional, the password for cassandra */

        skipSchemaValidation: true /* optional, defaults to false. Bypasses schema validation */
    };

Cassanova.createClient(options);
```

### Connecting to Cassandra

`Cassanova.connect([options], callback)`

Creating a connection is as follows. This is not necessary as the driver will automatically connect when a query is executed. If you do not create a client first, you can optional pass in the same options as you would when creating a client and one will be automatically created for you. If options are used, `options.hosts`, `options.port` and `options.keyspace` are required.

```javascript
var Cassanova = require('cassanova');
Cassanova.connect({
    hosts : ['localhost'],
    port : 9042,
    keyspace : 'cassanova_ks'
},function(error, success){

});
```

`Cassanova.isConnected()`

Returns a Boolean as to whether or not the client is connected.

`Cassanova.disconnect([callback])`

Disconnnects the client from the pool. The optional `callback` will be executed when the client is disconnected.


### Logging

The events from the driver are bubbled up through to Cassanova. Following that pattern, the level being passed to the listener can be `info` or `error`.

```javascript
Cassanova.on('log', function(level, message) {
    console.log(level, message);
});
```

## SchemaType

`Cassanova.SchemaType.PRIMARY_KEY(keys)`

Assigns primary keys for the Schema. `keys` can be either a String, Array or Multi-Dimensional Array containing matching column names.
When attached to another SchemaType as in `Cassanova.SchemaType.UUID().PRIMARY_KEY()`, no arguments are used. The schema types contain validation, although not all of the validation is complete or is simple as of right now.

`Cassanova.SchemaType.ASCII()`

`Cassanova.SchemaType.BIGINT()`

`Cassanova.SchemaType.BLOB()`

`Cassanova.SchemaType.BOOLEAN()`

`Cassanova.SchemaType.COUNTER()`

`Cassanova.SchemaType.DECIMAL()`

`Cassanova.SchemaType.DOUBLE()`

`Cassanova.SchemaType.FLOAT()`

`Cassanova.SchemaType.INET()`

`Cassanova.SchemaType.INT()`

`Cassanova.SchemaType.LIST()`

`Cassanova.SchemaType.MAP()`

`Cassanova.SchemaType.SET()`

`Cassanova.SchemaType.TEXT()`

`Cassanova.SchemaType.TIMESTAMP()`

`Cassanova.SchemaType.UUID()`

`Cassanova.SchemaType.TIMEUUID()`

`Cassanova.SchemaType.VARCHAR()`

`Cassanova.SchemaType.VARINT()`

## Schema

`new Cassanova.Schema(obj)`

Creates a Cassanova Schema, where `obj` refers to an object that contains key/value pairs of the column names and data types.

```javascript
var userTablechema = Cassanova.Schema({
        id : Cassanova.SchemaType.UUID().PRIMARY_KEY(),
        username : Cassanova.SchemaType.TEXT(),
    });
```

## Table

`new Cassanova.Table(name, schema)`

Creates a Cassanova Table, where `name` refers to the actual table name in Cassandra and `schema` is a Cassanova.Schema.

#### Table Usage

A table defines the schema to be associated with the model. The table is validated to ensure that all of the type are set appropriately and that a PRIMARY KEY has been set correctly. Schema types are used to validate against the actual data in the model. Composite and partition keys are supported..

```javascript
var Cassanova = require('cassanova'),
    userTablechema = Cassanova.Schema({
        id : Cassanova.SchemaType.UUID().PRIMARY_KEY(),
        username : Cassanova.SchemaType.TEXT(),
    }),
    userTable = Cassanova.Table("users", userTablechema),
    infoTableSchema = Cassanova,Schema({
        id : Cassanova.SchemaType.UUID(),
        email : Cassanova.SchemaType.TEXT(),
        username : Cassanova.SchemaType.TEXT(),
        PRIMARY_KEY: Schema.Type.PRIMARY_KEY([["id", "email"], "username"])
    }),
    infoTable = Cassanova.Table("info", infoTableSchema);
```



## Models

`new Cassanova.Model(name, table)`

Creates a Cassanova Model where `name` is a unique identifier to retreive the model from Cassanova and `table` is a Cassanova.Table.

A model requires a table to be associated with it when it is being defined. When creating the model, the first argument must be unique and refers to the name of the model which you can use to retrieve it.

```javascript
var schema = Cassanova.Schema({
        id : Cassanova.SchemaType.UUID(),
        email : Cassanova.SchemaType.TEXT(),
        username : Cassanova.SchemaType.TEXT(),
        PRIMARY_KEY: Schema.Type.PRIMARY_KEY("id")
    }),
    table = Cassanova.Table("user", schema),
    UserModel = Cassanova.model("userModel", table);
```

Once a model is created you can access via the same way, without requiring the table. An error will br thrown if you attempt to overwrite a model with a new table.
```js
var myUserModel = new Cassanova.model("userModel")();
myUserModel.save({id:'71c20c30-e461-11e3-ac10-0800200c9a66', firstname:"James", lastname:"Booth"}, function(err, result){

});
```

#### Model Methods

By default, a model has the following methods: `find`, `save`, `findAllBy`, `delete` and `CQL`.

```javascript
var myUserModel = new Cassanova.model("userModel")();
userModel.find({id,'71c20c30-e461-11e3-ac10-0800200c9a66'}, function(err, result){});
userModel.save({id:'71c20c30-e461-11e3-ac10-0800200c9a66', username:"jeboothjr"}, function(err, result){});
userModel.findAllBy('username','James', function(err, result){});
userModel.delete({id,'71c20c30-e461-11e3-ac10-0800200c9a66'}, function(err, result){});
userModel.CQL('SELECT * FROM users', function(err, result){});
```

#### Enhanced Models

You can create separate model files, if needed, and customise your queries.

`infoModel.js`
```javascript
var Cassanova = require('cassanova'),
    userInfoSchema = new Cassanova.Schema({
        userid : Cassanova.SchemaType.TIMEUUID(),
        info_date : Cassanova.SchemaType.TIMESTAMP(),
        info : Cassanova.SchemaType.TEXT(),
        PRIMARY_KEY : Cassanova.SchemaType.PRIMARY_KEY(["userid", "attempt_date"])
    }),
    userInfoTable = Cassanova.Table("user_by_info", userInfoSchema),
    infoModel = Cassanova.Model("userAuthLogModel", userInfoTable);

infoModel.prototype.getRecentInfo = function(id, count, callback){
    var query = this.Query();

    query.SELECT("*").WHERE_EQUALS("userid", id).ORDER_BY("info_date", true).LIMIT(count);
    query.execute(callback);
    return query;
};
```

## Queries

`new Cassanova.Query(client, table, [query])`

Creates a Cassanova Query where `client` is a Cassanova.client, `table` is a Cassanova.Table to which the queries will be made and 'query' is an optional raw CQL string. If the raw string is included, you have the ability to chain off of that raw CQL.

```
var query = new Query(Cassanova.Client, Cassanova.Table("users"), "SELECT * FROM users");
query.WHERE().EQUALS("location", "california").execute(function(err, result){ });
```

There are a variety of ways that queries can be executed.

#### Raw CQL via the Model

As seen in some of the earlier examples, the model contains a CQL method where you can execute a raw CQL statement.

```javascript
userModel.CQL('SELECT * FROM users', function(err, result){});
```

#### Chained CQL via the Model

The model also contains a query object by which raw or chained CQL can be executed.

```javascript
infoModel.prototype.getRecentInfo = function(id, count, callback){
    this.query.SELECT("*").WHERE_EQUALS("userid", id).ORDER_BY("info_date", true).LIMIT(count).execute(callback);
}
```

```javascript
infoModel.Query().SELECT("*").WHERE_EQUALS("userid", "71c20c30-e461-11e3-ac10-0800200c9a66").ORDER_BY("info_date", true).LIMIT(count).execute(function(err, result){ });
```

#### Chained using predefined model methods.

Complex queries can be done against the model by chaining the cql methods together.

```javascript
personModel.find({firstname:"John"}).AND().EQUALS("age", 37).execute(function(err, result){});
personModel.find({lastname:"Doe"}).ORDER_BY("join_date", true).LIMIT(10).execute(function(err, result){});
```

#### Query API

Below is a list of the supported CQL query clauses and predicates. *Bold* indicates the output when that method is called.

`clone()`

Takes the current value of the query and creates a new query using that as base.

`clear()`

Clears the query value.

`execute([callback])`

Executes the CQL statement.

`CQL(cql_str)`

Sets the base of the CQL statement to the value of `cql_str`.


Below are the supported chainable query methods, showing the resulting string from each.

```
//.SELECT(selector) - Chainable.
SELECT * FROM  users

//.INSERT(obj) - Chainable.
INSERT INTO users (firstname, lastname) VALUES ('james', 'booth');

//.DELETE(selector) - Chainable.
DELETE firstname, lastname FROM users;

//.WHERE() - Chainable.
WHERE

//.WHERE_EQUALS(key, value) - Chainable.
WHERE firstname = 'james';

//.WHERE_IN(key, options) - Chainable.
WHERE userid IN (123, 456);

//.EQUALS(key, value)` - Chainable.
firstname = 'james'

//.AND() - Chainable.
AND

//.GT(key, value) - Chainable.
age > 21

//.GTE(key, value) - Chainable.
age >= 21

//.LT(key, value) - Chainable.
age < 21

//.LTE(key, value) - Chainable.
age <= 21

//.IN(options) - Chainable. A single value or an Array of values.
IN (123, 456);

//.LIMIT(value) - Chainable.
LIMIT 10

//.ALLOW_FILTERING() - Chainable.
ALLOW FILTERING

//.ORDER_BY(column, doDescend) - Chainable.
ORDER BY (ASC)

//.USING_TTL(duration) - Chainable.
USING_TTL 86400

//.USING_TIMESTAMP(timestamp) - Chainable.
USING TIMESTAMP 1405446044378

//.COUNT(value, [doChain]) - Optionally Chainable.
COUNT(*)

//.COLUMN_TOKEN(columnName, [doChain]) - Optionally Chainable.
TOKEN(username)

//.VALUE_TOKEN(value, [doChain]) - Optionally Chainable.
TOKEN(123)

//.AS(key, alias, [doChain]) - Optionally Chainable.
userid AS id
```

#### Additional Query Execution

Addition information regarding the methods below can be found in the [node-cassandra-cql driver](https://github.com/jorgebay/node-cassandra-cql) documentation.

##### Cassanova.execute

`Cassanova.execute(query, [options], callback)`

*query* is a `Cassanova.Query` object, `options` current supports `consistency` where the value is one of `Cassanova.consistencies`.

##### Cassanova.executeAsPrepared

`Cassanova.executeAsPrepared(query, [options], callback)`

*query* is a `Cassanova.Query` object, `options` current supports `consistency` where the value is one of `Cassanova.consistencies`.

##### Cassanova.executeAsBatch

`Cassanova.executeBatch(queries, [options], callback)`

*query* is a `Cassanova.Query` object, `options` current supports `consistency` where the value is one of `Cassanova.consistencies`.

##### Cassanova.executeEachRow

`Cassanova.executeEachRow(query, [options],  rowCallback, endCallback)`

`query` is a *Cassanova.Query* object, `options` current supports `consistency` where the value is one of `Cassanova.consistencies`. rowCallback(n, row) is called for each row as soon as the first chunk of the last field is received, where n is the index of the row. endCallback(err, rowLength) is called when all rows have been received or there is an error retrieving the row.

##### Cassanova.executeStreamField - DEPRECATED
##### This method is DEPRECATED. It has been removed from the offical driver. The current functionality is identical to executeEachRow

`Cassanova.executeStreamField(query, [options],  rowCallback, [endCallback])`

`query` is a *Cassanova.Query* object, `options` current supports `consistency` where the value is one of `Cassanova.consistencies`. rowCallback(n, row, streamField) is called for each row as soon as the first chunk of the last field is received, where n is the index of the row. endCallback(err, rowLength) is called when all rows have been received or there is an error retrieving the row.

##### Cassanova.executeStream

`Cassanova.executeStream(query, [options], [callback])`

*query* is a `Cassanova.Query` object, `options` current supports `consistency` where the value is one of `Cassanova.consistencies`.

`Cassanova.consistencies`

The consistencies used for queries. Defaults to quorum.

`any` `one` `two` `three` `quorum` `all` `localQuorum` `eachQuorum` `localOne`

## CQL Runner

In the root of the repo is a file, CQL.js, which is a command-line utility to run scripts against the database. Instructions can be found by running the following :

```javascript
node CQL --help
```

## FAQ
#### What is left?
The validation on some of the data types is simplistic or non-existent (list, map, set). These need to be fleshed out.

The may be some functionality in the driver that was overlooked.

Not every CQL command has been implemented.

#### What is repl.js for?
I was using this to debug and experiment around, so I left it in. Below is some simple usage. You can also start the repl in --debug or --debug-brk and step through any of the code.

#### Can I see more examples?
Take a look at the test/EndToEndTest.js file. There are a lot of examples in there.

```
//Create the keyspace in Cassandra first:
//CREATE KEYSPACE cassanova WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
$ node repl
$ var opts = {
    "hosts" : ["localhost"],
    "port" : 9042
    "keyspace" : "cassanova"
};
$ Cassanova.connect(opts, function(err, result){
    console.log(err || "Connected");
});
$ var q = Cassanova.Query().CQL("SELECT * FROM system.local;");
q.execute(function(err, result){
    console.log(result.length);
    console.log(result[0]);
});
```

## License

cassanova is distributed under the [MIT license](http://opensource.org/licenses/MIT).

## Contributions

Feel free to join in and support the project!

Check the [Issue tracker](https://github.com/Incroud/cassanova/issues)
