var _ = require('lodash'),
    colors = require('colors'),
    Query = require('./lib/query'),
    Model = require('./lib/model'),
    Schema = require("./lib/schema"),
    Table = require("./lib/table"),
    EventEmitter = require('events').EventEmitter,
    util = require('util'),
    utils = require("./lib/utils"),
    driver = require('cassandra-driver'),
    noop = function(){};

/**
 * Cassanova. An object modeler for Cassandra CQL built upon the node-cassandra-cql driver.
 */
function Cassanova(){
    this.tables = {};
    this.models = {};
    this.schemas = {};
    this.client = null;
    this.options = null;

    EventEmitter.call(this);
}

//Used to bubble events out of the driver.
util.inherits(Cassanova, EventEmitter);

/**
 * Creates a client for models.
 * @return {client} A driver client.
 */
/**
 * A pass-through to the driver to connect to the pool.
 * @param  {Object} options Configureation options for the driver
 * @return {Client}         The newly created client.
 */
Cassanova.prototype.createClient = function(options){
    var authProvider,
        port,
        host,
        hosts,
        len,
        i;

    //NEW DRIVER CHANGE: Backwards compatibility. In the new driver, hosts has become contactPoints
    if(options.hosts){
        console.warn("DEPRECATION: ".bold.cyan, "options.hosts has been deprecated. Please use options.contactPoints and options.protocolOptions when creating connections.".cyan);
    }

    //Lets parse the deprecated code into the new structure.
    if(!options.contactPoints && options.hosts){
        options.contactPoints = [];
        hosts = options.hosts;
        len = hosts.length;

        for(i=0; i<len; i++){
            host = hosts[i].split(":");
            if(!port){
                port = host[1];
            }
            options.contactPoints.push(host[0]);
        }
        options.protocolOptions = { port:port };
        delete options.hosts;
    }

    //NEW DRIVER CHANGE: New driver has changed how authentication works.
    if(!options.authProvider && options.username && options.password){
        console.warn("options.username & options.password has been deprecated. Please use options.authProvider when creating connections.");
        authProvider = new driver.auth.PlainTextAuthProvider(options.username, options.password);
        options.authProvider = authProvider;
    }

    if(!options || !options.contactPoints){
        throw new Error("Creating a client requires contactPoint information when being created.");
    }

    if(!options.keyspace){
        cassanova.emit('log', 'warn', "keyspace has not been defined in the config.");
    }

    this.options = options;
    Query.skipSchemaValidation = options.skipSchemaValidation || false;

    this.client = new driver.Client(options);
    this.client.on('log', function(level, message) {
        cassanova.emit('log', level, message);
    });
    this.client.on('connectionFailed', function() {
        cassanova.emit('connectionFailed');
    });
    this.client.on('connectionAvailable', function() {
        cassanova.emit('connectionAvailable');
    });
    return this.client;
};


/**
 * A pass-through to the driver to determine the connected state.
 * @return {Boolean} Whether the client is connected or not.
 */
Cassanova.prototype.isConnected = function(){
    if(!this.client){
        return false;
    }
    return this.client.connected;
};

/**
 * Connects to the pool with options supported by the driver. It will not attempt to connect if it's already connected, but return a success.
 * @param  {Object}   options  Configuration for the connection.
 * @param  {Function} callback Called upon initialization.
 */
Cassanova.prototype.connect = function(options, callback){
    if (arguments.length < 2){
        callback = _.isFunction(options) ? options : noop;
        options = {};
    }
    if(!this.client){
        this.client = this.createClient(options);
    }
    if(this.isConnected()){
        cassanova.emit('log', "info", "Client is already connected.");
        callback(null, true);
        return;
    }
    this.client.connect(callback);
};

/**
 * A pass-through to the driver to shut down client.
 */
Cassanova.prototype.disconnect = function(callback){
    if(!this.client){
        return callback(new Error("No client to disconnect."), null);
    }

    if(!this.isConnected()){
        return callback(null, true);
    }

    this.client.shutdown(callback);
};

/**
 * Creates/Retrieves a schema based on object.
 */
Cassanova.prototype.Schema = function(obj){
    return new Schema(obj);
};

/**
 * Returns the Schema types to construct a schema.
 */
Cassanova.prototype.SchemaType = Schema.Type;

/**
 * Creates a new table object from a schema. Once created, they cannot be modified.
 * @param {String} name   The name of the table. This must match the actual table name in Cassandra.
 * @param {Schema} schema The schema of the table.
 * @return {Table} Returns the newly created table.
 */
Cassanova.prototype.Table = function(name, schema){
    var table_obj = this.tables[name];

    if(table_obj){
        if(!schema){
            return table_obj;
        }else if(table_obj.schema !== schema){
            throw new Error("Attempting to overwrite the schema for table : " + name);
        }
    }

    table_obj = new Table(name, schema, this.client);

    this.tables[name] = table_obj;

    return table_obj;
};

/**
 * Creates/Retrieves a model from a name/schema. If it exists, the schemas are compared (if require). If they are different,
 * an error is throw as to not overwrite a model. If the schema is the same or not defined, the model is returned.
 * @param  {String} name   The name of the model to create or retrieve.
 * @param  {Schema or Object} schema Used in model creation only, defines the schema for the model.
 * @return {Model}        Returns the existing or newly created model.
 */
Cassanova.prototype.Model = function (name, table){
    var _name = name,
        _table = table,
        _model = _name ? this.models[_name] : null;

    if(!_name || typeof _name !== "string"){
        throw new Error("Attempted to create a model with an invalid name.");
    }

    if(!_model){
        if(!_table || !(_table instanceof Table)){
            throw new Error("Attempted to retrieve a model that doesn't exist or create a model with an invalid table.");
        }
        _model = this.models[_name] = Model.create(_name, _table, this.client, this);
        this.schemas[_table.name] = _table.schema;
    }else{
        if(_table !== undefined && _model.table !== _table){
            throw new Error("Attempting to overwrite a model with a different table : " + _name);
        }
    }

    return _model;
};

Cassanova.prototype.Query = function(){
    return new Query();
};

Cassanova.prototype.cql = function(query, options, callback){
    if (arguments.length < 3){
        callback = _.isFunction(options) ? options : noop;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;
    options.consistency = options.consistency || this.consistencies.default;

    this.client.execute(query, null, options, function(err, result){
        if(!err){
            result = utils.scrubCollectionData(result.rows);
        }
        callback(err, result);
    });
};

Cassanova.prototype.execute = function(query, options, callback){
    if (arguments.length < 3){
        callback = _.isFunction(options) ? options : noop;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;

    options.consistency = options.consistency || this.consistencies.default;

    this.client.execute(query.toString(), null, options, function(err, result){
        if(!err){
            result = utils.scrubCollectionData(result.rows);
        }
        callback(err, result);
    });
};

Cassanova.prototype.executeAsPrepared = function(query, options, callback){
    if (arguments.length < 3){
        callback = _.isFunction(options) ? options : noop;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;

    options.prepare = true;
    options.consistency = options.consistency || this.consistencies.default;

    this.client.execute(query.toString(), null, options, function(err, result){
        if(!err){
            result = utils.scrubCollectionData(result.rows);
        }
        callback(err, result);
    });
};

Cassanova.prototype.executeBatch = function(queries, options, callback){
    var queryBatch = [],
        len = queries.length,
        query,
        i;

    if (arguments.length < 3){
        callback = _.isFunction(options) ? options : noop;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;

    options.consistency = (options && options.consistency) || this.consistencies.default;

    for(i=0; i<len; i++){
        query = queries[i];
        queryBatch.push(query.toString());
    }

    this.client.batch(queryBatch, options, function(err, result){
        if(!err){
            result = utils.scrubCollectionData(result.rows);
        }
        callback(err, result);
    });
};

Cassanova.prototype.executeEachRow = function(query, options, rowCallback, endCallback){
    if (arguments.length < 4){
        endCallback = _.isFunction(rowCallback) ? rowCallback : noop;
        rowCallback = _.isFunction(options) ? options : noop;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;

    options.consistency = options.consistency || this.consistencies.default;

    this.client.eachRow(query.toString(), null, options, rowCallback, endCallback);
};

Cassanova.prototype.executeStreamField = function(query, options, rowCallback, endCallback){
    throw new Error("executeStreamField is no longer supported by the driver.");
};

Cassanova.prototype.executeStream = function(query, options, callback){
    var consistency;

    if (arguments.length < 3){
        callback = _.isFunction(options) ? options : noop;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;

    options.consistency = options.consistency || this.consistencies.default;

    return this.client.stream(query.toString(), null, options)
        .on('end', function(){
            if(callback){
                callback(null, true);
            }
        })
        .on('error', function(err){
            if(callback){
                callback(err, false);
            }
        });
};

Cassanova.prototype.consistencies = Query.consistencies;

Cassanova.prototype.Cassanova = Cassanova;

module.exports = cassanova = new Cassanova();
