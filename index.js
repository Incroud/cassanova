var _ = require('lodash'),
    Query = require('./lib/query'),
    Model = require('./lib/model'),
    Schema = require("./lib/schema"),
    Table = require("./lib/table"),
    EventEmitter = require('events').EventEmitter,
    util = require('util'),
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
        console.warn("options.hosts has been deprecated. Please use options.contactPoints and options.protocolOptions when creating connections.");
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
console.log(options);
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
console.log(options);
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

    return !_.isEmpty(this.client.pools);
};

/**
 * Connects to the pool with options supported by the driver. It will not attempt to connect if it's already connected, but return a success.
 * @param  {Object}   options  Configuration for the connection.
 * @param  {Function} callback Called upon initialization.
 */
Cassanova.prototype.connect = function(options, callback){
    if (arguments.length < 2){
        callback = options;
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

    this.client.close(callback);
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

/**
 * Used to get direct access to the driver.
 * @return {Object} The cql drive/
 */
Cassanova.prototype.getDriver = function(){
    if(!this.client){
        return null;
    }
    return this.client.pools.default || this.client.pools[this.options.keyspace];
};

Cassanova.prototype.execute = function(query, options, callback){
    if (arguments.length < 3){
        callback = options;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;

    options.consistency = options.consistency || this.consistencies.default;

    var driver = this.getDriver();

    this.client.cql(query.toString(), null, options, callback);
};

Cassanova.prototype.executeAsPrepared = function(query, options, callback){
    if (arguments.length < 3){
        callback = options;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;

    options.consistency = options.consistency || this.consistencies.default;
    options.executeAsPrepared = true;

    this.client.cql(query.toString(), null, options, callback);
};

Cassanova.prototype.executeBatch = function(queries, options, callback){
    var queryBatch = this.client.beginBatch(),
        consistency;

    if (arguments.length < 3){
        callback = options;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;
    callback = callback || noop;

    consistency = (options && options.consistency) || this.consistencies.default;

    queries.map(function(query){
        queryBatch = queryBatch.addQuery(cassanova.client.beginQuery().query(query.toString()));
    });

    queryBatch.consistency(consistency).execute(function(err,result){
        callback(err, result);
    });
};

Cassanova.prototype.executeEachRow = function(query, options, rowCallback, endCallback){
    var driver = this.getDriver(),
        consistency;

    if (arguments.length < 4){
        endCallback = rowCallback;
        rowCallback = options;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;

    if(!driver){
        if(endCallback){
            return endCallback(new Error("Client has not been created yet. Use Cassanova.createClient([options]);"), false);
        }
        throw new Error("Client has not been created yet. Use Cassanova.createClient([options]);");
    }

    consistency = options.consistency || this.consistencies.default;

    driver.eachRow(query.toString(), null, consistency, rowCallback, endCallback);
};

Cassanova.prototype.executeStreamField = function(query, options, rowCallback, endCallback){
    var driver = this.getDriver(),
        consistency;

    if (arguments.length < 4){
        endCallback = rowCallback;
        rowCallback = options;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;

    if(!driver){
        if(endCallback){
            return endCallback(new Error("Client has not been created yet. Use Cassanova.createClient([options]);"), false);
        }
        throw new Error("Client has not been created yet. Use Cassanova.createClient([options]);");
    }

    consistency = options.consistency || this.consistencies.default;

    driver.streamField(query.toString(), null, consistency, rowCallback, endCallback);
};

Cassanova.prototype.executeStream = function(query, options, callback){
    var driver = this.getDriver(),
        consistency;

    if(!driver){
        return callback(new Error("Client has not been created yet. Use Cassanova.createClient([options]);"), false);
    }

    if (arguments.length < 3){
        callback = options;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;

    consistency = options.consistency || this.consistencies.default;

    driver.stream(query.toString(), null, consistency, callback);
};

Cassanova.prototype.consistencies = Query.consistencies;

Cassanova.prototype.Cassanova = Cassanova;

module.exports = cassanova = new Cassanova();
