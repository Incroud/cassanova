var Schema = require('./schema'),
    Query = require('./query'),
    _ = require("lodash");

/**
 * Cassanova.Table
 * @param {String} name       The name of the table. Must match the name of the table in Cassandra
 * @param {Schema} schema     The table associated with the model.
 */
function Table(name, schema){
    if(!name || typeof name !== "string"){
        throw new Error("Attempting to instantiate a table without a valid name.");
    }
    if(!schema || !(schema instanceof Schema)){
        throw new Error("Attempting to instantiate a table without a valid schema.");
    }
    
    this.name = name;
    this.schema = schema;
}

/**
 * Creates the table in the database
 * @param  {Object}   options Potential options for the method. Current A boolean value for "execute" is supported which will prevent immediate execution of the query and
 * ifNotExists (Boolean) will create the table if it dpesn't exist.
 * @param  {Function} callback  If executed, will be called with the result. Will be ignored if options.execute is false.
 * @return {Query}      Query for chainability.
 */
Table.prototype.create = function(options, callback){
    var query;

    if (arguments.length < 2){
        callback = options;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;

    query = new Query().CREATE_TABLE(this.name, this.schema, options.ifNotExists);

    if(options && (options.execute !== false) && callback){
        query.execute(callback);
    }

    return query;
};

module.exports = exports = Table;
