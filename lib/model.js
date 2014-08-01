var Query = require('./query'),
    _ = require("lodash");

/**
 * Cassanova.Model
 * @param {String} name       The name of the model. Used to creation and retrieve.
 * @param {Table} table     The table associated with the model.
 */
function Model(name, table) {
    this.model = this;

    if(!name || typeof name !== "string"){
        throw new Error("Attempting to instantiate a model without a valid name. Create models using the Cassanova.model API.");
    }

    if(!table){
        throw new Error("Attempting to instantiate a model, " + name + ", without a valid table. Create models using the Cassanova.model API.");
    }

    this.name = name;
    this.table = table;
}

/**
 * Creates the model
 * @param  {String} name  The name of the model. Also used to retrieve the model from Cassanova.
 * @param  {Table} table The table
 * @return {Model}       A newly created model
 */
Model.create = function(name, table){
    var args = arguments;

    function model (name, table) {
        Model.apply(this, args);
        this.params = args;
    }

    model.table = table;

    //Do we want to enforce instantiation? Comment these out...
    model.find = Model.prototype.find;
    model.delete = Model.prototype.delete;
    model.save = Model.prototype.save;
    model.findAllBy = Model.prototype.findAllBy;
    model.Query = Model.prototype.Query;
    model.CQL = Model.prototype.CQL;
    //^^^^^^^^^^^^^^^^^

    model.__proto__ = Model;
    model.prototype.__proto__ = Model.prototype;

    return model;
};

/**
 * Finds records based on the values in the query object. First it creates an array of the pairings, to facilitate chaining of the query.
 * @param  {Object}   query_obj Object with column/value mappings
 * @param  {Object}   options Potential options for the method. Current A boolean value for "execute" is supported which will prevent immediate execution of the query.
 * @param  {Function} callback  If executed, will be called with the result. Will be ignored if options.execute is false.
 * @return {Query}      Query for chainability.
 */
Model.prototype.find = function(query_obj, options, callback){
    var q,
        query = new Query(this.table).SELECT("*").WHERE(),
        q_arr = [],
        item;
        
    if (arguments.length < 3){
        callback = options;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;

    q_arr = _.pairs(query_obj);
    for(q=0; q<q_arr.length; q++){
        item = q_arr[q];
        query.EQUALS(item[0], item[1]);
        if(q < q_arr.length-1){
            query.AND();
        }
    }

    if(options && (options.execute !== false) && callback){
        query.execute(callback);
    }
    return query;
};

/**
 * Deletes records based on the values in the query object. First it creates an array of the pairings, to facilitate chaining of the query.
 * @param  {Object}   query_obj Object with column/value mappings
 * @param  {Object}   options Potential options for the method. Current A boolean value for "execute" is supported which will prevent immediate execution of the query.
 * @param  {Function} callback  If executed, will be called with the result. Will be ignored if options.execute is false.
 * @return {Query}      Query for chainability.
 */
Model.prototype.delete = function(query_obj, options, callback){
    var q,
        query,
        q_arr = [],
        item;

    if (arguments.length < 3){
        callback = options;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;

    query = new Query(this.table).DELETE();

    if(query_obj){
        query = query.WHERE();
        q_arr = _.pairs(query_obj);
        for(q=0; q<q_arr.length; q++){
            item = q_arr[q];
            query.EQUALS(item[0], item[1]);
            if(q < q_arr.length-1){
                query.AND();
            }
        }
    }

    if(options && (options.execute !== false) && callback){
        query.execute(callback);
    }
    return query;
};
/**
 * Allows a pure CQL statement to be used for the model.
 * @param {String}   cql       The CQL statement for the query
 * @param  {Object}   options Potential options for the method. Current A boolean value for "execute" is supported which will prevent immediate execution of the query.
 * @param {Function} callback  If executed, will be called with the result. Will be ignored if doExecute is false.
 */
Model.prototype.CQL = function(cql, options, callback){
    var query = new Query(this.table).CQL(cql);

    if (arguments.length < 3){
        callback = options;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;

    if(options && (options.execute !== false) && callback){
        query.execute(callback);
    }
    return query;
};

/**
 * Finds records based on the values in the query object. First it creates an array of the pairings, to facilitate chaining of the query.
 * @param  {String}   column The name of the column
 * @param  {String}   value The value to find
 * @param  {Object}   options Potential options for the method. Current A boolean value for "execute" is supported which will prevent immediate execution of the query.
 * @param  {Function} callback  If executed, will be called with the result. Will be ignored if options.execute is false.
 * @return {Query}      Query for chainability.
 */
Model.prototype.findAllBy = function(column, value, options, callback){
    var query;

    if (arguments.length < 4){
        callback = options;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;

    query = new Query(this.table).SELECT("*").WHERE_EQUALS(column, value);

    if(options && (options.execute !== false) && callback){
        query.execute(callback);
    }
    return query;
};


/**
 * Processes the data for the insert and maps appropriate data from the schema.
 * @param  {Object}   data      The object to insert. Key/value pairs where the key equals the column name and the value is of the appropriate schema type.
 * @param  {Object}   options Potential options for the method. Current A boolean value for "execute" is supported which will prevent immediate execution of the query.
 * @param  {Function} callback  If executed, will be called with the result. Will be ignored if options.execute is false.
 * @return {Query}      Query for chainability.
 */
Model.prototype.save = function(data, options, callback){
    var columns = [],
        values = [],
        key,
        query;

    if (arguments.length < 3){
        callback = options;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;

    if(!data){
        return callback(new Error("There is no data to be saved"), false);
    }

    query = new Query(this.table).INSERT(data);
    if(options && options.TTL){
        query.USING_TTL(options.TTL);
    }
    if(options && (options.execute !== false) && callback){
        query.execute(callback);
    }
    return query;
};

/**
 * A method to generate a query for new model instance methods
 */
Model.prototype.Query = function(){
    return new Query(this.table);
};

module.exports = exports = Model;
