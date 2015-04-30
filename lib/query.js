var _ = require("lodash"),
    SchemaType = require("./schemaType");

/**
 * Cassanova.Query
 * The query maintains a connection to the database and can be executed as needed.
 * @param {Table} table The Table object is used to properly generate and validate the query.
 * @param {String} q_str OPTIONAL Allows the query to be pre-generated.
 */
function Query(table, q_str) {
    this.table = table;
    this.cql_str = q_str || "";
}

Query.skipSchemaValidation = false;

/**
 * An internal method, it processes primary partition keys into a CQL statement.
 * @param  {Object} keys A string, Array or multi-dimensional Array
 * @return {String}      A CQL formatted string of the partition key
 */
function processPartitionKeys(keys){
    var result = "(",
        j,
        len,
        keyGroup;

    if(typeof keys === 'string'){
        return result += keys + ")";
    }

    len = keys.length;
    for(j = 0; j< keys.length; j++){
        keyGroup = keys[j];
        if(keyGroup instanceof Array){
            result += "(";
            result += keyGroup.join(", ");
            result += ")";
        }else{
            result += keyGroup;
        }
        result += (j < len-1) ? ", " : "";
    }
    result += ")";

    return result;
}

/**
 * A method to set the table on a newly created query. It is best to use the Model.Query as it will be automaticall set.
 * Due to the query dependency on the table and how the cql string is generated, setting the table after a chainable
 * method has been called, may have unintended consequences.
 * @param {Table} table The table that the query should use.
 */
Query.prototype.setTable = function(table){
    if(this.cql_str !== ""){
        console.warn("Setting the table, " + table.name + ", on query, " + this.cql_str + ", after it has been chained may have unintended consequences.");
    }
    this.table = table;
};

Query.prototype.wrapValue = function(value, wrapper){
    return wrapper.start + value + wrapper.end;
};

/**
 * Wraps a value in CQL according to the type in the schema. This could be quotes, braces, etc.
 * @param  {String} key   The schema key
 * @param  {Object} value The schema value
 * @return {String}       A CQL string wrapped with appropriate wrappers.
 */
Query.prototype.wrapKeyValue = function(key, value){
    var keySchema = this.table.schema.structure[key],
        keyParams,
        valueIsArray = (value instanceof Array),
        paramsIsSchemaType,
        wrapper,
        i,
        processedValues = [],
        keyProp,
        date;

    if(!keySchema){
        throw new Error("Attempted to process a key that is not a part of the schema, " + key + ", with value of " + value);
    }

    date = keySchema.type === 'timestamp' ? new Date(value) : null;

    if(date && date.toString() === 'Invalid Date'){
        throw new Error("Invalid date passed for key " + key + ", with value of " + value);
    }
    
    if(date){
        value = date.toISOString();
    }

    if(keySchema.type === 'text'){
        value = value.replace(/'/g, "''");
    }

    wrapper = keySchema.wrapper;
    keyParams = keySchema.parameters;
    paramsIsSchemaType = (keyParams instanceof SchemaType);

    if(paramsIsSchemaType && valueIsArray){
        for(i=0; i<value.length; i++){
            if(!Query.skipSchemaValidation && !keyParams.validate(value[i])){
                throw new Error("Mismatched key type for " + key + ". Expecting a " + keyParams.type);
            }
            processedValues.push(this.wrapValue(value[i], keyParams.wrapper));
        }
    }else if(keyParams && keyParams.key && keyParams.value){
        if(!valueIsArray){
            var valueArr = Object.keys(value);
            for(i=0; i < valueArr.length; i++){
                keyProp = valueArr[i];

                //Can't validate the key since it's always converted to a string.
                // if(!keyParams.key.validate(keyProp)){
                //     throw new Error("Mismatched key type for " + key + ". Expecting a " + keyParams.key.type);
                // }
                if(!Query.skipSchemaValidation && !keyParams.value.validate(value[keyProp])){
                    throw new Error("Mismatched value type for " + key + ". Expecting a " + keyParams.value.type);
                }
                processedValues.push(this.wrapValue(keyProp, keyParams.key.wrapper) + " : " + this.wrapValue(value[keyProp], keyParams.value.wrapper));
            }
        }
        else{
            for(i=0; i<value.length; i++){
                keyProp = Object.keys(value[i]);

                //Can't validate the key since it's always converted to a string.
                // if(!keyParams.key.validate(keyProp)){
                //     throw new Error("Mismatched key type for " + key + ". Expecting a " + keyParams.key.type);
                // }
                if(!Query.skipSchemaValidation && !keyParams.value.validate(value[i][keyProp])){
                    throw new Error("Mismatched value type for " + key + ". Expecting a " + keyParams.value.type);
                }
                processedValues.push(this.wrapValue(keyProp, keyParams.key.wrapper) + " : " + this.wrapValue(value[i][keyProp], keyParams.value.wrapper));
            }
        }

    }else{
        processedValues.push(value);
    }

    return wrapper.start + processedValues.join(", ") + wrapper.end;
};

/**
 * Creates a table CQL String.
 * @example
    var schema = new Schema({
        id: Schema.Type.UUID().PRIMARY_KEY(),
        username: Schema.Type.TEXT()
    }),
    query = new Query();
    query.CREATE_TABLE('users', schema);

    output> CREATE TABLE users (id uuid PRIMARY KEY , username text);

 * @param {String} tableName   The name of the table
 * @param {Schema} schema      The schema of the table
 * @param {Boolean} ifNotExists Will add "IF NOT EXISTS" into the CQL statement
 * @return {Query} The query object
 */
Query.prototype.CREATE_TABLE = function(tableName, schema, ifNotExists){
    var columns,
        name,
        type,
        columnInfo,
        isPrimary,
        hasCompoundPrimary = schema.structure.PRIMARY_KEY,
        tableCQL = [];

    ifNotExists = (typeof ifNotExists === 'undefined') ? false : ifNotExists;

    this.cql_str += "CREATE TABLE " + (ifNotExists ? "IF NOT EXISTS ": "") + tableName + " (";

    columns = schema.structure;
    for(name in columns){
        columnInfo = columns[name];
        type = columns[name].type;
        isPrimary = hasCompoundPrimary || name === "PRIMARY_KEY" ? false : columnInfo.isPrimary;
        if(name === "PRIMARY_KEY"){
            tableCQL.push("PRIMARY KEY " + processPartitionKeys(columnInfo.parameters));
        }else{
            tableCQL.push(name + " " + type + (isPrimary ? " PRIMARY KEY " : ""));
        }
    }

    this.cql_str += tableCQL.join(", ");
    this.cql_str += ")";

    return this;
};

/**
 * Creates the base SELECT statement where additional CQL can be added via chaining.
 * @example
 * SELECT * from users;
 * @param {Object} selector A The selector can be of type String or Array
 * @return {Query} The query object
 */
Query.prototype.SELECT = function(selector){
    var aliases,
        len,
        i;

    //selector = selector || "*";//Don't do this. Let's keep this explicit?

    this.cql_str += "SELECT ";

    if(_.isString(selector)){
        this.cql_str += selector + Query.utils.space;
    }else if(_.isArray(selector)){
        len = selector.length;
        aliases = [];
        for(i=0; i<len; i++){
            if(_.isFunction(selector[i])){
                aliases.push(this.wrapKeyValue(selector[i]).toString());
            }else{
                aliases.push(selector[i]);
            }
        }
        this.cql_str += aliases.join(", ") + Query.utils.space;
    }else{
        throw new TypeError("Unsupported selector for SELECT statement. Expected " + selector + " to be of type String or Array.");
    }

    this.cql_str += "FROM " + this.table.name + Query.utils.space;

    return this;
};

/**
 * Creates the base INSERT statement where additional CQL can be added via chaining.
 * @param {Object} data The key/values or columns/values of the data to be inserted.
 * @return {Query} The query object
 */
Query.prototype.INSERT = function(data){
    var columns = [],
        values = [],
        cName,
        cValue;

    this.cql_str += "INSERT ";

    for(cName in data){
        cValue = data[cName];
        Query.utils.verifySchemaKey(cName, cValue, this);
        cValue = this.wrapKeyValue(cName, cValue);
        columns.push(cName);
        values.push(cValue);
    }

    this.cql_str += "INTO " + this.table.name + " (" + columns.join(", ") + ") VALUES (" + values.join(", ") + ")" + Query.utils.space;

    return this;
};

/**
 * Creates the base DELETE statement where additional CQL can be added via chaining.
 * @param {Object} selector A The selector can be of type String or Array
 * @return {Query} The query object
 */
Query.prototype.DELETE = function(selector){
    var aliases,
        len,
        i;

    this.cql_str = "DELETE ";

    if(_.isString(selector)){
        this.cql_str += selector + Query.utils.space;
    }else if(_.isArray(selector)){
        len = selector.length;
        aliases = [];
        for(i=0; i<len; i++){
            if(_.isFunction(selector[i])){
                aliases.push(Query.wrapKeyValue(selector[i].toString()));
            }else{
                aliases.push(selector[i]);
            }
        }
        this.cql_str += aliases.join(", ") + Query.utils.space;
    }

    this.cql_str += "FROM " + this.table.name + Query.utils.space;

    return this;
};

/**
 * Chainable, adds "WHERE" into the CQL statement.
 * WHERE
 */
Query.prototype.WHERE = function(key){
    if(key) {
        Query.utils.verifySchemaKey(key, null, this, true);
        this.cql_str += "WHERE " + key + " ";
    } else {
        this.cql_str += "WHERE ";
    }

    return this;
};

/**
 * Chainable, A shortcut, it adds "WHERE" and "="" into the CQL statement.
 * WHERE firstname = 'james'
 */
Query.prototype.WHERE_EQUALS = function(key, value){

    Query.utils.verifySchemaKey(key, value, this);

    this.cql_str += "WHERE " + key + " = " + this.wrapKeyValue(key, value) + Query.utils.space;

    return this;
};

/**
 * Chainable, adds "=" into the CQL statement.
 * firstname = 'james'
 */
Query.prototype.EQUALS = function(key, value){
    Query.utils.verifySchemaKey(key, value, this);

    this.cql_str += key + " = " + this.wrapKeyValue(key, value) + Query.utils.space;

    return this;
};

/**
 * Chainable, adds "AND" into the CQL statement.
 * AND
 */
Query.prototype.AND = function(key){
    if(key) {
        Query.utils.verifySchemaKey(key, null, this, true);
        this.cql_str += "AND " + key + " ";
    } else {
        this.cql_str += "AND ";
    }

    return this;
};

/**
 * Chainable, adds ">" into the CQL statement.
 * age > 21
 */
Query.prototype.GT = function(key, value){
    Query.utils.verifySchemaKey(key, value, this);

    this.cql_str += key + " > " + value + Query.utils.space;

    return this;
};

/**
 * Chainable, adds ">=" into the CQL statement.
 * age >= 21
 */
Query.prototype.GTE = function(key, value){
    Query.utils.verifySchemaKey(key, value, this);

    this.cql_str += key + " >= " + value + Query.utils.space;

    return this;
};

/**
 * Chainable, adds "<" into the CQL statement.
 * age < 21
 */
Query.prototype.LT = function(key, value){
    Query.utils.verifySchemaKey(key, value, this);

    this.cql_str += key + " < " + value + Query.utils.space;

    return this;
};

/**
 * Chainable, adds "<=" into the CQL statement.
 * age <= 21
 */
Query.prototype.LTE = function(key, value){
    Query.utils.verifySchemaKey(key, value, this);

    this.cql_str += key + " <= " + value + Query.utils.space;

    return this;
};

/**
 * Chainable, adds "IN" into the CQL statement.
 * IN (123, 456)
 */
Query.prototype.IN = function(key, options){
    var inOpt = [];

    if(arguments.length === 1) {
        options = key;
        key = null;
    }

    if(!options){
        throw new Error("The predicate IN requires and Array argument.");
    }
    inOpt = Array.isArray(options) ? options : [options];
    inOpt = Query.utils.checkQuoteNeeds(inOpt);
    if(key) {
        Query.utils.verifySchemaKey(key, null, this, true);
        this.cql_str += key + " IN (" + inOpt.join(", ") + ")";
    } else {
        this.cql_str += "IN (" + inOpt.join(", ") + ")";
    }

    return this;
};

/**
 * Chainable, adds "LIMIT" into the CQL statement.
 * LIMIT 10
 */
Query.prototype.LIMIT =function(value){
    this.cql_str += "LIMIT " + value + Query.utils.space;

    return this;
};

/**
 * Chainable, adds "ALLOW FILTERING" into the CQL statement.
 * ALLOW FILTERING
 */
Query.prototype.ALLOW_FILTERING = function(){
    this.cql_str += "ALLOW FILTERING" + Query.utils.space;

    return this;
};

/**
 * Chainable, adds "ORDER BY" into the CQL statement.
 * ORDER BY (ASC)
 */
Query.prototype.ORDER_BY = function(column, doDescend){
    this.cql_str += "ORDER BY " + column + " " + ((doDescend) ? "DESC" : "ASC") + Query.utils.space;

    return this;
};

/**
 * Chainable, adds "USING TTL" into the CQL statement.
 * USING_TTL 86400
 */
Query.prototype.USING_TTL = function(duration){
    this.cql_str += "USING TTL " + duration;

    return this;
};

/**
 * Chainable, adds "USING TIMESTAMP" into the CQL statement.
 * USING TIMESTAMP 1405446044378
 */
Query.prototype.USING_TIMESTAMP = function(timestamp){

    if(!SchemaType.INT().validate(timestamp)){
        throw new Error("USING TIMSTAMP requires a valid timestamp, instead, received "+ timestamp);
    }

    this.cql_str += "USING TIMESTAMP " + timestamp;

    return this;
};

/**
 * Optionally Chainable, adds "COUNT" into the CQL statement.
 * Due to the complexity of some CQL statments, the abilty to chain everything was maintained, but some could cause issues.
 * COUNT(*)
 */
Query.prototype.COUNT = function(value, doChain){
    var c = "COUNT(" + value + ")";

    if(doChain){
        this.cql_str += c;
        return this;
    }
    return c;
};

/**
 * Optionally Chainable, adds "TOKEN" into the CQL statement.
 * The reason COLUMN_TOKEN and VALUE_TOKEN were separated is because a column token, while a string, does not require quotes, where as other
 * tokens which are strings, do and also to differentiate for validation purposes.
 * TOKEN(username)
 */
Query.prototype.COLUMN_TOKEN = function(columnName, doChain){
    var c = "TOKEN(" + columnName + ")";

    Query.utils.verifySchemaKey(columnName, null, this, true);

    if(doChain){
        this.cql_str += c;
        return this;
    }
    return c;
};

/**
 * Optionally Chainable, adds "TOKEN" into the CQL statement.
 * The reason COLUMN_TOKEN and VALUE_TOKEN were separated is because a column token, while a string, does not require quotes, where as other
 * tokens which are strings, do and also to differentiate for validation purposes.
 * TOKEN(123)
 */
Query.prototype.VALUE_TOKEN = function(value, doChain){
    var c = "TOKEN(" + Query.utils.checkQuoteNeed(value) + ")";

    if(doChain){
        this.cql_str += c;
        return this;
    }
    return c;
};

/**
 * Optionally Chainable, adds "AS" into the CQL statement.
 * userid AS id
 */
Query.prototype.AS = function(key, alias, doChain){
    var c = key + " AS " + alias;

    Query.utils.verifySchemaKey(key, null, this, true);

    if(doChain){
        this.cql_str += c;
        return this;
    }
    return c;
};

/**
 * Creates a clone of the query, using the current cql string as base.
 * @return {Query} A clone of the query object.
 */
Query.prototype.clone = function(){
    return new Query(this.table, this.cql_str);
};

/**
 * Executes the query against the database.
 * @param  {Function} callback The callback from Cassandra.
 * @return {String}            Returns the serialized query.
 */
Query.prototype.execute = function(options, callback){
    var serializedQuery = this.toString(),
        consistency;

    if (arguments.length < 2){
        callback = _.isFunction(options) ? options : null;
        options = {};
    }
    options = (!_.isObject(options)) ? {} : options;

    if(!Query.driver){
        //Due to how modules/dependencies are loaded and executed, we need to do this require here, the first time, or we can't access client.
        Query.driver = require("../index");
        if(!Query.driver.isConnected()){
            throw new Error("Client has not been created yet. Use Cassanova.createClient([options]);");
        }
    }

    options.consistency = options.consistency || Query.consistencies.default;
    Query.driver.cql(serializedQuery, options, callback);

    return serializedQuery;
};

/**
 * Clears the stored CQL statement
 */
Query.prototype.clear = function(){
    this.cql_str = "";
};

/**
 * Allows a pure CQL statement to be used for the model.
 * @param {String}   cql       The CQL statement for the query
 */
Query.prototype.CQL = function(cql){
    this.cql_str = cql;

    return this;
};

/**
 * The driver which the query will execute.
 */
Query.driver = null;

/**
 * Various utilities used for validation and formatting.
 */
Query.utils = {
    space: " ",
    verifySchemaKey: function(key, value, scope, keyOnly){
        var schema = scope.table.schema.structure[key],
            isValid,
            doValidate = /^[0-9a-zA-Z]+$/.test(value);//The the value has been process, ie. Token(123), or similar, ignore.

        if(Query.skipSchemaValidation || !doValidate){
            return true;
        }

        if(!schema){
            throw new Error("The column, " + key + ", is not found in the schema for the table, " + scope.table.name);
        }


        //We may only have the column name to validate that it si in the schema.
        if(!keyOnly){
            isValid = schema.validate(value);
            if(!isValid){
                throw new Error("The value, " + value + ", for key, " + key + ", is invalid. Expecting " + schema.type);
            }
        }

        return true;
    },
    addQuotes: function(value){
        return "'" + value + "'";
    },
    checkQuoteNeed: function(value){
        var result = value;

        if(_.isString(value)){
            result = this.addQuotes(value);
        }

        return result;
    },
    checkQuoteNeeds: function(arr){
        var result = [],
            item,
            i;

        for(i=0; i<arr.length; i++){
            item = arr[i];
            result.push(this.checkQuoteNeed(item));
        }
        return result;
    },
    //Removes black-listed properties from the data
    scrubCollectionData: function(collection, blacklist){
        var obj,
            result = [],
            prop,
            i,
            j,
            collLen,
            listLen;

        blacklist = blacklist || ["__columns"];

        if(!collection || !collection.length){
            return result;
        }

        collLen = collection.length;

        for(i=0; i<collLen; i++){
            obj = collection[i];

            //delete null properties
            for(prop in obj){
                if(obj[prop] === null){
                    delete obj[prop];
                }
            }

            listLen = blacklist.length;
            for(j=0; j<listLen; j++){
                delete obj[blacklist[j]];
            }
            //These are javascript objects and not json, so we have to do this magic to strip it of get methods.
            result.push(JSON.parse(JSON.stringify(obj)));
        }

        return result;
    }
};

/**
 * Serializes the query into a CQL statement.
 * @return {String}            The CQL query statement.
 */
Query.prototype.toString = function(){
    var semicolon = (this.cql_str.indexOf(";") === -1 && this.cql_str !== "") ? ";" : "",
        result;

    result = this.cql_str.trim();
    return result + semicolon;
};

Query.consistencies = {
    ANY:          0x00,
    any:          0x00,
    one:          0x01,
    ONE:          0x01,
    two:          0x02,
    TWO:          0x02,
    three:        0x03,
    THREE:        0x03,
    quorum:       0x04,
    QUORUM:       0x04,
    all:          0x05,
    ALL:          0x05,
    localQuorum:  0x06,
    LOCAL_QUORUM:  0x06,
    eachQuorum:   0x07,
    EACH_QUORUM:   0x07,
    localOne:     0x10,
    LOCAL_ONE:     0x10,
    default:        0x04
};

module.exports = exports = Query;
