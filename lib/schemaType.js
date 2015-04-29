/**
 * A schema data type for use with Cassandra
 * @param  {String} type       The data type associated with Cassandra
 * @param  {Function} validation A function to validate that the value associated matches the type.
 * @param  {Object} wrapper  Wraps the schema value if necessary
 * @param  {Object} params  Additional parameters that the schema type may need.
 * @return {[type]}            [description]
 */
function SchemaType(type, validation, wrapper, params){
    this.type = type;
    this.validate = validation;
    this.wrapper = wrapper;
    this.parameters = params;
    this.isPrimary = false;

    chainPrimaryKey();
}

/**
 * A helper method to add ability to chain the primary key to another schema type.
 */
function chainPrimaryKey(){
    this.PRIMARY_KEY = SchemaType.prototype.PRIMARY_KEY;
}

/**
 * Used to format values when generating CQL statements.
 */
SchemaType.WRAPPERS = {
    NONE: {
        start: "",
        end: ""
    },
    DOUBLE_QUOTES: {
        start: "\"",
        end: "\""
    },
    SINGLE_QUOTES: {
        start: "'",
        end: "'"
    },
    CURLY_BRACKETS: {
        start: "{",
        end: "}"
    },
    BRACKETS: {
        start: "[",
        end: "]"
    }
}; 

/**
 * Allows SchemaTypes to be defined as PRIMARY KEYS
 * var sType = SchemaType.ASCII().PRIMARY_KEY();
 */
SchemaType.prototype.PRIMARY_KEY = function(){
    this.isPrimary = true;

    return this;
};

/**
 * Generates Primary keys. They keys are multidimensional arrays
 * @param {[type]} keys [description]
 */
SchemaType.PRIMARY_KEY = function(keys){

    return new SchemaType("primary", function(){
        return (this.parameters === null || typeof this.parameters === 'undefined' || typeof this.parameters === 'string' || this.parameters instanceof Array);
    }, true, keys);
};

/**
 * Creates an ASCII SchemaType. Validates the value.
 * var sType = SchemaType.ASCII();
 */
SchemaType.ASCII = function(){

    return new SchemaType("ascii", function(value){
        return /^[\x00-\x7F]+$/.test(value);
    }, SchemaType.WRAPPERS.SINGLE_QUOTES);
};

/**
 * Creates an BIGINT SchemaType. Validates the value.
 * var sType = SchemaType.BIGINT();
 */
SchemaType.BIGINT = function(){

    return new SchemaType("bigint", function(value){
        return (typeof value === 'number' && (value % 1) === 0);
    }, SchemaType.WRAPPERS.NONE);
};

/**
 * Creates an BLOB SchemaType. Validates the value.
 * var sType = SchemaType.BLOB();
 */
SchemaType.BLOB = function(){

    return new SchemaType("blob", function(){
        return true;
    }, SchemaType.WRAPPERS.NONE);
};

/**
 * Creates an BOOLEAN SchemaType. Validates the value.
 * var sType = SchemaType.BOOLEAN();
 */
SchemaType.BOOLEAN = function(){

    return new SchemaType("boolean", function(value){
        value = value.toString().toLowerCase();
       return (value == "true" || value == "false");
    }, SchemaType.WRAPPERS.NONE);
};

/**
 * Creates an COUNTER SchemaType. Validates the value.
 * var sType = SchemaType.COUNTER();
 */
SchemaType.COUNTER = function(){

    return new SchemaType("counter", function(value){
        return (typeof value === 'number' && (value % 1) === 0);
    }, SchemaType.WRAPPERS.NONE);
};

/**
 * Creates an DECIMAL SchemaType. Validates the value.
 * var sType = SchemaType.DECIMAL();
 */
SchemaType.DECIMAL = function(){

    return new SchemaType("decimal", function(value){
        return (typeof value === 'number' && (value % 1) !== 0);
    }, SchemaType.WRAPPERS.NONE);
};

/**
 * Creates an DOUBLE SchemaType. Validates the value.
 * var sType = SchemaType.DOUBLE();
 */
SchemaType.DOUBLE = function(){

    return new SchemaType("double", function(value){
        return (typeof value === 'number' && (value % 1) === 0);
    }, SchemaType.WRAPPERS.NONE);
};

/**
 * Creates an FLOAT SchemaType. Validates the value.
 * var sType = SchemaType.FLOAT();
 */
SchemaType.FLOAT = function(){

    return new SchemaType("float", function(value){
        return (typeof value === 'number' && (value % 1) !== 0);
    }, SchemaType.WRAPPERS.NONE);
};

/**
 * Creates an INET SchemaType. Validates the value.
 * var sType = SchemaType.INET();
 */
SchemaType.INET = function(){

    return new SchemaType("inet", function(value){
        return /\b(?:\d{1,3}\.){3}\d{1,3}\b/.test(value);
    }, SchemaType.WRAPPERS.SINGLE_QUOTES);
};

/**
 * Creates an INT SchemaType. Validates the value.
 * var sType = SchemaType.INT();
 */
SchemaType.INT = function(){

    return new SchemaType("int", function(value){
        return (typeof value === 'number' && (value % 1) === 0);
    }, SchemaType.WRAPPERS.NONE);
};

/**
 * Creates an LIST SchemaType. Validates the value.
 * var sType = SchemaType.LIST();
 */
SchemaType.LIST = function(type){

    if(!type || !(type instanceof SchemaType)){
        throw new TypeError("List requires a SchemaType as it's argument.");
    }

    return new SchemaType("list", function(value){
        var result = (value instanceof Array),
            i;

        if(result){
            for(i=0; i<value.length; i++){
                if(!this.parameters.validate(value[i])){
                    return false;
                }
            }
        }
        return result;
    }, SchemaType.WRAPPERS.BRACKETS, type);
};

/**
 * Creates an MAP SchemaType. Validates the value.
 * var sType = SchemaType.MAP();
 */
SchemaType.MAP = function(key, value){

    if(!key || !(key instanceof SchemaType) || !value || !(value instanceof SchemaType)){
        throw new TypeError("Map requires key and value SchemaTypes as it's arguments.");
    }

    return new SchemaType("map", function(value){
        var valKey,
            i,
            arrVal,
            result = (value instanceof Array) || (value instanceof Object),
            isArray = (value instanceof Array);

        if(!isArray){
            for(valKey in value){
                if(!this.parameters.key.validate(valKey)){
                    return false;
                }
                if(!this.parameters.value.validate(value[valKey])){
                    return false;
                }
            }
        }else{
            for(i=0; i<value.length; i++){
                arrVal = value[i];
                for(valKey in arrVal){
                    if(!this.parameters.key.validate(valKey)){
                        return false;
                    }
                    if(!this.parameters.value.validate(arrVal[valKey])){
                        return false;
                    }
                }
            }
        }

        return result;
    }, SchemaType.WRAPPERS.CURLY_BRACKETS, {key:key, value:value});
};

/**
 * Creates an SET SchemaType. Validates the value.
 * var sType = SchemaType.SET();
 */
SchemaType.SET = function(type){

    if(!type || !(type instanceof SchemaType)){
        throw new TypeError("Set requires a SchemaType as it's argument.");
    }

    return new SchemaType("set", function(value){
        var result = (value instanceof Array),
            i;

        if(result){
            for(i=0; i<value.length; i++){
                if(!this.parameters.validate(value[i])){
                    return false;
                }
            }
        }
        return result;
    }, SchemaType.WRAPPERS.CURLY_BRACKETS, type);
};

/**
 * Creates an TEXT SchemaType. Validates the value.
 * var sType = SchemaType.TEXT();
 */
SchemaType.TEXT = function(){

    return new SchemaType("text", function(value){
        return (typeof value === "string");
    }, SchemaType.WRAPPERS.SINGLE_QUOTES);
};

/**
 * Creates an TIMESTAMP SchemaType. Validates the value.
 * var sType = SchemaType.TIMESTAMP();
 */
SchemaType.TIMESTAMP = function(){

    return new SchemaType("timestamp", function(value){
        return (new Date(value).getTime() > 0);
    }, SchemaType.WRAPPERS.SINGLE_QUOTES);
};

/**
 * Creates an UUID SchemaType. Validates the value.
 * var sType = SchemaType.UUID();
 */
SchemaType.UUID = function(){

    return new SchemaType("uuid", function(value){
        return /(\w{8}(-\w{4}){3}-\w{12}?)/.test(value);
    }, SchemaType.WRAPPERS.NONE);
};

/**
 * Creates an TIMEUUID SchemaType. Validates the value.
 * var sType = SchemaType.TIMEUUID();
 */
SchemaType.TIMEUUID = function(){

    return new SchemaType("timeuuid", function(value){
        return /(\w{8}(-\w{4}){3}-\w{12}?)/.test(value);
    }, SchemaType.WRAPPERS.NONE);
};

/**
 * Creates an VARCHAR SchemaType. Validates the value.
 * var sType = SchemaType.VARCHAR();
 */
SchemaType.VARCHAR = function(){

    return new SchemaType("varchar", function(value){
        return (typeof value === "string");
    }, SchemaType.WRAPPERS.SINGLE_QUOTES);
};

/**
 * Creates an VARINT SchemaType. Validates the value.
 * var sType = SchemaType.VARINT();
 */
SchemaType.VARINT = function(){

    return new SchemaType("varint", function(value){
        return (typeof value === 'number' && (value % 1) === 0);
    }, SchemaType.WRAPPERS.NONE);
};

module.exports = exports = SchemaType;
