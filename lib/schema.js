var SchemaType = require('./schemaType');

function Schema(obj) {

    this.structure = obj;

    Schema.verifyStructure(this.structure);
}

/**
 * Upon instantiation os a Schema, the structure is verified to make sure that all types are appropriate and accounted for.
 * @param  {Object} obj The structure of the Schema
 */
Schema.verifyStructure = function(obj){
    var objKey,
        structVal,
        structKey,
        hasPrimary = false,
        pkParams;

    for(objKey in obj){
        structVal = obj[objKey];

        if(objKey.toLowerCase() !== "primary_key"){
            if(structVal instanceof SchemaType === false){
                throw new TypeError("Expected " + objKey + " to be of type SchemaType.");
            }
            if(structVal.isPrimary){
                hasPrimary = true;
            }
        }else{
            pkParams = structVal.parameters;
            if(typeof pkParams === 'string'){
                structKey = obj[pkParams];
                if(!structKey){
                    throw new TypeError("Attemped to assign, as PRIMARY KEY, an unknown key : " + pkParams);
                }else{
                    structKey.isPrimary = true;
                }
            }else if(pkParams instanceof Array){
                Schema.verifyCompositeKey(obj, pkParams);
            }else{
                throw new TypeError("Attemped to assign an incorrect type as PRIMARY KEY. It must be a string or an array.");
            }
            hasPrimary = true;
        }
    }
    if(!hasPrimary){
        throw new Error("No primary key has been set.");
    }
};

/**
 * Recursively verifies the composite keys
 * @param  {Object} structure The Schema structure
 * @param  {Object} key       The key to verify, which can be a String or an Array
 */
Schema.verifyCompositeKey = function(structure, key){
    var len = key.length,
        structKey,
        keyVal,
        j;

    for(j=0; j<len; j++){
        keyVal = key[j];
        if(keyVal instanceof Array){
            Schema.verifyCompositeKey(structure, keyVal);
        }else{
            structKey = structure[keyVal];
            if(!structKey){
                throw new TypeError("Attemped to assign, as PRIMARY KEY, an unknown key : " + keyVal);
            }else{
                structKey.isPrimary = true;
            }
        }
    }
};

//Reference to SchemaType as to not need another "require" to create them.
Schema.Type = SchemaType;

/**
 * Validates that the data from the model adheres to the Schema.
 * @param  {Object} obj The model data to compare to the Schema.
 */
Schema.prototype.validate = function(obj){
    var objKey,
        objVal,
        structVal;

    for(objKey in obj){
        objVal = obj[objKey];
        structVal = this.structure[objKey];
        if(!structVal){
            throw new ReferenceError("Attempted to validate a model value that is not in the schema: " + objKey + ":" + objVal);
        }else{
            if(!structVal.validate(objVal)){
                throw new TypeError("Expected " + objVal + " to be of type " + structVal.type + ".");
            }
        }
    }

    return true;
};

module.exports = exports = Schema;
