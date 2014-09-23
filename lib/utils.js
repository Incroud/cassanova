/*jslint node: true */
"use strict";

//Removes black-listed properties from the data
var scrubCollectionData = function(collection, blacklist){
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
        result.push(obj);
    }

    return result;
};

module.exports.scrubCollectionData = scrubCollectionData;
