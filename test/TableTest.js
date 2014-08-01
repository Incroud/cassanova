/*jslint node: true */
/* global it, before, after, describe, xit, xdescribe */
"use strict";

var should = require('should'),
    common = require("./lib/common"),
    Table = require('../lib/table'),
    Schema = require('../lib/schema');

before(function(done){

    done();
});

after(function(done){
    done();
});

describe("Cassanova Table Tests",function(){
    it("Should throw error if instantiating a table without a name, although the Cassanove.model API should be used", function(done) {
        var table;

        (function(){
            table = new Table(null, new Schema({id:Schema.Type.UUID().PRIMARY_KEY()}));
        }).should.throw("Attempting to instantiate a table without a valid name.");

        (function(){
            table = new Table(1, new Schema({id:Schema.Type.UUID().PRIMARY_KEY()}));
        }).should.throw("Attempting to instantiate a table without a valid name.");

        done();
    });

    it("Should throw error if instantiating a table without a valid schema.", function(done) {
        var table;

        (function(){
            table = new Table("users");
        }).should.throw("Attempting to instantiate a table without a valid schema.");

        (function(){
            table = new Table("users", {id:Schema.Type.UUID().PRIMARY_KEY()});
        }).should.throw("Attempting to instantiate a table without a valid schema.");

        done();
    });

    it("Should create a valid table", function(done) {
        var table;

        (function(){
            table = new Table("users", new Schema({id:Schema.Type.UUID().PRIMARY_KEY()}));
        }).should.not.throw();

        (table.name).should.equal("users");
        (typeof table.schema !== "undefined").should.equal(true);
        (table.schema.structure.id.type).should.equal("uuid");
        (table.schema.structure.id.isPrimary).should.equal(true);

        done();
    });
});
