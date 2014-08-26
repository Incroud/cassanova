/*jslint node: true */
/* global it, before, after, describe, xit, xdescribe */
"use strict";

var should = require('should'),
    common = require("./lib/common"),
    Model = require('../lib/model'),
    Table = require('../lib/table'),
    Schema = require('../lib/schema');

before(function(done){
    done();
});

after(function(done){
    done();
});


describe("Cassanova Model Tests", function(){
    it("Should instantiate a model, although the Cassanove.model API should be used", function(done) {
        var table,
            model,
            schema = new Schema({
                id: Schema.Type.UUID().PRIMARY_KEY(),
                username: Schema.Type.TEXT()
            });

        (function(){
            table = new Table("users", schema);
            model = new Model("userModel", table);
        }).should.not.throw();

        done();
    });

    it("Should throw error if instantiating a model without a name, although the Cassanove.model API should be used", function(done) {
        var table,
            model,
            schema = new Schema({
                id: Schema.Type.UUID().PRIMARY_KEY(),
                username: Schema.Type.TEXT()
            });

        (function(){
            table = new Table("users", schema);
            model = new Model(null, table);
        }).should.throw("Attempting to instantiate a model without a valid name. Create models using the Cassanova.model API.");

        done();
    });

    it("Should throw error if instantiating a model without a valid schema, although the Cassanove.model API should be used", function(done) {
        var model,
            table,
            schema = new Schema({
                id: Schema.Type.UUID().PRIMARY_KEY(),
                username: Schema.Type.TEXT()
            });

        (function(){
            table = new Table("users", schema);
            model = new Model("test", null);
        }).should.throw("Attempting to instantiate a model, test, without a valid table. Create models using the Cassanova.model API.");

        done();
    });

    it("Should be able to create a query from the model", function(done) {
        var table,
            schema = new Schema({
                id: Schema.Type.UUID().PRIMARY_KEY(),
                username: Schema.Type.TEXT()
            }),
            model,
            query;

        table = new Table("users", schema);
        model = new Model("qModel", table);
        query = model.Query();

        query.should.not.equal(null);

        done();
    });

    it("Should be able to execute CQL from the model", function(done) {
        var table,
            schema = new Schema({
                id: Schema.Type.UUID().PRIMARY_KEY(),
                username: Schema.Type.TEXT()
            }),
            model,
            query;

        table = new Table("users", schema);
        model = new Model("qModel", table);
        query = model.Query();

        model.CQL("select * from users;", {execute:false}).should.not.equal(null);

        done();
    });
    it("Should generate a proper find where query", function(done) {
        var table,
            schema = new Schema({
                id: Schema.Type.UUID().PRIMARY_KEY(),
                username: Schema.Type.TEXT(),
                fname: Schema.Type.TEXT()
            }),
            model,
            query,
            query2;

        table = new Table("users", schema);
        model = new Model("qModel", table);
        query = model.find({username:"jeb"}, {execute:false});
        query2 = model.find({username:"jeb", fname:"James"}, {execute:false});

        query.toString().should.equal("SELECT * FROM users WHERE username = 'jeb';");
        query2.toString().should.equal("SELECT * FROM users WHERE username = 'jeb' AND fname = 'James';");

        done();
    });
    it("Should generate a proper find query without WHERE", function(done) {
        var table,
            schema = new Schema({
                id: Schema.Type.UUID().PRIMARY_KEY(),
                username: Schema.Type.TEXT()
            }),
            model,
            query,
            query2;

        table = new Table("users", schema);
        model = new Model("qModel", table);
        query = model.find(null, {execute:false});
        query2 = model.find({}, {execute:false});

        query.toString().should.equal("SELECT * FROM users;");
        query2.toString().should.equal("SELECT * FROM users;");

        done();
    });
});

