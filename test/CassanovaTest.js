/*jslint node: true */
/* global it, before, after, describe, xit, xdescribe */
"use strict";

var should = require('should'),
    Cassanova = require('../index'),
    common = require("./lib/common"),
    exec = require('child_process').exec;

before(function(done){

    exec("node CQL -f test/before.cql", function(error, stdout, stderr){

        if(error){
            throw error;
        }

        console.log('Setting up db for cassanova tests : ');

        done();
    });
});

after(function(done){
    exec("node CQL -f test/before.cql", function(error, stdout, stderr){
        if(error){
            throw error;
        }

        console.log('Cleaning up db for cassanova tests : ');

        done();
    });
});

beforeEach(function(done){
    Cassanova.models = {};
    Cassanova.schemas = {};
    Cassanova.tables = {};
    done();
});

describe("Cassanova Models", function(){
    it("Should have access to driver", function(done){
        (Cassanova.driver === null).should.equal(false);

        done();
    });
    it("Should have error trying to disconnect without having a connection", function(done){
        (Cassanova.isConnected()).should.equal(false);

        Cassanova.disconnect(function(err, res){
            (err === null).should.equal(false);
            (err.message).should.equal("No client to disconnect.");
            done();
        });
    });
    it("Should throw error when trying to connect without contactPoints", function(done){
        (Cassanova.isConnected()).should.equal(false);

        (function(){
            Cassanova.createClient({});
        }).should.throw("Creating a client requires hosts information when being created.");

        done();
    });
    it("Should have error when connecting without options.", function(done){
        (Cassanova.isConnected()).should.equal(false);

        (function(){
            Cassanova.connect(function(err, result){});
        }).should.throw("Creating a client requires hosts information when being created.");

        done();
    });
    it("Should be able to connect to DB", function(done){
        (Cassanova.isConnected()).should.equal(false);

        Cassanova.connect(common.options, function(err, result){
            (typeof err === 'undefined').should.equal(true);
            (Cassanova.isConnected()).should.equal(true);
            done();
        });
    });
    it("Should create a valid model via Cassanova.schema", function(done) {
        var table = Cassanova.Table("test", Cassanova.Schema({
                id : Cassanova.SchemaType.UUID().PRIMARY_KEY()
            })),
            Model = Cassanova.Model("test1", table),
            m = new Model();

        (m.table.schema.structure.id.type).should.equal(Cassanova.SchemaType.UUID().type);
        done();
    });
    it("Should create a valid model with a prototype method", function(done) {
        var table = Cassanova.Table("test", Cassanova.Schema({
                id : Cassanova.SchemaType.UUID().PRIMARY_KEY()
            })),
            Model = Cassanova.Model("test1", table),
            m = new Model();

        Model.prototype.test = function(){
            done();
        };
        m.test();
    });
    it("Should create a valid model", function(done) {
        var table = Cassanova.Table("test", Cassanova.Schema({
                id : Cassanova.SchemaType.UUID().PRIMARY_KEY()
            })),
            Model = Cassanova.Model("test1", table),
            m = new Model();

        (m.table.schema.structure.id.type).should.equal(Cassanova.SchemaType.UUID().type);
        done();
    });
    it("Should create a distinct instances of model", function(done) {
        var table = Cassanova.Table("test", Cassanova.Schema({
                id : Cassanova.SchemaType.UUID().PRIMARY_KEY()
            })),
            Model = Cassanova.Model("test1", table),
            m1 = new Model(),
            m1q = m1.save({id : '4febf030-e461-11e3-ac10-0800200c9a66'}),
            m2 = new Model(),
            m2q = m2.save({id : '71c20c30-e461-11e3-ac10-0800200c9a66'});

        (m1.table.schema.structure.id.type).should.equal(Cassanova.SchemaType.UUID().type);
        (m2.table.schema.structure.id.type).should.equal(Cassanova.SchemaType.UUID().type);

        (m1).should.not.equal(m2);

        (m1q.toString()).should.equal("INSERT INTO test (id) VALUES (4febf030-e461-11e3-ac10-0800200c9a66);");
        (m2q.toString()).should.equal("INSERT INTO test (id) VALUES (71c20c30-e461-11e3-ac10-0800200c9a66);");
        done();
    });
    it("Should properly instantiate models with 'new'", function(done) {
        var table = Cassanova.Table("test", Cassanova.Schema({
                id : Cassanova.SchemaType.UUID().PRIMARY_KEY()
            })),
            M1 = Cassanova.Model("test1", table),
            M2 = Cassanova.Model("test1"),
            M3 = Cassanova.Model("test1"),
            MA = new M2(),
            MB = new M2();

        (M1).should.equal(M2);
        (M2).should.equal(M3);
        (MA).should.not.equal(MB);

        (typeof M1).should.equal("function");
        (typeof M2).should.equal("function");
        (typeof MA).should.equal("object");
        (typeof MB).should.equal("object");
        done();
    });
    it("Should create a model schema from an object", function(done) {
        var table = Cassanova.Table("test", Cassanova.Schema({
                id : Cassanova.SchemaType.UUID().PRIMARY_KEY()
            })),
            Model = Cassanova.Model("test1", table),
            m = new Model();

        (m.table.schema.structure.id.type).should.equal(Cassanova.SchemaType.UUID().type);
        done();
    });
    it("Should not create a model without a name", function(done) {

        (function(){
            Cassanova.Model();
        }).should.throw("Attempted to create a model with an invalid name.");

        done();
    });
    it("Should not create a model without a table", function(done) {

        (function(){
            Cassanova.Model("test");
        }).should.throw("Attempted to retrieve a model that doesn't exist or create a model with an invalid table.");

        done();
    });
    it("Should throw an error when creating a model with an invalid table", function(done) {

        (function(){
            Cassanova.Model("test", {});
        }).should.throw("Attempted to retrieve a model that doesn't exist or create a model with an invalid table.");

        done();
    });
    it("Should throw an error when retrieving a model that doesn't exist", function(done) {

        (function(){
            Cassanova.Model("missing");
        }).should.throw("Attempted to retrieve a model that doesn't exist or create a model with an invalid table.");

        done();
    });
    it("Should retrieve the same model that was originally created", function(done) {
        var table = Cassanova.Table("test", Cassanova.Schema({
                id : Cassanova.SchemaType.UUID().PRIMARY_KEY()
            })),
            model = Cassanova.Model("test1", table),
            model2 = Cassanova.Model("test1");

        (model).should.equal(model2);
        done();
    });
    it("Should throw an error for attempting to overwrite a model with a different table with a different name", function(done) {
        var table = Cassanova.Table("test", Cassanova.Schema({
                id : Cassanova.SchemaType.UUID().PRIMARY_KEY()
            })),
            model = Cassanova.Model("user", table),
            table2 = Cassanova.Table("test2", Cassanova.Schema({
                id : Cassanova.SchemaType.TEXT().PRIMARY_KEY()
            }));

        (table).should.not.equal(table2);

        (function(){
            Cassanova.Model("user", table2);
        }).should.throw("Attempting to overwrite a model with a different table : user");

        done();
    });
    it("Should retrieve a known table", function(done) {
        var table = Cassanova.Table("test", Cassanova.Schema({
                id : Cassanova.SchemaType.UUID().PRIMARY_KEY()
            })),
            table2 = Cassanova.Table("test");

        (table).should.equal(table2);

        done();
    });
    it("Should throw an error for attempting to overwrite a table schema", function(done) {
        var table = Cassanova.Table("test", Cassanova.Schema({
                id : Cassanova.SchemaType.UUID().PRIMARY_KEY()
            })),
            table2;

        (function(){
            table2 = Cassanova.Table("test", Cassanova.Schema({
                userid : Cassanova.SchemaType.TEXT().PRIMARY_KEY()
            }));
        }).should.throw("Attempting to overwrite the schema for table : test");

        done();
    });
    it("Should throw an error saving non-schema data to a model", function(done) {
        var table = Cassanova.Table("user", Cassanova.Schema({
                id : Cassanova.SchemaType.UUID().PRIMARY_KEY(),
                firstname : Cassanova.SchemaType.TEXT(),
                lastname:Cassanova.SchemaType.TEXT()
            })),
            Model = Cassanova.Model("user", table),
            m = new Model();

        (function(){
             m.save({id:"80398220-e461-11e3-ac10-0800200c9a66", middleinitial:"e"}, {execute:false}, function(err, result){
            }, false);
        }).should.throw("The column, middleinitial, is not found in the schema for the table, user");

        done();
    });
    it("Should add TTL to the model query", function(done) {
        var table = Cassanova.Table("user", Cassanova.Schema({
                id : Cassanova.SchemaType.UUID().PRIMARY_KEY(),
                firstname : Cassanova.SchemaType.TEXT(),
                lastname:Cassanova.SchemaType.TEXT()
            })),
            Model = Cassanova.Model("user", table),
            m = new Model(),
            query = m.save({id:"80398220-e461-11e3-ac10-0800200c9a66"}, {TTL:1234567, execute:false}, function(err, result){
                console.log(err);
            });

            (query.toString()).should.equal("INSERT INTO user (id) VALUES (80398220-e461-11e3-ac10-0800200c9a66) USING TTL 1234567;");
            done();
    });
    it("Should create a CQL query from the model", function(done) {
        var table = Cassanova.Table("user", Cassanova.Schema({
                id : Cassanova.SchemaType.UUID().PRIMARY_KEY(),
                firstname : Cassanova.SchemaType.TEXT(),
                lastname:Cassanova.SchemaType.TEXT()
            })),
            Model = Cassanova.Model("user", table),
            m = new Model(),
            query = m.CQL("SELECT * FROM users;", function(){

            }, false);

            (query.toString()).should.equal("SELECT * FROM users;");
            done();
    });
    //model.js - comment out the 'enforce instantiation' section to use this.
    // it("Should not be able to call find without instantiating.", function(done) {
    //     var table = Cassanova.Table("user", Cassanova.Schema({
    //             id : Cassanova.SchemaType.UUID().PRIMARY_KEY(),
    //             firstname : Cassanova.SchemaType.TEXT(),
    //             lastname:Cassanova.SchemaType.TEXT()
    //         })),
    //         model = Cassanova.Model("user", table);

    //     (typeof model.find).should.equal("undefined");

    //      done();
    // });
    it("Should be able to create a Query", function(done) {
        var q = new Cassanova.Query().CQL("SELECT * FROM users;");

        (q.toString()).should.equal("SELECT * FROM users;");

         done();
    });
});
