/*jslint node: true */
/* global it, before, after, describe, xit, xdescribe */
"use strict";

var should = require('should'),
    Schema = require('../lib/schema');

describe("Cassanova Schema Tests",function(){
    it('Should properly detect a missing PRIMARY KEY', function(done) {
        var schema;

        (function(){
            schema = new Schema({
                id: Schema.Type.UUID(),
                username: Schema.Type.TEXT()
            });
        }).should.throw("No primary key has been set.");

        done();
    });
    it("Should create a valid schema", function(done) {
        var schema = new Schema({
            id: Schema.Type.UUID().PRIMARY_KEY(),
            username: Schema.Type.TEXT()
        });

        (schema.structure.id.isPrimary).should.equal(true);
        (schema.structure.id.type).should.equal(Schema.Type.UUID().type);
        (schema.structure.username.type).should.equal(Schema.Type.TEXT().type);
        done();
    });
    it("Should validate schema using PRIMARY KEY as a key/value", function(done) {
        var schema,
            schema2,
            schema3,
            schema4,
            schema5,
            schema6;

        (function(){
            schema = new Schema({
                id: Schema.Type.UUID(),
                username: Schema.Type.TEXT(),
                PRIMARY_KEY: Schema.Type.PRIMARY_KEY("id")
            });
        }).should.not.throw();

        (function(){
            schema2 = new Schema({
                id: Schema.Type.UUID(),
                username: Schema.Type.TEXT(),
                PRIMARY_KEY: Schema.Type.PRIMARY_KEY(["id", "username"])
            });
        }).should.not.throw();

        (function(){
            schema3 = new Schema({
                id: Schema.Type.UUID(),
                username: Schema.Type.TEXT(),
                PRIMARY_KEY: Schema.Type.PRIMARY_KEY("email")
            });
        }).should.throw("Attemped to assign, as PRIMARY KEY, an unknown key : email");

        (function(){
            schema4 = new Schema({
                id: Schema.Type.UUID(),
                username: Schema.Type.TEXT(),
                PRIMARY_KEY: Schema.Type.PRIMARY_KEY(["id", "email"])
            });
        }).should.throw("Attemped to assign, as PRIMARY KEY, an unknown key : email");

        (function(){
            schema5 = new Schema({
                id: Schema.Type.UUID(),
                username: Schema.Type.TEXT(),
                email: Schema.Type.TEXT(),
                PRIMARY_KEY: Schema.Type.PRIMARY_KEY([["id", "email"], "username"])
            });
        }).should.not.throw();

        (function(){
            schema6 = new Schema({
                id: Schema.Type.UUID(),
                username: Schema.Type.TEXT(),
                PRIMARY_KEY: Schema.Type.PRIMARY_KEY(12345)
            });
        }).should.throw("Attemped to assign an incorrect type as PRIMARY KEY. It must be a string or an array.");

        (schema.structure.id.isPrimary).should.equal(true);
        (schema2.structure.id.isPrimary).should.equal(true);
        (schema2.structure.username.isPrimary).should.equal(true);
        (schema5.structure.id.isPrimary).should.equal(true);
        (schema5.structure.email.isPrimary).should.equal(true);
        (schema5.structure.username.isPrimary).should.equal(true);
        done();
    });
    it("Should throw an error for not using a SchemaType", function(done) {
        var schema;
        
        (function(){
            schema = new Schema({
                id: "NotASchemaType",
                username: Schema.Type.TEXT()
            });
        }).should.throw("Expected id to be of type SchemaType.");

        done();
    });
    it("Should properly detect invalid references in the data", function(done) {
        var schema = new Schema({
            id: Schema.Type.UUID().PRIMARY_KEY(),
            username: Schema.Type.TEXT()
        }),
        model = {
            id:"550e8400-e29b-41d4-a716-446655440000",
            email:"abc123",
            username:"Foo"
        };

        (function(){
            schema.validate(model);
        }).should.throw("Attempted to validate a model value that is not in the schema: email:abc123");

        done();
    });
    it("Should properly validate the data", function(done) {
        var schema = new Schema({
            ascii: Schema.Type.ASCII().PRIMARY_KEY(),
            bigint: Schema.Type.BIGINT(),
            blob: Schema.Type.BLOB(),
            boolean: Schema.Type.BOOLEAN(),
            counter: Schema.Type.COUNTER(),
            decimal: Schema.Type.DECIMAL(),
            double: Schema.Type.DOUBLE(),
            float: Schema.Type.FLOAT(),
            inet: Schema.Type.INET(),
            int: Schema.Type.INT(),
            list: Schema.Type.LIST(Schema.Type.TEXT()),
            map: Schema.Type.MAP(Schema.Type.TEXT(), Schema.Type.TEXT()),
            map2: Schema.Type.MAP(Schema.Type.TEXT(), Schema.Type.TEXT()),
            set: Schema.Type.SET(Schema.Type.TEXT()),
            text: Schema.Type.TEXT(),
            timestamp: Schema.Type.TIMESTAMP(),
            uuid: Schema.Type.UUID(),
            timeuuid: Schema.Type.TIMEUUID(),
            varchar: Schema.Type.VARCHAR(),
            varint: Schema.Type.VARINT()
        }),
        model = {
            ascii: "abc123",
            bigint: 123456789,
            blob: "notReallyABlob",
            boolean: false,
            counter: 123,
            double: 123456789123456789,
            float: 3.14159,
            inet: "127.0.0.1",
            int: 1976,
            list: ["foo", "bar"],
            map: ["userid", "age"],
            map2: {foo:"bar"},
            set: ["test@email.com", "test2@email.com"],
            text: "HELLO WORLD",
            timestamp: 1401238326246,
            uuid: "550e8400-e29b-41d4-a716-446655440000",
            timeuuid: "550e8400-e29b-41d4-a716-446655440000",
            varchar: "Lots o' text can go here.",
            varint: 1234567890
        };

        (function(){
            schema.validate(model);
        }).should.not.throw();

        done();
    });
});
