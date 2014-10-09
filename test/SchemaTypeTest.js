/*jslint node: true */
/* global it, before, after, describe, xit, xdescribe */
"use strict";

var should = require('should'),
    SchemaType = require('../lib/schemaType');

describe("Cassanova SchemaType Tests", function(){
    describe("SchemaType Generation", function(){
        it("Should create unique SchemaType instances", function(done) {
            var sType = SchemaType.ASCII(),
                sType2 = SchemaType.ASCII("hello");

            (sType).should.not.equal(sType2);
            done();
        });
    });
    describe("PRIMARY_KEY as a TYPE Tests", function(){
        it("Should not validate for PRIMARY_KEY using object as key", function(done) {
            var sType = SchemaType.PRIMARY_KEY({name:"foo"}),
                result = sType.validate();

            (sType.type).should.equal("primary");
            (result).should.equal(false);
            done();
        });
        it("Should not validate for PRIMARY_KEY using number as key", function(done) {
            var sType = SchemaType.PRIMARY_KEY(12345),
                result = sType.validate();

            (sType.type).should.equal("primary");
            (result).should.equal(false);
            done();
        });
        it("Should validate for PRIMARY_KEY using no key", function(done) {
            var sType = SchemaType.PRIMARY_KEY(),
                result = sType.validate();

            (sType.type).should.equal("primary");
            (result).should.equal(true);
            done();
        });
        it("Should validate for PRIMARY_KEY using string as key", function(done) {
            var sType = SchemaType.PRIMARY_KEY("userid"),
                result = sType.validate();

            (sType.type).should.equal("primary");
            (result).should.equal(true);
            done();
        });
        it("Should validate for PRIMARY_KEY using null as key", function(done) {
            var sType = SchemaType.PRIMARY_KEY(null),
                result = sType.validate();

            (sType.type).should.equal("primary");
            (result).should.equal(true);
            done();
        });
        it("Should validate for PRIMARY_KEY using undefined as key", function(done) {
            var sType = SchemaType.PRIMARY_KEY(undefined),
                result = sType.validate();

            (sType.type).should.equal("primary");
            (result).should.equal(true);
            done();
        });
        it("Should validate for PRIMARY_KEY using array as key", function(done) {
            var sType = SchemaType.PRIMARY_KEY(["userid"]),
                result = sType.validate();

            (sType.type).should.equal("primary");
            (result).should.equal(true);
            done();
        });
    });
    describe("PRIMARY_KEY as a IDENTIFIER Tests", function(){
        it("Should make a type to be a primary key", function(done) {
            var sType = SchemaType.ASCII().PRIMARY_KEY(),
                sType2 = SchemaType.ASCII();

            (sType).should.not.equal(sType2);
            (sType.isPrimary).should.equal(true);
            (sType.type).should.equal("ascii");
            (sType2.isPrimary).should.equal(false);
            (sType2.type).should.equal("ascii");
            done();
        });
    });
    describe("ASCII SchemaType Tests", function(){
        it("Should properly validate ASCII SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.ASCII(),
                result = sType.validate("abc"),
                result2 = sType.validate("123"),
                result3 = sType.validate("اיהוה"),
                sType2 = SchemaType.ASCII().PRIMARY_KEY();

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("ascii");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.SINGLE_QUOTES);
            (result).should.equal(true);
            (result2).should.equal(true);
            (result3).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            done();
        });
    });
    describe("BIGINT SchemaType Tests", function(){
        it("Should properly validate BIGINT SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.BIGINT(),
                result = sType.validate(123),
                result2 = sType.validate("abcd"),
                sType2 = SchemaType.BIGINT().PRIMARY_KEY();

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("bigint");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.NONE);
            (result).should.equal(true);
            (result2).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            done();
        });
    });
    //TODO - Properly validate BLOB
    //Right now, validation allows everything.
    describe("BLOB SchemaType Tests", function(){
        it("Should properly validate BLOB SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.BLOB(),
                result = sType.validate(123),
                //result2 = sType.validate("abcd"),
                sType2 = SchemaType.BLOB().PRIMARY_KEY();

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("blob");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.NONE);
            (result).should.equal(true);
            //(result2).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            done();
        });
    });
    describe("BOOLEAN SchemaType Tests", function(){
        it("Should properly validate BOOLEAN SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.BOOLEAN(),
                result = sType.validate(true),
                result2 = sType.validate(false),
                result3 = sType.validate("abcd"),
                sType2 = SchemaType.BOOLEAN().PRIMARY_KEY();

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("boolean");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.NONE);
            (result).should.equal(true);
            (result2).should.equal(true);
            (result3).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            done();
        });
    });
    describe("COUNTER SchemaType Tests", function(){
        it("Should properly validate COUNTER SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.COUNTER(),
                result = sType.validate(123),
                result2 = sType.validate("abcd"),
                sType2 = SchemaType.COUNTER().PRIMARY_KEY();

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("counter");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.NONE);
            (result).should.equal(true);
            (result2).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            done();
        });
    });
    describe("DECIMAL SchemaType Tests", function(){
        it("Should properly validate DECIMAL SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.DECIMAL(),
                result = sType.validate(123.456),
                result2 = sType.validate(123),
                sType2 = SchemaType.DECIMAL().PRIMARY_KEY();

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("decimal");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.NONE);
            (result).should.equal(true);
            (result2).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            done();
        });
    });
    describe("DOUBLE SchemaType Tests", function(){
        it("Should properly validate DOUBLE SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.DOUBLE(),
                result = sType.validate(123),
                result2 = sType.validate(123.456),
                sType2 = SchemaType.DOUBLE().PRIMARY_KEY();

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("double");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.NONE);
            (result).should.equal(true);
            (result2).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            done();
        });
    });
    describe("FLOAT SchemaType Tests", function(){
        it("Should properly validate FLOAT SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.FLOAT(),
                result = sType.validate(123.456),
                result2 = sType.validate(123),
                sType2 = SchemaType.FLOAT().PRIMARY_KEY();

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("float");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.NONE);
            (result).should.equal(true);
            (result2).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            done();
        });
    });
    describe("INET SchemaType Tests", function(){
        it("Should properly validate INET SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.INET(),
                result = sType.validate("192.168.0.1"),
                result2 = sType.validate("127.0.0"),
                sType2 = SchemaType.INET().PRIMARY_KEY();

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("inet");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.SINGLE_QUOTES);
            (result).should.equal(true);
            (result2).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            done();
        });
    });
    describe("INT SchemaType Tests", function(){
        it("Should properly validate INT SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.INT(),
                result = sType.validate(123),
                result2 = sType.validate(123.456),
                sType2 = SchemaType.INT().PRIMARY_KEY();

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("int");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.NONE);
            (result).should.equal(true);
            (result2).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            done();
        });
    });
    describe("LIST SchemaType Tests", function(){
        it("Should properly validate LIST SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.LIST(SchemaType.TEXT()),
                result = sType.validate(["userid", "age"]),
                result2 = sType.validate("abc"),
                sType2 = SchemaType.LIST(SchemaType.TEXT()).PRIMARY_KEY(),
                sType3 = SchemaType.LIST(SchemaType.TEXT()),
                result3 = sType3.validate([123]),
                result4 = sType3.validate(["abc"]);

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("list");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.BRACKETS);
            (result).should.equal(true);
            (result2).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            (result3).should.equal(false);
            (result4).should.equal(true);

            done();
        });
    });
    describe("MAP SchemaType Tests", function(){
        it("Should properly validate MAP SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.MAP(SchemaType.TEXT(), SchemaType.TEXT()),
                result = sType.validate(["userid", "age"]),
                result2 = sType.validate({"foo":"bar"}),
                result3 = sType.validate(123),
                result4 = sType.validate("abc"),
                sType2 = SchemaType.MAP(SchemaType.TEXT(), SchemaType.TEXT()).PRIMARY_KEY(),
                sType3 = SchemaType.MAP(SchemaType.TEXT(), SchemaType.INT()),
                result5 = sType3.validate({"abc":"def"}),
                result6 = sType3.validate({"age":37}),
                result7 = sType3.validate([{"age":37}]),
                result8 = sType3.validate([{"age":"def"}]),
                result9 = sType3.validate({"age":37,"weight":160});

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("map");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.CURLY_BRACKETS);
            (result).should.equal(true);
            (result2).should.equal(true);
            (result3).should.equal(false);
            (result4).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            (result5).should.equal(false);
            (result6).should.equal(true);
            (result7).should.equal(true);
            (result8).should.equal(false);
            (result9).should.equal(true);
            done();
        });
    });
    describe("SET SchemaType Tests", function(){
        it("Should properly validate SET SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.SET(SchemaType.TEXT()),
                result = sType.validate(["test@email.com", "test2@email.com"]),
                result2 = sType.validate("abc"),
                result3 = sType.validate({foo:"bar"}),
                result4 = sType.validate(123),
                sType2 = SchemaType.SET(SchemaType.TEXT()).PRIMARY_KEY(),
                result5 = sType2.validate([123, "abc"]),
                result6 = sType2.validate(["abc", "def"]);

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("set");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.CURLY_BRACKETS);
            (result).should.equal(true);
            (result2).should.equal(false);
            (result3).should.equal(false);
            (result4).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            (result5).should.equal(false);
            (result6).should.equal(true);
            done();
        });
    });
    describe("TEXT SchemaType Tests", function(){
        it("Should properly validate TEXT SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.TEXT(),
                result = sType.validate("abc"),
                result2 = sType.validate(123),
                sType2 = SchemaType.TEXT().PRIMARY_KEY();

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("text");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.SINGLE_QUOTES);
            (result).should.equal(true);
            (result2).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            done();
        });
    });
    describe("TIMESTAMP SchemaType Tests", function(){
        it("Should properly validate TIMESTAMP SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.TIMESTAMP(),
                result = sType.validate(1401238326246),
                result2 = sType.validate("2011-02-03 04:05+000"),
                result3 = sType.validate("2011-02-03"),
                result4 = sType.validate("abc"),
                sType2 = SchemaType.TIMESTAMP().PRIMARY_KEY();

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("timestamp");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.NONE);
            (result).should.equal(true);
            (result2).should.equal(true);
            (result3).should.equal(true);
            (result4).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            done();
        });
    });
    describe("UUID SchemaType Tests", function(){
        it("Should properly validate UUID SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.UUID(),
                result = sType.validate("550e8400-e29b-41d4-a716-446655440000"),
                result2 = sType.validate("e29b-41d4-a716"),
                sType2 = SchemaType.UUID().PRIMARY_KEY();

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("uuid");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.NONE);
            (result).should.equal(true);
            (result2).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            done();
        });
    });
    describe("TIMEUUID SchemaType Tests", function(){
        it("Should properly validate TIMEUUID SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.TIMEUUID(),
                result = sType.validate("550e8400-e29b-41d4-a716-446655440000"),
                result2 = sType.validate("e29b-41d4-a716"),
                sType2 = SchemaType.TIMEUUID().PRIMARY_KEY();

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("timeuuid");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.NONE);
            (result).should.equal(true);
            (result2).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            done();
        });
    });
    describe("VARCHAR SchemaType Tests", function(){
        it("Should properly validate VARCHAR SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.VARCHAR(),
                result = sType.validate("abc"),
                result2 = sType.validate(123),
                sType2 = SchemaType.VARCHAR().PRIMARY_KEY();

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("varchar");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.SINGLE_QUOTES);
            (result).should.equal(true);
            (result2).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            done();
        });
    });
    describe("VARINT SchemaType Tests", function(){
        it("Should properly validate VARINT SchemaType and establish as PRIMARY KEY", function(done) {
            var sType = SchemaType.VARINT(),
                result = sType.validate(123),
                result2 = sType.validate(123.456),
                sType2 = SchemaType.VARINT().PRIMARY_KEY();

            (sType instanceof SchemaType).should.equal(true);
            (sType.type).should.equal("varint");
            (sType.wrapper).should.equal(SchemaType.WRAPPERS.NONE);
            (result).should.equal(true);
            (result2).should.equal(false);
            (sType2.isPrimary).should.equal(true);
            done();
        });
    });
});