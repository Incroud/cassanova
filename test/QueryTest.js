/*jslint node: true */
/* global it, before, after, describe, xit, xdescribe */
"use strict";

var should = require('should'),
    Cassanova = require('../index'),
    Query = require('../lib/query'),
    Schema = require('../lib/schema'),
    baseSchema,
    baseTable;

describe("Cassanova Query Tests", function(){

    before(function(done){
        baseSchema = new Schema({
            userid : Schema.Type.TEXT(),
            firstname : Schema.Type.TEXT(),
            birthdate : Schema.Type.TIMESTAMP(),
            lastname : Schema.Type.TEXT(),
            age : Schema.Type.INT(),
            zipcode : Schema.Type.INT(),
            PRIMARY_KEY : Schema.Type.PRIMARY_KEY("userid")
        });
        baseTable = Cassanova.Table("users", baseSchema);
        done();
    });

    after(function(done){
        Cassanova.tables = {};
        Cassanova.models = {};
        Cassanova.schemas = {};
        done();
    });

    describe("Basic Query Tests", function(){
        it("Should be able to be cloned and chained independently", function(done){
            var schema = new Schema({
                id: Schema.Type.UUID().PRIMARY_KEY(),
                username: Schema.Type.TEXT()
            }),
            query = new Query(baseTable),
            query2;

            query.SELECT("*", "users");
            query2 = query.clone();

            (query.toString()).should.equal("SELECT * FROM users;");
            (query2.toString()).should.equal("SELECT * FROM users;");
            (query2).should.not.equal(query);

            query2.WHERE_EQUALS("userid", "abcdefg");

            (query.toString()).should.equal("SELECT * FROM users;");
            (query2.toString()).should.equal("SELECT * FROM users WHERE userid = 'abcdefg';");

            done();
        });
    });
    describe("CREATE TABLE Tests", function(){
        it("Should create a valid create table", function(done){
            var schema = new Schema({
                id: Schema.Type.UUID().PRIMARY_KEY(),
                username: Schema.Type.TEXT()
            }),
            query = new Query();

            query.CREATE_TABLE('users', schema);

            (query.toString()).should.equal("CREATE TABLE users (id uuid PRIMARY KEY , username text);");
            done();
        });
        it("Should create a valid create table", function(done){
            var schema = new Schema({
                id: Schema.Type.UUID(),
                username: Schema.Type.TEXT(),
                PRIMARY_KEY: Schema.Type.PRIMARY_KEY("id")
            }),
            query = new Query();

            query.CREATE_TABLE('users', schema);

            (query.toString()).should.equal("CREATE TABLE users (id uuid, username text, PRIMARY KEY (id));");
            done();
        });
        it("Should create a valid create table with compound key", function(done){
            var schema = new Schema({
                id: Schema.Type.UUID(),
                username: Schema.Type.TEXT(),
                PRIMARY_KEY: Schema.Type.PRIMARY_KEY(["id", "username"])
            }),
            query = new Query();

            query.CREATE_TABLE('users', schema);

            (query.toString()).should.equal("CREATE TABLE users (id uuid, username text, PRIMARY KEY (id, username));");
            done();
        });
        it("Should create a valid create table with partition key", function(done){
            var schema = new Schema({
                id: Schema.Type.UUID(),
                username: Schema.Type.TEXT(),
                age: Schema.Type.INT(),
                PRIMARY_KEY: Schema.Type.PRIMARY_KEY([["id", "username"], "age"])
            }),
            query = new Query();

            query.CREATE_TABLE('users', schema);

            (query.toString()).should.equal("CREATE TABLE users (id uuid, username text, age int, PRIMARY KEY ((id, username), age));");
            done();
        });
        it("Should create a valid create table if it doesn't exist", function(done){
            var schema = new Schema({
                id: Schema.Type.UUID(),
                username: Schema.Type.TEXT(),
                PRIMARY_KEY: Schema.Type.PRIMARY_KEY("id")
            }),
            query = new Query();

            query.CREATE_TABLE('users', schema, true);

            (query.toString()).should.equal("CREATE TABLE IF NOT EXISTS users (id uuid, username text, PRIMARY KEY (id));");
            done();
        });
    });
    describe("SELECT Tests",function(){
        it("Should throw error for invalid selector", function(done){
            var query = new Query(baseTable);

            (function(){
                query.SELECT(123);
            }).should.throw("Unsupported selector for SELECT statement. Expected 123 to be of type String or Array.");

            done();
        });
        it("Should create a basic SELECT query", function(done){
            var query = new Query(baseTable);

            query.SELECT("*", "users");

            (query.toString()).should.equal("SELECT * FROM users;");
            done();
        });
        it("Should create a SELECT query with multiple columns", function(done){
            var query = new Query(baseTable);

            query.SELECT(["firstname", "lastname"], "users");

            (query.toString()).should.equal("SELECT firstname, lastname FROM users;");
            done();
        });
        it("Should create a SELECT query with mixed columns and alias", function(done){
            var query = new Query(baseTable);

            query.SELECT(["firstname", query.AS("userid", "id")], "users");

            (query.toString()).should.equal("SELECT firstname, userid AS id FROM users;");
            done();
        });
        it("Should create a SELECT query with an alias", function(done){
            var query = new Query(baseTable);

            query.SELECT([query.AS("userid", "id")], "users");

            (query.toString()).should.equal("SELECT userid AS id FROM users;");
            done();
        });
        it("Should create a SELECT query with multiple aliases", function(done){
            var query = new Query(baseTable);

            query.SELECT([query.AS("userid", "id"), query.AS("zipcode", "zip")], "users");

            (query.toString()).should.equal("SELECT userid AS id, zipcode AS zip FROM users;");
            done();
        });
        it("Should create a SELECT query with a WHERE", function(done){
            var query = new Query(baseTable);

            query.SELECT("*", "users").WHERE().EQUALS("userid", "abcdefg");

            (query.toString()).should.equal("SELECT * FROM users WHERE userid = 'abcdefg';");
            done();
        });
        it("Should create a SELECT query with a WHERE and a optional `key` argument.", function(done){
            var query = new Query(baseTable);

            query.SELECT("*", "users").WHERE("userid").IN([123, 456]);

            (query.toString()).should.equal("SELECT * FROM users WHERE userid IN (123, 456);");
            done();
        });
        it("Should create a SELECT query with a WHERE_EQUALS shortcut", function(done){
            var query = new Query(baseTable);

            query.SELECT("*", "users").WHERE_EQUALS("userid", "abcdefg");

            (query.toString()).should.equal("SELECT * FROM users WHERE userid = 'abcdefg';");
            done();
        });

        it("Should create a valid SELECT statement with WHERE, AND", function(done) {
            var query = new Query(baseTable);

            query.SELECT(['firstname', 'lastname'], "users").WHERE_EQUALS("firstname", "james").AND().EQUALS("lastname", "booth");

            (query.toString()).should.eql("SELECT firstname, lastname FROM users WHERE firstname = 'james' AND lastname = 'booth';");
            done();
        });
        it("Should create a valid SELECT statement with WHERE, AND, EQUALS using a number", function(done) {
            var query = new Query(baseTable);

            query.SELECT(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james").AND().EQUALS("age", 37);

            (query.toString()).should.eql("SELECT firstname, lastname FROM users WHERE firstname = 'james' AND age = 37;");
            done();
        });
        it("Should create a valid SELECT statement with WHERE, AND with optional key paramater, IN", function(done) {
            var query = new Query(baseTable);

            query.SELECT(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james").AND("userid").IN([123, 456]);

            (query.toString()).should.eql("SELECT firstname, lastname FROM users WHERE firstname = 'james' AND userid IN (123, 456);");
            done();
        });
        it("Should create a valid SELECT statement with WHERE, IN (non-array)", function(done) {
            var query = new Query(baseTable);

            query.SELECT(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james").IN(123);

            (query.toString()).should.eql("SELECT firstname, lastname FROM users WHERE firstname = 'james' IN (123);");
            done();
        });
        it("Should throw error for a valid SELECT statement with WHERE, and in invalid IN", function(done) {
            var query = new Query(baseTable);

            (function(){
                query.SELECT(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james").IN();
            }).should.throw("The predicate IN requires and Array argument.");

            done();
        });
        it("Should create a valid SELECT statement with WHERE, IN", function(done) {
            var query = new Query(baseTable);

            query.SELECT(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james").IN([123]);

            (query.toString()).should.eql("SELECT firstname, lastname FROM users WHERE firstname = 'james' IN (123);");
            done();
        });
        it("Should create a valid SELECT statement with WHERE, IN (multiple)", function(done) {
            var query = new Query(baseTable);

            query.SELECT(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james").IN([123,456]);

            (query.toString()).should.eql("SELECT firstname, lastname FROM users WHERE firstname = 'james' IN (123, 456);");
            done();
        });
        it("Should create a valid SELECT statement with WHERE, IN (multiple strings)", function(done) {
            var query = new Query(baseTable);

            query.SELECT(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james").IN(['state','country']);

            (query.toString()).should.eql("SELECT firstname, lastname FROM users WHERE firstname = 'james' IN ('state', 'country');");
            done();
        });
        it("Should create a valid SELECT statement with WHERE, IN (optional key parameter)", function(done) {
            var query = new Query(baseTable);

            query.SELECT(['firstname', 'lastname']).WHERE().IN("userid", [123, 456]);

            (query.toString()).should.eql("SELECT firstname, lastname FROM users WHERE userid IN (123, 456);");
            done();
        });
        it("Should create a valid SELECT statement with WHERE, AND, GT", function(done) {
            var query = new Query(baseTable);

            query.SELECT(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james").AND().GT("age", 37);

            (query.toString()).should.eql("SELECT firstname, lastname FROM users WHERE firstname = 'james' AND age > 37;");
            done();
        });
        it("Should create a valid SELECT statement with WHERE, AND, GTE", function(done) {
            var query = new Query(baseTable);

            query.SELECT(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james").AND().GTE("age", 37);

            (query.toString()).should.eql("SELECT firstname, lastname FROM users WHERE firstname = 'james' AND age >= 37;");
            done();
        });
        it("Should create a valid SELECT statement with WHERE, AND, LT", function(done) {
            var query = new Query(baseTable);

            query.SELECT(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james").AND().LT("age", 37);

            (query.toString()).should.eql("SELECT firstname, lastname FROM users WHERE firstname = 'james' AND age < 37;");
            done();
        });
        it("Should create a valid SELECT statement with WHERE, AND, LTE", function(done) {
            var query = new Query(baseTable);

            query.SELECT(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james").AND().LTE("age", 37);

            (query.toString()).should.eql("SELECT firstname, lastname FROM users WHERE firstname = 'james' AND age <= 37;");
            done();
        });
        it("Should create a valid SELECT statement with WHERE, AND, LTE, LIMIT", function(done) {
            var query = new Query(baseTable);

            query.SELECT(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james").AND().LTE("age", 37).LIMIT(10);

            (query.toString()).should.eql("SELECT firstname, lastname FROM users WHERE firstname = 'james' AND age <= 37 LIMIT 10;");
            done();
        });
        it("Should create a valid SELECT statement with WHERE, AND, LTE, LIMIT, ALLOW_FILTERING", function(done) {
            var query = new Query(baseTable);

            query.SELECT(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james").AND().LTE("age", 37).LIMIT(10).ALLOW_FILTERING();

            (query.toString()).should.eql("SELECT firstname, lastname FROM users WHERE firstname = 'james' AND age <= 37 LIMIT 10 ALLOW FILTERING;");
            done();
        });
        it("Should create a valid SELECT statement with WHERE, ORDER BY (ASC)", function(done) {
            var query = new Query(baseTable);

            query.SELECT(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james").ORDER_BY('lastname');

            (query.toString()).should.eql("SELECT firstname, lastname FROM users WHERE firstname = 'james' ORDER BY lastname ASC;");
            done();
        });
        it("Should create a valid SELECT statement with WHERE, ORDER BY (DESC)", function(done) {
            var query = new Query(baseTable);

            query.SELECT(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james").ORDER_BY('lastname', true);

            (query.toString()).should.eql("SELECT firstname, lastname FROM users WHERE firstname = 'james' ORDER BY lastname DESC;");
            done();
        });
        it("Should create a valid SELECT using COUNT", function(done) {
            var query = new Query(baseTable);

            query.SELECT(query.COUNT("*"));

            (query.toString()).should.eql("SELECT COUNT(*) FROM users;");
            done();
        });
        it("Should create a valid SELECT using TOKEN with the value token as a string", function(done) {
            var query = new Query(baseTable);

            query.SELECT("*").WHERE().GT(query.COLUMN_TOKEN('firstname'), query.VALUE_TOKEN('fourth'));

            (query.toString()).should.eql("SELECT * FROM users WHERE TOKEN(firstname) > TOKEN('fourth');");
            done();
        });
        it("Should create a valid SELECT using TOKEN with the value token as a number", function(done) {
            var query = new Query(baseTable);

            query.SELECT("*").WHERE().GT(query.COLUMN_TOKEN('userid'), query.VALUE_TOKEN(123));

            (query.toString()).should.eql("SELECT * FROM users WHERE TOKEN(userid) > TOKEN(123);");
            done();
        });
    });
    describe("INSERT Tests", function(){
        it("Should throw error for column/value mismatch", function(done) {
            var query = new Query(baseTable);

            (function(){
                query.INSERT({firstname:"James", lastname:null});
            }).should.throw("The value, null, for key, lastname, is invalid. Expecting text");

            done();
        });
        it("Should create a valid insert query", function(done) {
            var query = new Query(baseTable);

            query.INSERT({firstname:"James", lastname:"Booth", age: 37});

            (query.toString()).should.equal("INSERT INTO users (firstname, lastname, age) VALUES ('James', 'Booth', 37);");
            done();
        });
        it("Should create a valid insert query with valid ISO date.", function(done) {
            var query = new Query(baseTable);

            query.INSERT({firstname:"James", lastname:"Booth", birthdate: '2000-04-29T18:00:25.000Z'});

            (query.toString()).should.equal("INSERT INTO users (firstname, lastname, birthdate) VALUES ('James', 'Booth', '2000-04-29T18:00:25.000Z');");
            done();
        });
        it("Should throw an error if invalid date is passed.", function(done) {
            var query = new Query(baseTable);

            (function(){
                query.INSERT({firstname:"James", lastname:"Booth", birthdate: 'abc-xyz'});
            }).should.throw("Invalid date passed for key birthdate, with value of abc-xyz");

            done();

        });
        it("Should create a valid insert query with valid javascript date.", function(done) {
            var query = new Query(baseTable);

            query.INSERT({firstname:"James", lastname:"Booth", birthdate: 1430331195154});

            (query.toString()).should.equal("INSERT INTO users (firstname, lastname, birthdate) VALUES ('James', 'Booth', '2015-04-29T18:13:15.154Z');");
            done();
        });
        it("Should create a valid insert query with text with Apostrophe.", function(done) {
            var query = new Query(baseTable);

            query.INSERT({firstname:"John ", lastname:"O'Fadden"});

            (query.toString()).should.equal("INSERT INTO users (firstname, lastname) VALUES ('John ', 'O''Fadden');");
            done();
        });
        it("Should create a valid insert query with TTL", function(done) {
            var query = new Query(baseTable);

            query.INSERT({firstname:"James", lastname:"Booth"}).USING_TTL(86400);

            (query.toString()).should.equal("INSERT INTO users (firstname, lastname) VALUES ('James', 'Booth') USING TTL 86400;");
            done();
        });
    });
    describe("DELETE TESTS", function(){
        it("Should create a valid DELETE query without selectors", function(done){
            var query = new Query(baseTable);

            query.DELETE();

            (query.toString()).should.equal('DELETE FROM users;');
            done();
        });
        it("Should create a valid DELETE query", function(done){
            var query = new Query(baseTable);

            query.DELETE(['firstname', 'lastname']);

            (query.toString()).should.equal('DELETE firstname, lastname FROM users;');
            done();
        });
        it("Should create a valid DELETE statement with WHERE (not using WHERE_EQUALS shortcut)", function(done) {
            var query = new Query(baseTable);

            query.DELETE(['firstname', 'lastname']).WHERE().EQUALS('firstname', 'james');

            (query.toString()).should.eql("DELETE firstname, lastname FROM users WHERE firstname = 'james';");
            done();
        });
        it("Should create a valid DELETE statement with WHERE", function(done) {
            var query = new Query(baseTable);

            query.DELETE(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james");

            (query.toString()).should.eql("DELETE firstname, lastname FROM users WHERE firstname = 'james';");
            done();
        });
        it("Should create a valid DELETE statement with WHERE and timestamp", function(done) {
            var query = new Query(baseTable);

            query.DELETE(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james").USING_TIMESTAMP(1405446044378);

            (query.toString()).should.eql("DELETE firstname, lastname FROM users WHERE firstname = 'james' USING TIMESTAMP 1405446044378;");
            done();
        });
        it("Should throw error with a valid DELETE statement with WHERE and an invalid timestamp", function(done) {
            var query = new Query(baseTable);

            (function(){
                query.DELETE(['firstname', 'lastname']).WHERE_EQUALS("firstname", "james").USING_TIMESTAMP("abc");
            }).should.throw("USING TIMSTAMP requires a valid timestamp, instead, received abc");
            done();
        });
    });
    describe("Query Syntax Tests", function(){
        it("Should create a proper SET query", function(done){
            var schema = new Schema({
                user_id: Schema.Type.TEXT().PRIMARY_KEY(),
                first_name: Schema.Type.TEXT(),
                last_name: Schema.Type.TEXT(),
                emails: Schema.Type.SET(Schema.Type.TEXT())
            }),
            mapTable = Cassanova.Table("users_set_test", schema),
            query = new Query(mapTable);

            query.INSERT({user_id:'frodo', first_name:'Frodo', last_name:'Baggins', emails:['f@baggins.com', 'baggins@gmail.com']});
            (query.toString()).should.equal("INSERT INTO users_set_test (user_id, first_name, last_name, emails) VALUES ('frodo', 'Frodo', 'Baggins', {'f@baggins.com', 'baggins@gmail.com'});");

            (function(){
                query.INSERT({user_id:'frodo', first_name:'Frodo', last_name:'Baggins', emails:[123, 'baggins@gmail.com']});
            }).should.throw("Mismatched key type for emails. Expecting a text");

            done();
        });
        it("Should create a proper LIST query", function(done){
            var schema = new Schema({
                user_id: Schema.Type.TEXT().PRIMARY_KEY(),
                first_name: Schema.Type.TEXT(),
                last_name: Schema.Type.TEXT(),
                places: Schema.Type.LIST(Schema.Type.TEXT())
            }),
            mapTable = Cassanova.Table("users_list_test", schema),
            query = new Query(mapTable);

            query.INSERT({places:["rivendell", "Hell's kitchen"]});
            (query.toString()).should.equal("INSERT INTO users_list_test (places) VALUES (['rivendell', 'Hell''s kitchen']);");

            (function(){
                query.INSERT({places:['rivendell', 456]});
            }).should.throw("Mismatched key type for places. Expecting a text");

            done();
        });
        it("Should create a proper MAP query for an object", function(done){
            debugger;
            var schema = new Schema({
                user_id: Schema.Type.TEXT().PRIMARY_KEY(),
                todo: Schema.Type.MAP(Schema.Type.TEXT(), Schema.Type.TEXT())
            }),
            mapTable = Cassanova.Table("users_map_test", schema),
            query = new Query(mapTable);

            //checking Apostrophe
            query.INSERT({ todo:{'2013-9-22 12:01'  : "Meet patrick o'laughlin at Hell's kitchen", '2013-10-1 18:00' : 'Check into Inn of Prancing Pony'}});
            (query.toString()).should.equal("INSERT INTO users_map_test (todo) VALUES ({'2013-9-22 12:01' : 'Meet patrick o''laughlin at Hell''s kitchen', '2013-10-1 18:00' : 'Check into Inn of Prancing Pony'});");

            (function(){
                query.INSERT({todo:{'2013-9-22 12:01'  : 'birthday wishes to Bilbo','2013-10-1 18:00' : 123456789}});
            }).should.throw("Mismatched value type for todo. Expecting a text");

            done();
        });
        it("Should create a proper MAP query for Array", function(done){
            var schema = new Schema({
                    user_id: Schema.Type.TEXT().PRIMARY_KEY(),
                    todo: Schema.Type.MAP(Schema.Type.TEXT(), Schema.Type.TEXT())
                }),
                mapTable = Cassanova.Table("users_map_test", schema),
                query = new Query(mapTable);

            query.INSERT({todo:[{'2013-9-22 12:01'  : "Meet Patrick O'laughlin at Hell's kitchen"}, {'2013-10-1 18:00' : "Check into Inn of Prancing Pony"}]});
            (query.toString()).should.equal("INSERT INTO users_map_test (todo) VALUES ({'2013-9-22 12:01' : 'Meet Patrick O''laughlin at Hell''s kitchen', '2013-10-1 18:00' : 'Check into Inn of Prancing Pony'});");

            //Will not throw an exception since in javascript the keys are always converted to strings
            // (function(){
            //    query.INSERT({todo:[{123456789  : 'birthday wishes to Bilbo'}, {'2013-10-1 18:00' : 'Check into Inn of Prancing Pony'}]});
            // }).should.throw("Mismatched key type for todo. Expecting a text");

            (function(){
                query.INSERT({todo:[{'2013-9-22 12:01'  : 'birthday wishes to Bilbo'}, {'2013-10-1 18:00' : 123456789}]});
            }).should.throw("Mismatched value type for todo. Expecting a text");

            done();
        });
    });
});
