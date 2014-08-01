/*jslint node: true */
/* global it, before, after, describe, xit, xdescribe */
"use strict";

var should = require('should'),
    exec = require('child_process').exec,
    Cassanova = require('../index'),
    common = require("./lib/common"),
    userModel;

before(function(done){

    exec("node CQL -f test/before.cql", function(error, stdout, stderr){
        if(error){
            throw error;
        }

        console.log('Setting up db for End to End tests : ');

        userModel = common.createUserModel();

        done();
    });
});

after(function(done){
    exec("node CQL -f test/after.cql", function(error, stdout, stderr){
        if(error){
            throw error;
        }

        console.log('Cleaning up db after End to End tests : ');

        done();
    });
});

describe("Cassanova End To End Tests",function(){

    before(function(done){
        common.connectCassanovaClient(function(){
            done();
        });
    });

    after(function(done){
        common.disconnectCassanovaClient(function(){
            done();
        });
    });

    it("Should be able to insert a user in db via the static method", function(done) {

        userModel.save({
            username : 'James',
            password : 'password',
            email : 'google@gmail.com',
            session_token : '12345ab'
        }, function(err, result){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);
            done();
        });
    });

    it("Should be able to insert a user in db via the instance method", function(done) {
        userModel.save({
            username : 'James',
            password : 'password',
            email : 'google@gmail.com',
            session_token : '12345ab'
        }, function(err, result){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);
            done();
        });
    });

    it("Should send an error when attempting to save no data.", function(done) {
        userModel.save(null, function(err, result){
            (err === null).should.equal(false);
            (err.message).should.equal("There is no data to be saved");
            done();
        });
    });

    it("Should send error if attempting to use undefined schema data", function(done) {

        (function(){
            userModel.save({
                username : 'James',
                password : undefined,
                email : 'google@gmail.com',
                session_token : '12345ab'
            }, function(err, result){
                if(err){
                    console.log(err);
                }
                (err === null).should.equal(true);
            });
        }).should.throw("The value, undefined, for key, password, is invalid. Expecting text");
        done();
    });

    it("Should be able to find a user in db", function(done) {
        userModel.findAllBy('username','James', function(err,result){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);

            (result[0].username === 'James').should.equal(true);
            done();

        });
    });

    it("Should be able to find a user in db using a chained query via the static method", function(done) {
        userModel.find({username:"James", email:"google@gmail.com"}, {}).execute(function(err, result){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);
            (result[0].username === 'James').should.equal(true);
            done();
        });
    });

    it("Should be able to find a user in db using a chained query via the instance method", function(done) {
        userModel.find({username:"James", email:"google@gmail.com"}).execute(function(err, result){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);

            (result[0].username === 'James').should.equal(true);
            done();
        });
    });

    it("Should be able to find a user in db using a chained query via the static method", function(done) {
        userModel.find({username:"James"}).AND().EQUALS("email", "google@gmail.com").execute(function(err, result){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);

            (result[0].username === 'James').should.equal(true);
            done();
        });
    });

    it("Should be able to find a user in db using a chained query via the instance method", function(done) {
        userModel.find({username:"James"}).AND().EQUALS("email", "google@gmail.com").execute(function(err, result){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);

            (result[0].username === 'James').should.equal(true);
            done();
        });
    });

    it("Should be able to create a table in the db", function(done) {
        var schema = Cassanova.Schema({
                id:Cassanova.SchemaType.TEXT().PRIMARY_KEY(),
                message:Cassanova.SchemaType.TEXT()
            }),
            table = Cassanova.Table("cassanova_test.logging", schema);

        table.create(function(err, result){
            var q = new Cassanova.Query().CQL("select * from system.schema_columnfamilies where columnfamily_name = 'logging' ALLOW FILTERING;");

            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);

            Cassanova.execute(q, null, function(err, result){
                if(err){
                    console.log(err);
                }
                (err === null).should.equal(true);
                (result.length > 0).should.equal(true);
                done();
            });
        });
    });

    it("Should be able to delete a user in db.", function(done) {
        userModel.delete({username: "James"}, function(err, result){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);
            done();
        });
    });

    it('Should be able to run queries in batch',function(done){
        var query1 = userModel.Query();

        query1.INSERT({username:"James", firstname:"James", lastname:"Booth"});
        var query2 = userModel.Query();
        query2.INSERT({username:"Karan", firstname:"Karan", lastname:"Keswani"});

        Cassanova.executeBatch([query1, query2], null, function(err, result){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);
            done();
        });
    });

    it('Should be able to run queries in batch without options argument',function(done){
        var query1 = userModel.Query();

        query1.INSERT({username:"James", firstname:"James", lastname:"Booth"});
        var query2 = userModel.Query();
        query2.INSERT({username:"Karan", firstname:"Karan", lastname:"Keswani"});

        Cassanova.executeBatch([query1, query2], function(err, result){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);
            done();
        });
    });

    it('Should be able to execute',function(done){
        var query = userModel.Query();
        query.INSERT({username:"James1", firstname:"James", lastname:"Booth"});

        Cassanova.execute(query, null, function(err, result){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);
            done();
        });
    });

    it('Should be able to execute without options argument',function(done){
        var query = userModel.Query();
        query.INSERT({username:"James1", firstname:"James", lastname:"Booth"});

        Cassanova.execute(query, function(err, result){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);
            done();
        });
    });

    it('Should be able to executeAsPrepared',function(done){
        var query = userModel.Query();
        query.INSERT({username:"James2", firstname:"James", lastname:"Booth"});

        Cassanova.executeAsPrepared(query, null, function(err, result){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);
            done();
        });
    });

    it('Should be able to executeAsPrepared without options argument',function(done){
        var query = userModel.Query();
        query.INSERT({username:"James2", firstname:"James", lastname:"Booth"});

        Cassanova.executeAsPrepared(query, function(err, result){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);
            done();
        });
    });

    it('Should be able to executeStream',function(done){
        var query = userModel.Query();
        query.INSERT({username:"James2", firstname:"James", lastname:"Booth"});

        Cassanova.executeStream(query, null, function(err, result){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);
            done();
        });
    });

    it('Should be able to executeStream without options argument',function(done){
        var query = userModel.Query();
        query.INSERT({username:"James2", firstname:"James", lastname:"Booth"});

        Cassanova.executeStream(query, function(err, result){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);
            done();
        });
    });

    it('Should be able to executeEachRow',function(done){
        var query = userModel.Query();
        query.SELECT("*");

        Cassanova.executeEachRow(query, null, function(n, row){}, function(err, rowLength){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);
            done();
        });
    });

    it('Should be able to executeEachRow without options argument',function(done){
        var query = userModel.Query();
        query.SELECT("*");

        Cassanova.executeEachRow(query, function(n, row){}, function(err, rowLength){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);
            done();
        });
    });

    it('Should be able to executeStreamField',function(done){
        var query = userModel.Query();
        query.SELECT("*");

        Cassanova.executeStreamField(query, null, function(n, row){}, function(err, rowLength){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);
            done();
        });
    });

    it('Should be able to executeStreamField without options argument',function(done){
        var query = userModel.Query();
        query.SELECT("*");

        Cassanova.executeStreamField(query, function(n, row){}, function(err, rowLength){
            if(err){
                console.log(err);
            }
            (err === null).should.equal(true);
            done();
        });
    });
});
