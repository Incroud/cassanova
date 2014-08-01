var express = require('express'),
    app = express(),
    http = require("http"),
    Cassanova = require("./index"),
    userNum = 0,
    opts = {
        "hosts" : ["localhost:9042"],
        "keyspace" : "cassanova",
        "skipSchemaValidation":true
    },
    userSchema = new Cassanova.Schema({
        username : Cassanova.SchemaType.TEXT().PRIMARY_KEY(),
        firstname : Cassanova.SchemaType.TEXT(),
        lastname : Cassanova.SchemaType.TEXT(),
        email : Cassanova.SchemaType.TEXT(),
        password : Cassanova.SchemaType.TEXT(),
        session_token : Cassanova.SchemaType.TEXT()
    }),
    userTable = Cassanova.Table("users", userSchema),
    UserModel = Cassanova.Model("userModel", userTable),
    start = null;

Error.stackTraceLimit = Infinity;

require("heapdump");

// Cassanova.createClient(opts);
// userTable.create({ifNotExists:true}, function(err, result){
//     console.log(err || "User Table Created");
// });

Cassanova.connect(opts, function(err, result){
    console.log(err || "Connected");

    userTable.create({ifNotExists:true}, function(err, result){
        console.log(err || "User Table Created");
    });
});

app.get('/driver', function(req, res, next){
    var thisNum = ++userNum,
        username = "User_" + thisNum;
        
    Cassanova.client.execute("INSERT into users (username) values('" + username + "');", null, Cassanova.consistencies.quorum, function(err, result){
        var result = err || thisNum;
        console.log(result);
        if(err){
            res.send(500);
        }else{
            res.send(200);
        }
    });
});

app.get('/instance', function(req, res, next){
    var thisNum = ++userNum,
        username = "User_" + thisNum;

    var uModel = new UserModel();
    uModel.save({username:username}, function(err, result){
        var result = err || thisNum;
        console.log(result);
        if(err){
            res.send(500);
        }else{
            res.send(200);
        }
    });
});

app.get('/static', function(req, res, next){
    var thisNum = ++userNum,
        username = "User_" + thisNum;

    if(!start){
        start = new Date();
    }

    UserModel.save({username:username}, function(err, result){
        var result = err || thisNum;
        //console.log(result + " :: " + (new Date().getTime() - start.getTime()));
        console.log(new Date().getTime() - start.getTime());
        if(err){
            res.send(500);
        }else{
            res.send(200);
        }
    });
});

if(!module.parent) {
    app.listen(3000);
}else{
    module.exports = app;
}
