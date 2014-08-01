//CREATE KEYSPACE cassanova WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
//
var opts = {
    "hosts" : ["localhost:9042"],
    "keyspace" : "cassanova"
};
var userSchema = new Cassanova.Schema({
    username : Cassanova.SchemaType.TEXT().PRIMARY_KEY(),
    firstname : Cassanova.SchemaType.TEXT(),
    lastname : Cassanova.SchemaType.TEXT(),
    email : Cassanova.SchemaType.TEXT(),
    password : Cassanova.SchemaType.TEXT(),
    session_token : Cassanova.SchemaType.TEXT()
});
var itemSchema = new Cassanova.Schema({
    id : Cassanova.SchemaType.TEXT().PRIMARY_KEY(),
    title : Cassanova.SchemaType.TEXT()
});
var userTable = Cassanova.Table("users", userSchema);
var itemTable = Cassanova.Table("items", itemSchema);
var userModel = Cassanova.Model("userModel", userTable);
var itemModel = Cassanova.Model("itemModel", itemTable);
var saveUserQuery = userModel.save({username:"James"});
var saveItemQuery = itemModel.save({id:"abc123", title:"The Thing 1"});
var rawInsertQuery = Cassanova.Query().CQL("INSERT into users (username) values ('JEBoothjr')");
Cassanova.connect(opts, function(err, result){
    console.log(err || "Connected");
    userTable.create({ifNotExists:true}, function(err, result){
        console.log(err || "User Table Created");

        // rawInsertQuery.execute(function(err, result){
        //     console.log(err || result.rows[0]);
        // });
    });
});


rawInsertQuery.execute(function(err, result){
    console.log(err || result);
});

itemTable.create({ifNotExists:true}, function(err, result){
    console.log(err || "Item Table Created");
});

saveItemQuery.execute(function(err, result){
    console.log(err || result);
});
itemModel.save({id:"abc123", title:"The Thing 1"}, function(err, result){
    console.log(err || result);
});

saveUserQuery.execute(function(err, result){
    console.log(err || result);
});
userModel.save({username:"Bonita"}, function(err, result){
    console.log(err || result);
});