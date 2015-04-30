var Cassanova = require('../../index');

exports.options = options = {
    hosts:["localhost"],
    port:9042,
    keyspace:"cassanova_test"
};

exports.createUserModel = function(){
    var userSchema = Cassanova.Schema({
        username : Cassanova.SchemaType.TEXT().PRIMARY_KEY(),
        firstname : Cassanova.SchemaType.TEXT(),
        birthdate : Cassanova.SchemaType.TIMESTAMP(),
        lastname : Cassanova.SchemaType.TEXT(),
        password : Cassanova.SchemaType.TEXT(),
        email : Cassanova.SchemaType.TEXT(),
        session_token : Cassanova.SchemaType.TEXT()
    }),
    userTable = Cassanova.Table("users", userSchema),
    userModel = Cassanova.Model("userModel", userTable);

    return userModel;
};

exports.testCollectionModel = function(){
    var testCollectionSchema = Cassanova.Schema({
            username : Cassanova.SchemaType.TEXT().PRIMARY_KEY(),
            test_map : Cassanova.SchemaType.MAP(Cassanova.SchemaType.TEXT(),Cassanova.SchemaType.TEXT()),
            test_set : Cassanova.SchemaType.SET(Cassanova.SchemaType.TEXT()),
            test_list : Cassanova.SchemaType.LIST(Cassanova.SchemaType.TEXT())
        }),
        testCollectionTable = Cassanova.Table("test_collections", testCollectionSchema),
        testCollectionModel = Cassanova.Model("testCollectionModel", testCollectionTable);

    return testCollectionModel;
};
exports.createCassanovaClient = function(){
    return Cassanova.createClient(options);
};

exports.connectCassanovaClient = function(callback){
    Cassanova.connect(options, callback);
};

exports.disconnectCassanovaClient = function(callback){
    Cassanova.disconnect(callback);
};
