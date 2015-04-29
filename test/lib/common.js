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

exports.createCassanovaClient = function(){
    return Cassanova.createClient(options);
};

exports.connectCassanovaClient = function(callback){
    Cassanova.connect(options, callback);
};

exports.disconnectCassanovaClient = function(callback){
    Cassanova.disconnect(callback);
};
