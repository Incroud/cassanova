var repl = require("repl"),
    Cassanova = require("./index"),
    env = process.env.NODE_ENV || 'development';

require("heapdump");

var replServer = repl.start({
    prompt : "cassanova (" + env + ") > ",
    useColors : true,
    ignoreUndefined : true
});

replServer.on('exit', function () {
    console.log('Exiting Cassanova REPL...');
    process.exit();
});

replServer.context.Cassanova = Cassanova;
