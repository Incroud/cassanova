module.exports = function(grunt) {
    grunt.initConfig({
        pkg: grunt.file.readJSON('package.json'),
        clean: ['build'],
        githooks: {
            all: {
                "pre-push":"test"
            }
        },
        jshint: {
            files: ['Gruntfile.js', 'index.js', 'test/*.js', 'lib/*.js'],
            options: {
                proto: true,
                //undef: true,
                //unused: true,
                // options here to override JSHint defaults
                globals: {
                    console: true,
                    module: true,
                    require: true,
                    exports: true,
                    describe: true,
                    should: true,
                    it: true,
                    before: true,
                    after: true,
                    beforeEach: true
                }
            }
        },
        mocha_istanbul: {
            coverage: {
                src: 'test',
                options: {
                    reporter: "spec",
                    slow: "0",
                    timeout: 5000,
                    root: './',
                    coverage:true,
                    check: {
                       'statements': 85,
                       'branches': 85,
                       'functions': 85,
                       'lines': 85
                    },
                    reportFormats: ['text', 'lcov', 'cobertura'],
                    coverageFolder: 'build/report'
                }
            }
        },
        watch: {
            files: ['<%= jshint.files %>'],
            tasks: ['jshint']
        }
    });

    grunt.loadNpmTasks('grunt-githooks');
    grunt.loadNpmTasks('grunt-contrib-clean');
    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-mocha-istanbul');

    grunt.registerTask('js', ['clean', 'jshint']);
    grunt.registerTask('test', ['clean', 'mocha_istanbul']);
    grunt.registerTask('default',['clean', 'jshint', 'mocha_istanbul']);
};
