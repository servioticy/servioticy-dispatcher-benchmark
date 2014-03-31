/**
 * Created by alvaro on 27/03/14.
 */
var benchrest = require('bench-rest');
var fs = require('fs');

var urlgenerator = function () {
    var initial_streams = JSON.parse(fs.readFile('../streams.json', 'utf8', function (err, data) {
        if (err) {
            return console.log(err);
        }
        console.log(data)
    }));
    return 'http://172.20.11.1:8080/' + initial_streams[0][0] + '/streams/' + initial_streams[0][1];
};

var flow = {
    before: [],      // operations to do before anything
    beforeMain: [],  // operations to do before each iteration
    main: [  // the main flow for each iteration, #{INDEX} is unique iteration counter token
        { put: urlgenerator, headers: {"Content-Type": "application/json", "Authorization": "MWRiODFmZjAtMWQyYS00MDQ0LTg1ZDQtZGE2NzVkMGYwNDYzOGM2YjE1NTUtZmNjNi00MGYyLWI4NTEtNzdiMjQxMDZhZWEz"}, json: {"channels": {"channel0": {"current-value": 1}}, "lastUpdate": new Date().getTime()} },
        { get: 'http://172.20.11.1:8080/1396278163507a4737364ce6248499fe5f7cf7ec57d47/streams/stream0/lastUpdate', headers: {"Authorization": "MWRiODFmZjAtMWQyYS00MDQ0LTg1ZDQtZGE2NzVkMGYwNDYzOGM2YjE1NTUtZmNjNi00MGYyLWI4NTEtNzdiMjQxMDZhZWEz"} }
    ],
    afterMain: [
//        { del: 'http://localhost:8000/foo_#{INDEX}' }
    ],   // operations to do after each iteration
    after: []        // operations to do after everything is done
};
var runOptions = {
    limit: 1,         // concurrent connections
    iterations: 1,  // number of iterations to perform
    prealloc: 100      // only preallocate up to 100 before starting
};
var errors = [];
benchrest(flow, runOptions)
    .on('error', function (err, ctxName) {
        console.error('Failed in %s with err: ', ctxName, err);
    })
    .on('progress', function (stats, percent, concurrent, ips) {
        console.log('Progress: %s complete', percent);
    })
    .on('end', function (stats, errorCount) {

        console.log('error count: ', errorCount);
        console.log('stats', stats);
    });