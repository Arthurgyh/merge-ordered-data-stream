var assert = require('assert-callback');
var mergey = require('../');
var through = require('through2');

//set NODE_DEBUG=merge

describe('merge_testsuite', function () {

    it('merge_simple' + "_" + "test", function (done) {
        var t1 = through.obj()
        var t2 = through.obj();
        var m = mergey.MergeReadStream(null, null, undefined,null,function comparitor(data1,data2){
            if(data1 > data2) return 1
            else if(data1 < data2) return -1
            return 0
        });
        var s = through.obj();

        m.add([t1,t2]).pipe(s);
        m.start();

        var a = [];

        s.on('data',a.push.bind(a)).on('end',function(){
            assert.ok(1,'end should be called');
            assert.equal(a.join(''),'1234567', 'merget not ok')

            done();
        });


        t1.write('2')
        t1.write('4')
        t2.write('1')
        t2.write('3')
        t2.write('6')

        t2.end()

        t1.write('5')
        t1.write('7')

        t1.end()

    });

});

