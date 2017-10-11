"use strict";
var fs = require('fs');
var stream = require('stream');
var util = require('util');
var bs = require('binarysearch');
var glob = require("glob");
const debuglog_detail = util.debuglog('detail');
const debuglog = util.debuglog('merge');



const {Readable} = require('stream');
const { Writable } = require('stream');

var ex = exports = module.exports;

ex.MergeReadStream = MergeReadStream;

function MergeReadStream(file_or_glob, file_options, merge_options, prepare, comparitor) {
    // allow use without new
    if (!(this instanceof MergeReadStream)) {
        return new MergeReadStream(file_or_glob, file_options, merge_options, prepare, comparitor);
    }

    // init Transform
    if (!merge_options) merge_options = {}; // ensure object
    //options.objectMode = true; // forcing object mode
    //options.highWaterMark = 1;

    //options.allowHalfOpen = true;
    Readable.call(this, merge_options);
    this.sourceProps = merge_options;
    this.file_or_glob = file_or_glob;
    this.prepare = prepare;
    this.data_comparitor = comparitor;

    this.origins = [];
    this.sources = [];
    this.actives = [];
    //this.srcs_cb = [];
    this.data_buf = [];//[[]];
    this.sort_buf = [];//[];
    this.sort_sts = [];//[];
    this.started = false;
    this.read_cnt = 0;
    this.read_get_cnt = 0;
    this.read_wait_cnt = 0;
    this.read_retry_cnt = 0;
    this.read_resume_cnt = 0;
    this.stoping = false;
    var hw = merge_options.writableHighWaterMark;
    if(!hw || hw === 0){
        hw = 16;
    }
    this.writableHighWaterMark = hw;
    this.add_glob(file_or_glob, file_options);
    var self = this;
    this.once('end', function(){
        self._my_clean();
    })
}
util.inherits(MergeReadStream, Readable);

//----------------------------------------------------------

const src_sts = {
    //nead: Symbol('nead'),
    buffering: Symbol('buffering'),
    full: Symbol('full'),//buffering fulled
    end: Symbol('end'),
}

const sort_sts = {
    head: Symbol('head'),
    middle: Symbol('middle'),
    empty: Symbol('empty'),
    ended: Symbol('ended'),
}


const AgentOpt={
//    flags: 'wx',
    flags: 'w',
    encoding: 'utf8',
    fd: null,
    mode: 0o666,
    autoClose: true,
    highWaterMark: Math.pow(2, 16) // 64k
//ObjectMode:true,
//highWaterMark: 1
};


class AgentWritable extends Writable {
    constructor(merger,options) {
        super(options);
        this.cb = null;
        this.id = -1;
        this.wait_cnt = 1;
        this.wait_ind = 0;
        this.merger = merger;
    }

    _write(chunk, encoding, callback) {
        //debuglog_detail('agent[%d] get data....[%s]',this.id, chunk);
        this._transform_cb = callback;
        this.merger.back_push(this.id, chunk, encoding, callback);
    }
    set_wait_cnt(cnt){
        this.wait_cnt = cnt;
    }
    back_next() {
        var self = this;
        this.wait_ind ++;
        if(this.wait_ind >= this.wait_cnt){
            if (self._transform_cb) {
                self._transform_cb();
                //delete self._transform_cb;
                //self.resume();
            }
            this.wait_ind = 0;
        }
    }
    set_id(id){
        this.id = id;
    }
};

//-----------------------------------------------
function add(steams) {
    var self = this;
    var output = this;
    if (Array.isArray(steams)) {
        steams.forEach(function(one_stream, idx){
            //console.log("", typeof one_stream , typeof idx, idx);
            self.add(one_stream);
        });
        return this
    }
    var reader = steams;
    var source = new AgentWritable(self, AgentOpt);

    var len=0;
    len = this.sources.push(source);
    source.set_id(len-1);
    len = this.origins.push(reader);
        //this.srcs_cb.push(null);
    len = this.actives.push(src_sts.buffering);
    len = this.data_buf.push([]);
    len = this.sort_sts.push(sort_sts.empty);
    len = this.reading = false;

    source.once('finish', remove.bind(self, source.id));
    source.once('end', remove.bind(self, source.id));
    source.once('error', output.emit.bind(output, 'error'));

    if(this.started){
        this.connect(source, output);
    }
    return this;
}
MergeReadStream.prototype.connect=function(reader, agent){
    //debuglog_detail('1 connect....');
    if(this.prepare && typeof this.prepare == 'function'){
        reader.pipe(this.prepare()).pipe(agent);
    }else{
        reader.pipe(agent);
    }
};

MergeReadStream.prototype.start=function(){
    debuglog('merge start....');
    if(this.started)
        return;

    var total = this.origins.length;
    for(var i=0;i<total;i++){
        var reader = this.origins[i];
        this.connect(reader, this.sources[i]);
    }
    this.started=true;
};

MergeReadStream.prototype.add_glob=function(str, options){
    if(str == null || str == undefined){
        return;
    }
    if(str.length == 0){
        return;
    }
    var files = glob.sync(str, {nodir:true});
    var total = files.length;
    console.log(`Found ${total} flies:`);

    for(var i=0;i++;i){
        console.log(` ${i}.- ${files[i]}`);
        var fread = fs.createReadStream(files[i], config.readOpts);
        this.add(fread);
    }
};

/*
function remove (source) {
    var self = this;
    self.sources = self.sources.filter(function (it) { return it !== source });
    this.actives = sources.length;
    if (!this.actives && output.readable) { output.end() }
}
*/

function remove (index) {
    var self = this;
    self.actives[index] = src_sts.end;
    self.sources[index] = null;
    debuglog('---- agent[%d]end----------------------....', index);
    self.do_merge_buf(index, 'remove', true);
    //self.sources[srcs_cb] = null;
/*
    this.actives = sources.length;
    if (!this.actives && output.readable) { output.end() }
*/
}


MergeReadStream.prototype.add = add;
MergeReadStream.prototype.remove = remove;

MergeReadStream.prototype.back_push = function(id, chunk, encoding, callback){
    var length = this.data_buf[id].push(chunk);
    this.do_merge_buf(id,'back_push', true);
    if(length > this.writableHighWaterMark){
        this.actives[id] = src_sts.full;
    }else{
        callback();
    }
};

MergeReadStream.prototype._pick_src_buf = function(id){
    var data = this.data_buf[id].shift();
    if(this.data_buf[id].length ==0 &&  this.actives[id] == src_sts.full){
        //debuglog_detail('agent resume [%d]', id);
        this.actives[id] = src_sts.buffering;
        this.sources[id].back_next();
    }
    return data;
};
/**
 *
 * @param mself Object of MergeReadStream
 * @param d1 is [id, chunk]
 * @param d2 is [id, chunk]
 * @returns {*}
 */
var cmpobj = function(d1,d2){
    var self = this;
    return -1 * self.data_comparitor(d1[1],d2[1]);
};

function read_nt(self, n, many){
    //debuglog_detail("read_nt retry, %d", self.read_retry_cnt);
    self.read_retry_cnt ++;
    self._read_imp(n, many);
}

MergeReadStream.prototype.do_merge_buf = function(id,step, next_able){
    if(this.sort_sts[id] == sort_sts.empty){
        var data = this._pick_src_buf(id);
        if(data != undefined){
            //debuglog_detail("%s to mger:[%d,%s] ", step, id,data.toString());
            this.do_insert(id,data);
        }else{
            if(this.actives[id] == src_sts.end){
                //debuglog_detail("%s to mger:[%d, ended.]", step, id);
                this.sort_sts[id] = sort_sts.ended;
            }else{
                //debuglog_detail("%s to mger:[%d,nodata:%s] waiting.... ", step, id,data);
            }
        }
        if(this.reading && next_able){
            //debuglog_detail("%s nextTick read_nt:[%d] ", step, id);
            process.nextTick(read_nt, this, 1, false);
        }
    }else{
        //debuglog_detail("%s agent not waiting:[%d] ", step, id);
    }
};


MergeReadStream.prototype.do_insert = function(id, data){
    var obj = [id,data];
    var idx = bs.insert(this.sort_buf,obj,cmpobj.bind(this));
    //debuglog_detail("after_insert:[%d,%s] to buf %o", id,data.toString(), this.sort_buf);
    if(idx == this.sort_buf.length -1){
        this.sort_sts[id] = sort_sts.head;
    }else{
        this.sort_sts[id] = sort_sts.middle;
    }
};

/*
  has bufferd data.
 */

// each time the buffering has one event for each stream.
// send the lowest value.
// attempt to read another form the stream that just provided the value.
// repeat

function dump_stack() {
    var stack = new Error("unknow").stack
    console.log(stack)
};

MergeReadStream.prototype._check_imp=function (n){
    var wait_cnt = 0;
    var ended_cnt = 0;
    var total = this.sort_sts.length;
    for (var i=0;i<total;i++)
    {
        if(this.sort_sts[i] == sort_sts.empty){
            wait_cnt += 1;
            break;
        }
        if(this.sort_sts[i] == sort_sts.ended){
            ended_cnt += 1;
        }
    }
    if(total==0 || wait_cnt > 0){
        return sort_sts.empty;
    }

    if(ended_cnt === total && this.sort_buf.length == 0){
        //dump_stack();
        if(!this.stoping){
            console.error('all src end + cleared buffer., it\'s time stop...' +
                '(waiting cnt R[%d]G[%d]W[%d]T[%d]S[%d])'
                , this.read_cnt, this.read_get_cnt, this.read_wait_cnt, this.read_retry_cnt, this.read_resume_cnt);
            this.stoping = true;
        }
        return sort_sts.ended;
    }
    return sort_sts.head;
};
MergeReadStream.prototype._check = function(n){
    var status = this._check_imp(n);
    //debuglog_detail("current status is: %s %o", status, this.sort_sts);
    return status;
}
MergeReadStream.prototype._pick=function (n){
    var stat = this._check(n);
    switch (stat){
        case sort_sts.ended:
            return [stat,null];
        case sort_sts.empty:
            return [stat,null];
        default:
            var data = this.sort_buf.pop();
            var id = data[0];
            this.sort_sts[id] = sort_sts.empty;
            //debuglog_detail("merged picked [%d]-> %s then %s",id, data[1], this.sort_sts[id]);
            this.do_merge_buf(id, 'pick', false);
            return data;
    }
}
/*
MergeReadStream.prototype._back=function (obj){
    return buf.push(obj);
};
*/


MergeReadStream.prototype.isEmpty= function() {
    return this.actives == 0;
};

MergeReadStream.prototype.isEnding= function() {
    return this.actives == 0;
};

MergeReadStream.prototype._read_imp = function _read_imp(n,many){
    var continue_push = true;
    var cnt = false;
    while(continue_push){
        //pick data;
        var obj = this._pick(n);
        var id = obj[0], chunk = obj[1];
        if(chunk == null){
            if(id  == sort_sts.ended){
                this.push(null);
                return;
            }else{//id == sort_sts.empty   then to wait data readable
                if(!this.reading){
                    //debuglog_detail('merge hase no data.waiting[%d]....',this.read_wait_cnt);
                    this.reading = true;
                }else{
                    //dump_stack();
                }
                this.read_wait_cnt ++;
                return;
            }
        }else {
            if(this.reading){
                //debuglog_detail('merge status become readable.');
                this.reading = false;
            }

            continue_push = this.push(chunk);
            if (!many) {
                //debuglog_detail("merged from agent[%d] to send %s",id, chunk);
                //dump_stack();
                this.read_resume_cnt ++;
                break;
            }else{
                //debuglog_detail("merged from agent[%d] to send %s",id, chunk);
                this.read_get_cnt ++;
            }
            ;
        }//end if
    };//end while
};

MergeReadStream.prototype._read = function readn(n){
    this.read_cnt ++;
    this._read_imp(n, true);
};

MergeReadStream.prototype._my_clean = function(err) {

    this.origins = [];
    this.sources = [];
    this.actives = [];
    //this.srcs_cb = [];
    this.data_buf = [];//[[]];
    this.sort_buf = [];//[];
    this.sort_sts = [];//[];

    this.push(null);
    console.log("--merge end with wait count:%d.", this.read_wait_cnt);
    //cb(err);
};