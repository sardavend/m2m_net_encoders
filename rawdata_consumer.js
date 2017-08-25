const amqpOpen = require('amqplib').connect('amqp://imonnetplus.com.bo');
const url = 'mongodb://imonnetplus.com.bo:27017/platform2';
const co = require('co');
const persistenceOpen = require('./data_access.js');
// const persistenceOpen = require('./persistence.js').connect(url);
const writeToRawData = require('./data_access.js').writeToRawData;
const updateCurrentState = require('./data_access.js').updateCurrentState;
const updateLastnPositions = require('./data_access.js').updateLastnPositions;
// var all = require('bluebird').all
const xchange = 'main_valve';

/**
 * 
 * @param {string} uiid 
 * @param {Date} updateTime 
 */
function getUnitStateId(uiid,updateTime){
    let deltaBo = new Date(updateTime - 14400000);
    return `${uiid}/${deltaBo.toISOString().split('T')[0]}`
}
/**
 * 
 * @param {*} uiid 
 * @param {*} updateTime 
 */

function getUnitStateDate(updateTime){
    let deltaBo = new Date(updateTime - 14400000);
    return new Date(deltaBo.getFullYear,deltaBo.getMonth, deltaBo.getDay)
    // return `${uiid}/${deltaBo.toISOString().split('T')[0]}`
}


function getRawdata(msg) {
    let unitState = {};
    // unitState["_id"] = getUnitStateId(msg["unitInstanceId"], msg["updateTime"]);
    // unitState["_id"] = getUnitStateId(msg["unitInstanceId"], msg["updateTime"]);
    unitState["unitInstanceId"] = msg["unitInstanceId"];
    unitState["unitId"] = msg["unitId"];
    unitState["updateTime"] = new Date(msg["updateTime"]);
    unitState["driverId"] = msg["driver"]["id"];
    unitState["driverName"] = msg["driver"]["name"];
    unitState["driverKeyId"] = msg["driver"]["keyId"];
    unitState["latitude"] = msg["latitude"];
    unitState["longitude"] = msg["longitude"];
    unitState["speed"] = msg["speed"] / 0.036; //to cm/secs
    unitState["heading"] = msg["heading"];
    unitState["eventCode"] = msg["eventCode"];
    unitState["address"] = msg["geoReference"]["address"];
    unitState["geoReference"] = msg["geoReference"]["geoZone"];
    unitState["nearestGeoReference"] = msg["geoReference"]["nearest"];
    unitState["address"] = msg["geoReference"]["address"];
    unitState["altitude"] = msg["altitude"];
    /*
    unitState["currentState"] = {
        "driver":msg["driver"],
        "geoReference": msg["geoReference"],
        "heading": msg["heading"],
        "eventCode": msg["eventCode"],
        "altitude": msg["altitude"],
        "latitude": msg["latitude"],
        "longitude": msg["longitude"],
        "speed": msg["speed"],
        "updateTime":msg["updateTime"],
        "odometer":msg["odometer"]
    },
    unitState["tripDistance"] = 0;*/
    return unitState; 
}

co(persistenceOpen.connect(url)).then(() =>{
    console.log("Connected to Mongodb")
    amqpOpen
    .then(conn => {
        process.once('SIGINT', () => conn.close());
        console.log("Connected to amqp broker")
        return conn.createChannel();
    }).then(ch => {
        var ok = ch.assertExchange(xchange, 'fanout',{durable:false});
        ok = ok.then(() => {
            return ch.assertQueue('rawdata_log', {exclusive:false})
        });

        ok = ok.then(qok => {
            let queue = qok.queue;
            return ch.bindQueue(queue,xchange, 'raw_log').then(() => queue)
        });
        ok = ok.then(queue => {
            return ch.consume(queue, logMessage, {noAck:false});
        });
        return ok.then(() => {
            console.log('[*] Waiting for logs, To exit press CTRL+C');
        });

        function logMessage(msg){
            let routingKey = msg.fields.routingKey;
            let content = msg.content.toString();
            let contentJson = JSON.parse(content);
            let rawData= getRawdata(contentJson);
            co(writeToRawData(rawData))
            .then(() => {
                return co(updateCurrentState(rawData));
            }).then(() =>{
                return co(updateLastnPositions(rawData));
            }).then(() => {
            
                console.log(` [x] ${routingKey}:${contentJson}`)
                ch.ack(msg);
            }).catch(err =>{

                console.log(`An error has ocurred processing the message ${err}`);
            });

        }
    }).catch(console.warn);
   
})