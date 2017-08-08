const amqpOpen = require('amqplib').connect('amqp://imonnetplus.com.bo');
const url = 'mongodb://imonnetplus.com.bo:27017/platform2';
const co = require('co');
const persistenceOpen = require('./data_access.js');
// const persistenceOpen = require('./persistence.js').connect(url);
const updateUnitState= require('./data_access.js').updateUnitState;
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

function getUnitState(msg) {
    let unitState = {};
    unitState["_id"] = getUnitStateId(msg["unitInstanceId"], msg["updateTime"]);
    unitState["currentDevice"] = msg["unitId"];
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
    unitState["tripDistance"] = 0;
    return unitState; 
}

console.log(`Carajter ${persistenceOpen} la concha de la lora`);
co(persistenceOpen.connect(url)).then(() =>{
    console.log("Connected to Mongodb")
    amqpOpen
    .then(conn => {
        process.once('SIGINT', () => conn.close());
        console.log("Connected to amqp broker")
        return conn.createChannel();
    }).then(ch => {
        var ok = ch.assertExchange(xchange, 'fanout',{durable:false})
        ok = ok.then(() => {
            return ch.assertQueue('unit_status', {exclusive:false})
        })

        ok = ok.then(qok => {
            let queue = qok.queue;
            return ch.bindQueue(queue,xchange, 'unit_log').then(() => queue)
        })
        ok = ok.then(queue => {
            return ch.consume(queue, logMessage, {noAck:false});
        })
        return ok.then(() => {
            console.log('[*] Waiting for logs, To exit press CTRL+C');
        });

        function logMessage(msg){
            let routingKey = msg.fields.routingKey;
            let content = msg.content.toString();
            let contentJson = JSON.parse(content);
            let unitState = getUnitState(contentJson);
            co(updateUnitState(unitState))
            .then(() => {
                console.log(` [x] ${routingKey}:${content}`)
                ch.ack(msg);
            }).catch(err =>{
                console.log(`An error has ocurred processing the message ${err}`);
            });

        }
    }).catch(console.warn);
   
})