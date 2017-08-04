const amqpOpen = require('amqplib').connect('amqp://imonnetplus.com.bo');
const url = 'mongodb://imonnetplus.com.bo:27017/platform2';
const co = require('co');
const persistenceOpen = require('./data_access.js');
// const persistenceOpen = require('./persistence.js').connect(url);
const writeToDataUnitLog = require('./data_access.js').writeToDataUnitLog;
const writeToRawData = require('./data_access.js').writeToRawData;
// var all = require('bluebird').all
const xchange = 'main_valve';

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
            return ch.assertQueue('data_unit_log', {exclusive:false})
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
            co(writeToDataUnitLog(contentJson))
            .then(() => {
                return co(writeToRawData(contentJson));
            }).then(()=>{
                console.log(` [x] ${routingKey}:${content}`)
                ch.ack(msg);
            });

        }
    }).catch(console.warn);
   
})