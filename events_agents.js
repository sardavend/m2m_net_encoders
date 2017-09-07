const amqpOpen = require('amqplib').connect('amqp://imonnetplus.com.bo');
const url = 'mongodb://imonnetplus.com.bo:27017/platform2';
const co = require('co');
const moment = require('moment');
const persistenceOpen = require('./data_access.js');
// const persistenceOpen = require('./persistence.js').connect(url);
const writeToRawData = require('./data_access.js').writeToRawData;
const writeToEventMetric = require('./data_access.js').writeToEventMetric;
const writeToEventMetricMontly = require('./data_access.js').writeToEventMetricMontly;
const updateCurrentState = require('./data_access.js').updateCurrentState;
const updateLastnPositions = require('./data_access.js').updateLastnPositions;
// var all = require('bluebird').all
const xchange = 'main_valve';

/*
function getUnitStateId(uiid,updateTime){
    let deltaBo = new Date(updateTime - 14400000);
    return `${uiid}/${deltaBo.toISOString().split('T')[0]}`
}*/


function getUnitStateDate(updateTime){
    let deltaBo = new Date(updateTime - 14400000);
    return new Date(deltaBo.getFullYear,deltaBo.getMonth, deltaBo.getDay)
}

function generateWebNotification() {


}

function getEventHistoricData(msg) {
	//function to work with legacy structures
	let historicData = {
		"velocidad": msg["speed"],
		"referencia":msg["geoReference"]["nearest"],
		"fecha":msg["updateTime"],
		"latitude":msg["latitude"],
		"longitude":msg["longitude"]
	};
	return historicData;
}

function getMetricQuery(unitInstanceId, eventId, date){
	let query = {
		"id_vehiculo":unitInstanceId,
		"id_falta": eventId,
		"date": date,
	}
	return query;
}
function getMetricQueryWeekMonth(unitInstanceId, eventId, year) {
	let query = {
		"id_vehiculo":unitInstanceId,
		"id_falta":eventId, 
		"year":year,
	}
	return query;
}

function getNotificationObject(msg) {
	let notiObject = {
		"unit_id":ObjectId(msg["unitInstanceId"]),
		"description":ObjectId(msg["eventName"]),
		"datetime": moment(msg["updateTime"]),
		"positionn:{
			"latitude": parseFloat(msg["latitude"]),
			"longitude": parseFloat(msg["longitude"])
		},
		"read":[]
	}
	return notiObject;
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
	    let metricQuery = getMetricQuery(contentJson["unitInstanceId"], contentJson["eventId"], 
		    				getUnitStateDate(contentJson["updateTime"]));

            co(writeToEventMetric(metricQuery, getEventHistoricData(contentJson)));
            .then(() => {
		let dateTime = moment(contentJson["updateTime"]);
		let month = dateTime.format('M');
		let week = dateTime.format('W');
		let query = getMetricQueryWeekMonth(contentJson["unitInstanceId"],contentJson["eventId"],
							parseInt(dateTime.format('YYYY')))
                return co(writeToEventMetricMontly(query, week, month));
            }).then(() => {
		let notifObject = getNotificationObject(contentJson); 
		return co(writeWebNotification(notifObject));
            }).then(() => {
                console.log(` [x] ${routingKey}:${contentJson}`)
                ch.ack(msg);

	    }).catch(err =>{
                console.log(`An error has ocurred processing the message ${err}`);
            });

        }
    }).catch(console.warn);
   
})
