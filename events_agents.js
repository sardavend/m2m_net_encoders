import { WSAEINVALIDPROCTABLE } from 'constants';

const amqpOpen = require('amqplib').connect('amqp://imonnetplus.com.bo');
const url = 'mongodb://imonnetplus.com.bo:27017/platform2';
const co = require('co');
const moment = require('moment');
const persistenceOpen = require('./data_access.js');
// const persistenceOpen = require('./persistence.js').connect(url);
const writeToRawData = require('./data_access.js').writeToRawData;
const writeToEventMetric = require('./data_access.js').writeToEventMetric;
const writeToEventMetricMontly = require('./data_access.js').writeToEventMetricMontly;
const writeWebNotification = require('./data_access.js').writeWebNotification;
const updateCurrentState = require('./data_access.js').updateCurrentState;
const updateLastnPositions = require('./data_access.js').updateLastnPositions;
const utils = require('./utils.js');
// var all = require('bluebird').all
const xchange = 'main_valve';

/*
function getUnitStateId(uiid,updateTime){
    let deltaBo = new Date(updateTime - 14400000);
    return `${uiid}/${deltaBo.toISOString().split('T')[0]}`
}*/


function getUnitStateDate(updateTime){
    let deltaBo = new Date(updateTime - 14400000);
    //let deltaBo = new Date(updateTime);

    //return new Date(deltaBo.getFullYear(),deltaBo.getMonth() + 1, deltaBo.getDate());
    return new Date(Date.UTC(deltaBo.getFullYear(),deltaBo.getMonth(), deltaBo.getDate(),0,0,0));
}

function generateWebNotification() {


}

function getEventHistoricData(msg) {
	//function to work with legacy structures
	let historicData = {
		"velocidad": msg["speed"],
		"referencia":msg["geoReference"]["nearest"],
		"fecha":new Date(msg["updateTime"] - 14400000),
		"latitude":msg["latitude"],
		"longitude":msg["longitude"]
	};
	return historicData;
}

function getMetricQuery(unitInstanceId, eventId, eventType, date){
	let query = {
		"id_vehiculo":unitInstanceId,
		"id_falta": eventId,
		"id_tipo": eventType,
		"date": date,
	}
	return query;
}

function getMetricQueryDriver(driverInstanceId, eventId, eventType, date){
	let query = {
		"id_conductor":driverInstanceId,
		"id_falta": eventId,
		"id_tipo": eventType,
		"date": date,
	}
	return query;
}
function getMetricQueryWeekMonth(unitInstanceId, eventId, eventType, year) {
	let query = {
		"id_vehiculo":unitInstanceId,
		"id_falta":eventId, 
		"year":year,
		"id_tipo":eventType,
	}
	return query;
}


function getMetricQueryWeekMonthDriver(driverInstanceId, eventId, eventType, year) {
	let query = {
		"id_conductor":driverInstanceId,
		"id_falta":eventId, 
		"year":year,
		"id_tipo":eventType,
	}
	return query;
}


function getNotificationObject(msg) {
	if(msg["eventName"] === 'unregistered'){
		msg["eventName"] = msg["eventName"] + " / " + msg["eventCode"];
		console.log(msg["eventName"]);
		return 
	}
	let notiObject = {
		"unit_id":msg["unitInstanceId"],
		"description":msg["eventName"],
		"datetime": new Date(msg["updateTime"]),
		"position":{
			"latitude": parseFloat(msg["latitude"]),
			"longitude": parseFloat(msg["longitude"])
		},
		"read":[]
	}
	return notiObject;
}

function checkSchedule(updateTime, startDate, endDate) {
	if (updateTime >= startDate && updateTime <= endDate){
		return true;
	}
	return false;
}

function getUnauthorizedSchedule(updateTime, startHour, duration){
	// let bolivianTime = new Date(updateTime.getTime() - 14400000);
	let bolivianTime = utils.utcToBolDate(updateTime);
	let startDate = bolivianTime.setHours(startHour,0,0);
	let endDate = new Date(startDate.getTime() + duartion * 3600 * 1000);
	return {
		startDate: startDate,
		endDate: endDate
	}
}

function checkUnauthorizedSchedule(msg, days, startHour, duration){
	//let day = msg['updateTime'].getDay();
	let bolUpdateTime = utils.utcToBolDate(msg['updateTime']);
	let day = bolUpdateTime.getDay();
	let uaSchedule= getUnauthorizedSchedule(msg['updateTime'], startHour, duration);
	if (day in days && checkSchedule(bolUpdateTime, uaSchedule.startDate, uaSchedule.endDate)){
		return true;
	}
	return false;

}

function evaluateUnauthorizedDriving(unit, pMsg, faul) {
	let p = new Promise((resolve, reject) => {
		if (!pMsg.hasOwnProperty('setting') && !pMsg['setting'].hasOwnProperty('min_speed')){
			throw `Unit ${unit} without a valid setting`;
		}
		if(pMsg['speed']> pMsg['setting']['min_speed']){
			if(unit['isDrivingUnauthorized'] && !checkUnauthorizedSchedule(pMsg, faul['dias'],faul['hora_inicial'],faul['duracion'])){
				//if was driving unathorized but their schedule is no longer valid
					co(setUnathorizedDrivingState(unit['unitInstanceId'], false))
				.then(() => {
					co(saveUnauthorizedDrivingHistoric(unit['unitInstanceId'], 'end'));
				});
			}
			if(!unit['isDrivingUnauthorized'] && checkUnauthorizedSchedule(pMsg, faul['dias'],faul['hora_inicial'],faul['duracion'])){
				setUnathorizedDrivingState(unit['unitInstanceId'], true);
				increaseFaulCountDay(unitInstanceId, );
				saveUnauthorizedDrivingHistoric(unitInstanceId, 'start');
			}
		} else{
			setUnathorizedDrivingState(unitId, false);
			saveUnauthorizedDrivingHistoric(unitInstanceId, 'end');
		}
	})
	return p;


} 



//main
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
            return ch.assertQueue('event_log', {exclusive:false})
        });

        ok = ok.then(qok => {
            let queue = qok.queue;
            return ch.bindQueue(queue,xchange, 'ev_log').then(() => queue)
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
			let uT = getUnitStateDate(contentJson["updateTime"]);
			let metricQuery = getMetricQuery(contentJson["unitInstanceId"], contentJson["eventId"], contentJson["eventType"],uT);
			if(contentJson["driver"]["id"] !== "N/A"){
				//let metricQueryDriver = getMetricQueryDriver(contentJson["driver"]["id"], contentJson["eventId"], contentJson["eventType"], uT)
				//console.log(contentJson["driver"]["id"])
				//console.log(contentJson["driver"]["name"])
				let metricQueryDriver = getMetricQuery(contentJson["driver"]["id"], contentJson["eventId"], contentJson["eventType"], uT)
				co(writeToEventMetric(metricQueryDriver, getEventHistoricData(contentJson)))
				.then(() => {
					let dateTime = moment(contentJson["updateTime"]);
					let month = dateTime.format('M');
					let week = dateTime.format('W');
					//let query = getMetricQueryWeekMonthDriver(contentJson["driver"]["id"],contentJson["eventId"],contentJson["eventType"],
					let query = getMetricQueryWeekMonth(contentJson["driver"]["id"],contentJson["eventId"],contentJson["eventType"],
										parseInt(dateTime.format('YYYY')));
					return co(writeToEventMetricMontly(query, week, month));
				}).then(() =>{
					console.log(` [x] ${routingKey}:${contentJson}`)
					//ch.ack(msg);

				}).catch(err =>{
					console.log(`An error has ocurred processing the message ${err}`);
				});

			}

			co(writeToEventMetric(metricQuery, getEventHistoricData(contentJson)))
				.then(() => {
				let dateTime = moment(contentJson["updateTime"]);
				let month = dateTime.format('M');
				let week = dateTime.format('W');
				let query = getMetricQueryWeekMonth(contentJson["unitInstanceId"],contentJson["eventId"],contentJson["eventType"],
									parseInt(dateTime.format('YYYY')));
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
   
}).catch(console.warn);
