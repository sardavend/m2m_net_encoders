const co = require('co');
const getUnitInfo = require('./data_access.js').getUnitInfo;
const reverseGeocoding= require('./reversegeocoding');

/*************SKYPATROL ******************/

/******* Messages Types ************/

const ACK = /^\+ACK:GT/,
		RESP = /^\+RESP:GT/,
		BUFF = /^\+BUFF:GT/;

/************* Report Type - Position Related**************/
const POS_REL_TYPE = "POSITION_REL";
const EV_REL_TYPE = "EVENT_REL";
const QUY_REL_TYPE = "QUERING_REL";
const FIXED = "GTFRI",
		GEO = "GTGEO",
		SOS = "GTSOS",
		SPD = "GTSPD",
		DEVINF = "GTINF",
		FIRST_LOC = "GTPNL",
		NON_MOV = "GTNMR";
/************* Report Type - Event Related**************/
const	PWON = "GTPNA",
		PWOFF = "GTPFA",
		START_CHARGIN= "GTBTC",
		END_CHARGIN="GTSTC",
		PDP = "GTPDP";

/************* Report Type - Queryng Related ************/
const REAL_TIME_BAT ="GTBAT";


const POSITION_REL_REPORTS = [FIXED, FIRST_LOC, GEO, SOS, SPD, NON_MOV];
const EVENTS_REPORTS = [PWON, PWOFF, START_CHARGIN, END_CHARGIN, PDP];
const QUERYNG_REPORTS = [REAL_TIME_BAT];


/******************Parse Message functions *****************/
function validateHeader(header){
	if(header.match(RESP) || header.match(BUFF) || header.match(ACK)){
		return true;
	} else {
		false;
	}
}

function getReportType (header){
	if(validateHeader(header)){
		header = header.split(':');
		console.log(`HEADER: ${header}`)
		return header[1];
	} else {
		return false
	}
}

function generateAckMessage(countNumber){
	return `+SACK:${countNumber}`
}

function parseDateString(dateString){
//20170630152810	
//2017 06 30 15 28 10
//returns unix time in milliseconds
	let year = parseInt(dateString.substring(0,4)),
		month = parseInt(dateString.substring(4,6)),
		day = parseInt(dateString.substring(6,8)),
		hour = parseInt(dateString.substring(8,10)),
		minutes = parseInt(dateString.substring(10,12)),
		seconds =  parseInt(dateString.substring(12,));

	let dateTime= new Date(year ,month , day, hour,
				minutes, seconds);
	return dateTime.getTime();

}


/*****************Preparing the message to be sent to main_valve x-change ****************/
function getPotableMessage(msgObj, rt){
	/*TODO: handle unregistered units when (getUnitInfo, returns null, the function must register a new unit*/
	
	/**
	 * This function generates a messages that will be sent to the rabbitmq xchange main_valve
	 * this message will be used by the metric daemons(faults, logistics, etc), the attributes are the 
	 * following:
	 * {
	 * 	"unitInstanceId":"34342342",
	 *  "unitId":"513253423",
	 *  "plate":"XXX-ABZ",
	 *  "driver":{
	 *   "id":"hashobjectiddelconductor",
	 *   "name":"Mawui Arandia",
	 *   "keyId":"fefasdf",
	 *  }
	 *  "groupId":"",
	 *  "geoReference":{
	 *    "address":"Av. El Cristo",
	 *     "geoZone":"Margarita",
	 *     "nearest":"El Puente",
	 *     "distanceToNearest":14233 // meter
	 *    },
	 * 	"deviceStatus":{ 
	 *		"gpsAccuracy":1, // HDOP the smaller the value, the higher the precision but 0 is NO FIX 
	 * 		 "rssi":"fair"
	 *   },
	 *   "heading":0,
	 * 	 "eventCode":"" ,
	 * 	 "altitude":1545,
	 *   "carrier": "Tigo",
	 *   "latitude":-16.123123,
	 *   "longitude": -63.23234,
	 *   "speed":12,//meters per second
	 *   "updateTime":"",//DateString or unix time in milliseconds (JSON) 
	 *   "inputs":[1,0],
	 *   "odometer":123123123 //meters
	 * }
	 */
	let potableMessage = {};
	let companyId = null;
	console.log(`Report Type is ${rt}`);
	let p = new Promise((resolve, reject) => {

		if (rt == POS_REL_TYPE){
			co(getUnitInfo(msgObj["uniqueID"]))
			.then(unitInfo => {
				companyId = unitInfo["empresa"]
				potableMessage["unitInstanceId"] = unitInfo["_id"];
				potableMessage["unitId"] = unitInfo["dispositivo_actual"]; //currentDevice
				potableMessage["plate"] = unitInfo["descripcion"]; // plate
				potableMessage["heading"]= 0;
				potableMessage["altitude"] = parseFloat(msgObj["altitude"]);
				potableMessage["latitude"] = parseFloat(msgObj["latitude"]);
				potableMessage["longitude"] = parseFloat(msgObj["longitude"]);
				potableMessage["speed"] = msgObj["speed"] // in km/h
				// potableMessage["speed"] = msgObj["speed"] / 3.6; // in m/seg
				potableMessage["updateTime"] =  parseDateString(msgObj["gpsUTCtime"]);
				potableMessage["odometer"] = 0;
				return co(reverseGeocoding.getAddress(potableMessage["latitude"], potableMessage["longitude"]));
			}).then(address => {
				potableMessage["geoReference"] = {};
				potableMessage["geoReference"]["address"] = address;
				return co(reverseGeocoding.getGeozone(potableMessage["latitude"], potableMessage["longitude"], companyId));
			}).then(geoZone => {
				potableMessage["geoReference"]["geoZone"] = geoZone["name"];
				return co(reverseGeocoding.getNearestGeoreference(potableMessage["latitude"], potableMessage["longitude"], companyId));
			}).then(nearest => {
				potableMessage["geoReference"]["nearest"] = nearest["name"];
				potableMessage["geoReference"]["distanceToNearest"] = nearest["distance"];
				resolve(potableMessage);
			}).catch(err => {
				reject(err);
			})
		} else{

			reject("THE MESSAGE IS NOT RELATED TO POSITION(DOES NOT HAVE LAT, LON DATA)");

		}

	});

	return p;

}
/**
 * 
 * @param {string} msg a String in skpyatrol encondig
 */
function SkypatrolDecoder(msg) {
	let msgParameters = msg.split(',');

	let reportType = getReportType(msgParameters[0])
	var rt;
	console.log(`REPORT TYPE ${reportType}`)
	let decodedMessage = {
			"protocolVersion": msgParameters[1],
			"uniqueID": msgParameters[2],
			"deviceName": msgParameters[3],
	};
	if (POSITION_REL_REPORTS.includes(reportType)){ 
		rt = POS_REL_TYPE;
		Object.defineProperties(decodedMessage, {
			"reportID":{value:msgParameters[4]},
			"reportType":{value:msgParameters[5]},
			"number":{value:msgParameters[6]},
			"gpsAccuracy":{value:msgParameters[7]},
			"speed":{value:msgParameters[8]},
			"azimuth":{value:msgParameters[9]},
			"altitude":{value:msgParameters[10]},
			"longitude":{value:msgParameters[11]},
			"latitude":{value:msgParameters[12]},
			"gpsUTCtime":{value:msgParameters[13]},
			"mcc":{value:msgParameters[14]},
			"mnc":{value:msgParameters[15]},
			"lac":{value:msgParameters[16]},
			"cellID":{value:msgParameters[17]},
			"reserved":{value:msgParameters[18]},
			"batteryPercentage":{value:msgParameters[19]},
			"sendTime":{value:msgParameters[20]},
			"countNumber":{value:msgParameters[21]}
		})
		
	} else if(EVENTS_REPORTS.includes(reportType)) {
		switch(reportType){
			case PWON:
			case PWOFF:
				Object.defineProperties(decodedMessage, {
					"sendTime": {value: msgParameters[4]},
					"countNumber": {value: msgParameters[5]}
				})
				break;
			case START_CHARGIN:
			case END_CHARGIN:
				rt = EV_REL_TYPE;
				Object.defineProperties(decodedMessage, {
					"gpsAccuracy":{value: msgParameters[4]},
					"speed":{value: msgParameters[5]},
					"azimuth":{value: msgParameters[6]},
					"altitude":{value: msgParameters[7]},
					"lastLongitude":{value: msgParameters[8]},
					"lastLatitude":{value: msgParameters[9]},
					"gpsUTCtime":{value: msgParameters[10]},
					"mcc":{value: msgParameters[11]},
					"mnc":{value: msgParameters[12]},
					"lac":{value: msgParameters[13]},
					"cellID":{value: msgParameters[14]},
					"reserverd":{value: msgParameters[15]},
					"sendTime":{value: msgParameters[16]},
					"countNumber":{value: msgParameters[17]}
				})
				break;
			default:
				Object.defineProperties(decodedMessage, {
					"sendTime":{value: msgParameters[4]},
					"countNumber":{value: msgParameters[5]}
				})


		}
	} else if (QUERYNG_REPORTS.includes(reportType)) {
		rt = QUY_REL_TYPE;
		Object.defineProperties(decodedMessage, {
			"externalPowerSupply":{value:msgParameters[4]},
			"reserved":{value: msgParameters[5]},
			"batteryPercentage":{value: msgParameters[6]},
			"batteryVoltage": {value: msgParameters[7]},
			"chargin":{value: msgParameters[8]},
			"ledOn":{value: msgParameters[9]},
			"sendTime":{value: msgParameters[10]},
			"countNumber":{value: msgParameters[11]},
		});
	}
	console.log(`Decoded Message ${decodedMessage}`);
	console.log(`message: ${decodedMessage}, type: ${rt}`);
	return {"message":decodedMessage, "rt":rt};
}


module.exports = {
    SkypatrolDecoder,
    getPotableMessage,
    generateAckMessage,
}