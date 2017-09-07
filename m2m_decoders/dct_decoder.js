const reverseGeocoding = require("../reversegeocoding.js");
const getUnitInfo = require("../data_access.js").getUnitInfo;
const getDriverInfo = require("../data_access.js").getDriverInfo;
const getEventInfo = require("../data_access.js").getEventInfo;
const co = require('co');

// const accumCountFlag = 0x3F;
// const accumTypeFlag = 0xC0;
const unixGpsTimeDiff = 315964800; //unix seconds 1 january 1960 | GPS seconds 6 january 1980
function getUpdateTime(dm) {
    let weeksSenconds = dm["numOfWeeks"] * 7 * 24 * 3600;
    let daySeconds = dm["dayOfWeek"] * 24 * 3600;
    return (weeksSenconds + daySeconds + parseInt(dm["secondsOfDay"]) + unixGpsTimeDiff) * 1000; //milliseconds

}

function getCoordinate(degree, decimal) {
    return parseFloat(`${degree}.${decimal}`);
}


/**
 * 
 * @param {object} decodedMessage 
 */
function getPotableMessage(decodedMessage) {

    if(decodedMessage["isDecoded"] != true){
        throw new Error("You need to decoded the message first with decodedMessage function before calling this function");
    }
    let potableMessage = {};
    let companyId;
    let driverKeyId;
    let p = new Promise((resolve,reject) => {
        co(getUnitInfo(decodedMessage["unitId"]))
        .then(unitInfo => {
            console.log(`unitInfo is: ${JSON.stringify(unitInfo["dispositivo_actual"])}`);
            companyId = unitInfo["empresa"];
            potableMessage["unitInstanceId"] = unitInfo["_id"];
            potableMessage["unitId"] = unitInfo["dispositivo_actual"];
            potableMessage["plate"] = unitInfo["descripcion"];
            potableMessage["updateTime"] = getUpdateTime(decodedMessage);
           potableMessage["altitude"] = 0;
            potableMessage["latitude"] = getCoordinate(decodedMessage["latitudeDegree"], decodedMessage["latitudeDecimal"]);
            potableMessage["longitude"] = getCoordinate(decodedMessage["longitudeDegree"], decodedMessage["longitudeDecimal"]);
            potableMessage["speed"] = parseFloat(decodedMessage["speed"]) * 1.6; //mph to kmh
            potableMessage["eventCode"] = parseInt(decodedMessage["eventIndex"]);
            potableMessage["heading"] = parseInt(decodedMessage["heading"]);
            potableMessage["odometer"] = decodedMessage["odometer"];
            if (decodedMessage["ibutton"] !== 0){
                return co(getDriverInfo(decodedMessage["ibutton"]));
            }
            // if (decodedMessage["accumList"].length >= 3){
            //     if (decodedMessage["accumList"][2] !== 0){
            //         //when the unit is On the driver id comes at index 2 and must be not equal to zero
            //         driverKeyId = (decodedMessage["accumList"][2]).toString('16').toUpperCase();
            //         return co(getDriverInfo(driverKeyId))
            //     } else if(decodedMessage["accumList"][3] !== 0){
            //         //when the unit turned OFF , driver id comes at index 3 and index 2 becomes 0
            //         //this data is actually the last know driver or the last identified driver
            //         driverKeyId = (decodedMessage["accumList"][3]).toString('16').toUpperCase();
            //         return co(getDriverInfo(driverKeyId))
            //         // driverKeyId += "*";
            //     }
            // }
            //there is no driver info in the message (N/A)
            return new Promise((resolve, reject) => {
                resolve(null)
            })

        }).then(driverInfo => {
            potableMessage["driver"] = {};
            if (driverInfo === null){
                potableMessage["driver"]["id"] = "N/A";
                potableMessage["driver"]["name"] = "N/A";
                potableMessage["driver"]["keyId"] = "N/A";
            }
            else if(driverInfo == 'unregistered'){
                potableMessage["driver"]["id"] = "U/D";
                potableMessage["driver"]["name"] = "U/D";
                potableMessage["driver"]["keyId"] = driverKeyId;
            } else{
                potableMessage["driver"]["id"] = driverInfo["_id"];
                potableMessage["driver"]["name"] = driverInfo["name"];
                potableMessage["driver"]["keyId"] = driverKeyId; 
            } 

            return co(reverseGeocoding.getAddress(potableMessage["latitude"], potableMessage["longitude"], companyId));
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
	    return co(getEventInfo(potableMessage["eventCode"], companyId));
        }).then(eventInfo => {
	    if(eventInfo !== 'unregistered'){
	    	potableMessage["eventId"] = eventInfo["_id"];
		    potableMessage["eventName"] = eventInfo["nombre"];
            potableMessage["eventType"] = eventInfo["id_tipo"];
	    } else {
            potableMessage["eventId"] = eventInfo;
            potableMessage["eventName"] = eventInfo;
            potableMessage["eventType"] = eventInfo;
	    }
            resolve(potableMessage);
	}).catch(err => {
            reject(err);
        })
    }).catch(err => {
        console.log(`An error has ocurred in getPotableMessage ${err}`);
    })
    return p;
}
/**
 * 
 * @param {string} msg 
 */

function getMessageParts(msg){
    messageParts = msg.split(';');
    if (messageParts.length > 2){ //is an extended message
        return {'messageParts':messageParts, 'isExtended':true};
    }
    return {'messageParts':messageParts, 'isExtended':false};
}

const TEMPERATURE_PATTERN =/^EA/,
    VIRTUAL_OD_PATTERN = /^VO/,
    INPUTS_PATTERN = /^IO/;

const MESSAGE_PATTERNS = [TEMPERATURE_PATTERN, VIRTUAL_OD_PATTERN, INPUTS_PATTERN]

function getTemperature(tempData){
    let sensorRead = tempData.split('=')[1];
    let sensorList = [];
    if (sensorRead.indexOf("A") >= 0 ){
        sensorList.push(sensorRead.substring(1,4));
    }
    if (sensorRead.indexOf("B") >= 0){
        sensorList.push(sensorRead.substring(5,8));
    }
    if (sensorRead.indexOf("C") >= 0){
        sensorList.push(sensorRead.substring(9,sensorRead.length -1));
    }

    return {"sensors":{"temperature":sensorList}};
    
    
}

function getVirtualOdometer(odometerData){
    let odo = odometerData.split('=');// in meters?
    return {"odometer": odo[1]};
}

function getInputsOutputs(ioData){
    let inputs = ioData.split('=');
   return {"inputs":inputs[1]};
}

function getIbutton(ibData) {
        let ib = ibData.split('=');
        return {"ibutton": ib};
}

/**
 * 
 * @param {array} messageParts 
 */

function getExtendedMessage(messageParts) {
    let extendedData = {};
    let extendedParts = Array.from(messageParts
                                    .entries())
                                    .filter(e => e[0] != 0 && e[0] != messageParts.length - 1)
                                    .map(exEv => {
                                        switch (exEv.substring(0,3)) {
                                            case 'EA':
                                                Object.assign(extendedData, getTemperature(exEv));
                                                break;
                                            case 'VO':
                                                 Object.assign(extendedData, getVirtualOdometer(exEv));
                                                break;
                                            case 'IO':
                                                Object.assign(extendedData, getInputsOutputs(exEv));
                                                break;
                                            case 'IB':
                                                Object.assign(extendedData, getIbutton(exEv));
                                                break;
                                            }
                                    });


    return extendedData;
    /*
    let exEv = messageParts[1];
    //let extendedData;
    switch (exEv.substring(0,3)) {
        case 'EA':
            extendedData = getTemperature(exEv);
            break;
        case 'VO':
            extendedData = getVirtualOdometer(exEv);
            break;
        case 'IO':
            extendedData = getInputsOutputs(exEv);
            break;
        case 'IB':
            extendedData = getIbutton(exEv);
            break;
    }
    return extendedData;*/

}
/**
 * 
 * @param {array} messageParts 
 */
function getEventPart(messageParts) {
    let evPart  = messageParts[0];
    let eventReport = {};
    eventReport["eventIndex"] = evPart.substring(4,6);
    eventReport["numOfWeeks"] = evPart.substring(6,10);
    eventReport["dayOfWeek"] = evPart.substring(10,11);
    eventReport["secondsOfDay"] = evPart.substring(11,16);
    eventReport["latitudeDegree"] = evPart.substring(16,19);
    eventReport["latitudeDecimal"] = evPart.substring(19,24);
    eventReport["longitudeDegree"] = evPart.substring(24,28);
    eventReport["longitudeDecimal"] = evPart.substring(28,33);
    eventReport["speed"] = evPart.substring(33,36);
    eventReport["heading"] = evPart.substring(36,39);
    eventReport["gps"] = evPart.substring(39,40);
    eventReport["fixTime"] = evPart.substring(40,41);

    return eventReport;
}

/**
 * 
 * @param {Array} msgParts 
 */

function getDeviceId(msgParts){
    let dId = msgParts[msgParts.length -1].split('=');
    return dId[1].substring(0, dId[1].length -1);
}

function validateMessage(msg) {
    if(msg.startsWith(">REV") && msg.endsWith("<")){
        return true;
    }
    return false;
}



/**
 * @param {string} msg  A Ascii string msg
 * */
function decodeMessage(msg){
    msg = msg.replace(/(\r\n)/,"");

    if (!validateMessage(msg)){
        return false;
    }
 
    let {messageParts, isExtended } = getMessageParts(msg);
    let deviceId = getDeviceId(messageParts);
    let extendedParts = {"odometer":0, "ibutton":0, "inputs":"", "sensors":{"temperature":999}};
    if (isExtended){
        extendedParts = getExtendedMessage(messageParts);
    }
    let decodedMessage = {};
    decodedMessage = Object.assign(extendedParts, getEventPart(messageParts));
    decodedMessage["unitId"] = deviceId;
    decodedMessage["isDecoded"] = true;

    return decodedMessage;
}
/**
 * 
 * @param {Buffer} msg 
 */
function generateAckMessage(msg){
    console.log(0);
}

module.exports = {
    decodeMessage,
    generateAckMessage,
    getPotableMessage,
}
