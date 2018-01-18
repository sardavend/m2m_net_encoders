import { watch } from "fs";

const redis = require("redis"), 
      client = redis.createClient();
const currentPosition = 'CURRENT_POSITION', formerPosition = 'FORMER_POSITION';


client.on("error", err => console.log("Error " + err));


function isMoving(mvTh, currentSpeed) {
    if (currentSpeed > mvTh){
        return true;
    } else {
        return false;
    }
}

function setInitStopTime(isMoving, lastStopTime, lastMessageTime) {
    if (isMoving) {
        return 0
    } else {
        if(lastStopTime === undefined){
            return lastMessageTime;
        }
        if (lastStopTime !== 0 && lastStopTime < lastMessageTime) {
            return lastStopTime;
        } else {
            return lastMessageTime;
        }
    }

}

function getDistance(unitId, formerPosition, currentPosition) {
    let p = new Promise((resolve, reject) => {
        client.geodist(unitId, currentPosition, formerPosition, (err, result) => resolve(result));
    });
    return p;
}

function getFormerPositions(unitId){
    //Return 
    let p = new Promise((resolve, reject) => {
        client.geopos(unitId, currentPosition, formerPosition, (err, result) => resolve(result));
    })
    return p;
}

function hasSetting(msg){
	if (msg.hasOwnProperty('setting_id') && msg['setting_id'] !== null) {
		return true;
	}
	return false;
}

function hasFauls(msg){
	if (msg.hasOwnProperty('eventList') && msg['eventList'].length) {
		return true;
	}
    return false;
}


function utcToBolDate(utcDate){
    return new Date(utcDate.getTime() - 14400000);
}






module.exports = {
    isMoving,
    setInitStopTime,
    hasFauls,
    hasSetting,
    utcToBolDate,
}