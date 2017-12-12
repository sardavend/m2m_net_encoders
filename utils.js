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


module.exports = {
    isMoving,
    setInitStopTime,
}