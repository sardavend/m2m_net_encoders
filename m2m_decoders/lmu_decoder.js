const reverseGeocoding = require("../reversegeocoding.js");
const getUnitInfo = require("../data_access.js").getUnitInfo;
const getDriverInfo = require("../data_access.js").getDriverInfo;
const getEventInfo = require("../data_access.js").getEventInfo;
const co = require('co');

const accumCountFlag = 0x3F;
const accumTypeFlag = 0xC0;

/**
 * 
 * @param {object} msg 
 */

function getOdometer(msg){
    if (msg["accumList"].length > 0){
        return msg["accumList"][0]
    }
    return 0;
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
        co(getUnitInfo(decodedMessage["mobileId"]))
        .then(unitInfo => {
            console.log(`unitInfo is: ${JSON.stringify(unitInfo["dispositivo_actual"])}`);
            companyId = unitInfo["empresa"];
            potableMessage["unitInstanceId"] = unitInfo["_id"];
            potableMessage["unitId"] = unitInfo["dispositivo_actual"];
            potableMessage["plate"] = unitInfo["descripcion"];
            potableMessage["updateTime"] = decodedMessage["updateTime"];
            potableMessage["altitude"] = decodedMessage["altitude"];
            potableMessage["latitude"] = decodedMessage["latitude"];
            potableMessage["longitude"] = decodedMessage["longitude"];
            potableMessage["speed"] = decodedMessage["speed"] * 0.036;
            potableMessage["eventCode"] = decodedMessage["eventCode"];
            potableMessage["heading"] = decodedMessage["heading"];
            potableMessage["odometer"] = getOdometer(decodedMessage);
            if (decodedMessage["accumList"].length >= 3){
                if (decodedMessage["accumList"][2] !== 0){
                    //when the unit is On the driver id comes at index 2 and must be not equal to zero
                    driverKeyId = (decodedMessage["accumList"][2]).toString('16').toUpperCase();
                    return co(getDriverInfo(driverKeyId))
                } else if(decodedMessage["accumList"][3] !== 0){
                    //when the unit turned OFF , driver id comes at index 3 and index 2 becomes 0
                    //this data is actually the last know driver or the last identified driver
                    driverKeyId = (decodedMessage["accumList"][3]).toString('16').toUpperCase();
                    return co(getDriverInfo(driverKeyId))
                    // driverKeyId += "*";
                }
            }
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

            return co(reverseGeocoding.getAddress(decodedMessage["latitude"], decodedMessage["longitude"], companyId));
        }).then(address => {
            potableMessage["geoReference"] = {};
            potableMessage["geoReference"]["address"] = address;
            return co(reverseGeocoding.getGeozone(decodedMessage["latitude"], decodedMessage["longitude"], companyId));
        }).then(geoZone => {
            potableMessage["geoReference"]["geoZone"] = geoZone["name"];
            return co(reverseGeocoding.getNearestGeoreference(decodedMessage["latitude"], decodedMessage["longitude"], companyId));
        }).then(nearest => {
            potableMessage["geoReference"]["nearest"] = nearest["name"];
            potableMessage["geoReference"]["distanceToNearest"] = nearest["distance"];
            return co(getEventInfo(decodedMessage["eventCode"], companyId))
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
 * @param {number} offset 
 * @param {Buffer} msg 
 */

function getAccumList(offset, msg, accumCount){
    msg = msg.slice(offset)
    let i;
    let accumList = []
    for (i = 0; i < accumCount;i++){
        //accumList.push(msg.readInt32BE(i*4));
        accumList.push(msg.readUInt32BE(i*4));
    }
    return accumList;
}

/**
 * @param {Buffer} msg  A binary msg
 * */
function decodeMessage(msg){
        //check reference on ttps://puls.calamp.com/wiki/LM_Direct_Reference_Guide
    let decodedMessage = {};
    const mobileIdLength = msg.readUInt8(1);
    const offset = mobileIdLength + 2;
    const offsetMC = offset + 6;// offset Message Content
    //Option Header
    decodedMessage['options'] = (msg.readUInt8(0)).toString('16');
    decodedMessage['mobileIdLength'] = mobileIdLength 
    decodedMessage['mobileId'] = (msg.readIntBE(2,mobileIdLength)).toString('16');
    decodedMessage['mobileIdTypeLength'] = (msg.readUInt8(offset)).toString('16');
    decodedMessage['mobileIdType'] = (msg.readUInt8(offset + 1)).toString('16');
    //Message Header
    decodedMessage['serviceType'] = (msg.readUInt8(offset + 2)).toString('16');
    decodedMessage['messageType'] = (msg.readUInt8(offset + 3)).toString('16');
    decodedMessage['secuenceNumber'] = (msg.readUInt16BE(offset + 4)).toString('16');
    //Message Content
    decodedMessage['updateTime'] = msg.readUInt32BE(offsetMC) * 1000; // unix time in milliseconds
    decodedMessage['timeToFix'] = msg.readUInt32BE(offsetMC + 4) * 1000; // unix time in milliseconds
    decodedMessage['latitude'] = msg.readInt32BE(offsetMC + 8) * 1e-7; 
    decodedMessage['longitude'] = msg.readInt32BE(offsetMC + 12) * 1e-7;
    decodedMessage['altitude'] = msg.readInt32BE(offsetMC + 16);
    decodedMessage['speed'] = msg.readInt32BE(offsetMC + 20); 
    decodedMessage['heading'] = msg.readInt16BE(offsetMC + 24); 
    decodedMessage['satellites'] = msg.readInt8(offsetMC + 26); 
    decodedMessage['fixStatus'] = msg.readInt8(offsetMC + 27); 
    decodedMessage['carrier'] = msg.readInt16BE(offsetMC + 28); 
    decodedMessage['rssi'] = msg.readInt16BE(offsetMC + 30); 
    decodedMessage['commState'] = msg.readInt8(offsetMC + 32); 
    decodedMessage["hdop"] = msg.readInt8(offsetMC + 33);
    decodedMessage["inputs"] = msg.readInt8(offsetMC + 34);
    decodedMessage["unitStatus"] = msg.readInt8(offsetMC + 35);
    decodedMessage["eventIndex"] = msg.readInt8(offsetMC + 36);
    decodedMessage["eventCode"] = msg.readInt8(offsetMC + 37);
    decodedMessage["accums"] = msg.readInt8(offsetMC + 38);
    let accumCount = decodedMessage["accums"] & accumCountFlag;
    let accumType = decodedMessage["accums"] & accumTypeFlag;
    decodedMessage["append"] = msg.readInt8(offsetMC + 39);
    decodedMessage["accumList"] = getAccumList(offsetMC + 40, msg, accumCount);
    decodedMessage["isDecoded"] = true;

    return decodedMessage;
}
/**
 * 
 * @param {Buffer} msg 
 */
function generateAckMessage(msg){
    /*
    Raw Data:
        83 05 01 02 03 04 05 01 01 02 01 00 01 00 00 00 00 00 00

        Decoded:
        -------Message Header--------
        02           Service Type, for this message 2 = Response to Acknowledged Request
        01           Message Type, for this message 1 = ACK/NAK Message 
        -----Acknowledge Message-----
        00           Type 
        00           Ack 
        00           Spare/Unused
        000000       App Version

        */
    let ackMessage = Buffer.allocUnsafe(19);
    msg.copy(ackMessage, 0, 0, 13);
    ackMessage[9] = 2;	//Services Type
    ackMessage[10] = 1;	//MessageType
    ackMessage.fill(0,13); //Type, Ack, Spare/Unusued, App Version filled with zeroes
    return ackMessage;
}

module.exports = {
    decodeMessage,
    generateAckMessage,
    getPotableMessage,
}
