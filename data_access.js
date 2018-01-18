const MongoClient = require('mongodb').MongoClient;
const ObjectId = require('mongodb').ObjectID;

const co = require('co');



/*******Collections  **************/
const unitsCol = 'vehiculoTest';
const driverCol = 'driver';
const dataUnitColl = 'unitDataLog';
const rawDataCol = 'rawdata';
const unitStateCol = 'unitState';
const lastNCol = 'lastnpositions';
const eventCol = 'falta';
const dailyEventsCol = 'faltas.metricas.diarias';
const weeklyYearlyEventsCol = 'faltas.metricas';
const notifcationColl = 'notification';
const settingsCol = 'settings';
const UNREGISTERED_EVENT = 'unregistered'


var db;

function* connect(url) {
    db = yield MongoClient.connect(url)
};

function* save(coll, insertObject){
    yield db.collection(coll).insert(insertObject);
}
function* update(coll,cond, updateObject){
    yield db.collection(coll).update(cond, updateObject,{"upsert":true});
}

function * writeWebNotification(message) {
	//legacy function to mantain current web notification working
    if (message  == undefined){
        return
    }
    message["unit_id"] = ObjectId(message["unit_id"]);
	yield save(notifcationColl, message);
}

function * writeToDataUnitLog(message){
    message["unitInstanceId"] = ObjectId(message["unitInstanceId"]);
    message["updateTime"] = new Date(message["updateTime"]);
    console.log(message["updateTime"]);
    yield save(dataUnitColl, message);
}

function *setUnauthorizedDrivingState(unitInstanceId, state){
    let query = {"_id": ObjectId(unitInstanceId)},
        udp = {"$set":{"isDrivingUnauthorized":state}}
    yield update(unitsCol, query, udp);
}

function * writeToRawData(message){
    message["unitInstanceId"] = ObjectId(message["unitInstanceId"]);
    message["updateTime"] = new Date(message["updateTime"]);
    yield save(rawDataCol, message);
}

function* writeToEventMetric(queryObject, historicData){
	if (queryObject["id_falta"] === UNREGISTERED_EVENT){
		return 
	}
    /*
    if ("id_vehiculo" in queryObject){
	    queryObject["id_vehiculo"] = ObjectId(queryObject["id_vehiculo"]);
    } else {
        queryObject["id_conductor"] = ObjectId(queryObject["id_conductor"]);
    }*/

    queryObject["id_vehiculo"] = ObjectId(queryObject["id_vehiculo"]);
	queryObject["id_falta"] = ObjectId(queryObject["id_falta"]);
	let updateObject = {
		"$inc":{"total":1},
		"$push":{"historic":historicData}
	}
	yield update(dailyEventsCol, queryObject, updateObject)
}

function* writeToEventMetricMontly(queryObject, numWeek, month){
	if (queryObject["id_falta"] === UNREGISTERED_EVENT){
		return 
	}
	let weekString = `week.${numWeek}`;
	let monthString = `month.${month}`;
    /*
    if ("id_vehiculo" in queryObject){
        queryObject["id_vehiculo"] = ObjectId(queryObject["id_vehiculo"]);
    } else {
        queryObject["id_conductor"] = ObjectId(queryObject["id_conductor"]);
    }*/
    
    queryObject["id_vehiculo"] = ObjectId(queryObject["id_vehiculo"]);
	queryObject["id_falta"] = ObjectId(queryObject["id_falta"]);
    updateObject = {};
    updateObject["$inc"] = {};
    updateObject["$inc"][`${weekString}`] = 1,
    updateObject["$inc"][`${monthString}`] = 1,
    updateObject["$inc"]['total'] = 1

	yield update(weeklyYearlyEventsCol, queryObject, updateObject);
}

function * updateUnitState(message){
    message["currentState"]["updateTime"] = new Date(message["currentState"]["updateTime"]);
    let cond ={"_id":message["_id"]};
    yield update(unitStateCol, cond,  message);
}
// legacy function
function * updateCurrentState(message){
    //for legacy field, estado_actual
    console.log(message["unitInstanceId"].toString())
    let cond = {"_id":message["unitInstanceId"]};
    let msg = {"$set":{"estado_actual":message}};
    yield update(unitsCol, cond, msg);
}

function * updateLastnPositions(message){
    let position = {
        "date":message["updateTime"],
        "speed":message["speed"],
        "address":message["address"],
        "geoReference":message["geoReference"],
        "nearestGeoReference":message["nearestGeoReference"],
        "latitude":message["latitude"],
        "longitude":message["longitude"],
        "eventName":message["eventCode"],
        "heading":message["heading"],
    }
    let cond = {"_id":message["unitInstanceId"]};
    let msg = {
        "$push":{
            "positions":{
                "$each":[position],
                "$position":0,
                "$sort":{"date":-1},
                "$slice":10
            }
        }
    };
    yield update(lastNCol, cond, msg);
     

}

function writeToUnitStatus(message){
    return co(function* (){
        let unitStatusId = getUnitStatusId(message["unitInstanceId"], new Date(message["updateTime"]))
        console.log(`Unit Status id is: ${unitStatusId}`);
    })
}


function* getUnitInfo(unitId) {
    let col = db.collection(unitsCol)
    let result = yield col.findOne({"dispositivo_actual":unitId, "state":true})
    // let result = yield col.findOne({})
    console.log(`result is: ${result}`);
    if (result !== null){
        return result;
    } 
    // throw new Error('unregistered unit');
    // return registerUnit(unitId);

    console.log(`registering new Unit ${unitId}`)
    col = db.collection(unitsCol)
    let newUnitObj = getNewUnitObject(unitId);
    try{
        let newUnit = yield col.insertOne(newUnitObj);
        return yield col.findOne({"_id":newUnit["insertedId"]});
    } catch (e){
        console.log(`An error has ocurred when registering a new unit: ${e}`);
    }
    
}

function* getUnitSetting(unitSetting) {
    if (unitSetting == null) {
        return null;
    }
    let col = db.collection(settingsCol);
    let result = yield col.findOne({"_id":ObjectId(unitSetting)});
    return result;
}

function getNewUnitObject(unitId){
    return {
        "id_grupo" : null,
        "empresa" : "SIN-EMPRESA",
        "equipments" : [],
        "transmission" : null,
        "estado" : {
            "visitas" : [],
            "distancia" : 0
        },
        "owners" : [],
        "setting_id" : null,
        "wheelDrive" : null,
        "placa" : "",
        "fuel" : null,
        "state" : true,
        "loadCapacity" : null,
        "enginePower" : null,
        "descripcion" : "SIN-REGISTRO",
        "dispositivo_actual" : unitId,
        "icono" : "icon-icon_cart",
        "eventList":[],
        "sensores" : {
            "temperatura" : false,
            "compresion" : false,
            "puerta" : false
        },
        "dispositivo_info" : {
            "modelo" : "",
            "id" : unitId,
            "marca" : ""
        },
        "riderShip" : null,
        "estado_actual" : {
            "isActive" : false
        }
    }
}
function* registerUnit(unitId) {
    console.log(`registering new Unit ${unitId}`)
    let col = db.collection(unitsCol)
    let newUnit = getNewUnitObject(unitId);
    try{
        let result = yield col.insertOne(newUnit);
        return yield col.findOne({"_id":result["insertedId"]});
    } catch (e){
        console.log(`An error has ocurred when registering a new unit: ${e}`);
    }
}

function* getEventInfo(eventCode, companyId) {
    //event/faul
	eventCode = parseInt(eventCode);
	let col = db.collection(eventCol);
	let result = yield col.findOne({"evento":eventCode, "id_empresa":companyId})
	if (result !== null) {
        console.log('Registered event found')
        console.log(result);
		return result
	}
    console.log(`uregisted event ${eventCode}`);
	return UNREGISTERED_EVENT;
}

// function* hasFaulsAssigment(unitId) {
//     if(!(ObjectId.isValid(unitId))) {
//         throw 'unitId must be a valid ObjectId';
//     }
//     let col = db.collection(unitsCol);
//     let result = yield col.findOne({"_id":})
// }

function getEventInfoById(faulId,companyId) {
    let p = new Promise((resolve, reject) => {
        if (!(ObjectId.isValid(faulId)) || !(ObjectId.isValid(companyId))){
            throw "faulId and companyId both must be valid ObjectIds";
        }
        let col = db.collection(eventCol);
        col.findOne({"_id":faulId,"id_empresa":companyId})
        .then(result => {
            if (result !== null) {
                console.log('Event/Faul found');
                console.log(result);
                resolve(result);
            }
            resolve(UNREGISTERED_EVENT); 
        })
    });
    return p;

}

function getEventList(eventList, companyId) {
    let p = new Promise((resolve, reject) => {
        console.log(eventList);
        detailedEventList = [];
        eventList.map( eventId => {
            //detailedEventList.push(yield getEventInfoById(eventId, ))
            getEventInfoById(eventId, companyId);
        });
        return detailedEventList;
    });
    return p;



function* getDriverInfo(driverId){
    let col = db.collection(driverCol);
    let result = yield col.findOne({"ibutton.code":driverId,"state":true})
    if (result !== null){
        return result;
    }
    return 'unregistered'
}



module.exports = {
    connect,
    writeToDataUnitLog,
    writeToRawData,
    writeToEventMetric,
    writeToEventMetricMontly,
    writeWebNotification,
    getUnitInfo,
    getDriverInfo,
    getUnitSetting,
    updateUnitState,
    updateCurrentState,
    updateLastnPositions,
    getEventInfo,
    getEventInfoById,
    getEventList,
}
