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


function * writeToDataUnitLog(message){
    message["unitInstanceId"] = ObjectId(message["unitInstanceId"]);
    message["updateTime"] = new Date(message["updateTime"]);
    console.log(message["updateTime"]);
    yield save(dataUnitColl, message);
}

function * writeToRawData(message){
    message["unitInstanceId"] = ObjectId(message["unitInstanceId"]);
    message["updateTime"] = new Date(message["updateTime"]);
    yield save(rawDataCol, message);
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

function* getDriverInfo(driverId){
    let col = db.collection(driverCol);
    let result = yield col.findOne({"ibutton.code":driverId})
    if (result !== null){
        return result;
    }
    return 'unregistered'
}


module.exports = {
    connect,
    writeToDataUnitLog,
    writeToRawData,
    getUnitInfo,
    getDriverInfo,
    updateUnitState,
    updateCurrentState,
    updateLastnPositions,
}
