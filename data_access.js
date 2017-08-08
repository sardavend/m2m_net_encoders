const MongoClient = require('mongodb').MongoClient;
const ObjectId = require('mongodb').ObjectID;

const co = require('co');



/*******Collections  **************/
const unitsCol = 'vehiculoTest';
const driverCol = 'driver';
const dataUnitColl = 'data_unit_log';
const rawDataCol = 'rawdata';


var db;

function* connect(url) {
    db = yield MongoClient.connect(url)
};

function* save(coll, insertObject){
    yield db.collection(coll).insert(insertObject);
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
/**
 * 
 * @param {string} uiid 
 * @param {Date} updateTime 
 */
function getUnitStatusId(uiid,updateTime){
    return `${uiid}/${updateTime.toISOString.split('T')[0]}`
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
    return co(registerUnit(unitId))
    .then(newUnit => {
        console.log(newUnit);
        console.log(`a new unit was registered ${newUnit}`);
        return newUnit;
    });
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
function registerUnit(unitId) {
    return co(function* (){
        console.log(`registering new Unit ${unitId}`)
        let col = db.collection(unitsCol)
        let newUnit = getNewUnitObject(unitId);
        try{
            let result = yield col.insertOne(newUnit);
            return yield col.findOne({"_id":result["_id"]});
        } catch (e){
            console.log(`An error has ocurred when registering a new unit: ${e}`);
        }
    })
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
}