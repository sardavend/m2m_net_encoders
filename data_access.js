const MongoClient = require('mongodb').MongoClient;
const ObjectId = require('mongodb').ObjectID;



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
    message["unitInstanceId"] = Object(message["unitInstanceId"]);
    message["updateTime"] = new Date(message["updateTime"]);
    yield save(rawDataCol, message);
}

function* getUnitInfo(unitId) {
    let col = db.collection(unitsCol)
    let result = yield col.findOne({"dispositivo_actual":unitId, "state":true})
    // let result = yield col.findOne({})
    if (result !== null){
        return result;
    } 
    throw new Error('unregistered unit');
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
    getUnitInfo,
    getDriverInfo,
}