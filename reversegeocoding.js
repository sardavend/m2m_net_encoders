const MongoClient = require('mongodb').MongoClient;
const ObjectID = require('mongodb').ObjectID;
const url = 'mongodb://45.56.77.145:27017/platform2';
//const url = 'mongodb://imonnetplus.com.bo:27017/platform2';
var db;
MongoClient.connect(url)
    .then(resp => db = resp)
    .catch(err => console.log(err))


/*******Collections  **************/
const georeferenceCol = 'geocerca';
const addressCol = 'georeferencia';
const unitsCol = 'vehiculoTest';
const driverCol = 'driver';

/*******Properties ****************/
const compId = "properties.empresaId";
const currentDeviceId = "dispositivo_actual";

/***********Connect to mongo ***********/
function* connect() {
    db = yield MongoClient.connect(url)
 };


/**********geoFunction **************/
/**
 * @param {number} lat A latitude of the position
 * @param {number} lon A longitude of the position
 * @param {string} companyId A valid ObjectId in string 
 * @returns {object} 
 */
function* getGeozone(lat, lon, companyId){
    if (companyId == "SIN-EMPRESA"){
        return {"name":"N/A"};
    }
    let oid = new ObjectID(companyId);
    let col = db.collection(georeferenceCol);
    let queryIntersects = {"$geometry":{"type":"Point","coordinates":[lon, lat]}};
    let result = yield col.findOne({"properties.empresaId":oid,
                                    "properties.state": true,
                                    "geometry":{"$geoIntersects":queryIntersects}
                                });
    if(result){
        return result["properties"];
    } else {
        return {"name":"N/A"};
    }
    
}

const radiansDistance = 2/6378.1;// max distance in radians, change the dividend if you want to change the km
function* getAddress(lat, lon){
    let col = db.collection(addressCol);
    let docs = yield col.geoNear(lat, lon, {num:1, spherical:true,maxDistance:radiansDistance});
    //console.log(docs.results);
    if (docs.results.length) {
        return docs.results[0]["obj"]["desc1"] +", "+ docs.results[0]["obj"]["desc2"];
    } else {
        return "N/A";
    }
}
const distanceMeters = 10000
function* getNearestGeoreference(lat, lon, companyId) {
    if (companyId == "SIN-EMPRESA"){
        return {"name":"N/A", "distance":0}
    }
    let col = db.collection(georeferenceCol)
    let cursor = col.aggregate([
       {"$geoNear":{
           "near":{"type":"Point", "coordinates":[lon,lat]},
           "distanceField":"dist.calculated",
           "maxDistance":distanceMeters,
           "query":{"properties.empresaId":new ObjectID(companyId),"properties.state":true},
           "includeLocs":"dist.location",
           "num":1,
           "spherical":true
       }} 
    ])
    let results = yield cursor.next();
    if (results){
        return {
            "name":results.properties.name,
            "distance":results.dist.calculated
        }
    } else {
        return {"name":"N/A", "distance":0}
    }
}




module.exports = {
    connect,
    getGeozone,
    getAddress,
    getNearestGeoreference,
}
