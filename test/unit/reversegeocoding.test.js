const { reverseModule, expect, co } = require('../common');

describe('Reverse Geocoding Functions', function(){
    describe("getUnitInfo", function(){
        it('with argument "4332095991" should return a unit Object', function(){
            return co(reverseModule.connect())
                .then(() =>{
                    return co(reverseModule.getUnitInfo("4332095991"))
                }).then((unit) => {
                    //console.log(unit);
                    expect(unit).to.have.property('dispositivo_actual').to.equal("4332095991")
                    expect(unit).to.have.property('state').to.equal(true)
                })
                //.catch(err => console.log(err))
        })
        it('with argument "asdf" should return null', function(){
            return co(reverseModule.getUnitInfo("asdf"))
                .then(unit => {
                    expect(unit).to.be.null;
                })
        })
    })

    describe("getAddress", function(){
        it("should resolve a valid address", function(){
            return co(reverseModule.getAddress(-17.418596, -66.165185))
                .then(address => {
                    expect(address).to.be.a('string');
                    expect(address).to.contain('Guerrilleros, Ccba')
                })
        })
        it("should resolve N/A", function(){
            return co(reverseModule.getAddress(0,0))
                .then(address => {
                    expect(address).to.be.a('string');
                    expect(address).to.equal("N/A")
                })
        })
    })

    describe('getGeozone', function(){
        it("should resolve the current geozone", function(){
            return co(reverseModule.getGeozone(-17.812163, -63.184247,"54529f70421aa91b9da3f1d7"))
                .then(geozone => {
                    expect(geozone).to.have.property('name').to.equal("circutest");


                })
        })
        it("should resolve N/A", function(){
            return co(reverseModule.getGeozone(-17.772546, -63.194207,"54529f70421aa91b9da3f1d7"))
                .then(geozone => {
                    expect(geozone).to.be.a("string");
                    expect(geozone).to.equal("N/A");
                })
        })
    })

    describe('getNearestGeoreference', function(){
        it('should resolve the nearest point or zone within a max distance of 100km', function(){
            return co(reverseModule.getNearestGeoreference(-17.772546, -63.194207,"54529f70421aa91b9da3f1d7"))
                .then(nearestGeo => {
                    expect(nearestGeo).to.have.property("name").to.equal("circutest");
                    expect(nearestGeo).to.have.property("distance").to.be.a("number");
                })

        })
        it("should return N/A if no point or zone is found wihtin max distance of 100km", function(){
            return co(reverseModule.getNearestGeoreference(-15.899906, -63.197398,"54529f70421aa91b9da3f1d7"))
                .then(nearestGeo => {
                    expect(nearestGeo).to.be.a("string");
                    expect(nearestGeo).to.equal("N/A");
                })

        })
    })
})