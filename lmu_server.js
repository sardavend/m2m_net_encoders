const dgram = require('dgram');
const net = require('net');

const winston = require('winston')
const openAmqp = require('amqplib').connect('amqp://imonnetplus.com.bo');
const co = require('co');
const lmuDecoder= require('./m2m_decoders/lmu_decoder');
const reverseGeocoding = require('./reversegeocoding.js');
const dataAccessConnect = require('./data_access.js');
const port = 1996;
const xchange= 'main_valve';

const url = 'mongodb://imonnetplus.com.bo:27017/platform2';

winston.add(winston.transports.File, {filename: 'lmu.log', level:'debug'});

function LMU_Server(chan){
    let devices = []
    let clients = [];
    const server = dgram.createSocket("udp4");

    /**
     * 
     * @param {string} message An hex string to be send to all the connected clients
     */

    function broadcast(message){
        clients.forEach(client => {
            try {
                client.write(message);
            } catch (e){
                console.log("An error has ocurred when trying to replicate to clients")
                winston.log("An error has ocurred when trying to replicate to clients")
                winston.log('error', e.toString())

            }
        })
    }

    const serverClientCalamp = net.createServer(client => {
        console.log(client);
        client.name = `${client.remoteAddress}:${client.remotePort}`;
        clients.push(client);
        winston.log('debug', `Client connected: ${client.name}`);
        console.log(`Client connected: ${client.name}`);

        client.on('end',() => {
            winston.log(`Client disconnected: ${client.name}`);
        });
        client.on('data', (data) => {
            try {
             console.log('command recieved');
             let {unitId, command} = JSON.parse(data);
             sendTelecommand(unitId, command)
            } catch (error) {
                console.log(error);
                
            }
        });



    })
    serverClientCalamp.listen(8005,() => {
        winston.log('debug',`Server Calamp Clients started on port 8000`);
        console.log(`Server Calamp Clients started on port 8000`);

    })
    serverClientCalamp.on('error',err => {
        console.log(err);
    })

    server.on('listening',() => {
        let address = server.address();
        console.log(`LMU Server(UDP) ready: ${address.address}:${address.port}`);
    })

    server.on('message',(msg, rinfo) => {
        let deviceInfo = `${rinfo.address}:${rinfo.port}`;
        winston.log(msg);
        let decodedMessage= lmuDecoder.decodeMessage(msg);
        let ackMessage = lmuDecoder.generateAckMessage(msg);

        co(lmuDecoder.getPotableMessage(decodedMessage))
        .then(pMess => {
            console.log(`Potable Message is: ${pMess}`);
            console.log(`Potable Message is: ${pMess["driver"]["keyId"]}`);
		    chan.publish(xchange, '', new Buffer(JSON.stringify(pMess)));
            server.send(ackMessage,rinfo.port, rinfo.address); //ack



        }).catch(err => {
            
            console.log(`An error has ocurred when getting the potable message ${err}`);
            process.exit(1);
        });
        msg = msg.toString('hex');
        msg +='@';
        msg = msg.toUpperCase();
        winston.log('debug', msg);
        broadcast(msg);
    })

    server.on('close', () => {
        console.log('server closed');
    })

    server.on('error', error => {
        console.log(error.toString());
    })

    server.bind(port, '0.0.0.0');
}


var chan = undefined;
openAmqp.then(conn => {
	return conn.createChannel();
}).then(ch => {
	return ch.assertExchange(xchange, 'fanout', {durable:false})
	.then(ok => {
		chan = ch
		console.log("Connection stablished to Broker imnonetplus.com.bo")
		co(dataAccessConnect.connect(url))
		.then(()=>{
            new LMU_Server(chan);
		}).catch(err =>{
			winston.log('error',`An error has occurred when initializing the server: ${err}`)
		})

		chan.on('error', err => {
			process.exit(1);
		})

	})
}).catch(err =>{
	console.warn;
	winston.log('error',`An error has occurred when initializing the server: ${err}`)
	process.exit(1);
})

