const assert = require("assert");
const dgram = require("dgram");
const winston  = require("winston");

const openAmqp = require('amqplib').connect('amqp://imonnetplus.com.bo');

const skypatrolDec = require('./m2m_decoders/skypatrol_decoder.js').SkypatrolDecoder;
const getPotableMessageSkypatrol = require('./m2m_decoders/skypatrol_decoder.js').getPotableMessage;
const generateAckMessage = require('./m2m_decoders/skypatrol_decoder.js').generateAckMessage;
const dataAccessConnect = require('./data_access.js');
const co = require('co');
const xchange= 'main_valve';


const url = 'mongodb://imonnetplus.com.bo:27017/platform2';

let defaultSize = 16;
let port = 1999;


function Server(chan) {
	let devicesList= [];
	console.log("Creating UDP Server instance");
	const server = dgram.createSocket("udp4");

	server.on('message', (msg, rinfo) => {
		let deviceNetInfo = rinfo.address + ':' + rinfo.port;
		msg = msg.toString('ascii');
		if(!devicesList[deviceNetInfo]){
			devicesList[deviceNetInfo] = rinfo;
		}
		winston.log('debug', msg);
		winston.log('debug', devicesList);
		winston.log('info', msg);
		let {message, rt}  = skypatrolDec(msg);
		console.log(`NEW MESSAGE DECODED ${message.toString()}`);
		winston.log('info', message);
		getPotableMessageSkypatrol(message, rt)
		.then(pmess => {
				console.log("POTABLE MESSAGE" + pmess.toString());
				chan.publish(xchange, '', new Buffer(JSON.stringify(pmess)));
				let ackMessage = generateAckMessage(message.countNumber);
				console.log(ackMessage);
				winston.log('debug',ackMessage);
				winston.log(`debug','Enviando ack:${ackMessage} a:${deviceNetInfo}`)
				server.send(ackMessage,rinfo.port, rinfo.address);
				winston.log('debug','===============************=================');
		}).catch(err => {
			winston.log('error', err.toString());
			process.exit(1)
		})

	});

	server.on('listening', () => {
		let address = server.address()
		console.log(`Server ready: ${address.address}:${address.port}`);
	})

	server.on('close',() => {
			chan.close();
	})
	server.on('error', (error) => {
		console.log(error.toString());
		chan.close();

	})

	// server.bind(port);
	server.bind(port, '192.168.1.17');


}
winston.add(winston.transports.File, {filename: 'skypatrol.log', level:'debug'});
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
			new Server(chan);
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
