const mqtt = require("mqtt");

const mqttClient = mqtt.connect("mqtt://raspberrypi.lan:1883");

mqttClient.on('connect', () => {
    console.log('Connected');
    mqttClient.subscribe(['casa/temperature'], () => {
        console.log('Subscribed');
    });
});

mqttClient.on('message', (topic, payload) => {
    console.log('Message received:', `Topic:${topic}, Payload:${payload}`);
});
