import mqtt from "mqtt";
import sqlite3 from "sqlite3";

type SqliteErr = { errno: number, code: string };
type SensorData = { id: number, data: Data };
type Data = { temperature: number, humidity: number };

const deviceData: Map<number, Data[]> = new Map<number, Data[]>();

const mqttClient = mqtt.connect("mqtt://raspberrypi.lan:1883");
let db = new sqlite3.Database('./data.db', sqlite3.OPEN_READWRITE, (err: unknown) => {
    const e: SqliteErr = err as SqliteErr;
    if (e && e.code == "SQLITE_CANTOPEN") {
        console.log('Database not found. Creating new Database');
        createDB();
        return;
    } else if (e) {
        console.error('Error', err);
        process.exit(1);
    }
});

setInterval(() => {
    deviceData.forEach((data, key) => {
        const newData: SensorData = {
            id: key,
            data: {
                temperature: Math.round(data.reduce((a, b) => a + b.temperature, 0) / data.length),
                humidity: Math.round(data.reduce((a, b) => a + b.humidity, 0) / data.length)
            }
        };
        insertData(newData);
    });
    deviceData.clear();
}, 60000);

function createDB() {
    db = new sqlite3.Database('data.db', err => {
        if (err) {
            console.error('Error Creating DB', err);
            process.exit(1);
        }
        createTables();
    });
}

function createTables() {
    console.log('Creating tables');
    db.run(`
        CREATE TABLE "temperature" (
        "time" INTEGER NOT NULL,
        "id" INTEGER NOT NULL,
        "temperature" INTEGER,
        "humidity" INTEGER,
        PRIMARY KEY("time","id")
        ); 
    `, err => {
        if (err) {
            console.error('Error creating tables', err);
            process.exit(1);
        }
        console.log('DB Created');
    });
}

mqttClient.on('connect', () => {
    console.log('Connected');
    mqttClient.subscribe(['casa/temperature'], () => {
        console.log('Subscribed');
    });
});

mqttClient.on('message', (topic, payload) => {
    // console.log('Message received:', `Topic:${topic}, Payload:${payload}`);
    const data = parsePayload(payload.toString());
    if (deviceData.has(data.id)) {
        deviceData.get(data.id).push(data.data);
    } else {
        deviceData.set(data.id, [data.data]);
    }
});

function insertData(data: SensorData) {
    console.log('Inserting Data', data);
    db.run("INSERT INTO temperature (time, id, temperature, humidity) values ($time, $id, $temperature, $humidity)", {
        $time: Date.now(),
        $id: data.id,
        $temperature: data.data.temperature,
        $humidity: data.data.humidity
    });
}

function parsePayload(payload: string): SensorData {
    const data = payload.split(';');
    return {
        id: Number('0x'+data[0].replaceAll(':','')),
        data: {
            temperature: Number(data[1].split(':')[1]),
            humidity: Number(data[2].split(':')[1])
        }
    };
}
