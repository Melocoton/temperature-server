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
    db.serialize(() => {
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
            console.log('Temperature table created');
        }).run(`
            CREATE TABLE "device" (
                "id" INTEGER NOT NULL,
                "description" TEXT,
                PRIMARY KEY("id")
            ); 
        `, err => {
            if (err) {
                console.error('Error creating tables', err);
                process.exit(1);
            }
            console.log('Devices table created');
        });
    })

}

setInterval(() => {
    const flatDeviceData: unknown[] = [];
    deviceData.forEach((data, key) => {
        flatDeviceData.push(
            Date.now(),
            key,
            Math.round(data.reduce((a, b) => a + b.temperature, 0) / data.length),
            Math.round(data.reduce((a, b) => a + b.humidity, 0) / data.length)
        );
    });
    insertData(flatDeviceData);
    deviceData.clear();
}, 60000);

function insertData(flatDeviceData: unknown[]) {
    const placeHolders = Array(flatDeviceData.length/4).fill('(?, ?, ?, ?)').join(', ');
    const query = `INSERT INTO temperature (time, id, temperature, humidity) values ${placeHolders}`;
    db.serialize(() => {
        db.run(query, flatDeviceData, err => {
            if (err) {
                console.error('Error inserting', err);
            } else {
                console.log('Data inserted:', JSON.stringify(flatDeviceData));
            }
        })
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
