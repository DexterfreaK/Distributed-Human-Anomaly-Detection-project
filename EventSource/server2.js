const express = require("express");
const WebSocket = require("ws");
const kafka = require("kafka-node");
const bodyParser = require("body-parser");

// Kafka configuration
const kafkaHost = "localhost:9092"; // Update with your Kafka broker host
const kafkaTopic = "anomaly-detection";

const kafkaClient = new kafka.KafkaClient({ kafkaHost });
const producer = new kafka.Producer(kafkaClient);
const consumer = new kafka.Consumer(
  kafkaClient,
  [{ topic: kafkaTopic, partition: 0 }],
  { autoCommit: true }
);

// Express configuration
const app = express();
const port = 3000;

app.use(bodyParser.json()); // To parse JSON bodies

// Serve static files
app.use(express.static("public"));

// WebSocket server
const server = app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});

const wss = new WebSocket.Server({ server });

// Store WebSocket clients with their IDs
const clients = new Map();

let nextUserId = 1;

wss.on("connection", (ws) => {
  const userId = nextUserId++;
  clients.set(ws, userId);

  console.log(`Client connected with ID ${userId}`);

  ws.on("message", (message) => {
    console.log(`Received message from client ${userId}: ${message}`);
    // Forward the message to Kafka

    const m = JSON.parse(message);
    const kafkaPayloads = [
      {
        topic: kafkaTopic,
        messages: JSON.stringify({ userId, m }),
      },
    ];

    producer.send(kafkaPayloads, (err, data) => {
      if (err) {
        console.error("Failed to send message to Kafka:", err);
      } else {
        console.log("Message sent to Kafka:", data);
      }
    });
  });

  ws.on("close", () => {
    console.log(`Client ${userId} disconnected`);
    clients.delete(ws);
  });
});

// Function to notify all WebSocket clients
function notifyClients(message) {
  clients.forEach((userId, ws) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(message);
    }
  });
}

// Handle Kafka producer 'ready' and 'error' events
producer.on("ready", () => {
  console.log("Kafka Producer is connected and ready.");
});

producer.on("error", (err) => {
  console.error("Kafka Producer error:", err);
});

// Handle Kafka consumer messages
consumer.on("message", (message) => {
  console.log("Received message from Kafka:", message.value);
  notifyClients(message.value);
});

consumer.on("error", (err) => {
  console.error("Kafka Consumer error:", err);
});
