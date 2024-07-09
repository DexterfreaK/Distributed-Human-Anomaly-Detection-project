const express = require("express");
const WebSocket = require("ws");
const kafka = require("kafka-node");

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

// Serve static files
app.use(express.static("public"));

// WebSocket server
const server = app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});

const wss = new WebSocket.Server({ server });

// Store WebSocket clients
const clients = [];

wss.on("connection", (ws) => {
  clients.push(ws);
  console.log("Client connected");

  ws.on("message", (message) => {
    const anomalyData = JSON.parse(message);
    console.log("Received message from client:", anomalyData);
    sendAnomalyToKafka(anomalyData);
  });

  ws.on("close", () => {
    console.log("Client disconnected");
    const index = clients.indexOf(ws);
    if (index !== -1) {
      clients.splice(index, 1);
    }
  });
});

// Function to notify all WebSocket clients
function notifyClients(message) {
  clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

// Function to send anomaly data to Kafka
function sendAnomalyToKafka(anomalyData) {
  const kafkaPayloads = [
    {
      topic: kafkaTopic,
      messages: JSON.stringify(anomalyData),
    },
  ];

  producer.send(kafkaPayloads, (err, data) => {
    if (err) {
      console.error("Failed to send message to Kafka:", err);
    } else {
      console.log("Message sent to Kafka:", data);
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
