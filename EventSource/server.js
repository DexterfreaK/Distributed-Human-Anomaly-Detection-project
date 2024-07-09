const WebSocket = require("ws");
const kafka = require("kafka-node");

// Kafka configuration

const kafkaHost = "localhost:9092";
const kafkaTopic = "anomaly-detection";
const kafkaClient = new kafka.KafkaClient({ kafkaHost });
const producer = new kafka.Producer(kafkaClient);
const consumer = new kafka.Consumer(
  kafkaClient,
  [{ topic: kafkaTopic, partition: 0 }],
  { autoCommit: true }
);

// WebSocket configuration
const args = process.argv.slice(2);
const portArg = args.find((arg) => arg.startsWith("--port="));
const serverIdArg = args.find((arg) => arg.startsWith("--serverId="));
const port = portArg ? parseInt(portArg.split("=")[1], 10) : 8080;
const serverId = serverIdArg ? serverIdArg.split("=")[1] : `server-${port}`;

const server = new WebSocket.Server({ port });

server.on("connection", (ws) => {
  console.log("Client connected");

  ws.on("message", (message) => {
    console.log(`Received message: ${message}`);
  });

  ws.on("close", () => {
    console.log("Client disconnected");
  });
});

console.log(`WebSocket server is running on ws://localhost:${port}`);

// Function to handle anomaly detection
function detectAnomaly() {
  // Simulated anomaly detection logic
  const anomalyDetected = Math.random() < 0.1; // 10% chance to detect an anomaly

  if (anomalyDetected) {
    console.log("Anomaly detected! Notifying clients and other servers...");

    const notification = {
      type: "anomaly",
      message: "An anomaly has been detected!",
      timestamp: new Date().toISOString(),
      serverId: serverId, // Include server ID in the notification
    };

    const notificationString = JSON.stringify(notification);

    // Send notification to WebSocket clients
    server.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(notificationString);
      }
    });

    // Send notification to Kafka
    const kafkaPayloads = [
      {
        topic: kafkaTopic,
        messages: notificationString,
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
}

// Run anomaly detection every 5 seconds
setInterval(detectAnomaly, 2000);

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

  const notification = JSON.parse(message.value);

  // Send notification to WebSocket clients
  server.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message.value);
    }
  });
});

consumer.on("error", (err) => {
  console.error("Kafka Consumer error:", err);
});
