const kafka = require("kafka-node");
const fs = require("fs");
const path = require("path");

// Configuración de Kafka
const kafkaHost = "localhost:9092"; // Dirección del broker de Kafka
const topic = "topic-producer"; // Nombre del tópico

// Crear el cliente Kafka
const client = new kafka.KafkaClient({ kafkaHost });

// Crear el consumidor
const Consumer = kafka.Consumer;
const consumer = new Consumer(client, [{ topic: topic, partition: 0 }], {
  autoCommit: true,
  fromOffset: false, // Empezar desde el último offset disponible
});

// Ruta del archivo donde se guardarán los mensajes
const outputPath = path.join(
  "/Users/macbook/Proyectos/uniandes/results",
  "results_response_credits.txt"
);

// Función para escribir mensajes en el archivo
const writeMessageToFile = (message) => {
  fs.appendFile(outputPath, message + "\n", (err) => {
    if (err) {
      console.error("Error escribiendo archivo", err);
    } else {
      console.log("Escribiendo mensajes", message);
    }
  });
};

// Manejar los mensajes recibidos
consumer.on("message", (message) => {
  console.log("Mensajes recibidos:", message.value);
  writeMessageToFile(message.value);
});

// Manejar errores del consumidor
consumer.on("error", (err) => {
  console.error("error:", err);
});

// Manejar offset fuera de rango
consumer.on("offsetOutOfRange", (err) => {
  console.error("Rango:", err);

  const offset = new kafka.Offset(client);
  const topic = err.topic;
  const partition = err.partition;

  offset.fetch(
    [{ topic: topic, partition: partition, time: -1, maxNum: 1 }],
    (err, data) => {
      if (err) {
        console.error("Error  offset:", err);
        return;
      }

      const latestOffset = data[topic][partition][0];
      console.log("Reset offset:", latestOffset);

      consumer.setOffset(topic, partition, latestOffset);
    }
  );
});

client.on("ready", () => {
  console.log("Cliente consumidor Kafka esta listo!");
});

client.on("error", (err) => {
  console.error("Kafka client error:", err);
});

console.log("El servicio consumidor al tópico de Kafka está en ejecución...");
