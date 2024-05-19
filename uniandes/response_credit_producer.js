const Kafka = require("kafka-node");

// Configuración de Kafka
const client = new Kafka.KafkaClient({ kafkaHost: "localhost:9092" });
const producer = new Kafka.Producer(client);

// Tema de Kafka
const topic = "topic-producer";

// Función para generar un mensaje JSON con datos de crédito
function generateCreditMessage() {
  const creditIds = [
    1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012,
    1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020,
  ];
  const userIds = [
    11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 40, 50, 60, 70, 80, 90,
  ];
  const creditStatus = ["aprobado", "rechazado", "en estudio"];

  // Genera datos aleatorios
  const randomCreditId =
    creditIds[Math.floor(Math.random() * creditIds.length)];
  const randomUserId = userIds[Math.floor(Math.random() * userIds.length)];
  const randomStatus =
    creditStatus[Math.floor(Math.random() * creditStatus.length)];
  const currentDate = new Date().toISOString();

  // Construye el mensaje JSON
  const message = {
    id_credito: randomCreditId,
    id_usuario: randomUserId,
    estado_credito: randomStatus,
    fecha: currentDate,
  };

  return JSON.stringify(message);
}

// Función para enviar un arreglo de mensajes al tema de Kafka
function sendMessages() {
  const payloads = [];

  // Genera 10 mensajes y los agrega al arreglo de payloads
  for (let i = 0; i < 50; i++) {
    const message = generateCreditMessage();
    payloads.push({
      topic: topic,
      messages: message,
      partition: 0,
    });
  }

  producer.send(payloads, (err, data) => {
    if (err) {
      console.error("Error al enviar mensajes:", err);
    } else {
      console.log("Mensajes enviados:", payloads.length);
    }
    producer.close();
    console.log("Desconectado del broker de Kafka");
  });
}

// Inicializar el productor de Kafka
producer.on("ready", () => {
  console.log("Productor de Kafka está listo!");
  // Envía los mensajes al iniciar
  sendMessages();
});

producer.on("error", (err) => {
  console.error("Error en el productor de Kafka:", err);
});
