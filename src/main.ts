import { Kafka } from "kafkajs";

const kafka = new Kafka({
	brokers: ["localhost:9092"]
});

const consumer = kafka.consumer({ groupId: "homework-group" });

const run = async () => {
	await consumer.connect();
	await consumer.subscribe({topic: "delivery_details", fromBeginning: true});

	await consumer.run({
		eachMessage: async ({topic, partition, message}) => {
			console.log("Received: ", message.value.toString());
		}
	});

};

run().catch(console.error);
