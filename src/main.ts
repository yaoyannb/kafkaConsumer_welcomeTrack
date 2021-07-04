import { Kafka } from "kafkajs";
import { Client } from "@elastic/elasticsearch"

const kafka = new Kafka({
	brokers: ["localhost:9092"]
});

const client = new Client({node: 'http://localhost:9200'})

//Célia;Perrin;19183;;;MB4006977_1800070;212377667;00DQYOA3;GLS

const consumer = kafka.consumer({ groupId: "homework-group" });

const run = async () => {
	await consumer.connect();
	await consumer.subscribe({topic: "delivery_details", fromBeginning: true});

	await consumer.run({
		eachMessage: async ({topic, partition, message}) => {
			
			//console.log("Received : ", message.value.toString());
			var splitedMessage = message.value.toString().split(";");
			/*var jsonMessage = {
				"prenom_destinataire": splitedMessage[0],
				"nom_destinataire": splitedMessage[1],
				"code_postal": splitedMessage[2],
				"boutique": splitedMessage[3],
				"date_de_validation_commande_prevue": splitedMessage[4],
				"reference_de_colis": splitedMessage[5],
				"id": splitedMessage[6],
				"reference_de_lexpedition": splitedMessage[7]
			}*/

			client.index({
				index: "deliverydetails",
				type: "_doc",
				id :splitedMessage[6],
				body: {
                                   "prenom_destinataire": splitedMessage[0],
                                   "nom_destinataire": splitedMessage[1],
                                   "code_postal": splitedMessage[2],
                                   "boutique": splitedMessage[3],
                                   "date_de_validation_commande_prevue": splitedMessage[4],
                                   "reference_de_colis": splitedMessage[5],
                                   "id": splitedMessage[6],
                                   "reference_de_lexpedition": splitedMessage[7],
				   "transporteur": splitedMessage[8]
                        	}
			}, (err, result) => {if(err) {console.log(err)} else { console.log(result) } });
			
			//console.log("Received: ", jsonMessage);
		}
	});

};

run().catch(console.error);
