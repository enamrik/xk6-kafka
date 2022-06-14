/*
This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 100 Avro messages per iteration.
*/

import {check} from "k6";
import {
    writer,
    reader,
    consumeWithConfiguration,
    produceWithConfiguration,
    createTopic,
    deleteTopic,
} from "k6/x/kafka";

const bootstrapServers = ["localhost:9092"];
const kafkaTopic = "test_schema_registry_consume_magic_prefix";

const [producer, _writerError] = writer(bootstrapServers, kafkaTopic, null);

let configuration = JSON.stringify(
    {
        consumer: {
            keyDeserializer: "",
            valueDeserializer: "io.confluent.kafka.serializers.KafkaAvroDeserializer",
            userMagicPrefix: true,
        },
        producer: {
            keySerializer: "",
            valueSerializer: "io.confluent.kafka.serializers.KafkaAvroSerializer",
            subjectNameStrategy: "RecordNameStrategy"
        },
        schemaRegistry: {
            url: "http://localhost:8081",
        },
    });

if (__VU == 0) {
    createTopic(bootstrapServers[0], kafkaTopic);
}

export default function () {
    let firstMessage = {
        value: JSON.stringify({
            firstname: "first",
            lastname: "first",
        }),
    }
    let secondMessage = {
        value: JSON.stringify({
            firstname: "second",
            lastname: "second",
        }),
    }
    const valueSchema = JSON.stringify({
        "name": "MagicNameValueSchema",
        "type": "record",
        "namespace": "com.example",
        "fields": [
            {
                "name": "firstname",
                "type": "string"
            },
            {
                "name": "lastname",
                "type": "string"
            }
        ]
    });
    let error = produceWithConfiguration(producer, [firstMessage, secondMessage], configuration, null, valueSchema);
    check(error, { "is sent": (err) => err == undefined });

    const [earliestConsumer, _earliestReaderError] = reader(bootstrapServers, kafkaTopic, null, "", -2, null);
    const [latestConsumer, _latestReaderError] = reader(bootstrapServers, kafkaTopic, null, "", -1, null);

    let [earliestMessages, _earliestConsumeError] = consumeWithConfiguration(
        earliestConsumer,
        1,
        configuration,
        null,
        valueSchema
    );
    check(earliestMessages[0], { "earliest message returned": (msg) => msg.value["firstname"] === "first", });

    let [latestMessages, _latestConsumeError] = consumeWithConfiguration(
        earliestConsumer,
        1,
        configuration,
        null,
        valueSchema
    );
    check(latestMessages[0], { "latest message returned": (msg) => msg.value["firstname"] === "second", });

    earliestConsumer.close();
    latestConsumer.close()
}

export function teardown(data) {
    if (__VU == 0) {
        // Delete the kafkaTopic
        const error = deleteTopic(bootstrapServers[0], kafkaTopic);
        if (error === undefined) {
            // If no error returns, it means that the kafkaTopic
            // is successfully deleted
            console.log("Topic deleted successfully");
        } else {
            console.log("Error while deleting kafkaTopic: ", error);
        }
    }
    producer.close();
}
