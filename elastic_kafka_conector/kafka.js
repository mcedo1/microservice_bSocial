const { Kafka, Partitioners, logLevel } = require("kafkajs");
const { pushToElasticsearch } = require("./elastic");

const kafka = new Kafka({
    clientId: "user-service",
    logLevel: logLevel.NOTHING,
    brokers: ["pkc-4r087.us-west2.gcp.confluent.cloud:9092"],
    ssl: true,
    sasl: {
        mechanism: "plain",
        username: "IWDHIGVY55D7ZDBV",
        password:
            "bY3OiXzHc+EWlNhFmDn7TogDG2Un3kAiaGGsbg5ZsO+W8v2Kg58e2aU0u6C9YcFk",
    },
    createPartitioner: Partitioners.LegacyPartitioner,
});

const consumer = kafka.consumer({ groupId: "spec-group" });
const sampleData = {
    index: "milos_name",
    body: {},
};

async function runKafkaConsumer() {
    try {
        await consumer.connect();
        console.log("Connected to Kafka");
        await consumer.subscribe({
            topics: ["topic_comment", "topic_reg", "topic_login", "topic_post"],
            fromBeginning: true,
        });
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                try {
                    const parsedMessage = JSON.parse(
                        message.value.toString("utf8")
                    );
                    console.log("Received message:", parsedMessage);

                    await pushToElasticsearch({
                        index: topic,
                        body: parsedMessage,
                    });
                } catch (error) {
                    console.log(error);
                }
            },
        });
    } catch (err) {
        console.error("Error connecting to Kafka:", error);
    }
}

async function disconnectKafkaConsumer() {
    try {
        await consumer.disconnect();
        console.log("Disconnected from Kafka");
    } catch (error) {
        console.error("Error disconnecting, from Kafka:", error);
    }
}

module.exports = {
    runKafkaConsumer,
    disconnectKafkaConsumer,
};
