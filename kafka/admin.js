const { Kafka } = require('kafkajs');

const createTopics = async () => {
    const kafka = new Kafka({
        clientId: 'kafka-video-admin',
        brokers: ["localhost:9092"],
    });

    const admin = kafka.admin();

    try {
        await admin.connect();

        await admin.createTopics({
            topics: [{ topic: "Test_kafka_topic_for_video" }],
        });

        console.log("Topic created");
    } catch (error) {
        console.error("Error creating topics:", error.message);
    } finally {
        await admin.disconnect();
    }
};


module.exports = { createTopics };
