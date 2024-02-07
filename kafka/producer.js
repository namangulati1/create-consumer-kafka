const { Kafka } = require("kafkajs");

const produceTestMessage = async () => {
    const kafka = new Kafka({
        clientId: 'kafka-video-producer',
        brokers: ["localhost:9092"],
    });

    const producer = kafka.producer();

    await producer.connect();

    const videoPost = {
        videoLink: "https://file-examples.com/storage/fe63e96e0365c0e1e99a842/2017/04/file_example_MP4_640_3MG.mp4",
        title: "Test Video",
        description: "This is a test video.",
        // Add any other properties as needed
    };

    const messages = [
        {
            value: JSON.stringify(videoPost),
        },
    ];

    try {
        await producer.send({
            topic: "Test_kafak_topic_for_video",
            messages: messages,
        });

        console.log("Test message produced");
    } catch (error) {
        console.error("Error producing message:", error.message);
    } finally {
        await producer.disconnect();
    }
}

module.exports = { produceTestMessage };
