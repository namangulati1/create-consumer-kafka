const { Kafka } = require("kafkajs");
const axios = require("axios");
const js2xmlparser = require("js2xmlparser");
const fs = require("fs");

const runConsumer = async (nasDirectory) => {
    const kafka = new Kafka({
        clientId: 'kafka-video-producer',
        brokers: ["localhost:9092"],
    });

    const consumer = kafka.consumer({ groupId: "video-group" });
    consumer.subscribe({ topic: "Test_kafak_topic_for_video" });

    await consumer.connect();

    if (!fs.existsSync(nasDirectory)) {
        fs.mkdirSync(nasDirectory, { recursive: true });
    }

    await consumer.run({
        eachMessage: async ({ message }) => {
            const videoPost = JSON.parse(message.value.toString());

            const jsonDataXML = js2xmlparser.parse("videoPost", videoPost);
            const jsonDataXmlFileName = `${nasDirectory}/json_data_${Date.now()}.xml`;
            fs.writeFileSync(jsonDataXmlFileName, jsonDataXML);

            console.log("JSON data converted to XML saved to:", jsonDataXmlFileName);

            const videoLink = videoPost.videoLink;

            const videoResponse = await axios({
                method: "get",
                url: videoLink,
                responseType: "stream"
            });

            const videoFileName = `${nasDirectory}/video_${Date.now()}.mp4`;
            const videoStream = fs.createWriteStream(videoFileName);

            videoResponse.data.pipe(videoStream);

            await new Promise((resolve, reject) => {
                videoStream.on("finish", resolve);
                videoStream.on("error", reject);
            });
            console.log("Video downloaded and saved to:", videoFileName);

            const videoLinkDataXml = js2xmlparser.parse("videoLink", { link: videoLink });

            const videoLinkDataXmlFileName = `${nasDirectory}/video_link_data_${Date.now()}.xml`;
            fs.writeFileSync(videoLinkDataXmlFileName, videoLinkDataXml);

            console.log("Video link data converted to XML saved to:", videoLinkDataXmlFileName);
        }
    });
}

module.exports = { runConsumer };
