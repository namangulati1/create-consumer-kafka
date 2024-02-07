const { runConsumer } = require("./kafka/consumer");
const { produceTestMessage } = require("./kafka/producer");
const { createTopics } = require("./kafka/admin");

const nasDirectory = "/Users/namangulati/Desktop";

const main = async () => {
  await createTopics();
  await produceTestMessage();
  await runConsumer(nasDirectory);
};

main().catch(console.error);
