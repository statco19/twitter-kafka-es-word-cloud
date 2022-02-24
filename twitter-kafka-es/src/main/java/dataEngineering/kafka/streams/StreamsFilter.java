package dataEngineering.kafka.streams;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilter {

    private static String APPLICATION_NAME = "streams-filter-application";
    private static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static String STREAM_TWEET = "stream_tweet";
    private static String STREAM_DESTINATION = "stream_tweet_destination";

    public static void main(String[] args) {

        // A Gson instance in order to handle json objects from twitter
        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        // stream properties setting
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // A stream builder to create a KStream
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamTweet = builder.stream(STREAM_TWEET);

        // keep the messages that are truncated
        KStream<String, String> filterStream_truncated = streamTweet.filter(
                (key, value) -> extractFromTweet(gson, value,"truncated")
                                .equals("true")
        );

        // keep the messages that are NOT truncated
        KStream<String, String> filteredStream_not_truncated = streamTweet.filter(
                (key, value) -> extractFromTweet(gson, value,"truncated")
                                .equals("false")
        );

        // extract full text from truncated messages
        KStream<String, String> fromTruncated = filterStream_truncated.mapValues(
                value -> "{" + "\"created_at\":\""
                        + extractFromTweet(gson, value,"created_at") + "\""
                        + ",\"text\":\""
                        + gson.fromJson(value, JsonElement.class)
                                .getAsJsonObject()
                                .get("extended_tweet")
                                .getAsJsonObject()
                                .get("full_text")
                                .getAsString()
                                .replace("\n", "\\n")
                                .replace("\"", "")
                        + "\"}"
        );

        // extract text from NOT truncated messages
        KStream<String, String> fromNotTruncated = filteredStream_not_truncated.mapValues(
                value -> "{" + "\"created_at\":\""
                        + extractFromTweet(gson, value, "created_at") + "\""
                        + ",\"text\":\""
                        + extractFromTweet(gson, value, "text")
                            .replace("\n", "\\n")
                            .replace("\"", "")
                        + "\"}"
        );

        // send the texts from both messages to a topic
        fromTruncated.to(STREAM_DESTINATION);
        fromNotTruncated.to(STREAM_DESTINATION);

        // build a stream and start
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    private static String extractFromTweet(Gson gson, String value, String key) {
        return gson.fromJson(value, JsonElement.class)
                .getAsJsonObject()
                .get(key)
                .getAsString();
    }
}
