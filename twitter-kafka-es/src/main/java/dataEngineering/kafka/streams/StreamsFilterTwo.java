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

public class StreamsFilterTwo {

    private static String APPLICATION_NAME = "streams-filter-two-application";
    private static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static String STREAM_TWEET_NOT_TRUNCATED = "stream_tweet_not_truncated";
    private static String STREAM_TWEET_TRUNCATED = "stream_tweet_truncated";
    private static String STREAM_DESTINATION = "stream_tweet_destination";

    public static void main(String[] args) {

        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> notTruncated = builder.stream(STREAM_TWEET_NOT_TRUNCATED);
        KStream<String, String> Truncated = builder.stream(STREAM_TWEET_TRUNCATED);

        KStream<String, String> fromNotTruncated = notTruncated.mapValues(
                value -> gson.fromJson(value, JsonElement.class)
                        .getAsJsonObject()
                        .get("text")
                        .getAsString()
        );
        KStream<String, String> fromTruncated = Truncated.mapValues(
                value -> gson.fromJson(value, JsonElement.class)
                        .getAsJsonObject()
                        .get("extended_tweet")
                        .getAsJsonObject()
                        .get("full_text")
                        .getAsString()
        );

        fromNotTruncated.to(STREAM_DESTINATION);
        fromTruncated.to(STREAM_DESTINATION);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
