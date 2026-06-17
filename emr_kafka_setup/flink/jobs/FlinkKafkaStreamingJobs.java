import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FlinkKafkaStreamingJobs {
    static final String DEFAULT_BOOTSTRAP = "ip-172-31-14-56.ec2.internal:9092";
    static final String RAW_TOPIC = "raw_youtube_chat";
    static final String RESULTS_TOPIC = "nlp_stream_results";
    static final String ALERTS_TOPIC = "alerts_polarization";

    static class Args {
        String job = "job1";
        String bootstrap = DEFAULT_BOOTSTRAP;
        String inputTopic = RAW_TOPIC;
        String outputTopic = RESULTS_TOPIC;
        int maxMessages = 105;
        int idleMs = 3000;
        int delayMs = 10;
        int windowSeconds = 2;

        static Args parse(String[] argv) {
            Args args = new Args();
            for (int i = 0; i < argv.length; i++) {
                String k = argv[i];
                String v = (i + 1 < argv.length) ? argv[i + 1] : "";
                if ("--job".equals(k)) { args.job = v; i++; }
                else if ("--bootstrap-server".equals(k)) { args.bootstrap = v; i++; }
                else if ("--input-topic".equals(k)) { args.inputTopic = v; i++; }
                else if ("--output-topic".equals(k)) { args.outputTopic = v; i++; }
                else if ("--max-messages".equals(k)) { args.maxMessages = Integer.parseInt(v); i++; }
                else if ("--idle-ms".equals(k)) { args.idleMs = Integer.parseInt(v); i++; }
                else if ("--delay-ms".equals(k)) { args.delayMs = Integer.parseInt(v); i++; }
                else if ("--window-seconds".equals(k)) { args.windowSeconds = Integer.parseInt(v); i++; }
            }
            return args;
        }
    }

    static class RawEvent {
        String value;
        String topic;
        int partition;
        long offset;
        long timestamp;
        RawEvent() {}
        RawEvent(String value, String topic, int partition, long offset, long timestamp) {
            this.value = value;
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.timestamp = timestamp;
        }
    }

    static class RuleResult {
        String eventId = "";
        String text = "";
        String actor = "";
        boolean hasTerruqueo;
        boolean hasFraude;
        boolean hasElectoralInstitution;
        boolean hasPoliticalMention;
        boolean hasPolarizationSignal;
        boolean hasDiscriminatoryLanguage;
        boolean hasEthnicRacialSlur;
        boolean hasHomophobicSlur;
        boolean hasGeneralInsult;
        boolean isSpamNoise;
        int localRiskScore;
        String localRuleTags = "";
    }

    static class Metric {
        long count = 0;
        long totalLength = 0;
        long emptyCount = 0;
        long spamLikeCount = 0;
        Set<String> authors = new HashSet<String>();
    }

    static class ActorMetric {
        String actor = "";
        long mentionCount = 0;
        long insultCount = 0;
        long fraudCount = 0;
        long terruqueoCount = 0;
        long discriminatoryCount = 0;
    }

    static class KafkaRawSource implements SourceFunction<RawEvent> {
        private final String bootstrap;
        private final String topic;
        private final int maxMessages;
        private final int idleMs;
        private final int delayMs;
        private volatile boolean running = true;

        KafkaRawSource(String bootstrap, String topic, int maxMessages, int idleMs, int delayMs) {
            this.bootstrap = bootstrap;
            this.topic = topic;
            this.maxMessages = maxMessages;
            this.idleMs = idleMs;
            this.delayMs = delayMs;
        }

        public void run(SourceContext<RawEvent> ctx) throws Exception {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-demo-" + UUID.randomUUID());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
            consumer.subscribe(Arrays.asList(topic));
            int seen = 0;
            long lastDataAt = System.currentTimeMillis();
            try {
                while (running) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    if (records.isEmpty()) {
                        if (System.currentTimeMillis() - lastDataAt >= idleMs) break;
                        continue;
                    }
                    lastDataAt = System.currentTimeMillis();
                    for (ConsumerRecord<String, String> r : records) {
                        synchronized (ctx.getCheckpointLock()) {
                            ctx.collect(new RawEvent(r.value(), r.topic(), r.partition(), r.offset(), r.timestamp()));
                        }
                        seen++;
                        if (delayMs > 0) Thread.sleep(delayMs);
                        if (maxMessages > 0 && seen >= maxMessages) {
                            running = false;
                            break;
                        }
                    }
                }
            } finally {
                consumer.close();
            }
        }

        public void cancel() {
            running = false;
        }
    }

    static class KafkaJsonSink implements SinkFunction<String> {
        private final String bootstrap;
        private final String topic;
        private transient KafkaProducer<String, String> producer;

        KafkaJsonSink(String bootstrap, String topic) {
            this.bootstrap = bootstrap;
            this.topic = topic;
        }

        public void invoke(String value, Context context) {
            if (producer == null) {
                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.ACKS_CONFIG, "1");
                producer = new KafkaProducer<String, String>(props);
            }
            producer.send(new ProducerRecord<String, String>(topic, value));
            producer.flush();
        }
    }

    static String field(String json, String key) {
        Pattern p = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*\"((?:\\\\.|[^\"])*)\"");
        Matcher m = p.matcher(json == null ? "" : json);
        return m.find() ? unescape(m.group(1)) : "";
    }

    static String unescape(String s) {
        return s.replace("\\\"", "\"").replace("\\\\", "\\").replace("\\n", " ").replace("\\r", " ").replace("\\t", " ");
    }

    static String mainText(String json) {
        String clean = field(json, "message_clean");
        if (!clean.isEmpty()) return clean;
        String raw = field(json, "message_raw");
        return raw == null ? "" : raw;
    }

    static String normalize(String s) {
        return (s == null ? "" : s).toLowerCase(Locale.ROOT).replaceAll("\\s+", " ").trim();
    }

    static boolean containsAny(String text, String... terms) {
        String t = normalize(text);
        for (String term : terms) {
            if (t.contains(normalize(term))) return true;
        }
        return false;
    }

    static RuleResult rules(RawEvent event) {
        String text = mainText(event.value);
        RuleResult r = new RuleResult();
        r.eventId = field(event.value, "event_id");
        r.text = text;
        r.hasTerruqueo = containsAny(text, "terruco", "terruqueo", "senderista", "rojo", "comunista", "movadef", "mrta");
        r.hasFraude = containsAny(text, "fraude", "robo", "actas falsas", "actas impugnadas", "irregularidades");
        r.hasElectoralInstitution = containsAny(text, "onpe", "jne", "actas", "mesa", "personeros", "votos", "conteo");
        r.hasPoliticalMention = containsAny(text, "keiko", "fujimori", "fp", "jp", "juntos por el peru", "juntos por el perú", "castillo", "peru libre", "perú libre", "antauro", "porky", "rla");
        r.hasEthnicRacialSlur = containsAny(text, "cholo", "serrano", "paisano", "indio", "llama");
        r.hasHomophobicSlur = containsAny(text, "kbro", "kbros", "cabro", "rosquete", "maricon", "maricón");
        r.hasGeneralInsult = containsAny(text, "mierda", "csm", "ctm", "burro", "ignorante", "rata", "lacra");
        r.hasDiscriminatoryLanguage = r.hasEthnicRacialSlur || r.hasHomophobicSlur;
        r.hasPolarizationSignal = r.hasPoliticalMention && containsAny(text, "zurdo", "caviar", "corrupto", "terrorista", "lacra", "rata", "traidor", "mafia");
        r.isSpamNoise = normalize(text).length() <= 2 || containsAny(text, ":orange_heart:");
        r.localRiskScore =
            (r.hasTerruqueo ? 2 : 0) + (r.hasFraude ? 2 : 0) + (r.hasDiscriminatoryLanguage ? 2 : 0) +
            (r.hasHomophobicSlur ? 2 : 0) + (r.hasPoliticalMention ? 1 : 0) + (r.hasPolarizationSignal ? 1 : 0) +
            (r.hasGeneralInsult ? 1 : 0);
        List<String> tags = new ArrayList<String>();
        if (r.hasTerruqueo) tags.add("terruqueo");
        if (r.hasFraude) tags.add("fraude");
        if (r.hasElectoralInstitution) tags.add("electoral_institution");
        if (r.hasPoliticalMention) tags.add("political_mention");
        if (r.hasPolarizationSignal) tags.add("polarization");
        if (r.hasEthnicRacialSlur) tags.add("ethnic_racial_slur");
        if (r.hasHomophobicSlur) tags.add("homophobic_slur");
        if (r.hasGeneralInsult) tags.add("general_insult");
        if (r.isSpamNoise) tags.add("spam_noise");
        r.localRuleTags = String.join("|", tags);
        r.actor = actor(text);
        return r;
    }

    static String actor(String text) {
        if (containsAny(text, "keiko", "fujimori", "fuerza popular", " fp ")) return "keiko_fujimori_fp";
        if (containsAny(text, "castillo", "peru libre", "perú libre")) return "castillo_peru_libre";
        if (containsAny(text, "jp", "juntos por el peru", "juntos por el perú")) return "jp_juntos_por_el_peru";
        if (containsAny(text, "lopez aliaga", "lópez aliaga", "porky", "rla")) return "lopez_aliaga_porky";
        if (containsAny(text, "antauro")) return "antauro";
        if (containsAny(text, "onpe", "jne")) return "onpe_jne";
        return "";
    }

    static String jsonEscape(String s) {
        return (s == null ? "" : s).replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ").replace("\r", " ");
    }

    static String envelope(String job, String type, RawEvent e, String payloadJson) {
        return "{\"job_name\":\"" + job + "\",\"event_type\":\"" + type + "\",\"processing_ts\":\"" + Instant.now().toString() +
            "\",\"source_topic\":\"" + e.topic + "\",\"source_partition\":" + e.partition + ",\"source_offset\":" + e.offset +
            ",\"payload\":" + payloadJson + "}";
    }

    static String normalizedOutput(RawEvent e) {
        String text = mainText(e.value);
        String norm = normalize(text);
        String payload = "{\"event_id\":\"" + jsonEscape(field(e.value, "event_id")) + "\",\"stream_text\":\"" + jsonEscape(norm) +
            "\",\"message_length\":" + norm.length() + ",\"is_empty_message\":" + norm.isEmpty() + "}";
        return envelope("flink_job1_normalize_stream", "normalized_comment", e, payload);
    }

    static String rulesOutput(RawEvent e) {
        RuleResult r = rules(e);
        String payload = "{\"event_id\":\"" + jsonEscape(r.eventId) + "\",\"has_terruqueo\":" + r.hasTerruqueo +
            ",\"has_fraude\":" + r.hasFraude + ",\"has_electoral_institution\":" + r.hasElectoralInstitution +
            ",\"has_political_mention\":" + r.hasPoliticalMention + ",\"has_polarization_signal\":" + r.hasPolarizationSignal +
            ",\"has_discriminatory_language\":" + r.hasDiscriminatoryLanguage + ",\"has_ethnic_racial_slur\":" + r.hasEthnicRacialSlur +
            ",\"has_homophobic_slur\":" + r.hasHomophobicSlur + ",\"has_general_insult\":" + r.hasGeneralInsult +
            ",\"is_spam_noise\":" + r.isSpamNoise + ",\"local_risk_score_stream\":" + r.localRiskScore +
            ",\"local_rule_tags\":\"" + jsonEscape(r.localRuleTags) + "\",\"actor\":\"" + jsonEscape(r.actor) + "\"}";
        return envelope("flink_job3_political_signals", "political_signals", e, payload);
    }

    static String alertOutput(RawEvent e) {
        RuleResult r = rules(e);
        String type = r.hasEthnicRacialSlur ? "ethnic_racial_discrimination" :
            r.hasHomophobicSlur ? "homophobic_slur" :
            (r.hasTerruqueo && r.hasGeneralInsult) ? "terruqueo_plus_insult" :
            (r.hasFraude && r.hasElectoralInstitution) ? "fraude_plus_institution" :
            "high_local_risk";
        String severity = r.localRiskScore >= 5 ? "high" : "medium";
        String payload = "{\"alert_id\":\"" + UUID.randomUUID() + "\",\"alert_type\":\"" + type + "\",\"severity\":\"" + severity +
            "\",\"reason\":\"" + jsonEscape(r.localRuleTags) + "\",\"event_id\":\"" + jsonEscape(r.eventId) + "\",\"actor\":\"" +
            jsonEscape(r.actor) + "\",\"message_text\":\"" + jsonEscape(r.text) + "\",\"local_rule_tags\":\"" +
            jsonEscape(r.localRuleTags) + "\",\"local_risk_score_stream\":" + r.localRiskScore +
            ",\"created_at\":\"" + Instant.now().toString() + "\"}";
        return envelope("flink_job5_risk_alerts", "risk_alert", e, payload);
    }

    static void job1(Args args, StreamExecutionEnvironment env) throws Exception {
        env.addSource(new KafkaRawSource(args.bootstrap, args.inputTopic, args.maxMessages, args.idleMs, args.delayMs))
            .name("Kafka raw_youtube_chat source")
            .map(new MapFunction<RawEvent, String>() { public String map(RawEvent e) { return normalizedOutput(e); }})
            .name("Normalize comment")
            .addSink(new KafkaJsonSink(args.bootstrap, args.outputTopic)).name("Kafka nlp_stream_results sink");
        env.execute("Flink Job 1 - Normalizacion streaming");
    }

    static void job2(Args args, StreamExecutionEnvironment env) throws Exception {
        DataStream<String> out = env.addSource(new KafkaRawSource(args.bootstrap, args.inputTopic, args.maxMessages, args.idleMs, args.delayMs))
            .map(new MapFunction<RawEvent, Metric>() {
                public Metric map(RawEvent e) {
                    Metric m = new Metric();
                    String text = normalize(mainText(e.value));
                    m.count = 1; m.totalLength = text.length();
                    m.emptyCount = text.isEmpty() ? 1 : 0;
                    m.spamLikeCount = (text.length() <= 2 || text.contains(":orange_heart:")) ? 1 : 0;
                    String author = field(e.value, "author");
                    if (!author.isEmpty()) m.authors.add(author);
                    return m;
                }
            })
            .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(args.windowSeconds)))
            .apply(new AllWindowFunction<Metric, String, TimeWindow>() {
                public void apply(TimeWindow w, Iterable<Metric> vals, Collector<String> out) {
                    long count = 0, len = 0, empty = 0, spam = 0;
                    Set<String> authors = new HashSet<String>();
                    for (Metric m : vals) { count += m.count; len += m.totalLength; empty += m.emptyCount; spam += m.spamLikeCount; authors.addAll(m.authors); }
                    if (count == 0) return;
                    String payload = "{\"window_start\":\"" + Instant.ofEpochMilli(w.getStart()).toString() + "\",\"window_end\":\"" +
                        Instant.ofEpochMilli(w.getEnd()).toString() + "\",\"comment_count\":" + count + ",\"unique_authors\":" +
                        authors.size() + ",\"avg_message_length\":" + (len * 1.0 / count) + ",\"empty_count\":" + empty +
                        ",\"spam_like_count\":" + spam + "}";
                    RawEvent meta = new RawEvent("", RAW_TOPIC, -1, -1, System.currentTimeMillis());
                    out.collect(envelope("flink_job2_window_metrics", "window_metrics", meta, payload));
                }
            });
        out.addSink(new KafkaJsonSink(args.bootstrap, args.outputTopic));
        env.execute("Flink Job 2 - Metricas por ventanas");
    }

    static void job3(Args args, StreamExecutionEnvironment env) throws Exception {
        env.addSource(new KafkaRawSource(args.bootstrap, args.inputTopic, args.maxMessages, args.idleMs, args.delayMs))
            .map(new MapFunction<RawEvent, String>() { public String map(RawEvent e) { return rulesOutput(e); }})
            .addSink(new KafkaJsonSink(args.bootstrap, args.outputTopic));
        env.execute("Flink Job 3 - Deteccion de senales politicas");
    }

    static void job4(Args args, StreamExecutionEnvironment env) throws Exception {
        env.addSource(new KafkaRawSource(args.bootstrap, args.inputTopic, args.maxMessages, args.idleMs, args.delayMs))
            .flatMap(new FlatMapFunction<RawEvent, ActorMetric>() {
                public void flatMap(RawEvent e, Collector<ActorMetric> out) {
                    RuleResult r = rules(e);
                    if (r.actor.isEmpty()) return;
                    ActorMetric m = new ActorMetric();
                    m.actor = r.actor; m.mentionCount = 1; m.insultCount = r.hasGeneralInsult ? 1 : 0;
                    m.fraudCount = r.hasFraude ? 1 : 0; m.terruqueoCount = r.hasTerruqueo ? 1 : 0;
                    m.discriminatoryCount = r.hasDiscriminatoryLanguage ? 1 : 0;
                    out.collect(m);
                }
            })
            .keyBy(new KeySelector<ActorMetric, String>() { public String getKey(ActorMetric m) { return m.actor; }})
            .window(TumblingProcessingTimeWindows.of(Time.seconds(args.windowSeconds)))
            .apply(new WindowFunction<ActorMetric, String, String, TimeWindow>() {
                public void apply(String actor, TimeWindow w, Iterable<ActorMetric> vals, Collector<String> out) {
                    long mention = 0, insult = 0, fraud = 0, terruqueo = 0, discr = 0;
                    for (ActorMetric m : vals) { mention += m.mentionCount; insult += m.insultCount; fraud += m.fraudCount; terruqueo += m.terruqueoCount; discr += m.discriminatoryCount; }
                    long score = insult + fraud + terruqueo + discr;
                    String payload = "{\"actor\":\"" + actor + "\",\"window_start\":\"" + Instant.ofEpochMilli(w.getStart()).toString() +
                        "\",\"window_end\":\"" + Instant.ofEpochMilli(w.getEnd()).toString() + "\",\"mention_count\":" + mention +
                        ",\"insult_count\":" + insult + ",\"fraud_count\":" + fraud + ",\"terruqueo_count\":" + terruqueo +
                        ",\"discriminatory_count\":" + discr + ",\"polarization_score\":" + score + "}";
                    RawEvent meta = new RawEvent("", RAW_TOPIC, -1, -1, System.currentTimeMillis());
                    out.collect(envelope("flink_job4_actor_polarization", "actor_polarization_window", meta, payload));
                }
            })
            .addSink(new KafkaJsonSink(args.bootstrap, args.outputTopic));
        env.execute("Flink Job 4 - Polarizacion por actor politico");
    }

    static void job5(Args args, StreamExecutionEnvironment env) throws Exception {
        env.addSource(new KafkaRawSource(args.bootstrap, args.inputTopic, args.maxMessages, args.idleMs, args.delayMs))
            .flatMap(new FlatMapFunction<RawEvent, String>() {
                public void flatMap(RawEvent e, Collector<String> out) {
                    RuleResult r = rules(e);
                    boolean alert = (r.hasTerruqueo && r.hasGeneralInsult) || (r.hasFraude && r.hasElectoralInstitution) ||
                        r.hasEthnicRacialSlur || r.hasHomophobicSlur || r.localRiskScore >= 4;
                    if (alert) out.collect(alertOutput(e));
                }
            })
            .addSink(new KafkaJsonSink(args.bootstrap, args.outputTopic));
        env.execute("Flink Job 5 - Alertas de riesgo");
    }

    public static void main(String[] argv) throws Exception {
        Args args = Args.parse(argv);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        if ("job1".equals(args.job)) job1(args, env);
        else if ("job2".equals(args.job)) job2(args, env);
        else if ("job3".equals(args.job)) job3(args, env);
        else if ("job4".equals(args.job)) job4(args, env);
        else if ("job5".equals(args.job)) job5(args, env);
        else throw new IllegalArgumentException("Unknown --job: " + args.job);
    }
}

