package ru.prepor.kafka

import com.yammer.metrics.Metrics
import fi.iki.elonen.NanoHTTPD
import io.prometheus.client.CollectorRegistry
import kafka.metrics.KafkaMetricsReporter
import kafka.utils.VerifiableProperties
import io.prometheus.client.exporter.common.TextFormat
import java.io.StringWriter

class KafkaPrometheusReporter() : KafkaMetricsReporter {
    override fun init(props: VerifiableProperties) {
        val prefix = props.getString("external.kafka.prometheus.prefix", "kafka_broker")
        val port = props.getInt("external.kafka.prometheus.port", 8000)

        val collector = DropwizardExports(Metrics.defaultRegistry(), prefix + "_")
        CollectorRegistry.defaultRegistry.register(collector)
        Server(port)

    }

    class Server(port: Int) : NanoHTTPD(port) {
        init {
            start(NanoHTTPD.SOCKET_READ_TIMEOUT, true);
        }

        override fun serve(session: IHTTPSession): Response {
            return when (session.uri) {
                "/" -> newFixedLengthResponse("Ok")
                "/metrics" -> {
                    val sw = StringWriter()
                    TextFormat.write004(sw, CollectorRegistry.defaultRegistry.metricFamilySamples())
                    return newFixedLengthResponse(Response.Status.OK, TextFormat.CONTENT_TYPE_004, sw.toString())
                }
                else -> newFixedLengthResponse(Response.Status.NOT_FOUND, "plain/text", "Not found")
            }
        }
    }
}