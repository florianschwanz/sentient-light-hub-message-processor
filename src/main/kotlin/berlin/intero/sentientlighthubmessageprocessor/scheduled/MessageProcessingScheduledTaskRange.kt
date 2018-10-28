package berlin.intero.sentientlighthubmessageprocessor.scheduled

import berlin.intero.sentientlighthub.common.SentientProperties
import berlin.intero.sentientlighthub.common.model.MQTTEvent
import berlin.intero.sentientlighthub.common.model.payload.RangeLEDPayload
import berlin.intero.sentientlighthub.common.model.payload.SingleLEDPayload
import berlin.intero.sentientlighthub.common.tasks.MQTTPublishAsyncTask
import berlin.intero.sentientlighthub.common.tasks.MQTTSubscribeAsyncTask
import com.google.gson.Gson
import com.google.gson.JsonSyntaxException
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.MqttCallback
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.core.task.SyncTaskExecutor
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.*
import java.util.logging.Logger

/**
 * This scheduled task
 * <li> calls {@link MQTTSubscribeAsyncTask} to subscribe sensor valuesCurrent from MQTT broker
 * <li> calls {@link SentientMappingEvaluationAsyncTask} for each mapping from configuration
 */
@Component
class MessageProcessingScheduledTaskRange {
    val values: MutableMap<String, String> = HashMap()
    val valuesHistoric: MutableMap<String, String> = HashMap()

    companion object {
        private val log: Logger = Logger.getLogger(MessageProcessingScheduledTaskRange::class.simpleName)
    }

    init {
        val topic = "${SentientProperties.MQTT.Topic.MACRO}/#"

        log.info("topic $topic")

        val callback = object : MqttCallback {
            override fun messageArrived(topic: String, message: MqttMessage) {
                log.fine("MQTT value receiced")
                values[topic] = String(message.payload)
            }

            override fun connectionLost(cause: Throwable?) {
                log.fine("MQTT connection lost")
            }

            override fun deliveryComplete(token: IMqttDeliveryToken?) {
                log.fine("MQTT delivery complete")
            }
        }

        // Call MQTTSubscribeAsyncTask
        SimpleAsyncTaskExecutor().execute(MQTTSubscribeAsyncTask(topic, callback))
    }

    @Scheduled(fixedDelay = SentientProperties.Frequency.SENTIENT_WRITE_DELAY)
    @SuppressWarnings("unused")
    fun write() {
        log.fine("${SentientProperties.Color.TASK}-- MESSAGE PROCESSING TASK${SentientProperties.Color.RESET}")


        values.forEach { topic, value ->

            val macro = topic.split("/")[2]

            log.info("macro $macro")

            when(macro) {
                 SentientProperties.MQTT.Topic.Macro.RANGE -> {
                     try {
                         // Parse payload
                         val payload = Gson().fromJson(value, RangeLEDPayload::class.java)
                         log.fine("payload stripID ${payload.stripId}")
                         log.fine("payload startLedId ${payload.startLedId}")
                         log.fine("payload stopLedId ${payload.stopLedId}")
                         log.fine("payload warmWhite ${payload.warmWhite}")
                         log.fine("payload coldWhite ${payload.coldWhite}")
                         log.fine("payload amber ${payload.amber}")

                         val stripId = payload.stripId
                         val warmWhite = payload.warmWhite
                         val coldWhite = payload.coldWhite
                         val amber = payload.amber

                         if (value != valuesHistoric.get(topic)) {
                             valuesHistoric.set(topic, value)

                             val startLedId = Integer.parseInt(payload.startLedId)
                             val stopLedId = Integer.parseInt(payload.stopLedId)

                             val mqttEvents = ArrayList<MQTTEvent>()
                             for (ledId in startLedId..stopLedId) {
                                 val lowerLevelTopic = SentientProperties.MQTT.Topic.LED
                                 val lowerLevelPayload = SingleLEDPayload(stripId, ledId.toString(), warmWhite, coldWhite, amber)
                                 val mqttEvent = MQTTEvent(lowerLevelTopic, Gson().toJson(lowerLevelPayload), Date())

                                 mqttEvents.add(mqttEvent)
                             }

                             // Call SerialSetLEDAsyncTask
                             SyncTaskExecutor().execute(MQTTPublishAsyncTask(mqttEvents))
                         }
                     } catch (jse: JsonSyntaxException) {
                         log.severe("${SentientProperties.Color.ERROR}${jse}${SentientProperties.Color.RESET}")
                     }
                }
            }
        }
    }
}
