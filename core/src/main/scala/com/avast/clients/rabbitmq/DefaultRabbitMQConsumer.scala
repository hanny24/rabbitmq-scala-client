package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{AMQP, DefaultConsumer, Envelope, ShutdownSignalException}
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler

import scala.collection.JavaConverters._

class DefaultRabbitMQConsumer(override val name: String,
                              override protected val channel: ServerChannel,
                              override protected val queueName: String,
                              override protected val useKluzo: Boolean,
                              override protected val monitor: Monitor,
                              override protected val failureAction: DeliveryResult,
                              consumerListener: ConsumerListener,
                              override protected val blockingScheduler: Scheduler)(readAction: DeliveryReadAction[Task, Bytes])(
    override protected implicit val callbackScheduler: Scheduler)
    extends DefaultConsumer(channel)
    with ConsumerBase
    with RabbitMQConsumer
    with StrictLogging {

  override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException): Unit =
    consumerListener.onShutdown(this, channel, name, consumerTag, sig)

  override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
    handleDelivery(envelope, properties, body, readAction)

    ()
  }

  override protected def handleFailure(messageId: String,
                                       deliveryTag: Long,
                                       properties: AMQP.BasicProperties,
                                       routingKey: String,
                                       body: Array[Byte],
                                       t: Throwable): Task[Unit] = {
    consumerListener.onError(this, name, channel, t)

    super.handleFailure(messageId, deliveryTag, properties, routingKey, body, t)
  }
}
