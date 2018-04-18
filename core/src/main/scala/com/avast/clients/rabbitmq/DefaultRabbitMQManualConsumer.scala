package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{Delivery, DeliveryResult, RabbitMQManualConsumer}
import com.avast.metrics.scalaapi.Monitor
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.Future
import scala.language.higherKinds

class DefaultRabbitMQManualConsumer[F[_]: ToTask, A: DeliveryConverter](
    override protected val name: String,
    override protected val channel: ServerChannel,
    override protected val queueName: String,
    override protected val useKluzo: Boolean,
    override protected val monitor: Monitor,
    override protected val failureAction: DeliveryResult,
    override protected val blockingScheduler: Scheduler)(override protected implicit val callbackScheduler: Scheduler)
    extends RabbitMQManualConsumer[F, A]
    with ConsumerBase
    with StrictLogging {

  private def convertMessage(b: Bytes): Either[ConversionException, A] = implicitly[DeliveryConverter[A]].convert(b)

  override def get(userReadAction: Delivery[A] => F[DeliveryResult]): Future[Unit] = {
    val response = channel.basicGet(queueName, false)

    val readAction = convertAction(userReadAction)

    val envelope = response.getEnvelope
    val properties = response.getProps

    handleDelivery(envelope, properties, response.getBody, readAction)
  }

  private def convertAction(userReadAction: Delivery[A] => F[DeliveryResult]): DeliveryReadAction[Task, Bytes] = { (d: Delivery[Bytes]) =>
    {
      convertMessage(d.body) match {
        case Right(a) =>
          val devA = d.copy(body = a)
          implicitly[ToTask[F]].apply(userReadAction(devA))

        case Left(ce) => Task.raiseError(ce)
      }
    }
  }

}
