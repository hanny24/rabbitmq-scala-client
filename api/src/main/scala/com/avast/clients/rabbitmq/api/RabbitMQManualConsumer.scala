package com.avast.clients.rabbitmq.api

import scala.concurrent.Future
import scala.language.higherKinds

trait RabbitMQManualConsumer[F[_], A] {
  def get(f: Delivery[A] => F[DeliveryResult]): Future[Unit]
}
