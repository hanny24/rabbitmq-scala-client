package com.avast.clients.rabbitmq.api

import scala.concurrent.Future
import scala.language.higherKinds

trait RabbitMQManualConsumer[F[_], A] {

  /** Retrieves one message from the queue and tries to execute passed action for the delivery.<br>
    * Please note that the action doesn't have to be executed if there has some failure happened, however the resulting [[Future]] is always
    * completed (with either success or failure).
    *
    * @param f The action which should be executed for the delivery.
    * @return [[Future]] which completes when the delivery is processed. The [[Future]] is completed in all possible cases of success
    *         or failure.
    */
  def get(f: Delivery[A] => F[DeliveryResult]): Future[Unit]
}
