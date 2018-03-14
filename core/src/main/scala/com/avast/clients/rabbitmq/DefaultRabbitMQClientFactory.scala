package com.avast.clients.rabbitmq

import java.time.Duration
import java.util.concurrent.{ScheduledExecutorService, TimeoutException}

import com.avast.clients.rabbitmq.api.DeliveryResult.{Ack, Reject, Republish, Retry}
import com.avast.clients.rabbitmq.api._
import com.avast.continuity.Continuity
import com.avast.continuity.monix.Monix
import com.avast.kluzo.Kluzo
import com.avast.metrics.scalaapi.Monitor
import com.avast.utils2.Done
import com.avast.utils2.errorhandling.FutureTimeouter
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.AMQP.Queue
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import monix.execution.Scheduler
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.{higherKinds, implicitConversions}
import scala.util.Try
import scala.util.control.NonFatal

private[rabbitmq] object DefaultRabbitMQClientFactory extends LazyLogging {

  private type ArgumentsMap = Map[String, Any]

  private[rabbitmq] final val DeclareQueueRootConfigKey = "ffRabbitMQDeclareQueueDefaults"
  private[rabbitmq] final val DeclareQueueDefaultConfig = ConfigFactory.defaultReference().getConfig(DeclareQueueRootConfigKey)

  private[rabbitmq] final val BindExchangeRootConfigKey = "ffRabbitMQBindExchangeDefaults"
  private[rabbitmq] final val BindExchangeDefaultConfig = ConfigFactory.defaultReference().getConfig(BindExchangeRootConfigKey)

  private[rabbitmq] final val DeclareExchangeRootConfigKey = "ffRabbitMQDeclareExchangeDefaults"
  private[rabbitmq] final val DeclareExchangeDefaultConfig = ConfigFactory.defaultReference().getConfig(DeclareExchangeRootConfigKey)

  private[rabbitmq] final val ProducerRootConfigKey = "ffRabbitMQProducerDefaults"
  private[rabbitmq] final val ProducerDefaultConfig = {
    val c = ConfigFactory.defaultReference().getConfig(ProducerRootConfigKey)
    c.withValue("declare", c.getConfig("declare").withFallback(DeclareExchangeDefaultConfig).root())
  }

  private[rabbitmq] final val ConsumerRootConfigKey = "ffRabbitMQConsumerDefaults"
  private[rabbitmq] final val ConsumerDefaultConfig = {
    val c = ConfigFactory.defaultReference().getConfig(ConsumerRootConfigKey)
    c.withValue("declare", c.getConfig("declare").withFallback(DeclareQueueDefaultConfig).root())
  }

  private[rabbitmq] final val ConsumerBindingRootConfigKey = "ffRabbitMQConsumerBindingDefaults"
  private[rabbitmq] final val ConsumerBindingDefaultConfig = ConfigFactory.defaultReference().getConfig(ConsumerBindingRootConfigKey)

  private implicit final val JavaDurationReader: ValueReader[Duration] = (config: Config, path: String) => config.getDuration(path)

  private implicit final val DeliveryResultReader: ValueReader[DeliveryResult] = (config: Config, path: String) =>
    config.getString(path).toLowerCase match {
      case "ack" => Ack
      case "reject" => Reject
      case "retry" => Retry
      case "republish" => Republish()
  }

  private implicit final val rabbitDeclareArgumentsReader: ValueReader[DeclareArguments] = (config: Config, path: String) => {
    import scala.collection.JavaConverters._
    val argumentsMap = config
      .getObject(path)
      .asScala
      .toMap
      .mapValues(_.unwrapped())

    DeclareArguments(argumentsMap)
  }

  private implicit final val rabbitBindArgumentsReader: ValueReader[BindArguments] = (config: Config, path: String) => {
    import scala.collection.JavaConverters._
    val argumentsMap = config
      .getObject(path)
      .asScala
      .toMap
      .mapValues(_.unwrapped())

    BindArguments(argumentsMap)
  }

  object Producer {

    def fromConfig[F[_]: FromTask](providedConfig: Config,
                                   channel: ServerChannel,
                                   factoryInfo: RabbitMqFactoryInfo,
                                   scheduler: Scheduler,
                                   monitor: Monitor): DefaultRabbitMQProducer[F] = {
      val producerConfig = providedConfig.wrapped.as[ProducerConfig]("root")

      create[F](producerConfig, channel, factoryInfo, scheduler, monitor)
    }

    def create[F[_]: FromTask](producerConfig: ProducerConfig,
                               channel: ServerChannel,
                               factoryInfo: RabbitMqFactoryInfo,
                               scheduler: Scheduler,
                               monitor: Monitor): DefaultRabbitMQProducer[F] = {

      prepareProducer[F](producerConfig, channel, factoryInfo, scheduler, monitor)
    }
  }

  object Consumer {

    def fromConfig(providedConfig: Config,
                   channel: ServerChannel,
                   channelFactoryInfo: RabbitMqFactoryInfo,
                   monitor: Monitor,
                   consumerListener: ConsumerListener,
                   scheduledExecutorService: ScheduledExecutorService)(readAction: Delivery => Future[DeliveryResult])(
        implicit ec: ExecutionContext): RabbitMQConsumer = {

      val mergedConfig = providedConfig.withFallback(ConsumerDefaultConfig)

      // merge consumer binding defaults
      val updatedConfig = {
        val updated = mergedConfig.as[Seq[Config]]("bindings").map { bindConfig =>
          bindConfig.withFallback(ConsumerBindingDefaultConfig).root()
        }

        import scala.collection.JavaConverters._

        mergedConfig.withValue("bindings", ConfigValueFactory.fromIterable(updated.asJava))
      }

      val consumerConfig = updatedConfig.wrapped.as[ConsumerConfig]("root")

      create(consumerConfig, channel, channelFactoryInfo, monitor, consumerListener, scheduledExecutorService)(readAction)
    }

    def create(consumerConfig: ConsumerConfig,
               channel: ServerChannel,
               channelFactoryInfo: RabbitMqFactoryInfo,
               monitor: Monitor,
               consumerListener: ConsumerListener,
               scheduledExecutorService: ScheduledExecutorService)(readAction: (Delivery) => Future[DeliveryResult])(
        implicit ec: ExecutionContext): RabbitMQConsumer = {

      prepareConsumer(consumerConfig, readAction, channelFactoryInfo, channel, consumerListener, monitor, scheduledExecutorService)
    }
  }

  object Declarations {
    def declareExchange(config: Config, channel: ServerChannel, channelFactoryInfo: RabbitMqFactoryInfo): Try[Done] = {
      declareExchange(config.withFallback(DeclareExchangeDefaultConfig).as[DeclareExchange], channel, channelFactoryInfo)
    }

    def declareQueue(config: Config, channel: ServerChannel, channelFactoryInfo: RabbitMqFactoryInfo): Try[Done] = {
      declareQueue(config.withFallback(DeclareQueueDefaultConfig).as[DeclareQueue], channel, channelFactoryInfo)
    }

    def bindQueue(config: Config, channel: ServerChannel, channelFactoryInfo: RabbitMqFactoryInfo): Try[Done] = {
      bindQueue(config.withFallback(ConsumerBindingDefaultConfig).as[BindQueue], channel, channelFactoryInfo)
    }

    def bindExchange(config: Config, channel: ServerChannel, channelFactoryInfo: RabbitMqFactoryInfo): Try[Done] = {
      bindExchange(config.withFallback(BindExchangeDefaultConfig).as[BindExchange], channel, channelFactoryInfo)
    }

    def declareExchange(config: DeclareExchange, channel: ServerChannel, channelFactoryInfo: RabbitMqFactoryInfo): Try[Done] = {
      import config._

      Try {
        DefaultRabbitMQClientFactory.this.declareExchange(name, `type`, durable, autoDelete, arguments, channel, channelFactoryInfo)
        Done
      }
    }

    def declareQueue(config: DeclareQueue, channel: ServerChannel, channelFactoryInfo: RabbitMqFactoryInfo): Try[Done] = {
      import config._

      Try {
        DefaultRabbitMQClientFactory.this.declareQueue(channel, name, durable, exclusive, autoDelete, arguments)
        Done
      }
    }

    def bindQueue(config: BindQueue, channel: ServerChannel, channelFactoryInfo: RabbitMqFactoryInfo): Try[Done] = {
      import config._

      Try {
        routingKeys.foreach(
          DefaultRabbitMQClientFactory.this.bindQueue(channelFactoryInfo)(channel, queueName)(exchangeName, _, bindArguments.value))
        Done
      }
    }

    def bindExchange(config: BindExchange, channel: ServerChannel, channelFactoryInfo: RabbitMqFactoryInfo): Try[Done] = {
      import config._

      Try {
        routingKeys.foreach(
          DefaultRabbitMQClientFactory.this
            .bindExchange(channelFactoryInfo)(channel, sourceExchangeName, destExchangeName, arguments.value))
        Done
      }
    }
  }

  private def prepareProducer[F[_]: FromTask](producerConfig: ProducerConfig,
                                              channel: ServerChannel,
                                              channelFactoryInfo: RabbitMqFactoryInfo,
                                              scheduler: Scheduler,
                                              monitor: Monitor): DefaultRabbitMQProducer[F] = {
    import producerConfig._

    val finalScheduler = if (useKluzo) Monix.wrapScheduler(scheduler) else scheduler

    // auto declare of exchange
    // parse it only if it's needed
    // "Lazy" parsing, because exchange type is not part of reference.conf and we don't want to make it fail on missing type when enabled=false
    if (declare.getBoolean("enabled")) {
      val d = declare.wrapped.as[AutoDeclareExchange]("root")
      declareExchange(exchange, channelFactoryInfo, channel, d)
    }
    new DefaultRabbitMQProducer[F](producerConfig.name, exchange, channel, useKluzo, reportUnroutable, finalScheduler, monitor)
  }

  private[rabbitmq] def declareExchange(name: String,
                                        channelFactoryInfo: RabbitMqFactoryInfo,
                                        channel: ServerChannel,
                                        autoDeclareExchange: AutoDeclareExchange): Unit = {
    import autoDeclareExchange._

    if (enabled) {
      declareExchange(name, `type`, durable, autoDelete, arguments, channel, channelFactoryInfo)
    }
    ()
  }

  private def declareExchange(name: String,
                              `type`: String,
                              durable: Boolean,
                              autoDelete: Boolean,
                              arguments: DeclareArguments,
                              channel: ServerChannel,
                              channelFactoryInfo: RabbitMqFactoryInfo): Unit = {
    logger.info(s"Declaring exchange '$name' of type ${`type`} in virtual host '${channelFactoryInfo.virtualHost}'")
    val javaArguments = argsAsJava(arguments.value)
    channel.exchangeDeclare(name, `type`, durable, autoDelete, javaArguments)
    ()
  }

  private def prepareConsumer(consumerConfig: ConsumerConfig,
                              readAction: (Delivery) => Future[DeliveryResult],
                              channelFactoryInfo: RabbitMqFactoryInfo,
                              channel: ServerChannel,
                              consumerListener: ConsumerListener,
                              monitor: Monitor,
                              scheduledExecutor: ScheduledExecutorService)(implicit ec: ExecutionContext): RabbitMQConsumer = {

    // auto declare exchanges
    consumerConfig.bindings.foreach { bind =>
      import bind.exchange._

      // parse it only if it's needed
      if (declare.getBoolean("enabled")) {
        val d = declare.wrapped.as[AutoDeclareExchange]("root")

        declareExchange(name, channelFactoryInfo, channel, d)
      }
    }

    // auto declare queue
    {
      import consumerConfig.declare._
      import consumerConfig.queueName

      if (enabled) {
        logger.info(s"Declaring queue '$queueName' in virtual host '${channelFactoryInfo.virtualHost}'")
        declareQueue(channel, queueName, durable, exclusive, autoDelete, arguments)
      }
    }

    // set prefetch size (per consumer)
    channel.basicQos(consumerConfig.prefetchCount)

    // auto bind
    bindQueues(channelFactoryInfo, channel, consumerConfig)

    prepareConsumer(consumerConfig, channelFactoryInfo, channel, readAction, consumerListener, monitor, scheduledExecutor)(ec)
  }

  private[rabbitmq] def declareQueue(channel: ServerChannel,
                                     queueName: String,
                                     durable: Boolean,
                                     exclusive: Boolean,
                                     autoDelete: Boolean,
                                     arguments: DeclareArguments): Queue.DeclareOk = {
    channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments.value)
  }

  private def bindQueues(channelFactoryInfo: RabbitMqFactoryInfo, channel: ServerChannel, consumerConfig: ConsumerConfig): Unit = {
    import consumerConfig.queueName

    consumerConfig.bindings.foreach { bind =>
      import bind._
      val exchangeName = bind.exchange.name

      if (routingKeys.nonEmpty) {
        routingKeys.foreach { routingKey =>
          bindQueue(channelFactoryInfo)(channel, queueName)(exchangeName, routingKey, bindArguments.value)
        }
      } else {
        // binding without routing key, possibly to fanout exchange

        bindQueue(channelFactoryInfo)(channel, queueName)(exchangeName, "", bindArguments.value)
      }
    }
  }

  private[rabbitmq] def bindQueue(channelFactoryInfo: RabbitMqFactoryInfo)(
      channel: ServerChannel,
      queueName: String)(exchangeName: String, routingKey: String, arguments: ArgumentsMap): AMQP.Queue.BindOk = {
    logger.info(s"Binding exchange $exchangeName($routingKey) -> queue '$queueName' in virtual host '${channelFactoryInfo.virtualHost}'")

    channel.queueBind(queueName, exchangeName, routingKey, arguments)
  }

  private[rabbitmq] def bindExchange(channelFactoryInfo: RabbitMqFactoryInfo)(
      channel: ServerChannel,
      sourceExchangeName: String,
      destExchangeName: String,
      arguments: ArgumentsMap)(routingKey: String): AMQP.Exchange.BindOk = {
    logger.info(
      s"Binding exchange $sourceExchangeName($routingKey) -> exchange '$destExchangeName' in virtual host '${channelFactoryInfo.virtualHost}'")

    channel.exchangeBind(destExchangeName, sourceExchangeName, routingKey, arguments)
  }

  private def prepareConsumer(consumerConfig: ConsumerConfig,
                              channelFactoryInfo: RabbitMqFactoryInfo,
                              channel: ServerChannel,
                              userReadAction: Delivery => Future[DeliveryResult],
                              consumerListener: ConsumerListener,
                              monitor: Monitor,
                              scheduledExecutor: ScheduledExecutorService)(ec: ExecutionContext): RabbitMQConsumer = {
    import consumerConfig._

    implicit val finalExecutor: ExecutionContext = if (useKluzo) {
      Continuity.wrapExecutionContext(ec)
    } else {
      ec
    }

    val readAction = wrapReadAction(consumerConfig, userReadAction, scheduledExecutor)

    val consumer =
      new DefaultRabbitMQConsumer(name,
                                  channel,
                                  queueName,
                                  useKluzo,
                                  monitor,
                                  failureAction,
                                  consumerListener,
                                  bindQueue(channelFactoryInfo)(channel, queueName))(readAction)

    val finalConsumerTag = if (consumerTag == "Default") "" else consumerTag

    channel.basicConsume(queueName, false, finalConsumerTag, consumer)

    consumer
  }

  private def wrapReadAction(
      consumerConfig: ConsumerConfig,
      userReadAction: Delivery => Future[DeliveryResult],
      scheduledExecutor: ScheduledExecutorService)(implicit finalExecutor: ExecutionContext): (Delivery) => Future[DeliveryResult] = {
    import FutureTimeouter._
    import consumerConfig._

    (delivery: Delivery) =>
      try {
        // we try to catch also long-lasting synchronous work on the thread
        val action = Future {
          userReadAction(delivery)
        }.flatMap(identity)

        val traceId = Kluzo.getTraceId

        action
          .timeoutAfter(processTimeout)(finalExecutor, scheduledExecutor)
          .recover {
            case e: TimeoutException =>
              traceId.foreach(Kluzo.setTraceId)
              logger.warn(s"Task timed-out, applying DeliveryResult.${consumerConfig.timeoutAction}", e)
              consumerConfig.timeoutAction

            case NonFatal(e) =>
              traceId.foreach(Kluzo.setTraceId)

              logger.warn(s"Error while executing callback, applying DeliveryResult.${consumerConfig.failureAction}", e)
              consumerConfig.failureAction
          }(finalExecutor)
      } catch {
        case NonFatal(e) =>
          logger.error(s"Error while executing callback, applying DeliveryResult.${consumerConfig.failureAction}", e)
          Future.successful(consumerConfig.failureAction)
      }

  }

  implicit class WrapConfig(val c: Config) extends AnyVal {
    def wrapped: Config = {
      // we need to wrap it with one level, to be able to parse it with Ficus
      ConfigFactory
        .empty()
        .withValue("root", c.withFallback(ProducerDefaultConfig).root())
    }
  }

  private implicit def argsAsJava(value: ArgumentsMap): java.util.Map[String, Object] = {
    value.mapValues(_.asInstanceOf[Object]).asJava
  }

}
