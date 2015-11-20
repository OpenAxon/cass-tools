package com.evidence.techops.cass.statsd

/**
 * Created by pmahendra on 11/19/15.
 */

import java.util.Date
import com.timgroup.statsd.Event
import com.timgroup.statsd.{Event, StatsDClient}
import org.joda.time.{DateTimeZone, DateTime}

import scala.runtime.NonLocalReturnControl

trait StatsD {
  protected def statsd: StatsDClient

  /**
   * Measures the execution time of the code block, handling non-local return.
   * @param aspect The metric name, which will be prepended with your service name.
   * @param tags Additional tags to apply to your metric
   * @param thunk Code block
   * @tparam T Return value of block
   * @return Return value of block
   */
  def executionTime[T](aspect: String, tags: String*)(thunk: => T): T = {
    val t1 = System.currentTimeMillis

    val x = try {
      thunk
    } catch {
      case nlr: NonLocalReturnControl[T@unchecked] => nlr.value
    }

    val t2 = System.currentTimeMillis

    statsd.recordExecutionTime(aspect, t2 - t1, tags: _*)

    x
  }

  def recordInfoEvent(title: String, text: String, tags: String*): Unit = {
    val event = Event
      .builder()
      .withTitle(title)
      .withText(text)
      .withDate(new DateTime(DateTimeZone.UTC).toDate)
      .build()

    statsd.recordEvent(event, tags: _*)
  }

  def recordErrorEvent(title: String, text: String, tags: String*): Unit = {
    val event = Event
      .builder()
      .withTitle(title)
      .withText(text)
      .withDate(new DateTime(DateTimeZone.UTC).toDate)
      .withAlertType(Event.AlertType.ERROR)
      .build()

    statsd.recordEvent(event, tags: _*)
  }

  def recordSuccessEvent(title: String, text: String, tags: String*): Unit = {
    val event = Event
      .builder()
      .withTitle(title)
      .withText(text)
      .withDate(new DateTime(DateTimeZone.UTC).toDate)
      .withAlertType(Event.AlertType.SUCCESS)
      .build()

    statsd.recordEvent(event, tags: _*)
  }
}

/**
 * Todo: Fix the logger factories to appropriately mixin StatsD as a val versus a def.
 */
trait LazyStatsD extends StatsD {
  override protected def statsd: StatsDClient = {
    StatsdClient.statsd
  }
}

/**
 * Todo: Fix the logger factories to appropriately mixin StatsD as a val versus a def.
 */
trait StrictStatsD extends StatsD {
  override protected def statsd: StatsDClient = {
    StatsdClient.statsd
  }
}
