package io.buoyant.telemetry.influxdb

import com.twitter.finagle.Service
import com.twitter.finagle.http.{MediaType, Request}
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.tracing.NullTracer
import com.twitter.util.{Awaitable, Closable, Future}
import io.buoyant.admin.Admin
import io.buoyant.telemetry.{Metric, MetricsTree, Telemeter}

/**
 * This telemeter exposes metrics data in the InfluxDb LINE format, served on
 * an admin endpoint.  It does not provide a StatsReceiver or Tracer.  It reads
 * histogram summaries directly off of the MetricsTree and assumes that stats
 * are being snapshotted at some appropriate interval.
 */
class InfluxDbTelemeter(metrics: MetricsTree) extends Telemeter with Admin.WithHandlers {

  private[influxdb] val handler = Service.mk { request: Request =>
    val host = request.host.getOrElse("") // TODO: allow user to set host / unique ID?
    val response = request.response
    response.mediaType = MediaType.Txt
    val sb = new StringBuilder()
    writeMetrics(metrics, sb, Nil, Seq(("host", host)))
    response.contentString = sb.toString
    Future.value(response)
  }

  val adminHandlers: Seq[Admin.Handler] = Seq(
    Admin.Handler("/admin/metrics/influxdb", handler)
  )

  val stats = NullStatsReceiver
  def tracer = NullTracer
  def run(): Closable with Awaitable[Unit] = Telemeter.nopRun

  private[this] val disallowedChars = "[^a-zA-Z0-9:]".r
  private[this] def escapeKey(key: String) = disallowedChars.replaceAllIn(key, "_")

  private[this] def formatLabels(labels: Seq[(String, String)]): String =
    if (labels.nonEmpty) {
      labels.sortBy(_._1).map {
        case (k, v) =>
          s"""$k=$v"""
      }.mkString(",")
    } else {
      ""
    }

  private[this] def labelExists(labels: Seq[(String, String)], name: String) =
    labels.exists(_._1 == name)

  private[this] def writeMetrics(
    tree: MetricsTree,
    sb: StringBuilder,
    prefix0: Seq[String],
    tags0: Seq[(String, String)]
  ): Unit = {
    // TODO: share with prometheus telemeter?
    // Re-write elements out of the prefix into labels
    val (prefix1, tags1) = prefix0 match {
      case Seq("rt", router) if !labelExists(tags0, "rt") =>
        (Seq("rt"), tags0 :+ ("rt" -> router))
      case Seq("rt", "dst", "path", path) if !labelExists(tags0, "dst_path") =>
        (Seq("rt", "dst_path"), tags0 :+ ("dst_path" -> path))
      case Seq("rt", "dst", "id", id) if !labelExists(tags0, "dst_id") =>
        (Seq("rt", "dst_id"), tags0 :+ ("dst_id" -> id))
      case Seq("rt", "dst_id", "path", path) if !labelExists(tags0, "dst_path") =>
        (Seq("rt", "dst_id", "dst_path"), tags0 :+ ("dst_path" -> path))
      case Seq("rt", "srv", srv) if !labelExists(tags0, "srv") =>
        (Seq("rt", "srv"), tags0 :+ ("srv" -> srv))
      case _ => (prefix0, tags0)
    }

    // gather all metrics directly under a common parent
    val fields = {
      tree.children.toSeq map {
        case (name, child) =>
          // write metrics deeper in the tree (side effect)
          writeMetrics(child, sb, prefix1 :+ name, tags1)

          child.metric match {
            case c: Metric.Counter => Some((name, c.get.toString))
            case g: Metric.Gauge => Some((name, g.get.toString))
            case s: Metric.Stat => None // TODO
            case _ => None
          }
      }
    }.flatten

    // write sibling metrics as fields in a single measurement
    if (fields.nonEmpty) {
      sb.append(escapeKey(prefix1.mkString(":")))
      if (tags1.nonEmpty) {
        sb.append(",")
        sb.append(formatLabels(tags1))
      }
      sb.append(" ")
      sb.append(formatLabels(fields))
      sb.append("\n")
    }

    return
  }
}
