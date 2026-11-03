package net.activitywatch.android.workers

import android.content.Context
import android.util.Log
import androidx.work.Worker
import androidx.work.WorkerParameters
import java.io.IOException
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import net.activitywatch.android.BuildConfig
import net.activitywatch.android.RustInterface
import net.activitywatch.android.deviceHostname
import net.activitywatch.android.watcher.SessionEventWatcher
import net.activitywatch.android.watcher.UsageStatsWatcher
import org.json.JSONArray
import org.json.JSONObject
import org.threeten.bp.Instant
import org.threeten.bp.OffsetDateTime
import org.threeten.bp.format.DateTimeFormatter

private const val TAG = "RemotePushWorker"

// Between two pushes only a handful of events are new; read the whole bucket only if
// the cursor is further back than this.
private const val RECENT_EVENTS = 200
private const val INSERT_CHUNK = 500

/**
 * Copies this device's buckets to a self-hosted aw-server ([BuildConfig.REMOTE_SERVER_URL])
 * as `<bucket>_<hostname>`, so the phone shows up there next to the desktop watchers.
 *
 * The remote bucket is its own cursor: each run sends the local events that start at or
 * after the newest remote event. That newest event can still grow locally (the open app
 * session, the current tab), so it replaces the server's copy by id; later events are
 * inserted. A failed or repeated run therefore never duplicates events.
 */
class RemotePushWorker(context: Context, params: WorkerParameters) : Worker(context, params) {

    private val server = BuildConfig.REMOTE_SERVER_URL.trimEnd('/')

    override fun doWork(): Result {
        if (server.isEmpty()) return Result.success()
        return try {
            val ri = RustInterface(applicationContext)
            // Same guard as EventParsingWorker: ingesting while the datastore is unavailable
            // resets the session cursor and re-inserts history.
            if (JSONObject(ri.getBuckets()).opt("error") is String) {
                Log.w(TAG, "Local datastore unavailable; skipping push")
                return Result.failure()
            }
            // Otherwise usage events reach the datastore only hourly or when the app is opened.
            if (UsageStatsWatcher.isUsageAllowed(applicationContext)) {
                SessionEventWatcher(applicationContext).processEventsSinceLastUpdate()
            }

            val hostname = deviceHostname(applicationContext)
            val buckets = ri.getBucketsJSON()
            var sent = 0
            for (id in buckets.keys()) {
                val bucket = buckets.optJSONObject(id) ?: continue
                // Buckets pulled in from other devices by aw-sync carry their own hostname.
                if (bucket.optString("hostname") != hostname) continue
                sent += pushBucket(ri, id, bucket, hostname)
            }
            Log.i(TAG, "Pushed $sent events to $server")
            Result.success()
        } catch (e: Exception) {
            // Usually the server is unreachable (Tailscale off); the next period tries again.
            Log.w(TAG, "Push to $server failed", e)
            Result.failure()
        }
    }

    /** Returns the number of events sent. */
    private fun pushBucket(ri: RustInterface, localId: String, bucket: JSONObject, hostname: String): Int {
        val url = "$server/api/0/buckets/" + URLEncoder.encode("${localId}_$hostname", "UTF-8")
        val meta = JSONObject()
            .put("client", bucket.optString("client", "aw-android"))
            .put("type", bucket.getString("type"))
            .put("hostname", hostname)
        request("POST", url, meta.toString()) // 304 when the bucket already exists

        val newest = JSONArray(request("GET", "$url/events?limit=1")).optJSONObject(0)
        val cursor = newest?.let(::startOf)
        val events = localEventsSince(ri, localId, cursor)
        // The local row id: the server keys events by id and would overwrite its own event.
        events.forEach { it.remove("id") }
        val (atCursor, fresh) = events.partition { startOf(it) == cursor }
        // Other local events with exactly that start were sent in an earlier run.
        val replacement = atCursor.lastOrNull()?.put("id", newest!!.getLong("id"))
        val toSend = listOfNotNull(replacement) + fresh

        for (chunk in toSend.chunked(INSERT_CHUNK)) {
            request("POST", "$url/events", JSONArray(chunk).toString())
        }
        return toSend.size
    }

    /** Local events of [bucketId] starting at or after [cursor] (all when null), oldest first. */
    private fun localEventsSince(ri: RustInterface, bucketId: String, cursor: Instant?): List<JSONObject> {
        // getEvents returns the newest first; a limit of -1 means no limit.
        var events = ri.getEventsJSON(bucketId, if (cursor == null) -1 else RECENT_EVENTS)
        if (cursor != null && events.length() == RECENT_EVENTS &&
            startOf(events.getJSONObject(RECENT_EVENTS - 1)) >= cursor
        ) {
            events = ri.getEventsJSON(bucketId, -1)
        }
        return (events.length() - 1 downTo 0)
            .map(events::getJSONObject)
            .filter { cursor == null || startOf(it) >= cursor }
    }

    private fun startOf(event: JSONObject): Instant =
        OffsetDateTime.parse(event.getString("timestamp"), DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            .toInstant()

    private fun request(method: String, url: String, body: String? = null): String {
        val conn = URL(url).openConnection() as HttpURLConnection
        try {
            conn.requestMethod = method
            conn.connectTimeout = 15_000
            conn.readTimeout = 60_000
            if (body != null) {
                conn.doOutput = true
                conn.setRequestProperty("Content-Type", "application/json")
                conn.outputStream.use { it.write(body.toByteArray()) }
            }
            val code = conn.responseCode
            if (code == 304) return ""
            if (code !in 200..299) {
                val error = conn.errorStream?.bufferedReader()?.use { it.readText() }
                throw IOException("$method $url: HTTP $code $error")
            }
            return conn.inputStream.bufferedReader().use { it.readText() }
        } finally {
            conn.disconnect()
        }
    }
}
