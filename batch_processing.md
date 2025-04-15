
# Batch Processing


---

### 1. **Spark Transformations**

#### ✅ a. Convert `started_at` to timestamp
- Converts string to timestamp.

#### ✅ b. Add `record_time`
- Adds current timestamp to track ingestion time.

#### ✅ c. Filter out low-viewer streams
```sql
SELECT *
FROM twitch_streams_enriched
WHERE viewer_count > 1000;
```

#### ✅ d. Add popularity flag (`is_popular`)
- True if `viewer_count > 5000`, else False.
```sql
SELECT *
FROM twitch_streams_enriched
WHERE is_popular = TRUE;
```

#### ✅ e. Add viewer bucket (`viewer_bucket`)
- Buckets:
  - `High` (> 25,000)
  - `Medium` (> 15,000)
  - `Low` (≤ 15,000)

```sql
-- High
SELECT *
FROM twitch_streams_enriched
WHERE viewer_bucket = 'High';

-- Medium
SELECT *
FROM twitch_streams_enriched
WHERE viewer_bucket = 'Medium';

-- Low
SELECT *
FROM twitch_streams_enriched
WHERE viewer_bucket = 'Low';
```

---

### 2. **Batch Processing with foreachBatch**

Each batch is processed with the following analytics:

#### ✅ a. Total Viewers Per Game
```sql
SELECT 
    game_name,
    SUM(viewer_count) AS total_viewers,
    COUNT(*) AS num_streams,
    AVG(viewer_count) AS avg_viewers
FROM 
    twitch_streams_enriched
GROUP BY 
    game_name
ORDER BY 
    total_viewers DESC
LIMIT 5;
```

#### ✅ b. Popularity Distribution (Viewer Buckets)
```sql
SELECT 
    viewer_bucket,
    COUNT(*) AS count
FROM 
    twitch_streams_enriched
GROUP BY 
    viewer_bucket
ORDER BY 
    viewer_bucket;
```

#### ✅ c. Top Streamers
```sql
SELECT 
    user_name,
    SUM(viewer_count) AS total_viewers,
    COUNT(*) AS num_sessions
FROM 
    twitch_streams_enriched
GROUP BY 
    user_name
ORDER BY 
    total_viewers DESC
LIMIT 5;
```

---

### 3. TOPIC based split

#### ⏱️ Streams started in the last 30 minutes
```sql
SELECT *
FROM twitch_streams_enriched
WHERE started_at >= NOW() - INTERVAL 30 MINUTE;
```

#### ⏱️ Streams recorded in the last 5 minutes
```sql
SELECT *
FROM twitch_streams_enriched
WHERE record_time >= NOW() - INTERVAL 5 MINUTE;
```

---





