### 1. What is the partition key of `plays_by_user`?

The partition key is **`user_id`**

### 2. What are the clustering columns of `plays_by_user`?

The clustering columns are **`played_at`** and **`song_id`**

### 3. Why did we create both `plays_by_user` and `plays_by_song` instead of using one table?

Cassandra requires the queries to filter by the partition key, so plays_by_user is partitioned by user_id to answer what songs a given user recently played, and plays_by_song is partitioned by song_id to efficiently answer which users recently played a specific song.

### 4. What happens if you try to query `plays_by_user` by `song_id` only?

Cassandra will reject the query or require ALLOW FILTERING. Because song_id is a clustering column and not a partition key, there's no way for cassandra to locate the right partition without a user_id, so it would have to scan every partition across the whole cluster to find matching rows which is inefficient and defeats the whole point.

---

### 5. Why is data duplication common in Cassandra?

Data duplication is common in Cassandra because of its query-first design. Cassandra, unlike other relational databases, doesn't have joins, so you need to pre-organize data at write time so that reads are fast.

This means the same event, such as a user playing a song, gets written to multiple tables and each one is optimized for a different query. This uses more storage and makes writing complex because you need to maintain data integrity across the tables, but it also allows for very fast reads that never require scanning across partitions. Therefore, this tradeoff is worth it at scale because reads are much more frequent than writes.

---

### 6. Query Outputs

**Query:** `SELECT * FROM plays_by_user WHERE user_id = 'u1';`

```
 user_id | played_at                       | song_id | artist        | device | title
---------+---------------------------------+---------+---------------+--------+-----------------
      u1 | 2026-05-01 10:10:00.000000+0000 |      s3 | Billie Eilish | laptop |         bad guy
      u1 | 2026-05-01 10:05:00.000000+0000 |      s2 |    The Weeknd | iphone | Blinding Lights
      u1 | 2026-05-01 10:00:00.000000+0000 |      s1 |  Taylor Swift | iphone |       Anti-Hero

(3 rows)
```

**Query:** `SELECT * FROM plays_by_song WHERE song_id = 's1';`

```
 song_id | played_at                       | user_id | artist       | device  | title
---------+---------------------------------+---------+--------------+---------+-----------
      s1 | 2026-05-01 12:00:00.000000+0000 |      u3 | Taylor Swift |  laptop | Anti-Hero
      s1 | 2026-05-01 11:00:00.000000+0000 |      u2 | Taylor Swift | android | Anti-Hero
      s1 | 2026-05-01 10:00:00.000000+0000 |      u1 | Taylor Swift |  iphone | Anti-Hero

(3 rows)
```
