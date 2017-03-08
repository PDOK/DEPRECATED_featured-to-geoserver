(ns pdok.featured-to-geoserver.database)

(defprotocol Transaction
  (batch-insert [this table records] "Writes a record set to a table.")
  (commit [this] "Commit transaction"))

(defprotocol Buffering
  (append-record [this table record] "Append a single record")
  (commit-buffers [this] "Commit buffer"))

(deftype DefaultBuffering [tx batch-size]
  Buffering
  (append-record [this table record]
    (fn [buffers]
      (let [buffered-records (or (buffers table) [])
            new-buffered-records (conj buffered-records record)]
        (if (< (count new-buffered-records) batch-size)
          [(assoc buffers table new-buffered-records)]
          [(dissoc buffers table) [(batch-insert tx table new-buffered-records)]]))))
  (commit-buffers [this]
    (fn [buffers]
      [{} (concat
            (map (fn [[table records]] (batch-insert tx table records)) buffers)
            [(commit tx)])])))

(defn process-buffer-operations
  "Processes a sequence of buffer operations and returns a sequence of the resulting transaction operations"
  ([buffer-operations] (process-buffer-operations {} buffer-operations))
  ([buffers buffer-operations]
    (when-let [buffer-operation (first buffer-operations)]
      (let [[buffers database-operations] (buffer-operation buffers)]
        (lazy-seq (concat database-operations (process-buffer-operations buffers (next buffer-operations))))))))
