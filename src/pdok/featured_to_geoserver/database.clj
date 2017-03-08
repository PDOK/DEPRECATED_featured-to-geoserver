(ns pdok.featured-to-geoserver.database
  (:require [clojure.set :as set]))

(defprotocol Transaction
  (batch-insert [this table columns values] "Writes a record set to a table.")
  (commit [this] "Commits transaction."))

(defprotocol Buffering
  (append-record [this table record] "Appends a single record.")
  (commit-buffers [this] "Flushes buffers and commits transaction."))

(defn- do-batch-insert
  "Performs a batch insert by first determining which columns are used in the batch
  and subsequently builds a value sequence for each individual record."
  [tx table records]
  (let [columns (->> (->> records
                         (map keys)
                         (map set))
                    (reduce set/union)
                    (sort) ; ensure predictable column order
                    (vec))
        values (mapv #(map % columns) records)]
    (batch-insert tx table columns values)))

(deftype DefaultBuffering [tx batch-size]
  Buffering
  (append-record [this table record]
    (fn [buffers]
      (let [buffered-records (or (buffers table) [])
            new-buffered-records (conj buffered-records record)]
        (if (< (count new-buffered-records) batch-size)
          [(assoc buffers table new-buffered-records)]
          [(dissoc buffers table) [(do-batch-insert tx table new-buffered-records)]]))))
  (commit-buffers [this]
    (fn [buffers]
      [{} (concat
            (map (fn [[table records]] (do-batch-insert tx table records)) buffers)
            [(commit tx)])])))

(defn process-buffer-operations
  "Processes a sequence of buffer operations and returns a sequence of the resulting transaction operations."
  ([buffer-operations] (process-buffer-operations {} buffer-operations))
  ([buffers buffer-operations]
    (when-let [buffer-operation (first buffer-operations)]
      (let [[buffers database-operations] (buffer-operation buffers)]
        (lazy-seq (concat database-operations (process-buffer-operations buffers (next buffer-operations))))))))
