(ns pdok.featured-to-geoserver.database
  (:require [clojure.set :as set]))

(defprotocol Transaction
  (batch-insert [this table columns values] "Writes a record set to a table.")
  (commit [this] "Commits transaction.")
  (rollback [this reason] "Performs a rollback.")
  (reducer [this] "Provides the reducer ultimately producing a feedback value."))

(defprotocol Buffering
  (append-record [this table record] "Appends a single record.")
  (error [this error] "Raises an error.")
  (finish [this] "Flushes buffers and commits transaction."))

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
    (fn [state]
      (let [buffered-records (or (-> state :append table) [])
            new-buffered-records (conj buffered-records record)]
        (if (< (count new-buffered-records) batch-size)
          [(assoc-in state [:append table] new-buffered-records)]
          [(assoc state :append (dissoc (:append state) table)) [(do-batch-insert tx table new-buffered-records)]]))))
  (error [this error]
    (fn [state]
      [{:error error} [(rollback tx error)]]))
  (finish [this]
    (fn [state]
      [{} (concat
            (map (fn [[table records]] (do-batch-insert tx table records)) (:append state))
            [(commit tx)])])))

(defn process-buffer-operations
  "Processes a sequence of buffer operations and returns a sequence of the resulting transaction operations."
  ([buffer-operations] (process-buffer-operations {} buffer-operations))
  ([state buffer-operations]
    (when (not (:error state))
      (when-let [buffer-operation (first buffer-operations)]
        (let [[state database-operations] (buffer-operation state)]
          (lazy-seq (concat database-operations (process-buffer-operations state (next buffer-operations)))))))))

(defn generate-tx-summary
  ([] {})
  ([agg i]
    (if-let [error (:error i)]
      error
      (reduce
        (fn [agg [key value]]
          (update-in agg [key] #(+ value (or % 0))))
        agg
        (seq i)))))

; todo: actually send something to the db
(deftype DefaultTransaction []
  Transaction
  (batch-insert [this table columns values]
    (fn []
      (println "batch-insert" table columns values)
      {:insert (count values)}))
  (commit [this]
    (fn []
      (println "commit")
      {}))
  (rollback [this reason]
    (fn []
      (println "rollback" reason)
      {:error reason}))
  (reducer [this] generate-tx-summary))
