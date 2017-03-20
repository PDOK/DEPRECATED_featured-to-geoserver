(ns pdok.featured-to-geoserver.database
  (:require [clojure.set :as set]
            [clojure.string :as str]))

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

(defn- quote-escape [s]
  (str "\"" (str/replace s #"\"" "\"\"") "\""))

(deftype DefaultTransaction [^java.sql.Connection c]
  Transaction
  (batch-insert [this table columns values]
    (fn []
      (let [query (str 
                    "insert into " 
                    (-> table (name) (quote-escape)) 
                    "("
                    (->> columns
                      (map name)
                      (map quote-escape)
                      (str/join ", "))
                    ") values ("
                    (->> columns
                      (map (constantly "?"))
                      (str/join ", "))
                    ")"
                    )]
        (with-open [stmt (.prepareStatement c query)]
          (doseq [values values]
            (doseq [value (map-indexed vector values)]
              (.setObject
                ^java.sql.PreparedStatement stmt
                ^Integer (-> value first inc)
                ^Object (second value)))
            (.addBatch ^java.sql.PreparedStatement stmt))
          (.execute ^java.sql.PreparedStatement stmt)))
        {:insert (count values)}))
  (commit [this]
    (fn []
      (.commit c)
      (.close c)
      {}))
  (rollback [this reason]
    (fn []
      (.rollback c)
      (.close c)
      {:error reason}))
  (reducer [this] generate-tx-summary))

(defn- pg-connect [db]
  (do
    (java.lang.Class/forName "org.postgresql.Driver")
    (->DefaultTransaction
      (doto
        (java.sql.DriverManager/getConnection
          ^String (str "jdbc:postgresql://" (:host db) ":" (or (:port db) 5432) "/" (:dbname db))
          ^String (:user db)
          ^String (:password db))
        (.setAutoCommit false)))))

(defn connect [db]
  (condp = (:dbtype db)
    "postgresql" (pg-connect db)
    (throw (java.sql.SQLException. (str "Unsupported database type: " (:dbtype db))))))