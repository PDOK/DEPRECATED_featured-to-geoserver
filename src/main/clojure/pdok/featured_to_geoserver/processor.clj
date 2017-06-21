(ns pdok.featured-to-geoserver.processor
  (:require [pdok.featured-to-geoserver.result :refer :all]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clj-time [coerce :as tc]]
            [clojure.tools.logging :as log]
            [pdok.featured.feature :as feature]
            [pdok.featured-to-geoserver.util :refer :all]
            [pdok.featured-to-geoserver.changelog :as changelog]
            [pdok.featured-to-geoserver.database :as database]))

(defn- simple-value? [[key value]]
  (not
    (or
      (map? value)
      (seq? value)
      (vector? value))))

(defn- add-geometry-group [[key value]]
  (if (instance? pdok.featured.GeometryAttribute value)
    (list
      [key value]
      [(keyword (str (name key) "_group")) (feature/geometry-group value)])
    (list [key value])))

(defmulti convert-value class)

(defmethod convert-value clojure.lang.Keyword [value]
  (name value))

(defmethod convert-value pdok.featured.NilAttribute [value]
  nil)

(defmethod convert-value pdok.featured.GeometryAttribute [^pdok.featured.GeometryAttribute value]
  (when (feature/valid-geometry? value)
    (let [jts (feature/as-jts value)]
      ; feature/as-jts should always result in a valid JTS object when feature/valid-geometry? returns true
      (when (not jts)
        (throw (IllegalStateException. "Couldn't obtain JTS for geometry")))
      (.write
        ^com.vividsolutions.jts.io.WKBWriter (com.vividsolutions.jts.io.WKBWriter. 2 true)
        ^com.vividsolutions.jts.geom.Geometry jts))))

(defmethod convert-value org.joda.time.DateTime [value]
  (tc/to-sql-time value))

(defmethod convert-value org.joda.time.LocalDateTime [value]
  (tc/to-sql-time value))

(defmethod convert-value org.joda.time.LocalDate [value]
  (tc/to-sql-date value))

(defmethod convert-value :default [value]
  value)

(defn- complex-values [[key value]]
  (cond
    (map? value) (list [key value])
    (or
      (seq? value)
      (vector? value)) (map
                         #(vector
                            key
                            (if (map? %) % {:value %}))
                         value)))

(defn- new-records [collection mapping id version attributes]
  (cons
    [collection (merge
                  (->> attributes
                    (filter #(or (simple-value? %) (contains? (:array mapping) (first %))))
                    (mapcat add-geometry-group)
                    (map #(vector (first %) (convert-value (second %))))
                    (into {}))
                  {:_id id
                   :_version version})]
    (->> attributes
      (filter #(not (contains? (:array mapping) (first %))))
      (mapcat complex-values)
      (mapcat #(new-records
                 (keyword (str (name collection) "$" (name (first %))))
                 nil ; configurable mapping is currently only supported for toplevel attributes
                 id
                 version
                 (second %))))))

(defn- exclude-changelog-entry? [exclude-filter changelog-entry]
  (->> exclude-filter
    (map
      (fn [[action-field exclude-values]]
        (let [action-value (action-field changelog-entry)
              field-converter (action-field changelog/field-converters)]
          (map
            (fn [exclude-value]
              (= 
                (if field-converter
                  (field-converter exclude-value)
                  exclude-value)
                action-value))
            exclude-values))))
    (flatten)
    (some true?)))

(defn- process-changelog-entry
  "Processes a single changelog entry."
  [bfr dataset related-tables mapping exclude-filter changelog-entry-result]
  (bind-result
    (fn [changelog-entry]
      (let [collection (:collection changelog-entry)]
        (letfn [(append-records
                  []
                  (map
                    (fn [[collection record]]
                      (database/append-record
                        bfr
                        dataset
                        collection
                        record))
                    (new-records
                      collection
                      (collection mapping)
                      (:id changelog-entry)
                      (:version changelog-entry)
                      (:attributes changelog-entry))))
                (remove-records
                  []
                  (->> (cons
                         collection
                         (-> related-tables collection))
                    (map
                      #(database/remove-record
                         bfr
                         dataset
                         %
                         {:_version (:previous-version changelog-entry)}))))]
          (try
            (unit-result
              (if (exclude-changelog-entry? exclude-filter changelog-entry)
                (list)
                (condp = (:action changelog-entry)
                  :new (append-records)
                  :delete (remove-records)
                  :change (concat (remove-records) (append-records))
                  :close (remove-records))))
            (catch Throwable t
              (log/error t "Couldn't process changelog entry")
              (merge-result
                changelog-entry-result
                (error-result :action-failed :exception (exception-to-string t))))))))
      changelog-entry-result))

(defn- process-changelog-entries
  "Processes a sequence of changelog entries."
  [bfr dataset related-tables mapping exclude-filter changelog-entries]
  (concat
    (->> changelog-entries
      (map #(process-changelog-entry bfr dataset related-tables mapping exclude-filter %))
      (map unwrap-result)
      (mapcat (fn [[value error]] (or value [(database/error bfr error)]))))
    (list (database/finish bfr))))

(defn- reader [dataset changelog bfr related-tables mapping exclude-filter tx-channel exception-channel]
  (async/go
    (try
      (let [{changelog-entries :entries} changelog
            buffer-operations (process-changelog-entries bfr dataset related-tables mapping exclude-filter changelog-entries)
            tx-operations (database/process-buffer-operations buffer-operations)]
        (async/<! (async/onto-chan tx-channel tx-operations))) ; implicitly closes tx-channel when done
      (catch Throwable t
        (log/error t "Couldn't read data from changelog")
        ; after the exception is successfully posted to exception-channel,
        ; close tx-channel explicitly in order to ensure that the request processing
        ; terminates and the failure is reported back to the caller.
        (async/>! exception-channel t)
        (async/close! tx-channel)))))

(defn- writer [tx-channel feedback-channel exception-channel]
  (async/go
    (try
      (loop []
        (when-let [tx-operation (async/<! tx-channel)]
          (async/>! feedback-channel (tx-operation))
          (recur)))
      (catch Throwable t
        (log/error t "Couldn't write data to database")
        (async/>! exception-channel t)))))

(defn process
  ([tx related-tables mapping exclude-filter batch-size dataset changelog]
    (let [bfr (database/->DefaultBuffering tx batch-size)
          tx-channel (async/chan 10)
          feedback-channel (async/chan 10)
          reduced-feedback (let [reducer (database/reducer tx)]
                             (async/reduce reducer (reducer) feedback-channel))
          exception-channel (async/chan 10)]
      (log/info "Related tables found for dataset" dataset ":" related-tables)
      (async/go
        (reader dataset changelog bfr related-tables mapping exclude-filter tx-channel exception-channel)
        (async/<! (writer tx-channel feedback-channel exception-channel))
        ; parked until tx-channel is closed by the changelog reader
        (async/close! feedback-channel)
        (async/close! exception-channel)
        (let [exceptions (async/<! (async/reduce conj [] exception-channel))]
          (if (first exceptions)
            {:failure {:exceptions (map exception-to-string exceptions)}}
            {:done (async/<! reduced-feedback)}))))))
