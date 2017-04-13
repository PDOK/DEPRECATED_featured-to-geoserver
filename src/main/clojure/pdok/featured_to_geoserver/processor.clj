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

(defn- new-records [collection id version attributes]
  (cons
    [collection (merge
                  (->> attributes
                    (filter simple-value?)
                    (mapcat add-geometry-group)
                    (map #(vector (first %) (convert-value (second %))))
                    (into {}))
                  {:_id id
                   :_version version})]
    (->> attributes
      (mapcat complex-values)
      (mapcat #(new-records
                 (keyword (str (name collection) "$" (name (first %))))
                 id
                 version
                 (second %))))))

(defn- exclude-action? [exclude-filter action]
  (->> exclude-filter
    (map
      (fn [[action-field exclude-values]]
        (let [action-value (action-field action)
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

(defn- process-action
  "Processes a single changelog action."
  [bfr dataset related-tables exclude-filter action-result]
  (bind-result
    (fn [action]
      (let [collection (:collection action)]
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
                      (:id action)
                      (:version action)
                      (:attributes action))))
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
                         {:_version (:previous-version action)}))))]
          (try
            (unit-result
              (if (exclude-action? exclude-filter action)
                (list)
                (condp = (:action action)
                  :new (append-records)
                  :delete (remove-records)
                  :change (concat (remove-records) (append-records))
                  :close (remove-records))))
            (catch Throwable t
              (log/error t "Couldn't process action")
              (merge-result
                action-result
                (error-result :action-failed :exception (exception-to-string t))))))))
      action-result))

(defn- process-actions
  "Processes a sequence of changelog actions."
  [bfr dataset related-tables exclude-filter actions]
  (concat
    (->> actions
      (map #(process-action bfr dataset related-tables exclude-filter %))
      (map unwrap-result)
      (mapcat (fn [[value error]] (or value [(database/error bfr error)]))))
    (list (database/finish bfr))))

(defn- reader [dataset changelog bfr related-tables exclude-filter tx-channel exception-channel]
  (async/go
    (try
      (let [{actions :actions} changelog
            buffer-operations (process-actions bfr dataset related-tables exclude-filter actions)
            tx-operations (database/process-buffer-operations buffer-operations)]
        (async/<! (async/onto-chan tx-channel tx-operations))) ; implicitly closes tx-channel
      (catch Throwable t
        (log/error t "Couldn't read data from changelog")
        (async/close! tx-channel)
        (async/>! exception-channel t)))))

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
  ([tx related-tables exclude-filter batch-size dataset changelog]
    (let [bfr (database/->DefaultBuffering tx batch-size)
          tx-channel (async/chan 10)
          feedback-channel (async/chan 10)
          reduced-feedback (let [reducer (database/reducer tx)]
                             (async/reduce reducer (reducer) feedback-channel))
          exception-channel (async/chan 10)]
      (log/info "Related tables found for dataset" dataset ":" related-tables)
      (async/go
        (reader dataset changelog bfr related-tables exclude-filter tx-channel exception-channel)
        (async/<! (writer tx-channel feedback-channel exception-channel))
        ; parked until tx-channel is closed by the changelog reader
        (async/close! feedback-channel)
      (async/close! exception-channel)
        (let [exceptions (async/<! (async/reduce conj [] exception-channel))]
          (if (first exceptions)
              {:failure {:exceptions (map exception-to-string exceptions)}}
              {:done (async/<! reduced-feedback)}))))))
