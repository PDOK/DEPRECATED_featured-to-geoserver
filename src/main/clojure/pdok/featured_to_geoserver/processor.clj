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

(defn- not-technical? [[key value]]
  (or
    (= key :_geometry)
    (not (-> (name key) (str/starts-with? "_")))))

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

(defn- new-records [object-type object-id version-id object-data]
  ; todo: remove the 'not-technical?' filter in a future version (if possible)
  (let [object-data (filter not-technical? object-data)]
    (cons
      [object-type (merge
                     (->> object-data
                       (filter simple-value?)
                       (mapcat add-geometry-group)
                       (map #(vector (first %) (convert-value (second %))))
                       (into {}))
                     {:_id object-id
                      :_version version-id})]
      (->> object-data
        (mapcat complex-values)
        (mapcat #(new-records
                   (keyword (str (name object-type) "$" (name (first %))))
                   object-id
                   version-id
                   (second %)))))))

(defn- process-action
  "Processes a single changelog action."
  [bfr schema-name related-tables object-type action-result]
  (bind-result
    (fn [action]
      (letfn [(append-records
                []
                (map
                  (fn [[object-type record]]
                    (database/append-record
                      bfr
                      schema-name
                      object-type
                      record))
                  (new-records
                    object-type
                    (:object-id action)
                    (:version-id action)
                    (:object-data action))))
              (remove-records [version-field]
                []
                (->> (cons
                       object-type
                       (->
                         related-tables
                         schema-name
                         object-type))
                  (map
                    #(database/remove-record
                       bfr
                       schema-name
                       %
                       {:_id (:object-id action)
                        :_version (version-field action)}))))]
        (try
          (unit-result
            (condp = (:action action)
              :new (append-records)
              :delete (remove-records :version-id)
              :change (concat (remove-records :prev-version-id) (append-records))
              :close (remove-records :prev-version-id)))
          (catch Throwable t
            (log/error t "Couldn't process action")
            (merge-result
              action-result
              (error-result :action-failed :exception (exception-to-string t)))))))
    action-result))

(defn- process-actions
  "Processes a sequence of changelog actions."
  [bfr schema-name related-tables object-type actions]
  (concat
    (->> actions
      (map #(process-action bfr schema-name related-tables object-type %))
      (map unwrap-result)
      (mapcat (fn [[value error]] (or value [(database/error bfr error)]))))
    (list (database/finish bfr))))

(defn- reader [changelog bfr related-tables tx-channel exception-channel]
  (async/go
    (try
      (let [{schema-name :schema-name
             object-type :object-type
             actions :actions} changelog
            buffer-operations (process-actions bfr schema-name related-tables object-type actions)
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
  ([tx related-tables batch-size changelog]
    (let [bfr (database/->DefaultBuffering tx batch-size)
          tx-channel (async/chan 10)
          feedback-channel (async/chan 10)
        reduced-feedback (let [reducer (database/reducer tx)]
                             (async/reduce reducer (reducer) feedback-channel))
          exception-channel (async/chan 10)]
      (async/go
        (reader changelog bfr related-tables tx-channel exception-channel)
        (async/<! (writer tx-channel feedback-channel exception-channel))
        ; parked until tx-channel is closed by the changelog reader
        (async/close! feedback-channel)
      (async/close! exception-channel)
        (let [exceptions (async/<! (async/reduce conj [] exception-channel))]
          (if (first exceptions)
              {:failure {:exceptions (map exception-to-string exceptions)}}
              {:done (async/<! reduced-feedback)}))))))
