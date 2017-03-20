(ns pdok.featured-to-geoserver.processor
  (:require [pdok.featured-to-geoserver.result :refer :all] 
            [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [pdok.featured.feature :as feature]
				    [pdok.featured-to-geoserver.changelog :as changelog]
            [pdok.featured-to-geoserver.database :as database]))

(defn- object-data-filter [[key value]]
  ; todo: remove the _* filter (if possible)
  (or
    (= key :_geometry)
    (and
      (not (-> (name key) (str/starts-with? "_")))
      (not (seq? value))
      (not (map? value)))))

(def ^:private wkb-writer (com.vividsolutions.jts.io.WKBWriter. 2 true))

(defn- convert-geometry [^pdok.featured.GeometryAttribute value]
  (let [jts (feature/as-jts value)]
    (.write 
      ^com.vividsolutions.jts.io.WKBWriter wkb-writer 
      ^com.vividsolutions.jts.geom.Geometry jts)))

(defn- convert-value [value]
  ; todo: support more types
  (condp = (type value)
    pdok.featured.GeometryAttribute (convert-geometry value)
    value))

(defn- new-records [object-type object-id version-id object-data]
  (cons
    [object-type (merge
                   (->> object-data
                     (filter object-data-filter)
                     (map (fn [[key value]] [key (convert-value value)]))
                     (into {}))
                   {:_id object-id
                    :_version version-id})]
    (->> object-data
      (mapcat
        (fn [[key value]]
          (cond
            (map? value) (list [key value])
            (seq? value) (map #(vector key %) value))))
      (mapcat #(new-records
                 (keyword (str (name object-type) "$" (name (first %))))
                 object-id
                 version-id
                 (second %))))))

(defn- process-action
  "Processes a single changelog action."
  [bfr schema-name object-type action]
  (result<- [action action]
            (condp = (:action action)
              ; todo: add support for all action types
              :new (map
                     (fn [[object-type record]]
                       (database/append-record
                         bfr
                         object-type
                         record))
                     (new-records
                       object-type 
                       (:object-id action)
                       (:version-id action)
                       (:object-data action))))))

(defn- process-actions
  "Processes a sequence of changelog actions."
  [bfr schema-name object-type actions]
  (concat
    (->> actions
      (map #(process-action bfr schema-name object-type %))
      (map unwrap-result)
      (mapcat (fn [[value error]] (or value [(database/error bfr error)]))))
    (list (database/finish bfr))))

(defn- reader [changelog bfr tx-channel exception-channel]
  (async/go
    (try
      (let [{schema-name :schema-name
             object-type :object-type
             actions :actions} changelog
            buffer-operations (process-actions bfr schema-name object-type actions)
            tx-operations (database/process-buffer-operations buffer-operations)]
        (async/<! (async/onto-chan tx-channel tx-operations))) ; implicitly closes tx-channel
      (catch Throwable t
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
        (async/>! exception-channel t)))))

(defn process [tx changelog]
  (let [bfr (database/->DefaultBuffering tx 100)
        tx-channel (async/chan 10)
        feedback-channel (async/chan 10)
        reduced-feedback (let [reducer (database/reducer tx)]
                           (async/reduce reducer (reducer) feedback-channel))
        exception-channel (async/chan 10)]
    (async/go
      (reader changelog bfr tx-channel exception-channel)
      (async/<! (writer tx-channel feedback-channel exception-channel))
      ; parked until tx-channel is closed by the changelog reader
      (async/close! feedback-channel)
      (async/close! exception-channel)
      (let [exceptions (async/<! (async/reduce conj [] exception-channel))]
          (if (first exceptions)
            {:failure {:exceptions exceptions}}
            {:done (async/<! reduced-feedback)})))))