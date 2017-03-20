(ns pdok.featured-to-geoserver.processor
  (:require [pdok.featured-to-geoserver.result :refer :all] 
            [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [pdok.featured.feature :as feature]
				    [pdok.featured-to-geoserver.changelog :as changelog]
            [pdok.featured-to-geoserver.database :as database]))

(defn- object-data-filter [[key value]]
  (or 
    (= key :_geometry)
    (not (-> (name key) (str/starts-with? "_")))))

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

(defn- process-action
  "Processes a single changelog action."
  [bfr schema-name object-type action]
  (result<- [action action]
            (condp = (:action action)
              ; todo: add support for all action types
              :new [(database/append-record
                      bfr
                      object-type ; todo: add schema-name
                      (merge
                        (->> (:object-data action)
                          (filter object-data-filter) ; todo: remove this filter (if possible)
                          (map (fn [[key value]] [key (convert-value value)]))
                          (into {}))
                        {:_id (:object-id action)
                         :_version (:version-id action)}))])))

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