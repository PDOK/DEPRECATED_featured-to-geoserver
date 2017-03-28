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

(defn- convert-geometry [^pdok.featured.GeometryAttribute value]
  (when (feature/valid-geometry? value)
    (let [jts (feature/as-jts value)]
      (.write 
        ^com.vividsolutions.jts.io.WKBWriter (com.vividsolutions.jts.io.WKBWriter. 2 true)
        ^com.vividsolutions.jts.geom.Geometry jts))))

(defn- convert-value [value]
  ; todo: support more types
  (condp = (type value)
    clojure.lang.Keyword (name value)
    pdok.featured.GeometryAttribute (convert-geometry value)
    org.joda.time.DateTime (tc/to-sql-time value)
    org.joda.time.LocalDateTime (tc/to-sql-time value)
    org.joda.time.LocalDate (tc/to-sql-date value)
    value))

(defn- new-records [object-type object-id version-id object-data]
  ; todo: remove the _* filter in a future version (if possible)
  (let [object-data (filter
                      (fn [[key value]]
                        (or
                          (= key :_geometry)
                          (not (-> (name key) (str/starts-with? "_"))))) object-data)]
    (cons
      [object-type (merge
                     (->> object-data
                       (filter (fn [[key value]] (not (or (map? value) (seq? value) (vector? value)))))
                       (mapcat 
                         (fn [[key value]]
                           (if (instance? pdok.featured.GeometryAttribute value)
                             (list 
                               [key value]                             
                               [(keyword (str (name key) "_group")) (feature/geometry-group value)])
                             (list [key value]))))
                       (map (fn [[key value]] [key (convert-value value)]))
                       (into {}))
                     {:_id object-id
                      :_version version-id})]
      (->> object-data
        (mapcat
          (fn [[key value]]
            (cond
              (map? value) (list [key value])
              (or (seq? value) (vector? value)) (map #(vector key (if (map? %) % {:value %})) value))))
        (mapcat #(new-records
                   (keyword (str (name object-type) "$" (name (first %))))
                   object-id
                   version-id
                   (second %)))))))

(defn- process-action
  "Processes a single changelog action."
  [bfr schema-name related-tables object-type action]
  (result<- [action action]
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
              (condp = (:action action)
                :new (append-records)
                :delete (remove-records :version-id)
                :change (concat (remove-records :prev-version-id) (append-records))
                :close (remove-records :prev-version-id)))))

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
