(ns pdok.featured-to-geoserver.changelog
  (:require [clojure.string :as str]
            [pdok.transit :as transit]
            [pdok.featured-to-geoserver.util :refer :all]
            [pdok.featured-to-geoserver.result :refer :all]))

(defn- as-numbered [coll nkey]
  (map-indexed #(unit-result %2 nkey (inc %1)) coll))

(defn- read-transit-line [line]
  (try
    (unit-result (transit/from-json line))
    (catch Exception e
      (merge-result
        {:exception (str e)}
        (error-result :invalid-object-data)))))

(def field-converters {:action keyword
                       :collection keyword}) 

(defn- read-action [line]
  (let [action-fields {:new [:collection :id :version :attributes]
               :change [:collection :id :previous-version :version :attributes]
               :close [:collection :previous-version]
               :delete [:collection :previous-version]}]
    (->> line
      (read-transit-line)
      (map-result
        (fn [action]
          (reduce
            (fn [action [key value]]
              (let [field-converter (key field-converters)]
                (assoc action key (field-converter value))))
            action
            (select-keys action (keys field-converters)))))
      (bind-result
        (fn [action]
          (let [action-type (:action action)
                required-fields (action-type action-fields)
                missing-fields (filter #(not (% action)) required-fields)]
            (cond
              (not required-fields) (error-result :unknown-action :action action-type)
              (first missing-fields) (error-result :fields-missing :action action-type :fields missing-fields)
              :else (unit-result action))))))))

(defn read-changelog [lines]
  (let [[header actions] (split-at 2 (as-numbered lines :line))
        [version meta-info] header
        version (or version (error-result :line-missing :line 1))
        meta-info (or meta-info (error-result :line-missing :line 2))]
    (result<- [version (filter-result #(= "pdok-featured-changelog-v2" %) :unsupported-version version)
               meta-info (->> meta-info (bind-result read-transit-line) (merge-result meta-info))]
              {:meta-info meta-info
               :actions (map
                          (fn [line]
                            (->> line
                              (bind-result read-action)
                              (merge-result line)))
                          actions)})))
