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

(defn- read-changelog-entry [line]
  (let [action-fields {:new [:collection :id :version :attributes]
               :change [:collection :id :previous-version :version :attributes]
               :close [:collection :previous-version]
               :delete [:collection :previous-version]}]
    (->> line
      (read-transit-line)
      (map-result
        (fn [changelog-entry]
          (reduce
            (fn [changelog-entry [key value]]
              (let [field-converter (key field-converters)]
                (assoc changelog-entry key (field-converter value))))
            changelog-entry
            (select-keys changelog-entry (keys field-converters)))))
      (bind-result
        (fn [changelog-entry]
          (let [action (:action changelog-entry)
                required-fields (action action-fields)
                missing-fields (filter #(not (% changelog-entry)) required-fields)]
            (cond
              (not required-fields) (error-result :unknown-action :action action)
              (first missing-fields) (error-result :fields-missing :action action :fields missing-fields)
              :else (unit-result changelog-entry))))))))

(defn read-changelog [lines]
  (let [[header entries] (split-at 2 (as-numbered lines :line))
        [version meta-info] header
        version (or version (error-result :line-missing :line 1))
        meta-info (or meta-info (error-result :line-missing :line 2))]
    (result<- [version (filter-result
                         #(or
                            (= "pdok-featured-changelog-v2" %)
                            (= "pdok-featured-changelog-v3" %))
                         :unsupported-version version)
               meta-info (->> meta-info (bind-result read-transit-line) (merge-result meta-info))]
              {:meta-info meta-info
               :entries (map
                          (fn [line]
                            (->> line
                              (bind-result read-changelog-entry)
                              (merge-result line)))
                          entries)})))
