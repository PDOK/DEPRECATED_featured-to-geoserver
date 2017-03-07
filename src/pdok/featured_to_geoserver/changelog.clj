(ns pdok.featured-to-geoserver.changelog
  (:require [pdok.transit :as transit]
            [clojure.string :as str]))

(defn index-of-seq
  "Provides a sequence of indexes for occurences of a given value in a string."
  ([s value] (index-of-seq s value 0))
  ([s value from-index]
    (let [next (.indexOf ^String s value from-index)]
      (when (>= next 0)
        (lazy-seq (cons next (index-of-seq s value (inc next))))))))

(defn read-action
  "Parses a single changelog action line."
  [line]
  (let [separators (->> (index-of-seq line ",") (take 3) (vec))
        _ (assert (= 3 (count separators)) "Expected four columns (action, id, version, attributes) separated by a ',' character")
        [action id version] (map #(.substring ^String line (inc %1) %2)
                                 (cons -1 separators)
                                 separators)
        _ (assert (not (str/blank? action)) "Expected non-empty action value")
        _ (assert (not (str/blank? id)) "Expected non-empty id value")
        _ (assert (not (str/blank? version)) "Expected non-empty version value")
        attr-str (.substring ^String line (-> (last separators) (inc)))
        _ (assert (not (str/blank? attr-str)) "Expected non-empty attributes value")]
    {:action (keyword action)
     :id id
     :version version
     :attr (transit/from-json attr-str)}))

(defn from-buffered-reader
  "Parses a changelog."
  [rdr]
  (let [lines (line-seq rdr)
        [version & lines] lines
        _ (assert (= version "v1") "Expected supported (= v1) changelog version on first changelog line")
        [desc & lines] lines
        [dataset feature-type] (->> (str/split desc #",") (filter #(not (str/blank? %))) (map keyword))
        _ (assert (and dataset feature-type) "Expected two non-empty columns (dataset, feature-type) separated by a ',' character on second changelog line")]
    {:dataset dataset
     :feature-type feature-type
     :actions (map-indexed 
                (fn [idx value]
                  (try
                    (read-action value)
                    (catch AssertionError e 
                      (throw 
                        (AssertionError. 
                          (str "Assert failed: Couldn't parse line " (+ idx 3)) 
                          e)))))
                lines)}))
