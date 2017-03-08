(ns pdok.featured-to-geoserver.changelog
  (:require [clojure.string :as str]
            [pdok.transit :as transit]
            [pdok.featured-to-geoserver.result :refer :all]))

(defn index-of-seq
  "Provides a sequence of indexes for occurences of a given value in a string."
  ([str value] (index-of-seq str value 0))
  ([str value from-index]
    (let [next (.indexOf ^String str value from-index)]
      (when (>= next 0)
        (lazy-seq (cons next (index-of-seq str value (inc next))))))))

(defn- str-split-seq [str idx]
  (when-let [first (first idx)]
    (let [begin (inc first)
          end (second idx)]
      (lazy-seq
        (cons
          (unit-result
            (if end 
              (.substring ^String str begin end) 
              (.substring ^String str begin))
            :col
            begin)
          (str-split-seq str (next idx)))))))

(defn str-split
  "Splits string on given separator with an optional split limit."
  ([str separator] (str-split str separator nil))
  ([str separator limit]
    (let [idx (cons -1 (index-of-seq str separator))
          idx (if limit (take limit idx) idx)]
      (str-split-seq str idx))))

(defn- as-numbered [coll nkey]
  (map-indexed #(unit-result %2 nkey (inc %1)) coll))

(defn- check-field [x field-name]
  (let [field (or x (error-result :field-missing))
        field (merge-result {:field field-name} field)]
    (filter-result (complement str/blank?) :field-empty field)))

(defn- read-object-data [object-data]
  (->> (check-field object-data :object-data)
    (bind-result 
      (fn [object-data]
        (try
          (unit-result (transit/from-json object-data))
          (catch Exception e 
            (merge-result 
              {:exception (str e)}
              (error-result :invalid-object-data))))))
    (merge-result object-data)))

(defn- read-new-action [line]
  (let [[_ object-id version-id object-data] (str-split line "," 4)]
    (result<- [object-id (check-field object-id :object-id)
               version-id (check-field version-id :version-id)
               object-data (read-object-data object-data)]
              {:object-id object-id
               :version-id version-id
               :object-data object-data})))

(defn- read-delete-action [line]
  (let [fields (str-split line ",")]
    (if (< (count fields) 4)
        (let [[_ object-id, version-id] fields]
          (result<- [object-id (check-field object-id :object-id)
                     version-id (check-field version-id :version-id)]
                    {:object-id object-id
                     :version-id version-id}))
        (error-result :too-many-fields))))

(defn read-change-or-close-action [line]
    (let [[_ object-id prev-version-id version-id object-data] (str-split line "," 5)]
    (result<- [object-id (check-field object-id :object-id)
               prev-version-id (check-field prev-version-id :prev-version-id)
               version-id (check-field version-id :version-id)
               object-data (read-object-data object-data)]
              {:object-id object-id
               :prev-version-id prev-version-id
               :version-id version-id
               :object-data object-data})))

(defn- read-action [line]
  (let [actions {:new read-new-action
                 :delete read-delete-action
                 :change read-change-or-close-action
                 :close read-change-or-close-action}
        [action] (str-split line ",")]
    (result<- [action (->> (check-field action :action)
                        (map-result keyword)
                        (filter-result actions :unknown-action))
               result ((action actions) line)]
              (merge {:action action} result))))

(defn read-changelog [lines]
  (let [[header actions] (split-at 2 (as-numbered lines :line))
        [version dataset] header
        version (or version (error-result :line-missing :line 1))
        dataset (or dataset (error-result :line-missing :line 2))]
    (result<- [version (filter-result #(= "v1" %) :unsupported-version version)
               [schema-name object-type] (->> dataset 
                                           (map-result #(str-split % ","))
                                           (filter-result #(< (count %) 3) :too-many-fields))
               schema-name (merge-result dataset (check-field schema-name :schema-name))
               object-type (merge-result dataset (check-field object-type :object-type))]
              {:schema-name (keyword schema-name)
               :object-type (keyword object-type)
               :actions (map 
                          (fn [line]
                            (->> line 
                              (bind-result read-action)
                              (merge-result line)))
                          actions)})))
