(ns pdok.featured-to-geoserver.processor-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [pdok.featured-to-geoserver.result :refer :all]
            [clojure.core.async :as async]
            [pdok.transit :as transit]
            [pdok.featured-to-geoserver.util :refer :all]
            [pdok.featured-to-geoserver.database :as database]
            [pdok.featured-to-geoserver.changelog :as changelog]
            [pdok.featured-to-geoserver.processor :as processor]))

(defn mock-tx-reducer
  ([] [])
  ([a b] (conj a b)))

(defn mock-tx []
  (reify database/Transaction
    (batch-insert [this schema table columns batch] (fn [] [:insert schema table columns batch]))
    (batch-delete [this schema table columns batch] (fn [] [:delete schema table columns batch]))
    (rollback [this error] (fn [] [:rollback error]))
    (commit [this] (fn [] [:commit]))
    (reducer [this] mock-tx-reducer)))

(def ^:private default-batch-size 100)

(defn- process-changelog
  ([tx dataset content] (process-changelog tx dataset default-batch-size content))
  ([tx dataset batch-size content] (process-changelog tx dataset {} batch-size content))
  ([tx dataset related-tables batch-size content] (process-changelog tx dataset related-tables {} batch-size content))
  ([tx dataset related-tables exclude-filter batch-size content]
    (->> (changelog/read-changelog content)
      (map-result #(async/<!! (processor/process tx related-tables {} exclude-filter batch-size dataset %)))
      (unwrap-result))))

(deftest test-process
  (is
    (=
      [nil {:error :unsupported-version, :error-details {:line 1}}]
      (process-changelog (mock-tx) :dataset (list "pdok-featured-changelog-v3"))))
  (is
    (-> (process-changelog
         (reify database/Transaction
           ; implements just enough to start the processor
           ; and crashes while generating tx operations
           (reducer [this] mock-tx-reducer))
         :dataset
         (list
           "pdok-featured-changelog-v2"
           (transit/to-json {})))
      first
      :failure))
  (is
    (-> (process-changelog
         (reify database/Transaction
           ; implements just enough to start the processor
           ; and generate tx operations, crashing when
           ; those operations are actually being executed
           (batch-insert [this schema table columns batch] (fn [] (assert false)))
           (batch-delete [this schema table columns batch] (fn [] (assert false)))
           (rollback [this error] (fn [] (assert false)))
           (commit [this] (fn [] (assert false)))
           (reducer [this] mock-tx-reducer))
         :dataset
         (list
           "pdok-featured-changelog-v2"
           (transit/to-json {})))
      first
      :failure))
  (is
    (=
      [{:done [
               [:insert
                :dataset
                :collection
                '(:_id :_version :i)
                `(
                   ("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" ~(uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a") 42))]
               [:commit]]}
       nil]
      (process-changelog
        (mock-tx)
        :dataset
        (list
          "pdok-featured-changelog-v2"
          (transit/to-json {})
          (transit/to-json
            {:action "new"
             :collection "collection"
             :id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
             :version (uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")
             :attributes {:i 42}})))))
  (is
    (=
      [{:done [
               [:insert
                :dataset
                :collection
                '(:_id :_version :j)
                `(
                   ("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" ~(uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a") 47))]
               [:insert
                :dataset
                :collection$complex
                '(:_id :_version :i :s)
                `(
                   ("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" ~(uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a") 42 "Hello, world!"))]
               [:commit]]}
       nil]
      (process-changelog
        (mock-tx)
        :dataset
        (list
          "pdok-featured-changelog-v2"
          (transit/to-json {})
          (transit/to-json
            {:action "new"
             :collection "collection"
             :id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
             :version (uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")
             :attributes {:j 47 :complex {:i 42 :s "Hello, world!"}}})))))
  (is
    (=
      [{:done [
               [:insert
                :dataset
                :collection
                '(:_id :_version :j)
                `(
                   ("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" ~(uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a") 47))]
               [:insert
                :dataset
                :collection$list
                '(:_id :_version :idx :value)
                `(
                   ("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" ~(uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a") 0 "first")
                   ("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" ~(uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a") 1 "second"))]
               [:commit]]}
       nil]
      (process-changelog
        (mock-tx)
        :dataset
        (list
          "pdok-featured-changelog-v2"
          (transit/to-json {})
          (transit/to-json
            {:action "new"
             :collection "collection"
             :id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
             :version (uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")
             :attributes {:j 47 :list '({:idx 0 :value "first"} {:idx 1 :value "second"})}})))))
  (is
    (=
      [{:done [
               [:insert
                :dataset
                :collection
                '(:_id :_version :i)
                `(
                   ("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" ~(uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a") 42))]
               [:insert
                :dataset
                :collection$list
                '(:_id :_version :value)
                `(
                   ("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" ~(uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a") "first")
                   ("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" ~(uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a") "second"))]
               [:commit]]}
       nil]
      (process-changelog
        (mock-tx)
        :dataset
        (list
          "pdok-featured-changelog-v2"
          (transit/to-json {})
          (transit/to-json
            {:action "new"
             :collection "collection"
             :id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
             :version (uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")
             :attributes {:i 42 :list '("first" "second")}})))))
  (is
    (=
      [{:done [
               [:insert
                :dataset
                :collection
                '(:_id :_version :key)
                `(
                   ("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" ~(uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a") "value"))]
               [:commit]]}
       nil]
      (process-changelog
        (mock-tx)
        :dataset
        {} ; related-tables
        default-batch-size
        (list
          "pdok-featured-changelog-v2"
          (transit/to-json {})
          (transit/to-json
            {:action "new"
             :collection "collection"
             :id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
             :version (uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")
             :attributes {:key :value}})))))
  (is
    (=
      [{:done [
               [:insert
                :dataset
                :collection
                '(:_id :_version :i)
                `(
                   ("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" ~(uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a") 42))]
               [:insert
                :dataset
                :collection$related
                '(:_id :_version :value)
                `(
                   ("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" ~(uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a") "first")
                   ("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" ~(uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a") "second"))]
               [:delete
                :dataset
                :collection
                '(:_version)
                `(
                   (~(uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")))]
               [:delete
                :dataset
                :collection$related
                '(:_version)
                `(
                   (~(uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")))]
               [:commit]]}
       nil]
      (process-changelog
        (mock-tx)
        :dataset
        {:collection [:collection$related]} ; related-tables
        default-batch-size
        (list
          "pdok-featured-changelog-v2"
          (transit/to-json {})
          (transit/to-json
            {:action "new"
             :collection "collection"
             :id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
             :version (uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")
             :attributes {:i 42 :related ["first" "second"] }})
          (transit/to-json
            {:action "delete"
             :collection "collection"
             :id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
             :previous-version (uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")})))))
  (is
    (=
      [{:done [
               [:insert
                :dataset
                :alt-collection
                '(:_id :_version :i)
                `(
                   ("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" ~(uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a") 47))]
               [:delete
                :dataset
                :alt-collection
                '(:_version)
                `(
                   (~(uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")))]
               [:commit]]}
       nil]
      (process-changelog
        (mock-tx)
        :dataset
        {} ; related-tables
        {:collection ["collection"]} ; exclude-filter
        default-batch-size
        (list
          "pdok-featured-changelog-v2"
          (transit/to-json {})
          (transit/to-json
            {:action "new"
             :collection "collection"
             :id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
             :version (uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")
             :attributes {:i 42 :related ["first" "second"]}})
          (transit/to-json
            {:action "new"
             :collection "alt-collection"
             :id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
             :version (uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")
             :attributes {:i 47 }})
          (transit/to-json
            {:action "delete"
             :collection "collection"
             :id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
             :previous-version (uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")})
          (transit/to-json
            {:action "delete"
             :collection "alt-collection"
             :id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
             :previous-version (uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")}))))))
