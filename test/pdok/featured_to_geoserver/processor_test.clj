(ns pdok.featured-to-geoserver.processor-test
  (:require [clojure.test :refer :all]
            [pdok.featured-to-geoserver.result :refer :all]
            [clojure.core.async :as async]
            [pdok.featured-to-geoserver.database :as database]
            [pdok.featured-to-geoserver.changelog :as changelog]
            [pdok.featured-to-geoserver.processor :as processor]))

(defn mock-tx []
  (reify database/Transaction
    (batch-insert [this table columns values] (fn [] [:insert table columns values]))
    (rollback [this error] (fn [] [:rollback error]))
    (commit [this] (fn [] [:commit]))
    (reducer [this]
      (fn
        ([] [])
        ([a b] (conj a b))))))

(defn process-changelog [& content]
  (->> (changelog/read-changelog content)
        (map-result #(async/<!! (processor/process (mock-tx) %)))
        (unwrap-result)))

(deftest test-process
  (is
    (=
      [nil {:error :unsupported-version, :error-details {:line 1}}]
      (process-changelog "v2")))
  (is
    (=
      [{:done [
               [:insert 
                :object-type 
                [:_id :_version :i] 
                ['("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" "115ba9a3-275f-4022-944a-dcacdc71ff6a" 42)]]
               [:commit]]} 
       nil]
      (process-changelog
        "v1" 
        "schema-name,object-type"
        "new,b5ab7b8a-7474-49b7-87ea-44bd2fea13e8,115ba9a3-275f-4022-944a-dcacdc71ff6a,[\"^ \",\"~:i\",42]"))))
