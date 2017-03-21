(ns pdok.featured-to-geoserver.processor-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [pdok.featured-to-geoserver.result :refer :all]
            [clojure.core.async :as async]
            [pdok.transit :as transit]
            [pdok.featured-to-geoserver.database :as database]
            [pdok.featured-to-geoserver.changelog :as changelog]
            [pdok.featured-to-geoserver.processor :as processor]))

(defn mock-tx-reducer
  ([] [])
  ([a b] (conj a b)))

(defn mock-tx []
  (reify database/Transaction
    (batch-insert [this table columns values] (fn [] [:insert table columns values]))
    (rollback [this error] (fn [] [:rollback error]))
    (commit [this] (fn [] [:commit]))
    (reducer [this] mock-tx-reducer)))
      
(defn process-changelog [tx & content]
  (->> (changelog/read-changelog content)
    (map-result #(async/<!! (processor/process tx %)))
    (unwrap-result)))

(deftest test-process
  (is
    (=
      [nil {:error :unsupported-version, :error-details {:line 1}}]
      (process-changelog (mock-tx) "v2")))
  (is
    (-> (process-changelog 
         (reify database/Transaction 
           ; implements just enough to start the processor
           ; and crashes while generating tx operations
           (reducer [this] mock-tx-reducer))
         "v1"
         "schema-name,object-type")
      first
      :failure))
  (is
    (-> (process-changelog 
         (reify database/Transaction 
           ; implements just enough to start the processor
           ; and generate tx operations, crashing when 
           ; those operations are actually being executed
           (batch-insert [this table columns values] (fn [] (assert false)))
           (rollback [this error] (fn [] (assert false)))
           (commit [this] (fn [] (assert false)))
           (reducer [this] mock-tx-reducer))
         "v1"
         "schema-name,object-type")
      first
      :failure))
  (is
    (=
      [{:done [
               [:insert 
                :object-type 
                '(:_id :_version :i) 
                '(("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" "115ba9a3-275f-4022-944a-dcacdc71ff6a" 42))]
               [:commit]]} 
       nil]
      (process-changelog
        (mock-tx)
        "v1" 
        "schema-name,object-type"
        (str "new,b5ab7b8a-7474-49b7-87ea-44bd2fea13e8,115ba9a3-275f-4022-944a-dcacdc71ff6a," (transit/to-json {:i 42})))))
  (is
    (=
      [{:done [
               [:insert 
                :object-type 
                '(:_id :_version :j)
                '(("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" "115ba9a3-275f-4022-944a-dcacdc71ff6a" 47))]
               [:insert 
                :object-type$complex
                '(:_id :_version :i :s) 
                '(("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" "115ba9a3-275f-4022-944a-dcacdc71ff6a" 42 "Hello, world!"))]
               [:commit]]}
       nil]
      (process-changelog
        (mock-tx)
        "v1" 
        "schema-name,object-type"
        (str 
          "new,b5ab7b8a-7474-49b7-87ea-44bd2fea13e8,115ba9a3-275f-4022-944a-dcacdc71ff6a,"
          (transit/to-json {:j 47 :complex {:i 42 :s "Hello, world!"}})))))
  (is
    (=
      [{:done [
               [:insert 
                :object-type 
                '(:_id :_version :j)
                '(("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" "115ba9a3-275f-4022-944a-dcacdc71ff6a" 47))]
               [:insert
                :object-type$list
                '(:_id :_version :idx :value)
                '(("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" "115ba9a3-275f-4022-944a-dcacdc71ff6a" 0 "first")
                   ("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" "115ba9a3-275f-4022-944a-dcacdc71ff6a" 1 "second"))]
               [:commit]]}
       nil]
      (process-changelog
        (mock-tx)
        "v1" 
        "schema-name,object-type"
        (str 
          "new,b5ab7b8a-7474-49b7-87ea-44bd2fea13e8,115ba9a3-275f-4022-944a-dcacdc71ff6a,"
          (transit/to-json {:j 47 :list '({:idx 0 :value "first"} {:idx 1 :value "second"})})))))
  (is
    (=
      [{:done [
               [:insert 
                :object-type 
                '(:_id :_version :i)
                '(("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" "115ba9a3-275f-4022-944a-dcacdc71ff6a" 42))]
               [:insert
                :object-type$list
                '(:_id :_version :value)
                '(("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" "115ba9a3-275f-4022-944a-dcacdc71ff6a" "first")
                   ("b5ab7b8a-7474-49b7-87ea-44bd2fea13e8" "115ba9a3-275f-4022-944a-dcacdc71ff6a" "second"))]
               [:commit]]}
       nil]
      (process-changelog
        (mock-tx)
        "v1" 
        "schema-name,object-type"
        (str 
          "new,b5ab7b8a-7474-49b7-87ea-44bd2fea13e8,115ba9a3-275f-4022-944a-dcacdc71ff6a,"
          (transit/to-json {:i 42 :list '("first" "second")}))))))
