(ns pdok.featured-to-geoserver.database-test
  (:require [clojure.test :refer :all]
            [pdok.featured-to-geoserver.database :as database]))

(defn buffering-with-mock-tx
  ([] (buffering-with-mock-tx 5))
  ([batch-size]
    (database/->DefaultBuffering
      (reify database/Transaction
        (batch-insert [this table columns values] [:insert table columns values])
        (commit [this] [:commit]))
      batch-size)))

(deftest test-buffering
  (is (= 
        '([:insert :table [:column] [["value"]]] [:commit]) 
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/append-record b :table {:column "value"})
              (database/commit-buffers b)))))
      "Should insert a single record and subsequently commit")
  (is (= 
        '([:insert :table [:column-a :column-b] [["value-a" nil] [nil "value-b"]]] [:commit]) 
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/append-record b :table {:column-a "value-a"})
              (database/append-record b :table {:column-b "value-b"})
              (database/commit-buffers b)))))
      "Should insert a two record in a single batch and subsequently commit")
  (is (= 
        '([:insert :table-a [:column] [["value"]]] [:insert :table-b [:column] [["value"]]] [:commit]) 
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/append-record b :table-a {:column "value"})
              (database/append-record b :table-b {:column "value"})
              (database/commit-buffers b)))))
      "Should insert a two records in separate batches and subsequently commit")
  (is (let [batch-size 5]
        (=
          `([:insert :table [:column] ~(->> ["value"] (repeat) (take batch-size) (into []))] [:commit])
          (database/process-buffer-operations
            (let [b (buffering-with-mock-tx batch-size)]
              (concat
                (->> (database/append-record b :table {:column "value"}) (repeat) (take batch-size))
                (list (database/commit-buffers b)))))))
      "Should insert a single batch of records and subsequently commit")
  (is (let [batch-size 2] 
        (=
          `([:insert :table [:column] ~(->> ["value"] (repeat) (take batch-size) (into []))] [:insert :table [:column] [["value"]]] [:commit])
          (database/process-buffer-operations
            (let [b (buffering-with-mock-tx batch-size)]
              (concat
                (->> (database/append-record b :table {:column "value"}) (repeat) (take (inc batch-size)))
                (list (database/commit-buffers b)))))))
      "Should insert two batches (one containing two records and one containing a single record) and subsequently commit"))
