(ns pdok.featured-to-geoserver.database-test
  (:require [clojure.test :refer :all]
            [pdok.featured-to-geoserver.database :as database]))

(defn buffering-with-mock-tx
  ([] (buffering-with-mock-tx 5))
  ([batch-size]
    (database/->DefaultBuffering
      (reify database/Transaction
        (batch-insert [this table columns values] [:insert table columns values])
        (batch-delete [this table columns values] [:delete table columns values])
        (rollback [this error] [:rollback error])
        (commit [this] [:commit]))
      batch-size)))

(deftest test-buffering
  (is (=
        '([:delete :table [:id :version] [[42 0]]] [:commit])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/remove-record b :table {:version 0 :id 42})
              (database/finish b)))))
      "Should delete a single record")
  (is (=
        '([:delete :table [:id :version] [[42 0] [47 1]]] [:commit])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/remove-record b :table {:version 0 :id 42})
              (database/remove-record b :table {:id 47 :version 1})
              (database/finish b)))))
      "Should delete two records in a single batch")
  (is (=
        '([:delete :table [:id :version] [[42 0]]] [:delete :table [:id] [[47]]] [:commit])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/remove-record b :table {:version 0 :id 42})
              (database/remove-record b :table {:id 47})
              (database/finish b)))))
      "Should delete two records in two different batches")
  (is (=
        '([:rollback :problem])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/remove-record b :table {:version 0 :id 42})
              (database/remove-record b :table {:version 1 :id 42})
              (database/error b :problem)))))
      "Should result in a summary with just the error")
  (is (=
        '([:delete :table [:id :version] [[42 0] [42 1]]] [:delete :table [:id :version] [[42 2]]] [:commit])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx 2)]
            (list
              (database/remove-record b :table {:version 0 :id 42})
              (database/remove-record b :table {:version 1 :id 42}) ; last item in current batch for this table
              (database/remove-record b :table {:version 2 :id 42})
              (database/finish b)))))
      "Should delete three records in two different batches")
  (is (=
        '([:insert :table [:id :value :version] [[42 "Hello, world!" 0]]] [:delete :table [:id :version] [[42 0]]] [:commit])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/append-record b :table {:version 0 :id 42 :value "Hello, world!"})
              (database/remove-record b :table {:version 0 :id 42})
              (database/finish b)))))
      "Should insert and subsequently delete a single record")
  (is (=
        '(
           [:insert :table [:id :value :version] [[42 "Hello, world!" 0]]] 
           [:delete :table [:id :version] [[42 0] [42 1]]] 
           [:delete :table [:id :version] [[42 2]]]
           [:commit])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx 2)]
            (list
              (database/append-record b :table {:version 0 :id 42 :value "Hello, world!"})
              (database/remove-record b :table {:version 0 :id 42})
              (database/remove-record b :table {:version 1 :id 42}) ; last item in current batch for this table
              (database/remove-record b :table {:version 2 :id 42}) ; should result in a flush of append and remove buffers
              (database/finish b))))) ; should result in a flush of remove buffer
      "Should insert a single record and subsequently delete three records")
  (is (=
        '([:rollback :problem])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/append-record b :table {:column "first value"})
              (database/error b :problem) ; should be the last buffer operation processed
              (database/append-record b :table {:column "second value"})
              (database/finish b)))))
      "Should result in only a rollback")
  (is (= 
        '([:insert :table [:column] [["value"]]] [:commit]) 
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/append-record b :table {:column "value"})
              (database/finish b)))))
      "Should insert a single record and subsequently commit")
  (is (= 
        '([:insert :table [:column-a :column-b] [["value-a" nil] [nil "value-b"]]] [:commit]) 
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/append-record b :table {:column-a "value-a"})
              (database/append-record b :table {:column-b "value-b"})
              (database/finish b)))))
      "Should insert a two record in a single batch and subsequently commit")
  (is (= 
        (list
          [:insert :table-b [:column] [["value-1"]
                                       ["value-2"]]]
          [:insert :table-a [:column] [["value"]]]
          [:insert :table-b [:column] [["value-3"]]]
          [:commit]) 
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx 2)]
            (list
              (database/append-record b :table-a {:column "value"})
              (database/append-record b :table-b {:column "value-1"})
              (database/append-record b :table-b {:column "value-2"}) ; last item in current batch for this table
              (database/append-record b :table-b {:column "value-3"})
              (database/finish b)))))
      "Should insert a four record in a three separate batches and subsequently commit")
  (is (= 
        '([:insert :table-a [:column] [["value"]]] [:insert :table-b [:column] [["value"]]] [:commit]) 
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/append-record b :table-a {:column "value"})
              (database/append-record b :table-b {:column "value"})
              (database/finish b)))))
      "Should insert a two records in separate batches and subsequently commit")
  (is (let [batch-size 5]
        (=
          `([:insert :table [:column] ~(->> ["value"] (repeat) (take batch-size) (into []))] [:commit])
          (database/process-buffer-operations
            (let [b (buffering-with-mock-tx batch-size)]
              (concat
                (->> (database/append-record b :table {:column "value"}) (repeat) (take batch-size))
                (list (database/finish b)))))))
      "Should insert a single batch of records and subsequently commit")
  (is (let [batch-size 2] 
        (=
          `([:insert :table [:column] ~(->> ["value"] (repeat) (take batch-size) (into []))] [:insert :table [:column] [["value"]]] [:commit])
          (database/process-buffer-operations
            (let [b (buffering-with-mock-tx batch-size)]
              (concat
                (->> (database/append-record b :table {:column "value"}) (repeat) (take (inc batch-size)))
                (list (database/finish b)))))))
      "Should insert two batches (one containing two records and one containing a single record) and subsequently commit"))

(deftest test-generate-tx-summary
  (is (= {} (database/generate-tx-summary)) "Should result in empty initial summary")
  (is 
    (= 
      {:insert 5 :delete 1}
      (reduce 
        database/generate-tx-summary 
        (database/generate-tx-summary) 
        (list
          {:insert 3}
          {:insert 2}
          {:delete 1})))
    "Should result in a summary with the correct summerized values")
  (is 
    (= 
      {:insert 5 :delete 1}
      (reduce 
        database/generate-tx-summary 
        (database/generate-tx-summary) 
        (list
          {:insert 3}
          {:insert 2}
          {:delete 1})))
    "Should result in a summary with the correct summerized values")
  (is
    (= 
      {:error :problem :error-detail {:detail 42}}
      (reduce 
        database/generate-tx-summary 
        (database/generate-tx-summary) 
        (list
          {:insert 3}
          {:insert 2}
          {:delete 1}
          {:error {:error :problem :error-detail {:detail 42}}})))
    "Should result in a summary with ony the error"))
