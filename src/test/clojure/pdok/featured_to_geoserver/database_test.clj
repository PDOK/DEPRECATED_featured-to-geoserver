(ns pdok.featured-to-geoserver.database-test
  (:require [clojure.test :refer :all]
            [pdok.featured-to-geoserver.database :as database]))

(defn buffering-with-mock-tx
  ([] (buffering-with-mock-tx 5))
  ([batch-size]
    (database/->DefaultBuffering
      (reify database/Transaction
        (batch-insert [this schema table columns batch] [:insert schema table columns batch])
        (batch-delete [this schema table columns batch] [:delete schema table columns batch])
        (rollback [this error] [:rollback error])
        (commit [this] [:commit]))
      batch-size)))

(deftest test-buffering
  (is (=
        '([:delete :public :table [:id :version] [[42 0]]] [:commit])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/remove-record b :public :table {:version 0 :id 42})
              (database/finish b)))))
      "Should delete a single record")
  (is (=
        '([:delete :public :table [:id :version] [[42 0] [47 1]]] [:commit])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/remove-record b :public :table {:version 0 :id 42})
              (database/remove-record b :public :table {:id 47 :version 1})
              (database/finish b)))))
      "Should delete two records in a single batch")
  (is (=
        '([:delete :public :table [:id :version] [[42 0]]] [:delete :public :table [:id] [[47]]] [:commit])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/remove-record b :public :table {:version 0 :id 42})
              (database/remove-record b :public :table {:id 47})
              (database/finish b)))))
      "Should delete two records in two different batches")
  (is (=
        '([:rollback :problem])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/remove-record b :public :table {:version 0 :id 42})
              (database/remove-record b :public :table {:version 1 :id 42})
              (database/error b :problem)))))
      "Should result in a summary with just the error")
  (is (=
        '([:delete :public :table [:id :version] [[42 0] [42 1]]] [:delete :public :table [:id :version] [[42 2]]] [:commit])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx 2)]
            (list
              (database/remove-record b :public :table {:version 0 :id 42})
              (database/remove-record b :public :table {:version 1 :id 42}) ; last item in current batch for this table
              (database/remove-record b :public :table {:version 2 :id 42})
              (database/finish b)))))
      "Should delete three records in two different batches")
  (is (=
        '([:insert :public :table [:id :value :version] [[42 "Hello, world!" 0]]] [:delete :public :table [:id :version] [[42 0]]] [:commit])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/append-record b :public :table {:version 0 :id 42 :value "Hello, world!"})
              (database/remove-record b :public :table {:version 0 :id 42})
              (database/finish b)))))
      "Should insert and subsequently delete a single record")
  (is (=
        '(
           [:insert :public :table [:id :value :version] [[42 "Hello, world!" 0]]]
           [:delete :public :table [:id :version] [[42 0] [42 1]]]
           [:delete :public :table [:id :version] [[42 2]]]
           [:commit])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx 2)]
            (list
              (database/append-record b :public :table {:version 0 :id 42 :value "Hello, world!"})
              (database/remove-record b :public :table {:version 0 :id 42})
              (database/remove-record b :public :table {:version 1 :id 42}) ; last item in current batch for this table
              (database/remove-record b :public :table {:version 2 :id 42}) ; should result in a flush of append and remove buffers
              (database/finish b))))) ; should result in a flush of remove buffer
      "Should insert a single record and subsequently delete three records")
  (is (=
        '([:rollback :problem])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/append-record b :public :table {:column "first value"})
              (database/error b :problem) ; should be the last buffer operation processed
              (database/append-record b :public :table {:column "second value"})
              (database/finish b)))))
      "Should result in only a rollback")
  (is (=
        '([:insert :public :table [:column] [["value"]]] [:commit])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/append-record b :public :table {:column "value"})
              (database/finish b)))))
      "Should insert a single record and subsequently commit")
  (is (=
        '([:insert :public :table [:column-a :column-b] [["value-a" nil] [nil "value-b"]]] [:commit])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/append-record b :public :table {:column-a "value-a"})
              (database/append-record b :public :table {:column-b "value-b"})
              (database/finish b)))))
      "Should insert a two record in a single batch and subsequently commit")
  (is (=
        (list
          [:insert :public :table-b [:column] [["value-1"]
                                       ["value-2"]]]
          [:insert :public :table-a [:column] [["value"]]]
          [:insert :public :table-b [:column] [["value-3"]]]
          [:commit])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx 2)]
            (list
              (database/append-record b :public :table-a {:column "value"})
              (database/append-record b :public :table-b {:column "value-1"})
              (database/append-record b :public :table-b {:column "value-2"}) ; last item in current batch for this table
              (database/append-record b :public :table-b {:column "value-3"})
              (database/finish b)))))
      "Should insert a four record in a three separate batches and subsequently commit")
  (is (=
        '([:insert :public :table-a [:column] [["value"]]] [:insert :public :table-b [:column] [["value"]]] [:commit])
        (database/process-buffer-operations
          (let [b (buffering-with-mock-tx)]
            (list
              (database/append-record b :public :table-a {:column "value"})
              (database/append-record b :public :table-b {:column "value"})
              (database/finish b)))))
      "Should insert a two records in separate batches and subsequently commit")
  (is (let [batch-size 5]
        (=
          `([:insert :public :table [:column] ~(->> ["value"] (repeat) (take batch-size) (into []))] [:commit])
          (database/process-buffer-operations
            (let [b (buffering-with-mock-tx batch-size)]
              (concat
                (->> (database/append-record b :public :table {:column "value"}) (repeat) (take batch-size))
                (list (database/finish b)))))))
      "Should insert a single batch of records and subsequently commit")
  (is (let [batch-size 2]
        (=
          `([:insert :public :table [:column] ~(->> ["value"] (repeat) (take batch-size) (into []))] [:insert :public :table [:column] [["value"]]] [:commit])
          (database/process-buffer-operations
            (let [b (buffering-with-mock-tx batch-size)]
              (concat
                (->> (database/append-record b :public :table {:column "value"}) (repeat) (take (inc batch-size)))
                (list (database/finish b)))))))
      "Should insert two batches (one containing two records and one containing a single record) and subsequently commit"))

(deftest test-generate-tx-summary
  (is (= {:insert 0, :delete 0} (database/generate-tx-summary)) "Should result in all zero values")
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
      {:insert 5 :delete 0}
      (reduce
        database/generate-tx-summary
        (database/generate-tx-summary)
        (list
          {:insert 3}
          {:insert 2})))
    "Should result in a summary with the correct summerized values with zero as default value")
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

(defn- mock-result-set [rows]
  (let [cur (atom -1)]
    (reify java.sql.ResultSet
      (close [this])
      (next [this]
        (do
          (swap! cur inc)
          (< @cur (count rows))))
      (getObject [this ^int columnIndex]
        (let [v (-> rows
                  (nth @cur)
                  (nth (dec columnIndex)))]
          (if (vector? v)
            (reify java.sql.Array
              (getArray [this] v))
            v))))))

(deftest test-result-seq
  (is
    (=
      []
      (database/result-seq
        (mock-result-set [])
        :a :b)))
  (is
    (=
      [{:a 1 :b 2}]
      (database/result-seq
        (mock-result-set [[1 2]])
        :a :b)))
  (is
    (=
      [{:a [1 2]}]
      (database/result-seq
        (mock-result-set [[[1 2]]])
        :a)))
  (is
    (=
      [{:a 1 :b 2} {:a 3 :b 4}]
      (database/result-seq
        (mock-result-set [[1 2] [3 4]])
        :a :b))))

(defn- mock-query-result [rows]
  (database/fetch-related-tables
    (reify java.sql.Connection
      (prepareStatement [this query]
        (reify java.sql.PreparedStatement
          (setString [this idx value])
          (close [this])
          (^java.sql.ResultSet executeQuery [this]
            (mock-result-set rows)))))
    :dataset))

(deftest test-fetch-related-tables
  (is
    (=
      {}
      (mock-query-result [])))
  (is
    (=
      {:table-a [:table-a$first-related :table-a$second-related] :table-b [:table-b$related]}
      (mock-query-result [
                          ["table-a" ["table-a$first-related" "table-a$second-related"]]
                          ["table-b" ["table-b$related"]]])))
  (is
    (=
      {:table-a [:table-a$first-related :table-a$second-related]
       :table-b [:table-b$related]}
      (mock-query-result [
                          ["table-a" ["table-a$first-related" "table-a$second-related"]]
                          ["table-b" ["table-b$related"]]]))))

(deftest test-sql-identifier
  (is (= "\"table-name\"" (database/sql-identifier :table-name))))
