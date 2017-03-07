(ns pdok.featured-to-geoserver.changelog-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [pdok.featured-to-geoserver.changelog :as changelog]))

(deftest test-index-of-seq
  (is (empty? (changelog/index-of-seq "" ",")) "Should be empty for an empty input string")
  (is (empty? (changelog/index-of-seq "no-separator" ",")) "Should be empty when separator is not present in string")
  (is (= (list 0) (changelog/index-of-seq "," ",")) "A string with just the separator should result in (list 0)")
  (is (= (list 0 6 7) (changelog/index-of-seq ",first,,second" ",")) "Empty colums should be handled correctly")
  (is (= (list 5) (changelog/index-of-seq "first,second" ",")) "Should contain a single index for a single occurrence of the separator")
  (is (= (list 5 12 18) (changelog/index-of-seq "first,second,third,fourth" ",")) "Should contain multiple indexes for multiple occurrences of the separator")
  (is (= (list 5 13) (changelog/index-of-seq "first,,second,,third" ",,")) "Multicharacter separator should also work as expected"))

(deftest test-read-action
  (is (thrown? AssertionError (changelog/read-action "")) "Should not accept an empty string")
  (is (thrown? AssertionError (changelog/read-action ",c3c8dacd-a156-435f-a25c-ad583b561f91,4f523aa2-50ac-4194-bdfd-1d02d617a073,[\"^ \"]")) "Should not accept an empty action column")
  (is (thrown? AssertionError (changelog/read-action "new,,4f523aa2-50ac-4194-bdfd-1d02d617a073,[\"^ \"]")) "Should not accept an empty id column")
  (is (thrown? AssertionError (changelog/read-action "new,c3c8dacd-a156-435f-a25c-ad583b561f91,,[\"^ \"]")) "Should not accept an empty version column")
  (is (thrown? AssertionError (changelog/read-action "new,c3c8dacd-a156-435f-a25c-ad583b561f91,4f523aa2-50ac-4194-bdfd-1d02d617a073,")) "Should not accept an empty attributes column")
  (is (thrown? AssertionError (changelog/read-action "new")) "Should not accept missing id, version and attributes columns")
  (is (thrown? AssertionError (changelog/read-action "new,c3c8dacd-a156-435f-a25c-ad583b561f91")) "Should not accept missing version and attributes columns")
  (is (thrown? AssertionError (changelog/read-action "new,c3c8dacd-a156-435f-a25c-ad583b561f91,4f523aa2-50ac-4194-bdfd-1d02d617a073")) "Should not accept missing attributes columns")
  (is 
    (= 
      {:action :new :id "c3c8dacd-a156-435f-a25c-ad583b561f91" :version "4f523aa2-50ac-4194-bdfd-1d02d617a073" :attr {}} 
      (changelog/read-action "new,c3c8dacd-a156-435f-a25c-ad583b561f91,4f523aa2-50ac-4194-bdfd-1d02d617a073,[\"^ \"]")) 
    "Should result in a :new action with the correct id and version")
  (is 
    (= 
      {:str "Hello, world!" :int 42} 
      (-> "new,c3c8dacd-a156-435f-a25c-ad583b561f91,4f523aa2-50ac-4194-bdfd-1d02d617a073,[\"^ \",\"~:str\",\"Hello, world!\",\"~:int\",42]"
        changelog/read-action 
        :attr))
    "Should result in an action with the correct attributes attached"))

(defn string-reader [str]
  (java.io.BufferedReader.
    (java.io.StringReader. str)))

(defn generate-actions [action-count]
  (apply str
         (map
           (fn [_] (str/join "," (list "new" (java.util.UUID/randomUUID) (java.util.UUID/randomUUID) "[\"^ \"]\n"))) 
           (range action-count))))

(defn parsed-changelist [dataset feature-type actions]
  (->> (str "v1\n" dataset "," feature-type "\n" actions) (string-reader) (changelog/from-buffered-reader)))

(deftest test-from-buffered-reader
  (is (thrown? AssertionError (changelog/from-buffered-reader (string-reader "v2"))) "Should not accept an unsupported changelog version")
  (is (thrown? AssertionError (changelog/from-buffered-reader (string-reader "v1\n\n"))) "Should not accept missing dataset and feature-type")
  (is (thrown? AssertionError (changelog/from-buffered-reader (string-reader "v1\ndataset"))) "Should not accept missing feature-type")
  (is (thrown? AssertionError (changelog/from-buffered-reader (string-reader "v1\ndataset,"))) "Should not accept empty feature-type")
  (is (thrown? AssertionError (changelog/from-buffered-reader (string-reader "v1\n,feature-type"))) "Should not accept empty dataset")
  (is (thrown? AssertionError (changelog/from-buffered-reader (string-reader "v1\n,"))) "Should not accept empty dataset and feature-type")
  (is 
    (= {:dataset :dataset
        :feature-type :feature-type
        :actions '()} (changelog/from-buffered-reader (string-reader "v1\ndataset,feature-type"))) 
    "Should result in an empty changelog")
  (is 
    (= {:dataset :dataset 
        :feature-type :feature-type 
        :actions '({:action :new 
                    :id "c3c8dacd-a156-435f-a25c-ad583b561f91" 
                    :version "4f523aa2-50ac-4194-bdfd-1d02d617a073" 
                    :attr {:str "Hello, world!" :int 42}})} 
       (changelog/from-buffered-reader (string-reader "v1\ndataset,feature-type\nnew,c3c8dacd-a156-435f-a25c-ad583b561f91,4f523aa2-50ac-4194-bdfd-1d02d617a073,[\"^ \",\"~:str\",\"Hello, world!\",\"~:int\",42]")))
    "Should result in a correctly parsed changelog")
  (is (= 100 (-> (parsed-changelist "dataset" "feature-type" (generate-actions 100)) (:actions) (count))) "Should be able to parse multiple lines")
  (is
    (thrown-with-msg? AssertionError #"42" (-> (parsed-changelist "dataset" "feature-type" (str (generate-actions 39) "invalid-line")) (:actions) (count)))
    "Assert message should contain changelog line number"))
