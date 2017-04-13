(ns pdok.featured-to-geoserver.api-test
  (:require [schema.core :as s]
            [clojure.test :refer :all]
            [pdok.featured-to-geoserver.api :as api]))

(deftest test-check-request
  (is (not (s/check api/ProcessRequest {:file "http://example.com/file.changelog" 
                                        :dataset "test-dataset"})))
  (is (not (s/check api/ProcessRequest {:file "http://example.com/file.changelog" 
                                        :dataset "test-dataset"
                                        :format "plain"})))
  (is (not (s/check api/ProcessRequest {:file "http://example.com/file.changelog" 
                                        :dataset "test-dataset"
                                        :callback "http://example.com/callback"})))
  (is (not (s/check api/ProcessRequest {:file "http://example.com/file.changelog" 
                                        :dataset "test-dataset"
                                        :exclude-filter {}})))
  (is (not (s/check api/ProcessRequest {:file "http://example.com/file.changelog" 
                                        :dataset "test-dataset"
                                        :exclude-filter {:collection []}})))
  (is (not (s/check api/ProcessRequest {:file "http://example.com/file.changelog" 
                                        :dataset "test-dataset"
                                        :exclude-filter {:collection ["test-collection"]}})))
  (is (not (s/check api/ProcessRequest {:file "http://example.com/file.changelog" 
                                        :dataset "test-dataset"
                                        :exclude-filter {:collection ["test-collection-a" "test-collection-b"]}})))
  (is (not (s/check api/ProcessRequest {:file "http://example.com/file.changelog" 
                                        :dataset "test-dataset"
                                        :exclude-filter {:collection ["test-collection"] :action ["new"]}}))))
