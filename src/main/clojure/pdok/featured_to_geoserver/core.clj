(ns pdok.featured-to-geoserver.core
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.tools.cli :as cli]
            [clojure.java.io :as io]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [pdok.featured-to-geoserver.util :refer :all]
            [pdok.featured-to-geoserver.result :refer :all]
            [pdok.featured-to-geoserver.config :as config]
            [pdok.featured-to-geoserver.database :as database]
            [pdok.featured-to-geoserver.changelog :as changelog]
            [pdok.featured-to-geoserver.processor :as processor]))

(defn- execute-callback [url body]
  (try
    (http/post
      url
      {:body (json/generate-string body)
       :headers {"Content-Type" "application/json"}}
      (fn [{status :status}]
        (if (= status 200)
          (log/info (str "Callback succeeded, url: " url))
          (log/error (str "Callback failed, url: " url " http-status: " status)))))
    (catch Throwable t
      (log/error t (str "Callback error, url: " url)))))

(defn- no-close [is]
  (proxy [java.io.FilterInputStream] [is]
    (close [])))

(defn- single-zip-entry [file]
  (let [^java.util.zip.ZipInputStream zip (-> file (io/input-stream) (java.util.zip.ZipInputStream.))]
    (when (not (.getNextEntry zip))
      (throw (IllegalStateException. "Zip is empty")))
    (proxy [java.io.BufferedReader] [(io/reader (no-close zip))]
      (close []
        (when (.getNextEntry zip)
          (throw (IllegalStateException. "More than one entry in zip")))
        (.close zip)))))

(defn- read-file [file format]
  (if (= "plain" format)
    (io/reader file)
    (single-zip-entry file)))

(def ^:private batch-size 5000)

(defn- execute-process-request [^java.sql.Connection c request]
  (async/go
    (try
      (with-open [rdr (read-file (:file request) (:format request))]
        (let [dataset (-> request :dataset keyword)
              [value error] (->> rdr
                              (line-seq)
                              (changelog/read-changelog)
                              (map-result
                                #(processor/process
                                   (database/->DefaultTransaction c)
                                   (database/fetch-related-tables c dataset)
                                   (or (:exclude-filter request) {})
                                   batch-size
                                   dataset
                                   %))
                              (unwrap-result))]
          (cond
            error error
            value (async/<! value))))
      (catch Throwable t
        (log/error t "Couldn't execute request")
        {:failure {:exceptions (list (exception-to-string t))}}))))

(defn create-workers
  [n-workers db process-channel terminate-channel]
  (doseq [worker (range n-workers)]
    (async/go
      (try
        (with-open [^java.sql.Connection c (database/connect
                                             db
                                             (str "Featured to GeoServer - worker " worker))]
          (log/info "Worker" worker "initialized")
          (loop []
            (when-let [request (async/<! process-channel)]
                (if (.isValid c 5)
                  (do
                    (log/info (str "Changelog processing started " request))
                    (let [result (async/<! (execute-process-request c request))
                          response {:result result :request request :worker worker}]
                      (log/info (str "Changelog processed " response))
                      (when (:failure result)
                        (do
                          (log/error "Failure while processing request -> perform rollback")
                          (.rollback c)))
                      (when-let [callback (:callback request)]
                        (execute-callback callback response)))
                    (recur))
                  (do
                    (let [msg "Database connection is no longer valid"]
                      (log/error msg)
                      (when-let [callback (:callback request)]
                        (execute-callback callback
                          {:result {:failure {:exceptions (list msg)}}
                           :request request
                           :worker worker})))
                    (async/>! terminate-channel worker)
                    (System/exit 1))))))
        (catch Throwable t
          (log/error t "Couldn't initialize worker")))
      (async/>! terminate-channel worker))))
