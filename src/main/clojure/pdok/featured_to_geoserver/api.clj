(ns pdok.featured-to-geoserver.api
   (:require
     [schema.core :as s]
     [clojure.tools.logging :as log]
     [clojure.core.async :as async]
     [ring.middleware.json :refer :all]
     [ring.util.response :as r]
     [clj-time [local :as tl]]
     [clojure.java.io :as io]
     [compojure.core :refer :all]
     [compojure.route :as route]
     [org.httpkit.client :as http]
     [cheshire.core :as json]
     [pdok.featured-to-geoserver.util :refer :all]
     [pdok.featured-to-geoserver.result :refer :all]
     [pdok.featured-to-geoserver.database :as database]
     [pdok.featured-to-geoserver.changelog :as changelog]
     [pdok.featured-to-geoserver.processor :as processor]))

(extend-protocol cheshire.generate/JSONable
  org.joda.time.DateTime
  (to-json [t ^com.fasterxml.jackson.core.JsonGenerator jg] (.writeString jg (str t)))
  schema.utils.ValidationError
  (to-json [t ^com.fasterxml.jackson.core.JsonGenerator jg] (.writeString jg (pr-str t))))

(defn- uri [str]
  (try
    (let [uri (java.net.URI. str)]
      uri)
    (catch java.net.URISyntaxException e nil)))

(def ^:private URI (s/pred uri 'URI ))

(def ^:private ProcessRequest
  "A schema for a JSON process request"
  {:file URI
   (s/optional-key :format) (s/enum "plain" "zip")
   (s/optional-key :callback) URI})

(def ^:private workers 5)

(def ^:private process-channel (async/chan (* workers 4)))

(def ^:private terminate-channel (async/chan workers))

(defn- process [http-req]
  (let [request (:body http-req)]
    (if (map? request)
      (if-let [invalid (s/check ProcessRequest request)]
        (r/status (r/response {:request request :result :invalid-request :details invalid}) 400)
        (if (async/offer! process-channel request)
          (r/response {:request request :result :ok})
          (r/status (r/response {:request request :result :error :details "queue full"}) 429)))
      (r/status (r/response {:result :invalid-request}) 400))))

(defroutes api-routes
  (context "/api" []
           (GET "/ping" [] (r/response {:pong (tl/local-now)}))
           (POST "/process" [] process))
  (route/not-found "NOT FOUND"))

(defn- no-close [is]
  (proxy [java.io.FilterInputStream] [is]
    (close [])))

(defn- single-zip-entry [file]
  (let [^java.util.zip.ZipInputStream zip (-> file (io/input-stream) (java.util.zip.ZipInputStream.))]
    (assert (.getNextEntry zip) "Zip is empty")
    (proxy [java.io.BufferedReader] [(io/reader (no-close zip))]
      (close []
        (assert (not (.getNextEntry zip)) "More than one entry in zip")
        (.close zip)))))

(defn- read-file [file format]
  (if (= "plain" format)
    (io/reader file)
    (single-zip-entry file)))

(defn- execute-process-request [^java.sql.Connection c request]
  (async/go
    (try
      (with-open [rdr (read-file (:file request) (:format request))]
        (let [[value error] 
              (->> rdr 
                (line-seq) 
                (changelog/read-changelog)
                (map-result 
                  #(processor/process 
                     (database/->DefaultTransaction c)
                     (database/fetch-related-tables c)
                     100
                     %))
                (unwrap-result))]
          (cond 
            error error
            value (async/<! value))))
      (catch Throwable t
        (log/error t "Couldn't execute request")
        {:failure {:exceptions (list (exception-to-string t))}}))))

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

(defn destroy! []
  (log/info "Terminating workers")
  (async/close! process-channel)
  (doseq [_ (range workers)]
    (log/info (str "Worker " (async/<!! terminate-channel) " terminated")))
  (log/info "Application terminated"))

(defn init! []
  (doseq [worker (range workers)]
    (async/go
      (try
        (with-open [^java.sql.Connection c (database/connect 
                        {:dbtype "postgresql"
                         :dbname "pdok" 
                         :host "192.168.99.100"
                         :user "postgres"
                         :password "postgres"
                         :application-name (str "Featured to GeoServer - worker " worker)})]
          (loop []            
            (when-let [request (async/<! process-channel)]
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
                (recur)))))
        (catch Throwable t
          (log/error t "Couldn't initialize worker")))
      (async/>! terminate-channel worker)))
  (log/info "Workers initialized"))

(def app
  (-> api-routes
    (wrap-json-body {:keywords? true})
    (wrap-json-response)))