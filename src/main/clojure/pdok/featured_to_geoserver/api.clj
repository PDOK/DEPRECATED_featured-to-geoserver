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
     [schema.core :as s]
     [pdok.featured-to-geoserver.util :refer :all]
     [pdok.featured-to-geoserver.result :refer :all]
     [pdok.featured-to-geoserver.core :as core]
     [pdok.featured-to-geoserver.config :as config]
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

(def ProcessRequest
  "A schema for a JSON process request"
  {:file URI
   :dataset s/Str
   (s/optional-key :exclude-filter) {(s/pred keyword?) [s/Str]}
   (s/optional-key :format) (s/enum "plain" "zip")
   (s/optional-key :callback) URI})

(def ^:private process-channel (async/chan (config/queue-length)))

(def ^:private terminate-channel (async/chan (config/n-workers)))

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
           (GET "/info" [] (r/response {:version (implementation-version)}))
           (GET "/ping" [] (r/response {:pong (tl/local-now)}))
           (POST "/ping" [] (fn [r] (log/info "!ping pong!" (:body r)) (r/response {:pong (tl/local-now)})))
           (POST "/process" [] process))
  (route/not-found "NOT FOUND"))

(defn destroy! []
  (log/info "Terminating workers")
  (async/close! process-channel)
  (doseq [_ (range (config/n-workers))]
    (log/info (str "Worker " (async/<!! terminate-channel) " terminated")))
  (log/info "Application terminated"))

(defn init! []
  (core/create-workers
    (config/n-workers)
    (config/db)
    process-channel
    terminate-channel))

(defn wrap-exception-handling
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception e
        (log/error e)
        {:status 400 :body (.getMessage e)}))))

(def app
  (-> api-routes
    (wrap-json-body {:keywords? true})
    (wrap-json-response)
    (wrap-exception-handling)))
