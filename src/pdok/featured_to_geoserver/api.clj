(ns pdok.featured-to-geoserver.api
   (:require
     [ring.middleware.json :refer :all]
     [ring.util.response :as r]
     [clj-time [local :as tl]]
     [compojure.core :refer :all]
     [compojure.route :as route]))

(extend-protocol cheshire.generate/JSONable
  org.joda.time.DateTime
  (to-json [t ^com.fasterxml.jackson.core.JsonGenerator jg] (.writeString jg (str t))))

(defroutes api-routes
  (context "/api" []
           (GET "/ping" [] (r/response {:pong (tl/local-now)})))
  (route/not-found "NOT FOUND"))

(def app
  (-> api-routes
    (wrap-json-response)))