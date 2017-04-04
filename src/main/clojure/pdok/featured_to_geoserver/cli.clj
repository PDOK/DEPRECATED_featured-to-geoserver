(ns pdok.featured-to-geoserver.cli
  (:require [clojure.core.async :as async] 
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.tools.cli :as cli]
            [pdok.featured-to-geoserver.core :as core]))

(def cli-options 
  [["-f" "--format  FORMAT" "File format (zip or plain)"
    :default "zip"
    :validate [#(some (partial = %) ["zip" "plain"]) "File format must be zip or plain"]]
   [nil "--workers WORKERS" "Number of workers"
    :default 5
    :parse-fn #(Integer/parseInt %)
    :validate [#(> % 0) "At least one worker required"]]
   [nil "--host HOST" "Database host"
    :default "localhost"]
   [nil "--port PORT" "Database port"
    :default 5432
    :parse-fn #(Integer/parseInt %)
    :validate [#(> % 0) "Port number should be positive"]]
   [nil "--user USER" "Database user"
    :default "postgres"]
   [nil "--password PASSWORD" "Database password"
    :default "postgres"]
   [nil "--database DATABASE" "Database name"
    :default "pdok"]])

(defn log-usage [summary]
  (log/info "")
  (log/info "Usage: lein run [options] files")
  (log/info "")
  (log/info "Options")
  (doseq [option (str/split summary #"\n")]
    (log/info option))
  (log/info ""))

(defn -main [& args]
  (log/info "This is the featured-to-geoserver CLI")
  (let [{files :arguments summary :summary options :options errors :errors} (cli/parse-opts args cli-options)
        {format :format n-workers :workers host :host  port :port user :user password :password database :database} options]
    (cond
      errors (do (log-usage summary) (doseq [error errors] (log/error error)))
      (empty? files) (log-usage summary)
      :else (let [n-workers (min (count files) n-workers)
                  db {:url (str "//" host ":" port "/" database)
                      :user user
                      :password password}
                  process-channel (async/chan 1)
                  terminate-channel (async/chan n-workers)]
              (core/create-workers n-workers db process-channel terminate-channel)
              (doseq [file files]
                (log/info "Processing file:" file)
                (async/>!! process-channel {:file file :format format}))
              (async/close! process-channel)
              (doseq [_ (range n-workers)]
                (log/info (str "Worker " (async/<!! terminate-channel) " terminated")))
              (log/info "Application terminated")))))
