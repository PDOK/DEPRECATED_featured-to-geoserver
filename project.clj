(defproject featured-to-geoserver "0.1.0-SNAPSHOT"
  :description "Open-source GeoServer loading software for Featured"
  :url "https://github.com/PDOK/featured-to-geoserver"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.3.426"]
                 [nl.pdok/featured-shared "1.1"]
                 [cheshire/cheshire "5.7.0"]
                 [ring/ring-defaults "0.2.3"]
                 [ring/ring-json "0.4.0"]
                 [org.postgresql/postgresql "9.4.1212.jre7"]
                 [compojure/compojure "1.5.2"]
                 [clj-time/clj-time "0.13.0"]]
  :target-path "target/%s"
  :plugins [[lein-ring/lein-ring "0.11.0"]
            [lein-cloverage "1.0.9"]]
  :ring {:port 5000
         :handler pdok.featured-to-geoserver.api/app}
  :profiles {:uberjar {:aot :all}})
