(defproject featured-to-geoserver "0.1.0-SNAPSHOT"
  :description "Open-source GeoServer loading software for Featured"
  :url "https://github.com/PDOK/featured-to-geoserver"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [nl.pdok/featured-shared "1.1"]
                 [ring/ring-defaults "0.2.3"]]
  :target-path "target/%s"
  :plugins [[lein-ring/lein-ring "0.11.0"]]
  :ring {:port 5000
         :handler pdok.featured-to-geoserver.api/app}
  :profiles {:uberjar {:aot :all}})
