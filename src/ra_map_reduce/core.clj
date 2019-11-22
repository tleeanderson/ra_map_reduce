(ns ra-map-reduce.core
  (:gen-class))

(require '[clojure.string :as str])

(defn records [in-str table]
  "Takes newline separated string with leading header and returns list of maps"
  (let [[header & rows] (clojure.string/split-lines in-str)
        h (map keyword (str/split header #","))
        rs (map #(vector %1 (str/split %2 #","))
                (range 1 (count rows)) rows)]
    (map
     (fn [[o r]] (assoc (assoc
                         (zipmap h r)
                         :table table)
                        :offset o)) rs)))

;; (map-reduce team-data (fn [offset record] [1 record]) 1)
;; (defn map-reduce [data map-func reduce-func]
;;   (let [map-stage (map (fn [mr] (map-func 1 mr)) data)]
;;     map-stage))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
