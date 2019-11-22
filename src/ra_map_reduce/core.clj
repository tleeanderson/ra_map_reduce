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

(defn map-reduce [data map-func reduce-func]
  (let [map-stage (map (fn [mr] (map-func mr)) data)
        group-stage (into (sorted-map)
                          (map (fn [[k v]] [k (mapv #(second %) v)])
                               (group-by (fn [[k _]] k) map-stage)))
        reduce-stage (map (fn [[k v]] (reduce-func k v)) group-stage)]
    reduce-stage))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
