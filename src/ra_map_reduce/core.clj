(ns ra-map-reduce.core
  (:gen-class))

(require '[clojure.string :as str])

(defn records [in-str]
  (let [[header & rows] (clojure.string/split-lines in-str)
        h (str/split header #",")
        rs (map #(str/split % #",") rows)]
    (map (fn [r]
           (map
            (fn [k v] {(keyword k) v}) h r)) rs)))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
