(ns ra-map-reduce.core
  (:gen-class))

(require '[clojure.string :as str])

(defn records [in-str table]
  "Takes newline separated string with leading header and returns list of maps."
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
  "Takes data, map function, reduce function, and mimics map reduce
   behavior on the data stream."
  (let [map-stage (map (fn [mr] (map-func mr)) data)
        group-stage (into (sorted-map)
                          (map (fn [[k v]] [k (mapv #(second %) v)])
                               (group-by (fn [[k _]] k) map-stage)))
        reduce-stage (map (fn [[k v]] (reduce-func k v)) group-stage)]
    reduce-stage))

(defn reduce-identity [key vals]
  "Intended to be passed to map-reduce. Takes key and vals and returns them."
  [key vals])

(defn select [cond-func]
  "Semantic equivalent to RA select. Takes function and evaluates it with given
   record. passed and failed are used as group keys."
  {:map (fn [record] (if (cond-func record)
                       ["passed" record]
                       ["failed" (record :offset)]))
   :reduce reduce-identity})

(defn select-conference-query [data]
  "Uses generic select function to filter teams by conference."
  (let [{mp :map rd :reduce} (select (fn [r]
                                       (= (r :conference) "BIG 10")))]
    (map-reduce data mp rd)))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
