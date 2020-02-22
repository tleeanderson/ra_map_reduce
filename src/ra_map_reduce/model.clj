(ns ra-map-reduce.model
  (:require [clojure.string])
  (:require [clojure.java.io :as io]))

(def schema-path "schema/")

(defn str-to-long
  "Converts input string to java Long."
  [s]
  (Long/parseLong s))

(def type-map {:game {:home_points str-to-long 
                      :away_points str-to-long 
                      :attendance str-to-long}
               :conference {:number_teams str-to-long}
               :stadium {:capacity str-to-long
                         :year_built str-to-long}})

(defn records
  "Takes newline separated string with leading header and returns list of maps."
  [in-str table]
  (let [[header & rows] (clojure.string/split-lines in-str)
        h (map keyword (clojure.string/split header #","))
        rs (map #(vector %1 (clojure.string/split %2 #","))
                (range 1 (inc (count rows))) rows)]
    (map
     (fn [[o r]]
       (assoc (assoc (zipmap h r)
                         :table table)
                        :offset o)) rs)))

(defn convert-rec-types
  "Takes a record and type map and converts types in record according
   to type map."
  [record tm]
  (apply assoc record (flatten (mapv (fn [[k f]]
                              [k (f (record k))])
                                     (tm (keyword (record :table)))))))

(defn data-records
  "Takes a path to a csv file and table name and returns list of maps
   where each map is a record from the table. Types are mapped according
   to type-map."
  [path table]
  (let [full-path (str path table ".csv")
        recs (records (slurp (io/resource full-path)) table)]
    (if (contains? (set (keys type-map)) (keyword table))
      (map (fn [r]
            (convert-rec-types r type-map)) recs)
      recs)))

(defn map-reduce
  "Takes data, map function, reduce function, and mimics map reduce
   behavior on the data stream."
  [data map-func reduce-func]
  (let [flat-lis (flatten data)
        map-stage (map (fn [mr]
                         (map-func mr)) flat-lis)
        group-stage (into (sorted-map)
                          (map (fn [[k v]]
                                 [k (mapv #(second %) v)])
                               (group-by (fn [[k _]]
                                           k) map-stage)))
        reduce-stage (map (fn [[k v]]
                            (reduce-func k v)) group-stage)]
    reduce-stage))

(defn grab-records
  "Takes output of map reduce and changes shape such that map
   records are in flat."
  [mr-out]
  (flatten (map second mr-out)))

(defn passed-records
  "Gets passed records from sel-mr-out."
  [sel-mr-out]
  (second (first (filter #(= (first %) "passed") sel-mr-out))))
