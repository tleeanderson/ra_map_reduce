(ns ra-map-reduce.model)

(require '[clojure.string :as str])

(def schema-path "/home/tanderson/git/ra_map_reduce/resources/schema/")
;;(def schema-path "schema/")

(defn str-to-long [s]
  "Converts input string to java Long."
  (Long/parseLong s))

(def type-map {:game {:home_points str-to-long 
                      :away_points str-to-long 
                      :attendance str-to-long}
               :conference {:number_teams str-to-long}
               :stadium {:capacity str-to-long
                         :year_built str-to-long}})

(defn records [in-str table]
  "Takes newline separated string with leading header and returns list of maps."
  (let [[header & rows] (clojure.string/split-lines in-str)
        h (map keyword (str/split header #","))
        rs (map #(vector %1 (str/split %2 #","))
                (range 1 (inc (count rows))) rows)]
    (map
     (fn [[o r]]
       (assoc (assoc (zipmap h r)
                         :table table)
                        :offset o)) rs)))

(defn convert-rec-types [record tm]
  "Takes a record and type map and converts types in record according
   to type map."
  (apply assoc record (flatten (mapv (fn [[k f]]
                              [k (f (record k))])
                                     (tm (keyword (record :table)))))))

(defn data-records [path table]
  "Takes a path to a csv file and table name and returns list of maps
   where each map is a record from the table. Types are mapped according
   to type-map."
  (let [full-path (str path table ".csv")
        _ (println "Reading records from" full-path)
        recs (records (slurp full-path) table)]    
    (if (contains? (set (keys type-map)) (keyword table))
      (map (fn [r]
            (convert-rec-types r type-map)) recs)
      recs)))

(defn map-reduce [data map-func reduce-func]
  "Takes data, map function, reduce function, and mimics map reduce
   behavior on the data stream."
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

(defn grab-records [mr-out]
  "Takes output of map reduce and changes shape such that map
   records are in flat."
  (flatten (map second mr-out)))
