(ns ra-map-reduce.core
  (:gen-class))

(require '[clojure.string :as str])

;;map reduce system functions
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

(defn data-records [path table]
  (let [full-path (str path table ".csv")]
    (println "Reading records from " full-path)
    (records (slurp full-path) table)))

(defn map-reduce [data map-func reduce-func]
  "Takes data, map function, reduce function, and mimics map reduce
   behavior on the data stream."
  (let [map-stage (map (fn [mr]
                         (map-func mr)) data)
        group-stage (into (sorted-map)
                          (map (fn [[k v]]
                                 [k (mapv #(second %) v)])
                               (group-by (fn [[k _]]
                                           k) map-stage)))
        reduce-stage (map (fn [[k v]]
                            (reduce-func k v)) group-stage)]
    reduce-stage))

;;generic functions, semantic equivalents to RA operators
(defn reduce-identity [key vals]
  "Default reduce function. Takes key and vals and returns them."
  [key vals])

(defn select [cond-func]
  "Semantic equivalent to RA select. Takes function and evaluates it with given
   record. passed and failed are used as group keys."
  {:map (fn [record]
          (if (cond-func record)
            ["passed" record]
            ["failed" (record :offset)]))
   :reduce reduce-identity})

(defn project [attr-keys]
  "Takes a set of attribute keys and selects a subset of the record
   map by those attribute keys. Groups by table by default."
  {:map (fn [record]
          [(record :table)
           (select-keys record attr-keys)])
   :reduce reduce-identity})

(defn agg-group [group-keys agg-func-map]
  {:map (fn [record]
          [(if (some? group-keys)
             (mapv record (sort group-keys))
             (record :table)) record])
   :reduce (fn [key vals]
             [key (map (fn [[k af]]
                         {k (af (map k vals))}) agg-func-map)])})

;;example query functions
(defn add-offsets-query [data]
  "Uses generic agg-group. Default grouping on table name and sum the offsets."
  (let [{mp :map rd :reduce} (agg-group nil {:offset (fn [vs]
                                                       (reduce + vs))})]
    (map-reduce data mp rd)))

(defn home-team-stats [data]
  "Uses generic agg-group. Sum home points and attendance for each home team."
  (let [{mp :map rd :reduce} (agg-group #{:home-team}
                                        {:home_points (fn [vs]
                                                        (reduce + vs))
                                         :attendance (fn [vs]
                                                        (reduce + vs))})]
    (map-reduce data mp rd)))

(defn select-conference-query [data]
  "Uses generic select function to filter teams by conference."
  (let [{mp :map rd :reduce} (select (fn [r]
                                       (= (r :conference) "BIG 10")))]
    (map-reduce data mp rd)))

(defn project-query [data]
  "Uses generic project to select name, city, stadium from each record."
  (let [{mp :map rd :reduce} (project #{:name :city :stadium})]
    (map-reduce data mp rd)))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
