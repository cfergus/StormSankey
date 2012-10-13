;; java -cp target\;C:\Apps\storm-0.8.1\storm-0.8.1.jar;C:\Apps\storm-0.8.1\lib\*;.
;; clojure.main -i backtype/storm/ui/sankey/sankeyview.clj -e (backtype.storm.ui.sankey/-main)

;; todo : if no stats are present, no viz appears at all. Instead, present a helpful error

(ns backtype.storm.ui.sankey
  (:use compojure.core)
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util])
  (:use [backtype.storm.ui helpers])
  (:use [backtype.storm.daemon [common :only [ACKER-COMPONENT-ID system-id?]]])
  (:use [ring.adapter.jetty :only [run-jetty]])
  (:use [clojure.string :only [trim]])
  (:import [backtype.storm.generated ExecutorSpecificStats
            ExecutorStats ExecutorSummary TopologyInfo SpoutStats BoltStats
            ErrorInfo ClusterSummary SupervisorSummary TopologySummary
            Nimbus$Client StormTopology GlobalStreamId])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [backtype.storm [thrift :as thrift]]
            [backtype.storm.ui.core :as storm-ui])
  (:gen-class))

(defn sankey-ui-template [body]
  "Include all relevant css and js relevant to creating a sankey visualization of a storm topology."
  (html
   [:head
    [:title "Storm Sankey UI"]
    (include-css "/css/bootstrap-1.1.0.css")
    (include-css "/css/topology-sankey.css")
    (include-js "/js/jquery-1.6.2.min.js")
    (include-js "/js/d3.v2.js")
    (include-js "/js/sankey.js")
    ]
    [:body
    [:h1 (link-to "/" "Storm UI")]
    (seq body)
     (include-js "/js/topology-sankey.js")
    ]))

; TODO : include sys stats conditionally

; TODO : Post filter on streamId based on desired stream to view
; Filter out those counts not part of the stream of interest
(defn bolt-input-stats [window ^TopologyInfo topology-info component executors include-sys?]
  "Extract the statistics for a topology in order to create visualizations."
  (let [stats (storm-ui/get-filled-stats executors)
        stream-summary (-> stats (storm-ui/aggregate-bolt-stats include-sys?))
        all-inputs-to-bolt (get (:acked stream-summary) window  )
        ]

        (filter
            #(= "default" (.get_streamId (first %)))
            all-inputs-to-bolt
        )
    )
  )


(defn generate-nodes-json [components-in-order]
   "Generate the json required to visualize nodes/components/rectangles."
   (interpose ","
     (map
        #( str "{\"name\":\"" (trim %1) "\"}" )
        components-in-order )
   )
  )


(defn lookup-component-index [ bolt-id components-in-order ]
  "Given an ordered vector of component-ids, lookup the index of a component for visualization purposes."
  (.indexOf components-in-order ( trim  bolt-id ) )
)


(defn generate-links-json-for-target-from-value [components-in-order vector-of-links]
  "Given the vector of link data, generates the actual json required to
  represent the metrics for a sankey visualization."
  ; input is vector of maps, where each map is :target, :source, :value

    (apply str
      (interpose ","
        (map
          #(str "{\"source\":" (lookup-component-index (:source %) components-in-order ) ","
             "\"target\":" ( lookup-component-index (:target %) components-in-order ) ","
             "\"value\":" (:value %)
             "}\n"
             )
          vector-of-links
          )
        )
      )
  )

(defn generate-links-json-for-target [components-in-order link-stats-for-target]
  "Given a specific target component, create json for incoming data metrics."

  (apply str
    (interpose ","

     (map
       (partial generate-links-json-for-target-from-value components-in-order)
       (vals link-stats-for-target))
      )
    )
  )

(defn generate-links-json [components-in-order link-stats]
  "Generate json to support the data required for links between nodes.
  This represents tuples sent between a component and a bolt"
  (interpose ","
    (map
      ( partial generate-links-json-for-target components-in-order)
      link-stats
      )
    )
)


(defn link-stats-for-bolt-target [bolt-id summ topology window include-sys?]
  "Given a bolt-id, this function generates the statistics about the components that feed it."
  (let [task-summs (storm-ui/component-task-summs summ topology bolt-id)
        input-stats (bolt-input-stats window summ bolt-id task-summs include-sys?)
        count-map ( into []
                    (for [[ gsid count ] input-stats]
                         [(.get_componentId gsid) count]) )
        ]

    ; for a given bolt, count map is [[ src count] ]
    ; eg [[second-sentence-spout 1140] [first-sentence-spout 2560]]

     (group-by :target (map
        #( hash-map
            :source (trim (first % )) :target (trim bolt-id) :value ( second % ) )
        count-map )
     )
  )
)

(defn generate-json-sankey-data [ bolts-in-order components-in-order
                                 bolt-comp-summs summ topology window include-sys? ]
  "Generate the javascript variables required for sankey visualization"
  (def link-stats
    (map
       #(link-stats-for-bolt-target % summ topology window include-sys? )
       bolts-in-order
    )
  )

  (str
    "var tupleStats=\n"  ; root level variable, used for sankey diagram
    "{\"nodes\":["      ; the 'rectangles'
    (apply str (generate-nodes-json components-in-order) )
    "],"
    "\"links\":["       ; the paths connecting rectangles/nodes
    (apply str (generate-links-json components-in-order
        (filter #(seq %) link-stats) ) )
    "]};"
    )
)

(defn topology-sankey-page [topology-id window include-sys?]
  "Create html relevant to visualization of a storm topology"
  (storm-ui/with-nimbus nimbus
    (let [
          window (if window window ":all-time")
          summ (.getTopologyInfo ^Nimbus$Client nimbus topology-id)
          topology (.getTopology ^Nimbus$Client nimbus topology-id)
          bolt-summs (filter (partial storm-ui/bolt-summary? topology) (.get_executors summ))
          spout-summs (filter (partial storm-ui/spout-summary? topology) (.get_executors summ))
          bolt-comp-summs (storm-ui/group-by-comp bolt-summs)
          spout-comp-summs (storm-ui/group-by-comp spout-summs)
          bolts-in-order (into [] (keys bolt-comp-summs))
          spouts-in-order (into [] (keys spout-comp-summs))
          components-in-order ( concat spouts-in-order bolts-in-order )
          ]
      (concat

         [[:script (apply str
            ( generate-json-sankey-data bolts-in-order components-in-order
                    bolt-comp-summs summ topology window include-sys? ))
         ]]

         [[:h2 "Sankey Chart"]
           [:p {:id "chart"}]
           ]
       )
    ))
)


(defroutes main-routes
  (GET "/sankey/topology/:id" [:as {cookies :cookies} id & m]
    (let [include-sys? (storm-ui/get-include-sys? cookies)]
       (-> (topology-sankey-page id (:window m) include-sys?)
           sankey-ui-template)))

  (route/resources "/")
  (route/not-found "Page not found"))

(def app
  (handler/site main-routes)
 )

(defn -main []
  (run-jetty app {:port 8080}))
; {:port (Integer. (*STORM-CONF* UI-PORT))}