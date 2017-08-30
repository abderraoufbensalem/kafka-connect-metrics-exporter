package main

import (
	"log"
	"net/http"

	metrics "github.com/abderraoufbensalem/kafka-connect-metrics-exporter/prometheus"
)

func main() {
	go metrics.RetrievePrometheusMetrics()
	http.HandleFunc("/metrics", metrics.PublishPrometheusMetrics)
	log.Fatal(http.ListenAndServe(":7071", nil))
}
