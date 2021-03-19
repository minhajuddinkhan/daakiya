package daakiyaa

import (
	"fmt"
	"net/http"
)

func SSEReadHandler(dr DaakiyaReader, auth Authenticator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		_, err := auth.Authenticate(r.Header.Get("Authorization"))
		if err != nil {
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		flusher := w.(http.Flusher)
		hash := r.Header.Get("client_id")
		topic := r.Header.Get("topic")

		for message := range dr.Read(r.Context(), hash, topic, 0) {
			fmt.Fprintf(w, "data: %s\n\n", message.Value)
			flusher.Flush()
		}
	}
}
