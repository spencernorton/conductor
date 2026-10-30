package store_test

import (
	"context"
	"encoding/json"
	"testing"
)

// The TVmaze client caches through the TVDb cache under its own kinds; both
// must round-trip, or every enrichment pass re-queries TVmaze.
func TestIntegrationTVDbCacheStoresTVmazeKinds(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	for _, kind := range []string{"series", "tvmaze_search", "tvmaze_show"} {
		want := map[string]string{"kind": kind}
		body, _ := json.Marshal(want)
		if err := db.PutTVDbCache(ctx, kind, "hash-"+kind, "label", body); err != nil {
			t.Fatalf("put %s: %v", kind, err)
		}
		raw, ok, err := db.GetTVDbCache(ctx, kind, "hash-"+kind)
		if err != nil || !ok {
			t.Fatalf("get %s: ok=%v err=%v", kind, ok, err)
		}
		var got map[string]string
		if err := json.Unmarshal(raw, &got); err != nil || got["kind"] != kind {
			t.Fatalf("get %s returned %s (err=%v)", kind, raw, err)
		}
	}
}
