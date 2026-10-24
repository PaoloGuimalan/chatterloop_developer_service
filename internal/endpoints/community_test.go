package endpoints

import (
	"encoding/json"
	"testing"
)

func TestCommentAnonymityRequiresJSONBoolean(t *testing.T) {
	for _, value := range []string{`null`, `"true"`, `1`, `[]`, `{}`} {
		var body commentBody
		if err := json.Unmarshal([]byte(`{"is_anonymous":`+value+`}`), &body); err == nil {
			t.Errorf("accepted nonboolean %s", value)
		}
	}
	for _, value := range []string{`true`, `false`} {
		var body commentBody
		if err := json.Unmarshal([]byte(`{"is_anonymous":`+value+`}`), &body); err != nil {
			t.Fatal(err)
		}
	}
}
