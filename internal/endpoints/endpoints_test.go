package endpoints

import (
	"net/http/httptest"
	"testing"
)

// readIDs is the only part of the moderation handlers that can be wrong
// quietly - everything either side of it is a database read. The cap in
// particular is a limit somebody will try to get past.

func idsFor(t *testing.T, rawQuery string, cap int) []string {
	t.Helper()
	request := httptest.NewRequest("GET", "/v1/moderation/messages?"+rawQuery, nil)
	return readIDs(request, "messageID", cap)
}

// What a Go or Python client produces from a list.
func TestReadIDsTakesARepeatedParameter(t *testing.T) {
	got := idsFor(t, "messageID=a&messageID=b&messageID=c", 50)

	if len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Fatalf("wanted a,b,c in order, got %v", got)
	}
}

// What somebody types into curl. Refusing it would be a difference nothing in
// the request explains.
func TestReadIDsTakesACommaSeparatedParameter(t *testing.T) {
	got := idsFor(t, "messageID=a,b,c", 50)

	if len(got) != 3 || got[1] != "b" {
		t.Fatalf("wanted a,b,c, got %v", got)
	}
}

func TestReadIDsTrimsAndSkipsBlanks(t *testing.T) {
	got := idsFor(t, "messageID=+a+,,+b+&messageID=", 50)

	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("wanted a,b, got %v", got)
	}
}

// The cap counts IDS, not parameters - otherwise "?messageID=1,2,3,...,900" is
// one parameter and nine hundred reads.
func TestReadIDsCapsTheBatchAcrossBothForms(t *testing.T) {
	got := idsFor(t, "messageID=a,b,c,d,e&messageID=f&messageID=g", 3)

	if len(got) != 3 {
		t.Fatalf("wanted 3, got %d: %v", len(got), got)
	}
}

func TestReadIDsWithNothingToRead(t *testing.T) {
	for _, query := range []string{"", "limit=10", "messageID=", "messageID=,,"} {
		if got := idsFor(t, query, 50); len(got) != 0 {
			t.Fatalf("%q should read as nothing, got %v", query, got)
		}
	}
}

// The parameter name is not shared: a commentID must not satisfy a route that
// asked for messages.
func TestReadIDsIgnoresOtherParameters(t *testing.T) {
	request := httptest.NewRequest("GET", "/v1/moderation/messages?commentID=a", nil)

	if got := readIDs(request, "messageID", 50); len(got) != 0 {
		t.Fatalf("wanted nothing, got %v", got)
	}
}
