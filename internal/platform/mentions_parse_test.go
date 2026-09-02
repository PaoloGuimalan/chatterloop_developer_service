package platform

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// The platform's mention pattern, byte for byte. Duplicated here as a literal
// so the drift test below can assert the real sources still contain it.
const platformMentionPattern = `(?:^|\s)@([A-Za-z0-9._-]{1,30})(?=$|\s|[.,!?;:])`

func TestExtractMentions(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"plain", "hey @ana how are you", []string{"ana"}},
		{"at start of string", "@ana hello", []string{"ana"}},
		{"at end of string", "hello @ana", []string{"ana"}},
		{"terminated by punctuation", "thanks @ana!", []string{"ana"}},
		{"comma", "@ana, @ben", []string{"ana", "ben"}},
		{"dot is part of the handle when it ends there", "ping @ana.b", []string{"ana.b"}},
		{"deduplicated, first order kept", "@ana @ben @ana", []string{"ana", "ben"}},
		{"mid-word at is not a mention", "email ana@example.com", nil},
		{"bare at", "@ ana", nil},
		{"empty", "", nil},
		{
			// The case that rules out an RE2 rewrite: the greedy run
			// "foo.bar" is followed by "<", which is not a terminator, so a
			// backtracking engine falls back to "foo" - terminated by the ".".
			"backtracks to a shorter handle",
			"see @foo.bar<tag>",
			[]string{"foo"},
		},
		{
			"handle longer than thirty characters is not silently truncated",
			"@" + strings.Repeat("a", 35),
			nil,
		},
		{
			"exactly thirty is fine",
			"@" + strings.Repeat("a", 30),
			[]string{strings.Repeat("a", 30)},
		},
		{"underscores and hyphens", "@neon-systems and @some_one", []string{"neon-systems", "some_one"}},
		{"newline terminates", "@ana\n@ben", []string{"ana", "ben"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ExtractMentions(tc.in)
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("ExtractMentions(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestSanitizeForStorage(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"plain text untouched", "hello there", "hello there"},
		{"tags stripped", "<b>bold</b>", "bold"},
		{"self closing", "a<br/>b", "ab"},
		{"unterminated tag is dropped to end of string", "safe<script", "safe"},
		{"attributes", `<a href="x">link</a>`, "link"},
		// Expectations below were taken from RUNNING the platform's
		// sanitizeForStorage, not from reading the regex - two of them are
		// counter-intuitive and the reading was wrong the first time.
		//
		// "<>" has nothing for `[^>]+` to match, so it survives; "</>" does
		// not survive, because the engine backtracks and lets `[^>]+` consume
		// the "/" itself.
		{"empty tag survives", "a<>b", "a<>b"},
		{"empty closing tag is stripped", "a</>b", "ab"},
		// `(>|$)` means an unterminated "<" eats the rest of the message.
		// Surprising, and real: a bot replying "2 < 3" stores "2 ".
		{"unclosed less-than swallows the remainder", "2 < 3", "2 "},
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SanitizeForStorage(tc.in); got != tc.want {
				t.Fatalf("SanitizeForStorage(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizeConversationType(t *testing.T) {
	cases := map[string]string{
		"":         "single",
		"single":   "single",
		"group":    "group",
		"server":   "channel", // the one rename the platform performs
		"SERVER":   "channel",
		"Channel":  "channel",
		"CONFEREN": "conferen",
	}
	for in, want := range cases {
		if got := normalizeConversationType(in); got != want {
			t.Fatalf("normalizeConversationType(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestMentionPatternHasNotDriftedInThePlatform reads the two live sources and
// asserts they still hold the pattern this package reimplements.
//
// The implementation here is hand-rolled because Go has no lookahead, so the
// usual protection - sharing the regex - is unavailable. This is the
// substitute: if either source changes, this fails and names the file, rather
// than the three implementations quietly disagreeing about who was mentioned.
//
// Skipped when the sources are not on disk, so the suite still runs in a
// container that only has this service.
func TestMentionPatternHasNotDriftedInThePlatform(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Skip("cannot resolve the chatterloop root")
	}

	sources := map[string]string{
		"node":   filepath.Join(root, "server", "reusables", "hooks", "transformers.js"),
		"django": filepath.Join(root, "services", "user_service", "newsfeed", "services", "comment_mentions.py"),
	}

	checked := 0
	for name, path := range sources {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Logf("%s source not present (%s), skipping that half", name, path)
			continue
		}
		if !strings.Contains(string(body), platformMentionPattern) {
			t.Errorf("%s no longer contains the mention pattern this package reimplements.\n"+
				"  expected: %s\n  in: %s\n"+
				"  If the platform's pattern changed on purpose, update "+
				"ExtractMentions and platformMentionPattern together.",
				name, platformMentionPattern, path)
		}
		checked++
	}
	if checked == 0 {
		t.Skip("neither platform source is on disk")
	}
}

func TestToEpochMillisHandlesBothLiveShapes(t *testing.T) {
	// The messages collection holds two shapes and both are real: a BSON date
	// on anything the platform's send route wrote, and an embedded
	// {date, time} of formatted strings on older rows.
	const jan1 = int64(1704067200000) // 2024-01-01T00:00:00Z

	if got := ToEpochMillis(primitive.DateTime(jan1)); got != jan1 {
		t.Fatalf("BSON date: got %d, want %d", got, jan1)
	}
	if got := ToEpochMillis(time.Unix(0, jan1*int64(time.Millisecond)).UTC()); got != jan1 {
		t.Fatalf("time.Time: got %d, want %d", got, jan1)
	}
	if got := ToEpochMillis(bson.M{"date": "2024-01-01T00:00:00Z"}); got != jan1 {
		t.Fatalf("embedded shape: got %d, want %d", got, jan1)
	}
	// The two shapes must agree, or a conversation's messages interleave
	// wrongly the moment it contains both.
	if ToEpochMillis(primitive.DateTime(jan1)) != ToEpochMillis(bson.M{"date": "2024-01-01T00:00:00Z"}) {
		t.Fatal("the two live shapes disagree")
	}
}

func TestToEpochMillisSortsUnreadableToTheBeginning(t *testing.T) {
	// 0, deliberately: an unparseable timestamp must sort to the START of
	// history, so a parsing gap can never make an old message look newest.
	for _, bad := range []any{nil, "", "not-a-date", bson.M{}, bson.M{"date": "13 Jan, 4pm"}, struct{}{}} {
		if got := ToEpochMillis(bad); got != 0 {
			t.Fatalf("ToEpochMillis(%v) = %d, want 0", bad, got)
		}
	}
}
