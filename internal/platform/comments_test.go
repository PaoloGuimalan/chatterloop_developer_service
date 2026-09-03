package platform

import (
	"strings"
	"testing"
	"time"
)

func TestFlatteningParentsToTheTopLevelAncestor(t *testing.T) {
	// Replying to a REPLY must re-parent to that reply's ancestor rather than
	// nesting a third time. Nesting deeper produces rows the thread reader
	// cannot paginate, and a soft-deleted middle comment would strand them.
	repliedTo := &parentComment{CommentID: "reply-1", ParentCommentID: "top-1"}

	if got := storedParentFor(repliedTo); got != "top-1" {
		t.Fatalf("expected the top-level ancestor, got %q", got)
	}
}

func TestFlatteningParentsDirectlyToATopLevelComment(t *testing.T) {
	repliedTo := &parentComment{CommentID: "top-1", ParentCommentID: ""}

	if got := storedParentFor(repliedTo); got != "top-1" {
		t.Fatalf("expected the comment itself, got %q", got)
	}
}

func TestATopLevelCommentHasNoParent(t *testing.T) {
	if got := storedParentFor(nil); got != "" {
		t.Fatalf("expected no parent, got %q", got)
	}
}

// The person REPLIED TO is notified, which is not the same entity as the one
// the row is stored under. Conflating them means the reply notification goes to
// whoever started the thread instead of whoever was answered.
func TestTheNotifiedEntityIsTheOneRepliedToNotTheAncestors(t *testing.T) {
	repliedTo := &parentComment{
		CommentID: "reply-1", ParentCommentID: "top-1", EntityID: "answered-them",
	}

	if storedParentFor(repliedTo) == repliedTo.EntityID {
		t.Fatal("the stored parent and the notified entity must not be conflated")
	}
	if repliedTo.EntityID != "answered-them" {
		t.Fatalf("notify target lost: %q", repliedTo.EntityID)
	}
}

func TestTruncateForPreviewMatchesDjangosSlice(t *testing.T) {
	short := "still accurate?"
	if got := truncateForPreview(short); got != short {
		t.Fatalf("a short comment must be quoted whole, got %q", got)
	}

	long := strings.Repeat("a", 31)
	got := truncateForPreview(long)
	if got != strings.Repeat("a", 30)+"..." {
		t.Fatalf("expected 30 characters and an ellipsis, got %q", got)
	}
}

func TestTruncateForPreviewCountsCharactersNotBytes(t *testing.T) {
	// Python slices CHARACTERS. Slicing bytes would cut a multi-byte character
	// in half and put a replacement glyph in somebody's notification tray.
	long := strings.Repeat("é", 40)

	got := truncateForPreview(long)

	if runes := []rune(strings.TrimSuffix(got, "...")); len(runes) != 30 {
		t.Fatalf("expected 30 runes, got %d", len(runes))
	}
	if strings.ContainsRune(got, '�') {
		t.Fatal("a character was cut in half")
	}
}

func TestTruncateForPreviewHandlesAnEmptyParent(t *testing.T) {
	// An attachment-only parent has no text. Django's len(None) used to 500 the
	// whole reply here, which is why the `or ""` exists there.
	if got := truncateForPreview(""); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

func TestMentionCandidatesOfferBothFormsOfATrailingDot(t *testing.T) {
	// The platform's pattern includes "." in the handle class and is greedy, so
	// "thanks @ana." captures "ana.". Only one of the two can be a real handle,
	// so both are offered and resolution stays unambiguous.
	candidates := mentionCandidates("thanks @ana.")

	if !contains(candidates, "ana.") || !contains(candidates, "ana") {
		t.Fatalf("expected both forms, got %v", candidates)
	}
}

func TestMentionCandidatesAreLowercased(t *testing.T) {
	// The lookup compares against lower(username); a handle left cased would
	// silently match nothing.
	candidates := mentionCandidates("hey @Ana")

	if !contains(candidates, "ana") {
		t.Fatalf("expected a lowercased candidate, got %v", candidates)
	}
	for _, candidate := range candidates {
		if candidate != strings.ToLower(candidate) {
			t.Fatalf("%q was not lowercased", candidate)
		}
	}
}

func TestMentionCandidatesDoNotDuplicateOneHandle(t *testing.T) {
	// "@Ana" and "@ana" are one person and must not spend two of the budget.
	candidates := mentionCandidates("@Ana and @ana")

	if len(candidates) != 1 {
		t.Fatalf("expected one candidate, got %v", candidates)
	}
}

func TestMentionCandidatesIgnoreAnEmailAddress(t *testing.T) {
	// The pattern's leading (?:^|\s) is what stops this, and it is the reason
	// ExtractMentions is not a naive scan for "@".
	if candidates := mentionCandidates("mail me at you@example.com"); len(candidates) != 0 {
		t.Fatalf("expected nothing, got %v", candidates)
	}
}

func TestMentionCandidatesAreBounded(t *testing.T) {
	// Past MAX_MENTIONS_PER_COMMENT the input is a spam vector: every mention
	// costs a notification write plus a realtime publish.
	var text strings.Builder
	for i := 0; i < 60; i++ {
		text.WriteString("@user")
		text.WriteByte(byte('a' + i%26))
		text.WriteString(" ")
	}

	if candidates := mentionCandidates(text.String()); len(candidates) > maxMentionsPerComment+1 {
		t.Fatalf("expected a bounded list, got %d", len(candidates))
	}
}

func TestNewCommentIDLooksLikeDjangosUUID4(t *testing.T) {
	// Django stores str(uuid.uuid4()) into a CharField: canonical, hyphenated,
	// lowercase. A different shape would still be unique and would stand out in
	// the column.
	id, err := newCommentID()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(id) != 36 {
		t.Fatalf("expected 36 characters, got %d (%q)", len(id), id)
	}
	if id != strings.ToLower(id) {
		t.Fatalf("expected lowercase, got %q", id)
	}
	parts := strings.Split(id, "-")
	if len(parts) != 5 {
		t.Fatalf("expected five groups, got %q", id)
	}
	for i, want := range []int{8, 4, 4, 4, 12} {
		if len(parts[i]) != want {
			t.Fatalf("group %d is %d characters, expected %d (%q)", i, len(parts[i]), want, id)
		}
	}
	if parts[2][0] != '4' {
		t.Fatalf("expected version 4, got %q", id)
	}
	if !strings.ContainsRune("89ab", rune(parts[3][0])) {
		t.Fatalf("expected an RFC 4122 variant, got %q", id)
	}
}

func TestNewCommentIDsDoNotRepeat(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		id, err := newCommentID()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if seen[id] {
			t.Fatalf("repeated id %q", id)
		}
		seen[id] = true
	}
}

func TestPythonDateStringShape(t *testing.T) {
	// str(datetime.now().astimezone()) - "2026-09-03 09:50:12.345678+08:00".
	// A space rather than "T", six fractional digits, and an offset with a
	// colon in it.
	got := pythonDateString(time.Now())

	if strings.Contains(got, "T") {
		t.Fatalf("expected a space separator, got %q", got)
	}
	fraction := strings.SplitN(got, ".", 2)
	if len(fraction) != 2 || len(fraction[1]) < 7 {
		t.Fatalf("expected six fractional digits then an offset, got %q", got)
	}
	if !strings.Contains(got[10:], ":") {
		t.Fatalf("expected a colon in the offset, got %q", got)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
