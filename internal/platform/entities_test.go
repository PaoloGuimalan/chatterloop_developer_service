package platform

import "testing"

// kindFilter is the pure half of entity search. The query itself needs a
// database; which kinds a caller asked for does not, and it is the part where
// a wrong answer is silent - narrowing to nothing returns an empty directory
// rather than an error.

func TestKindFilterWithNothingAskedForMeansEveryKind(t *testing.T) {
	if want := kindFilter(nil); want != nil {
		t.Fatalf("expected nil for all kinds, got %v", want)
	}
	if want := kindFilter([]string{}); want != nil {
		t.Fatalf("expected nil for all kinds, got %v", want)
	}
}

func TestKindFilterKeepsTheKindsWeHave(t *testing.T) {
	want := kindFilter([]string{"user", "bot"})
	if len(want) != 2 || want[0] != "user" || want[1] != "bot" {
		t.Fatalf("unexpected filter: %v", want)
	}
}

func TestKindFilterNormalisesCaseAndSpacing(t *testing.T) {
	want := kindFilter([]string{" User ", "REALM"})
	if len(want) != 2 || want[0] != "user" || want[1] != "realm" {
		t.Fatalf("unexpected filter: %v", want)
	}
}

func TestKindFilterDropsDuplicates(t *testing.T) {
	if want := kindFilter([]string{"bot", "bot", "BOT"}); len(want) != 1 {
		t.Fatalf("expected one kind, got %v", want)
	}
}

// A kind this service does not have is a typo in a filter, not a reason to
// refuse the search - the caller should get the kinds that do exist.
func TestKindFilterIgnoresKindsWeDoNotHave(t *testing.T) {
	want := kindFilter([]string{"user", "spaceship"})
	if len(want) != 1 || want[0] != "user" {
		t.Fatalf("unexpected filter: %v", want)
	}
}

// Every value unknown must mean "no filter", not "match nothing". The second
// would return an empty directory and look like a platform with no users on it.
func TestKindFilterWithOnlyUnknownKindsMeansEveryKind(t *testing.T) {
	if want := kindFilter([]string{"spaceship", "teapot"}); want != nil {
		t.Fatalf("expected nil for all kinds, got %v", want)
	}
}

// ProfileRedirect is the only pure part of building a result row, and the case that
// matters is the empty one: most active realms have a NULL slug, so a naive
// concatenation links to the bare origin for a large slice of the platform.

func TestProfileRedirectBuildsAClientLink(t *testing.T) {
	if got := ProfileRedirect("https://chatterloop.app", "neon"); got != "https://chatterloop.app/neon" {
		t.Fatalf("unexpected url: %q", got)
	}
}

func TestProfileRedirectIsEmptyWithoutAHandle(t *testing.T) {
	for _, handle := range []string{"", "   "} {
		if got := ProfileRedirect("https://chatterloop.app", handle); got != "" {
			t.Fatalf("a handleless entity must have no link, got %q", got)
		}
	}
}

func TestProfileRedirectTrimsTheHandle(t *testing.T) {
	if got := ProfileRedirect("https://chatterloop.app", "  neon  "); got != "https://chatterloop.app/neon" {
		t.Fatalf("unexpected url: %q", got)
	}
}

func TestProfileRedirectIsEmptyWithoutABase(t *testing.T) {
	if got := ProfileRedirect("", "neon"); got != "" {
		t.Fatalf("no base means no link, got %q", got)
	}
}

func TestProfileRedirectToleratesATrailingSlashOnTheBase(t *testing.T) {
	if got := ProfileRedirect("https://chatterloop.app/", "neon"); got != "https://chatterloop.app/neon" {
		t.Fatalf("unexpected url: %q", got)
	}
}
