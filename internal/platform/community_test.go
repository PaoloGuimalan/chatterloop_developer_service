package platform

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestAnonymousActivityCannotPublishActor(t *testing.T) {
	actor := normalizeActor("secret-owner", "secret-handle", "Secret Name", "user")
	frame := postActivityMessage("post", activityComment, &actor, map[string]any{"is_anonymous": true})
	raw, _ := json.Marshal(frame)
	if strings.Contains(string(raw), "secret") {
		t.Fatalf("anonymous frame leaked identity: %s", raw)
	}
	body := frame["result"].(map[string]any)
	if entity, ok := body["entity"]; !ok || entity != nil {
		t.Fatalf("anonymous actor must be explicit null: %#v", body)
	}
}

func TestCommunityRealmCannotBeConversationEvenForItsOwnEntity(t *testing.T) {
	id := "community-entity"
	err := assertRealmMember(context.Background(), nil, "realm", id, &id, "community", false)
	if !errors.Is(err, ErrNoAccess) {
		t.Fatalf("community conversation authorized: %v", err)
	}
}

func TestPostAudienceAndContributionBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name        string
		scope       postAccess
		viewer      string
		write, want bool
	}{
		{"public community reader", postAccess{CommunityCount: 1, CommunityActive: true}, "viewer", false, true},
		{"private former author", postAccess{CommunityCount: 1, CommunityActive: true, CommunityPrivate: true, AuthorEntityID: "author"}, "author", false, false},
		{"private current member", postAccess{CommunityCount: 1, CommunityActive: true, CommunityPrivate: true, CommunityMember: true}, "viewer", false, true},
		{"public nonmember cannot comment", postAccess{CommunityCount: 1, CommunityActive: true}, "viewer", true, false},
		{"member can comment", postAccess{CommunityCount: 1, CommunityActive: true, CommunityMember: true}, "viewer", true, true},
		{"inactive community", postAccess{CommunityCount: 1, CommunityMember: true}, "viewer", false, false},
		{"multiple destinations fail closed", postAccess{CommunityCount: 2, CommunityActive: true, CommunityMember: true}, "viewer", false, false},
		{"legacy reader has no viewer", postAccess{CommunityCount: 1, CommunityActive: true}, "", false, false},
		{"ordinary permitted", postAccess{OrdinaryReadable: true}, "viewer", false, true},
		{"ordinary private denied", postAccess{}, "viewer", false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.scope.permits(tt.viewer, tt.write); got != tt.want {
				t.Fatalf("permit=%v want %v", got, tt.want)
			}
		})
	}
}

func TestAnonymousRecordsNeverMarshalOwnerIdentity(t *testing.T) {
	for _, record := range []any{
		PostThread{AuthorEntityID: "secret", AuthorHandle: "secret", IsAnonymous: true},
		PostComment{AuthorEntityID: "secret", AuthorHandle: "secret", IsAnonymous: true},
		Mention{AuthorEntityID: "secret", AuthorHandle: "secret", IsAnonymous: true},
	} {
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "secret") || !strings.Contains(string(raw), `"is_anonymous":true`) {
			t.Fatalf("unsafe projection: %s", raw)
		}
	}
}

func TestAnonymousCommentRejectedOutsideCommunity(t *testing.T) {
	if err := validateCommentAnonymity(true, 0); err == nil {
		t.Fatal("ordinary anonymous comment allowed")
	}
	if err := validateCommentAnonymity(false, 0); err != nil {
		t.Fatal(err)
	}
	if err := validateCommentAnonymity(true, 1); err != nil {
		t.Fatal(err)
	}
}

func TestCommunityNotificationSanitizesAnonymousIdentity(t *testing.T) {
	original := NewNotification{ToEntityID: "member", FromEntityID: "secret", Details: "@secret replied", TargetType: "post", TargetID: "post"}
	scope := postAccess{CommunityCount: 1, CommunityActive: true, CommunityMember: true, Community: &Community{EntityID: "community"}}
	safe, allowed := communityNotification(original, &scope, true)
	if !allowed || safe.FromEntityID != "community" || strings.Contains(safe.Details, "secret") {
		t.Fatalf("unsafe anonymous notification: %#v allowed=%v", safe, allowed)
	}
	identified, allowed := communityNotification(original, &scope, false)
	if !allowed || identified.FromEntityID != "community" || identified.Details != "@secret replied" {
		t.Fatalf("nonanonymous actor lost: %#v", identified)
	}
	scope.CommunityMember = false
	if _, allowed := communityNotification(original, &scope, false); allowed {
		t.Fatal("nonmember receives community notice")
	}
}

func TestCommunityStreamRechecksMembershipAtDelivery(t *testing.T) {
	payload := `{"event":"notifications","message":{"result":{"post_id":"post","community_entity_id":"community","is_anonymous":true}}}`
	checks := 0
	read := func(post string) (*postAccess, error) {
		checks++
		return &postAccess{CommunityCount: 1, CommunityActive: true, CommunityPrivate: true, CommunityMember: false}, nil
	}
	if communityFrameAllowed(payload, "former-member", read) {
		t.Fatal("former member received private activity")
	}
	if checks != 1 {
		t.Fatalf("access checks=%d", checks)
	}
	read = func(post string) (*postAccess, error) {
		return &postAccess{CommunityCount: 1, CommunityActive: true, CommunityPrivate: true, CommunityMember: true}, nil
	}
	if !communityFrameAllowed(payload, "member", read) {
		t.Fatal("current member lost activity")
	}
	read = func(post string) (*postAccess, error) { return nil, errors.New("unavailable") }
	if communityFrameAllowed(payload, "member", read) {
		t.Fatal("failed lookup allowed frame")
	}
	if !communityFrameAllowed(`{"event":"message","message":{"result":""}}`, "viewer", read) {
		t.Fatal("ordinary frame altered")
	}
}
