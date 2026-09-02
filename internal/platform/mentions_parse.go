package platform

import "strings"

// mentionRunes is the platform's mention character class: [A-Za-z0-9._-].
func isMentionRune(r byte) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	case r == '.' || r == '_' || r == '-':
		return true
	}
	return false
}

// isMentionTerminator matches the platform's lookahead: (?=$|\s|[.,!?;:]).
func isMentionTerminator(r byte) bool {
	switch r {
	case ' ', '\t', '\n', '\r', '\f', '\v',
		'.', ',', '!', '?', ';', ':':
		return true
	}
	return false
}

// ExtractMentions returns the handles mentioned in a message, deduplicated and
// in order of first appearance.
//
// # WHY THIS IS HAND-ROLLED
//
// The platform's pattern is
//
//	(?:^|\s)@([A-Za-z0-9._-]{1,30})(?=$|\s|[.,!?;:])
//
// - identical in Node (transformers.js) and Django (comment_mentions.py). Go's
// regexp is RE2 and has no lookahead, and no RE2 rewrite is faithful, because
// the JS engine BACKTRACKS: "." is both a valid handle character and a valid
// terminator, so in "@foo.bar<" the greedy match "foo.bar" fails the lookahead
// and the engine falls back to "foo", terminated by the ".". A leftmost-longest
// RE2 match finds nothing there and the two implementations would silently
// disagree about who was mentioned.
//
// So this reproduces the backtracking directly: take the longest run of at most
// 30 handle characters whose following character is a terminator or end of
// string, trying shorter runs before giving up. Same answers, no regex engine.
func ExtractMentions(text string) []string {
	const maxHandle = 30

	var (
		found []string
		seen  = map[string]bool{}
	)

	for i := 0; i < len(text); i++ {
		if text[i] != '@' {
			continue
		}
		// (?:^|\s) - the @ must start the string or follow whitespace.
		if i > 0 && !isSpaceByte(text[i-1]) {
			continue
		}

		start := i + 1
		end := start
		for end < len(text) && end-start < maxHandle && isMentionRune(text[end]) {
			end++
		}
		if end == start {
			continue
		}

		// Longest first, exactly as the backtracking engine would.
		for stop := end; stop > start; stop-- {
			if stop < len(text) && !isMentionTerminator(text[stop]) {
				continue
			}
			handle := text[start:stop]
			if !seen[handle] {
				seen[handle] = true
				found = append(found, handle)
			}
			break
		}
	}
	return found
}

func isSpaceByte(r byte) bool {
	switch r {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	}
	return false
}

// SanitizeForStorage strips HTML tags, matching the platform's
// sanitizeForStorage (server/reusables/hooks/transformers.js), whose live body
// is a single `content.replace(/<\/?[^>]+(>|$)/g, "")` - the entity-escaping
// above it is commented out there, so escaping here would double-encode
// relative to every message the platform has already stored.
func SanitizeForStorage(content string) string {
	if content == "" {
		return ""
	}
	var out strings.Builder
	out.Grow(len(content))

	for i := 0; i < len(content); i++ {
		if content[i] != '<' {
			out.WriteByte(content[i])
			continue
		}

		// `<` then AT LEAST ONE non-`>` character, then `>` or end of string.
		//
		// The `\/?` in the JS pattern is redundant and must NOT be treated as
		// a separate optional prefix here: in "</>" the engine backtracks and
		// lets `[^>]+` consume the "/" itself, so "</>" IS stripped - while
		// "<>" has nothing for `[^>]+` to match and survives verbatim.
		// Verified against the live function, not inferred.
		body := i + 1
		for body < len(content) && content[body] != '>' {
			body++
		}
		if body == i+1 {
			out.WriteByte('<')
			continue
		}
		// `(>|$)` consumes the closing bracket or the end of the string; an
		// unterminated "<" therefore swallows the rest of the message, which
		// is the platform's real behaviour and not an accident here.
		i = body
	}
	return out.String()
}
