package trace

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractPullRequestReferences(t *testing.T) {
	t.Parallel()
	var tests = []struct {
		name     string
		input    string
		expected []PullRequest
	}{
		{
			name:     "no links",
			input:    "some random text",
			expected: nil,
		},
		{
			name:  "one github pr link",
			input: "PR: https://github.com/owner/repo/pull/123",
			expected: []PullRequest{
				{
					Owner:  "owner",
					Repo:   "repo",
					Number: "123",
				},
			},
		},
		{
			name:  "multiple links with a single github pr",
			input: "https://google.com/ https://github.com/ https://github.com/owner/repo/pull/123 https://example.com/something https://github.com/owner/repo",
			expected: []PullRequest{
				{
					Owner:  "owner",
					Repo:   "repo",
					Number: "123",
				},
			},
		},
		{
			name:  "multiple github prs",
			input: "https://google.com/ https://github.com/ https://github.com/owner/repo/pull/123 https://example.com/something https://github.com/owner/repo/pull/456 https://github.com/owner/repo",
			expected: []PullRequest{
				{
					Owner:  "owner",
					Repo:   "repo",
					Number: "123",
				},
				{
					Owner:  "owner",
					Repo:   "repo",
					Number: "456",
				},
			},
		},
		{
			name: "markdown text",
			input: `# **some-app** release [v1.2.3](https://github.com/owner/some-app/releases/tag/v1.2.3)

### Bug Fixes
			
* some bug fix ([#42](https://github.com/owner/some-app/issues/42)) ([some-dev](https://github.com/some-dev))
			
### Pull Requests
			
* [#42](https://github.com/owner/some-app/pull/42) fix: some bug fix ([some-dev](https://github.com/some-dev))`,
			expected: []PullRequest{
				{
					Owner:  "owner",
					Repo:   "some-app",
					Number: "42",
				},
			},
		},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			actual := extractPullRequestReferences(test.input)
			assert.Equal(t, test.expected, actual)
		})
	}
}
