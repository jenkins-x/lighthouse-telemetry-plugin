package trace

import (
	"net/url"
	"strings"

	"github.com/ucarion/urlpath"
	"mvdan.cc/xurls/v2"
)

type PullRequest struct {
	Owner  string
	Repo   string
	Number string
}

func (p PullRequest) String() string {
	return p.Owner + "/" + p.Repo + "#" + p.Number
}

func extractPullRequestReferences(text string) []PullRequest {
	var (
		pullRequests []PullRequest
		links        = xurls.Strict().FindAllString(text, -1)
	)
	for _, link := range links {
		u, err := url.Parse(link)
		if err != nil {
			continue
		}
		pr := parsePullRequest(u)
		if pr != nil {
			pullRequests = append(pullRequests, *pr)
		}
	}
	return pullRequests
}

func parsePullRequest(prURL *url.URL) *PullRequest {
	var (
		githubPathTemplate = urlpath.New("/:owner/:repo/pull/:number")
	)
	switch {
	case strings.HasSuffix(prURL.Host, "github.com"):
		if m, ok := githubPathTemplate.Match(prURL.Path); ok {
			return &PullRequest{
				Owner:  m.Params["owner"],
				Repo:   m.Params["repo"],
				Number: m.Params["number"],
			}
		}
		// TODO handle more providers
	}
	return nil
}
