// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multiadmin

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"golang.org/x/net/html"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/servenv"
)

type proxyPathInfo struct {
	serviceType string
	cellName    string
	serviceName string
}

type serviceTarget struct {
	host          string
	port          int
	proxyBasePath string
}

// parseProxyPath extracts routing information from the proxy path
func parseProxyPath(path string) (*proxyPathInfo, error) {
	trimmed := strings.TrimPrefix(path, "/proxy/")
	parts := strings.SplitN(trimmed, "/", 4)

	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid proxy path: expected at least 3 parts")
	}

	return &proxyPathInfo{
		serviceType: parts[0],
		cellName:    parts[1],
		serviceName: parts[2],
	}, nil
}

// resolveServiceTarget determines the target host, port, and base path for the proxy
func resolveServiceTarget(r *http.Request, pathInfo proxyPathInfo) (*serviceTarget, error) {
	switch pathInfo.serviceType {
	case "admin":
		// Global service - multiadmin proxying to itself
		return &serviceTarget{
			host:          servenv.Hostname,
			port:          servenv.HTTPPort(),
			proxyBasePath: fmt.Sprintf("/proxy/admin/%s", pathInfo.cellName),
		}, nil

	case "gate", "pool", "orch":
		// Cell services - multigateway, multipooler, multiorch
		var portMap map[string]int32
		var hostname string
		var err error

		switch pathInfo.serviceType {
		case "gate":
			id := &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIGATEWAY,
				Cell:      pathInfo.cellName,
				Name:      pathInfo.serviceName,
			}
			gwInfo, lookupErr := ts.GetMultiGateway(r.Context(), id)
			err = lookupErr
			if gwInfo != nil {
				portMap = gwInfo.PortMap
				hostname = gwInfo.Hostname
			}
		case "pool":
			id := &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      pathInfo.cellName,
				Name:      pathInfo.serviceName,
			}
			poolerInfo, lookupErr := ts.GetMultiPooler(r.Context(), id)
			err = lookupErr
			if poolerInfo != nil {
				portMap = poolerInfo.PortMap
				hostname = poolerInfo.Hostname
			}
		case "orch":
			id := &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIORCH,
				Cell:      pathInfo.cellName,
				Name:      pathInfo.serviceName,
			}
			orchInfo, lookupErr := ts.GetMultiOrch(r.Context(), id)
			err = lookupErr
			if orchInfo != nil {
				portMap = orchInfo.PortMap
				hostname = orchInfo.Hostname
			}
		}

		if err != nil {
			return nil, fmt.Errorf("service not found: %w", err)
		}

		var targetPort int
		if httpPort, ok := portMap["http"]; ok && httpPort > 0 {
			targetPort = int(httpPort)
		}

		if hostname == "" {
			return nil, fmt.Errorf("service hostname not found")
		}
		if targetPort == 0 {
			return nil, fmt.Errorf("service port not found")
		}

		return &serviceTarget{
			host:          hostname,
			port:          targetPort,
			proxyBasePath: fmt.Sprintf("/proxy/%s/%s/%s", pathInfo.serviceType, pathInfo.cellName, pathInfo.serviceName),
		}, nil

	default:
		return nil, fmt.Errorf("invalid service type: %s", pathInfo.serviceType)
	}
}

// handleProxy routes requests to backend services based on path:
// /proxy/admin/{cell}/{name} -> routes to multiadmin (proxying to itself)
// /proxy/gate/{cell}/{name} -> routes to multigateway
// /proxy/pool/{cell}/{name} -> routes to multipooler
// /proxy/orch/{cell}/{name} -> routes to multiorch
func handleProxy(w http.ResponseWriter, r *http.Request) {
	pathInfo, err := parseProxyPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	target, err := resolveServiceTarget(r, *pathInfo)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if target.host == "" {
		http.Error(w, "Service hostname not found", http.StatusNotFound)
		return
	}
	if target.port == 0 {
		http.Error(w, "Service port not found", http.StatusNotFound)
		return
	}

	// Create reverse proxy to the target service
	targetURL, _ := url.Parse(fmt.Sprintf("http://%s:%d", target.host, target.port))
	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	// Modify the director to strip the proxy prefix from the request path
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		// Strip the proxy prefix to get the actual path the backend expects
		req.URL.Path = strings.TrimPrefix(r.URL.Path, target.proxyBasePath)
		if req.URL.Path == "" {
			req.URL.Path = "/"
		}
		req.Host = targetURL.Host
	}

	// Intercept the response to rewrite HTML content
	proxy.ModifyResponse = func(resp *http.Response) error {
		contentType := resp.Header.Get("Content-Type")

		// Only rewrite HTML responses
		if strings.Contains(contentType, "text/html") {
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return err
			}

			// Rewrite HTML to fix asset and link paths
			rewrittenHTML, err := rewriteHTML(body, target.proxyBasePath)
			if err != nil {
				// If rewriting fails, return original content
				logger.Error("Failed to rewrite HTML", "error", err)
				resp.Body = io.NopCloser(bytes.NewReader(body))
				return nil
			}

			// Update response body
			resp.Body = io.NopCloser(bytes.NewReader(rewrittenHTML))
			resp.Header.Set("Content-Length", fmt.Sprintf("%d", len(rewrittenHTML)))
		}

		return nil
	}

	proxy.ServeHTTP(w, r)
}

// rewriteHTML injects a <base> tag and rewrites absolute URLs in HTML content
func rewriteHTML(htmlContent []byte, proxyBasePath string) ([]byte, error) {
	doc, err := html.Parse(bytes.NewReader(htmlContent))
	if err != nil {
		return nil, err
	}

	// Traverse the document and rewrite URLs
	var f func(*html.Node)
	baseInjected := false
	f = func(n *html.Node) {
		if n.Type == html.ElementNode {
			// Inject <base> tag into <head>
			if n.Data == "head" && !baseInjected {
				// Create <base> element
				baseNode := &html.Node{
					Type: html.ElementNode,
					Data: "base",
					Attr: []html.Attribute{
						{Key: "href", Val: proxyBasePath + "/"},
					},
				}
				// Insert as first child of <head>
				if n.FirstChild != nil {
					n.InsertBefore(baseNode, n.FirstChild)
				} else {
					n.AppendChild(baseNode)
				}
				baseInjected = true
			}

			// Rewrite absolute URLs in href and src attributes
			for i, attr := range n.Attr {
				if (attr.Key == "href" || attr.Key == "src") && strings.HasPrefix(attr.Val, "/") {
					// Don't rewrite if it's already a proxy path or already under the current proxy path
					if !strings.HasPrefix(attr.Val, "/proxy/") && !strings.HasPrefix(attr.Val, proxyBasePath) {
						// Rewrite absolute path to be relative to proxy base
						n.Attr[i].Val = proxyBasePath + attr.Val
					}
				}
			}
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)

	// Render the modified HTML back to bytes
	var buf bytes.Buffer
	if err := html.Render(&buf, doc); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
