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

// handleProxy routes requests to backend services based on path:
// /proxy/admin/multiadmin -> routes to multiadmin (proxying to itself)
// /proxy/gate/{cell}/{name} -> routes to multigateway
// /proxy/pool/{cell}/{name} -> routes to multipooler
// /proxy/orch/{cell}/{name} -> routes to multiorch
func handleProxy(w http.ResponseWriter, r *http.Request) {
	// Parse the path to extract routing information
	path := strings.TrimPrefix(r.URL.Path, "/proxy/")
	parts := strings.SplitN(path, "/", 4)

	if len(parts) < 2 {
		http.Error(w, "Invalid proxy path", http.StatusBadRequest)
		return
	}

	serviceType := parts[0] // "admin", "gate", "pool", "orch"
	var targetPort int
	var proxyBasePath string

	ctx := r.Context()

	switch serviceType {
	case "admin":
		// Global service - multiadmin proxying to itself
		// Format: /proxy/admin/multiadmin
		if len(parts) < 2 {
			http.Error(w, "Missing service name", http.StatusBadRequest)
			return
		}
		// Multiadmin is always at localhost with its own HTTP port
		targetPort = servenv.HTTPPort()
		proxyBasePath = fmt.Sprintf("/proxy/admin/%s", parts[1])

	case "gate":
		// Cell service - multigateway
		// Format: /proxy/gate/{cell}/{name}
		if len(parts) < 3 {
			http.Error(w, "Missing cell or gateway name", http.StatusBadRequest)
			return
		}
		cellName := parts[1]
		gatewayName := parts[2]

		// Lookup gateway by ID
		id := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIGATEWAY,
			Cell:      cellName,
			Name:      gatewayName,
		}
		gwInfo, err := ts.GetMultiGateway(ctx, id)
		if err != nil {
			http.Error(w, fmt.Sprintf("Gateway not found: %v", err), http.StatusNotFound)
			return
		}

		if httpPort, ok := gwInfo.PortMap["http"]; ok && httpPort > 0 {
			targetPort = int(httpPort)
		}
		proxyBasePath = fmt.Sprintf("/proxy/gate/%s/%s", cellName, gatewayName)

	case "pool":
		// Cell service - multipooler
		// Format: /proxy/pool/{cell}/{name}
		if len(parts) < 3 {
			http.Error(w, "Missing cell or pooler name", http.StatusBadRequest)
			return
		}
		cellName := parts[1]
		poolerName := parts[2]

		// Lookup pooler by ID
		id := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      cellName,
			Name:      poolerName,
		}
		poolerInfo, err := ts.GetMultiPooler(ctx, id)
		if err != nil {
			http.Error(w, fmt.Sprintf("Pooler not found: %v", err), http.StatusNotFound)
			return
		}

		if httpPort, ok := poolerInfo.PortMap["http"]; ok && httpPort > 0 {
			targetPort = int(httpPort)
		}
		proxyBasePath = fmt.Sprintf("/proxy/pool/%s/%s", cellName, poolerName)

	case "orch":
		// Cell service - multiorch
		// Format: /proxy/orch/{cell}/{name}
		if len(parts) < 3 {
			http.Error(w, "Missing cell or orch name", http.StatusBadRequest)
			return
		}
		cellName := parts[1]
		orchName := parts[2]

		// Lookup orch by ID
		id := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIORCH,
			Cell:      cellName,
			Name:      orchName,
		}
		orchInfo, err := ts.GetMultiOrch(ctx, id)
		if err != nil {
			http.Error(w, fmt.Sprintf("Orch not found: %v", err), http.StatusNotFound)
			return
		}

		if httpPort, ok := orchInfo.PortMap["http"]; ok && httpPort > 0 {
			targetPort = int(httpPort)
		}
		proxyBasePath = fmt.Sprintf("/proxy/orch/%s/%s", cellName, orchName)

	default:
		http.Error(w, fmt.Sprintf("Invalid service type: %s", serviceType), http.StatusBadRequest)
		return
	}

	if targetPort == 0 {
		http.Error(w, "Service port not found", http.StatusNotFound)
		return
	}

	// Create reverse proxy to the target service
	targetURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d", targetPort))
	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	// Modify the director to strip the proxy prefix from the request path
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		// Strip the proxy prefix to get the actual path the backend expects
		req.URL.Path = strings.TrimPrefix(r.URL.Path, proxyBasePath)
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
			rewrittenHTML, err := rewriteHTML(body, proxyBasePath)
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
