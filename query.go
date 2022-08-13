package veap

import (
	"net/url"
	"path"
	"strings"
)

func childPaths(objPath string, links []Link) []string {
	results := make([]string, 0)
	for _, link := range links {
		// skip services
		if link.Role == ServiceMarker {
			continue
		}
		linkPath := link.Target
		// convert relative to absolute path
		if !path.IsAbs(linkPath) {
			linkPath = path.Join(objPath, linkPath)
		}
		// direct child?
		if path.Dir(linkPath) == objPath {
			results = append(results, linkPath)
		}
	}
	return results
}

func traverseTree(service Service, pathPattern, startPath string, handle func(item QueryResult) Error) Error {
	// read all properties
	attrs, links, err := service.ReadProperties(startPath)
	if err != nil {
		return err
	}
	// no more path levels?
	if pathPattern == "" {
		handle(QueryResult{
			Path:       startPath,
			Attributes: attrs,
			Links:      links,
		})
		return nil
	}
	// pattern for next level
	rawPattern, pathPattern, _ := strings.Cut(pathPattern, "/")
	pattern, pathErr := url.PathUnescape(rawPattern)
	if pathErr != nil {
		return NewErrorf(StatusBadRequest, "Invalid content '%s' in URL parameter ~path for query service: %v", rawPattern, pathErr)
	}
	// find matching children
	for _, childPath := range childPaths(startPath, links) {
		childIdent, pathErr := url.PathUnescape(path.Base(childPath))
		if pathErr != nil {
			return NewErrorf(StatusInternalServerError, "Invalid identifier '%s' for object: %v", path.Base(childPath), pathErr)
		}
		matches, matchErr := path.Match(pattern, childIdent)
		if matchErr != nil {
			return NewErrorf(StatusBadRequest, "Invalid content '%s' in URL parameter ~path for query service: %v", rawPattern, matchErr)
		}
		if matches {
			if err = traverseTree(service, pathPattern, childPath, handle); err != nil {
				return err
			}
		}
	}
	return nil
}
