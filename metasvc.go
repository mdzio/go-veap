package veap

import "strings"

const (
	// meta service markers
	ExgDataMarker = "~exgdata"
	QueryMarker   = "~query"

	// property markers
	PathMarker = "~path"
)

// Parameters for a single WritePV (q.v. MetaService).
type WritePVParam struct {
	Path string
	PV   PV
}

// Results of a single ReadPV (q.v. MetaService).
type ReadPVResult struct {
	PV    PV
	Error Error
}

// QueryResult (q.v. MetaService)
type QueryResult struct {
	Path       string
	Attributes AttrValues
	Links      []Link
}

// MetaService provides additional services. Usually they can be implemented on
// the basis of the Service interface. However, for performance gains they can
// be implemented optimized for the target system.
type MetaService interface {
	// With ExgData multiple services can be used in one request. This is
	// recommended e.g. for networks with high latencies, for transactions or
	// for optimized requests to the target system. The services are executed in
	// the following order: WritePV, ReadPV.
	ExgData(writePVs []WritePVParam, readPaths []string) (writeErrors []Error, readResults []ReadPVResult, serviceError Error)

	// Query searches for VEAP objects that match any of the specified path
	// masks.
	Query(pathPatterns []string) (queryResults []QueryResult, serviceError Error)
}

// BasicMetaService implements MetaService based on a provided Service.
type BasicMetaService struct {
	Service
}

// Make sure that BasicMetaService implements MetaService.
var _ MetaService = (*BasicMetaService)(nil)

// ExgData implements MetaService.ExgData.
func (m *BasicMetaService) ExgData(writePVs []WritePVParam, readPaths []string) (writeErrors []Error, readResults []ReadPVResult, serviceError Error) {
	// service WritePV
	writeErrors = make([]Error, len(writePVs))
	for i := range writePVs {
		writeErrors[i] = m.WritePV(writePVs[i].Path, writePVs[i].PV)
	}
	// service ReadPV
	readResults = make([]ReadPVResult, len(readPaths))
	for i := range readPaths {
		pv, err := m.ReadPV(readPaths[i])
		readResults[i] = ReadPVResult{pv, err}
	}
	// no serviceError
	return
}

// Query implements MetaService.ExgData.
func (m *BasicMetaService) Query(pathPatterns []string) ([]QueryResult, Error) {
	results := make([]QueryResult, 0)
	// loop over all path masks
	for _, pathPattern := range pathPatterns {
		// find matching VEAP objects
		if err := traverseTree(m, strings.TrimPrefix(pathPattern, "/"), "/", func(item QueryResult) Error {
			results = append(results, item)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return results, nil
}

// ReadProperties overrides Service.ReadProperties.
func (m *BasicMetaService) ReadProperties(path string) (attr AttrValues, links []Link, err Error) {
	attr, links, err = m.Service.ReadProperties(path)
	if path != "/" || err != nil {
		return
	}
	// add /~exgdata link
	links = append(links, Link{
		Role:   ServiceMarker,
		Target: ExgDataMarker,
		Title:  "ExgData Service",
	})
	// add /~search link
	links = append(links, Link{
		Role:   ServiceMarker,
		Target: QueryMarker,
		Title:  "Search Service",
	})
	return
}
