package model

import (
	"sync/atomic"
	"time"

	"github.com/mdzio/go-veap"
)

// VendorCfg configures a ~vendor object.
type VendorCfg struct {
	ServerName        string
	ServerVersion     string
	ServerDescription string
	VendorName        string
	Collection        ChangeableCollection
}

// NewVendor constructs a new ~vendor domain.
func NewVendor(c *VendorCfg) *Domain {
	domain := &Domain{
		BasicObject: BasicObject{
			Identifier:  "~vendor",
			Title:       "Vendor Information",
			Description: "Information about the server and the vendor",
			AdditionalAttr: veap.AttrValues{
				"serverName":        c.ServerName,
				"serverVersion":     c.ServerVersion,
				"serverDescription": c.ServerDescription,
				"vendorName":        c.VendorName,
				"veapVersion":       "1",
			},
		},
		BasicItem: BasicItem{
			Collection: c.Collection,
		},
	}
	if c.Collection != nil {
		c.Collection.PutItem(domain)
	}
	return domain
}

// NewHandlerStats creates a model adapter for veap.HandlerStats.
func NewHandlerStats(col ChangeableCollection, handlerStats *veap.HandlerStats) *Domain {
	domain := &Domain{
		BasicObject: BasicObject{
			Identifier:  "statistics",
			Title:       "HTTP(S) Handler Statistics",
			Description: "Statistics of the HTTP and HTTPS handler",
		},
		BasicItem: BasicItem{
			Collection: col,
		},
	}
	if col != nil {
		col.PutItem(domain)
	}

	stats := []struct {
		id     string
		title  string
		descr  string
		pvFunc func() (veap.PV, veap.Error)
	}{
		{
			"requests",
			"HTTP(S) Requests",
			"Number of requests",
			func() (veap.PV, veap.Error) { return statAsPV(&handlerStats.Requests) },
		},
		{
			"requestBytes",
			"HTTP(S) Request Bytes",
			"Number of received bytes",
			func() (veap.PV, veap.Error) { return statAsPV(&handlerStats.RequestBytes) },
		},
		{
			"responseBytes",
			"HTTP(S) Response Bytes",
			"Number of sent bytes",
			func() (veap.PV, veap.Error) { return statAsPV(&handlerStats.ResponseBytes) },
		},
		{
			"errorResponses",
			"HTTP(S) Error Responses",
			"Number of sent error responses",
			func() (veap.PV, veap.Error) { return statAsPV(&handlerStats.ErrorResponses) },
		},
	}
	for _, stat := range stats {
		NewROVariable(&ROVariableCfg{
			Identifier:  stat.id,
			Title:       stat.title,
			Description: stat.descr,
			ReadPVFunc:  stat.pvFunc,
			Collection:  domain,
		})
	}
	return domain
}

func statAsPV(i *uint64) (veap.PV, veap.Error) {
	return veap.PV{
		Time:  time.Now(),
		Value: atomic.LoadUint64(i),
		State: veap.StateGood,
	}, nil
}
