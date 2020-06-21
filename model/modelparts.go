package model

import (
	"sync"
	"time"

	"github.com/mdzio/go-veap"
)

// BasicObject implements Object.
type BasicObject struct {
	Identifier     string
	Title          string
	Description    string
	AdditionalAttr veap.AttrValues
}

// GetIdentifier implements Object.
func (o *BasicObject) GetIdentifier() string {
	return o.Identifier
}

// GetTitle implements Object.
func (o *BasicObject) GetTitle() string {
	return o.Title
}

// GetDescription implements Object.
func (o *BasicObject) GetDescription() string {
	return o.Description
}

// ReadAttributes implements Object.
func (o *BasicObject) ReadAttributes() veap.AttrValues {
	return o.AdditionalAttr
}

// BasicCollection implements Collection. The collection can be manipulated by
// multiple goroutines.
type BasicCollection struct {
	ItemRole string

	objects map[string]ItemObject
	mutex   sync.RWMutex
}

// Items implements Collection.
func (c *BasicCollection) Items() []ItemObject {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	r := make([]ItemObject, 0, len(c.objects))
	for _, i := range c.objects {
		r = append(r, i)
	}
	return r
}

// Item implements Collection.
func (c *BasicCollection) Item(id string) (ItemObject, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	r, ok := c.objects[id]
	return r, ok
}

// GetItemRole implements Collection.
func (c *BasicCollection) GetItemRole() string {
	if c.ItemRole == "" {
		return "item"
	}
	return c.ItemRole
}

// PutItem adds an item to a collection. If the collection already contains an
// object with the same identifier, it is replaced. The replaced object is
// returned.
func (c *BasicCollection) PutItem(obj ItemObject) ItemObject {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	pobj := c.objects[obj.GetIdentifier()]
	if c.objects == nil {
		c.objects = make(map[string]ItemObject)
	}
	c.objects[obj.GetIdentifier()] = obj
	return pobj
}

// RemoveItem removes the item with the specified identifier. The
// removed item is then returned, or nil, if the item is not found.
func (c *BasicCollection) RemoveItem(id string) ItemObject {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	pobj := c.objects[id]
	delete(c.objects, id)
	return pobj
}

// BasicItem implements Item.
type BasicItem struct {
	Collection     CollectionObject
	CollectionRole string
}

// GetCollection implements Item.
func (c *BasicItem) GetCollection() CollectionObject {
	return c.Collection
}

// GetCollectionRole implements Item.
func (c *BasicItem) GetCollectionRole() string {
	if c.CollectionRole == "" {
		return "collection"
	}
	return c.CollectionRole
}

// BasicLink implements Link.
type BasicLink struct {
	Target Object
	Role   string
}

// GetTarget implements Link.
func (l BasicLink) GetTarget() Object {
	return l.Target
}

// GetRole implements Link.
func (l BasicLink) GetRole() string {
	return l.Role
}

// BasicLinks implements LinkReader. It can be manipulated by multiple goroutines.
type BasicLinks struct {
	lobj  map[BasicLink]struct{}
	mutex sync.RWMutex
}

// ReadLinks implements LinkReader.
func (l *BasicLinks) ReadLinks() []Link {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	r := make([]Link, 0, len(l.lobj))
	for link := range l.lobj {
		r = append(r, link)
	}
	return r
}

// PutLink adds a link. Returns true, if the link was added, and false, if the
// link was already put.
func (l *BasicLinks) PutLink(target Object, role string) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	li := BasicLink{Target: target, Role: role}
	if _, ok := l.lobj[li]; ok {
		return false
	}
	if l.lobj == nil {
		l.lobj = make(map[BasicLink]struct{})
	}
	l.lobj[li] = struct{}{}
	return true
}

// RemoveLink removes a link.
func (l *BasicLinks) RemoveLink(target Object, role string) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	li := BasicLink{Target: target, Role: role}
	if _, ok := l.lobj[li]; ok {
		delete(l.lobj, li)
		return true
	}
	return false
}

// FuncPVReader implements PVReader.
type FuncPVReader struct {
	ReadPVFunc func() (veap.PV, veap.Error)
}

// ReadPV implements PVReader.
func (n *FuncPVReader) ReadPV() (veap.PV, veap.Error) {
	return n.ReadPVFunc()
}

// FuncPVWriter implements PVWriter.
type FuncPVWriter struct {
	WritePVFunc func(veap.PV) veap.Error
}

// WritePV implements PVWriter.
func (n *FuncPVWriter) WritePV(pv veap.PV) veap.Error {
	return n.WritePVFunc(pv)
}

// FuncHistoryReader implements HistoryReader.
type FuncHistoryReader struct {
	ReadHistoryFunc func(begin time.Time, end time.Time, limit int64) ([]veap.PV, veap.Error)
}

// ReadHistory implements HistoryReader.
func (h *FuncHistoryReader) ReadHistory(begin time.Time, end time.Time, limit int64) ([]veap.PV, veap.Error) {
	return h.ReadHistoryFunc(begin, end, limit)
}

// FuncHistoryWriter implements HistoryWriter.
type FuncHistoryWriter struct {
	WriteHistoryFunc func(timeSeries []veap.PV) veap.Error
}

// WriteHistory implements HistoryWriter.
func (h *FuncHistoryWriter) WriteHistory(timeSeries []veap.PV) veap.Error {
	return h.WriteHistoryFunc(timeSeries)
}

// ChangeableCollection defines additionally PutItem and RemoveItem for a CollectionObject.
type ChangeableCollection interface {
	CollectionObject
	PutItem(obj ItemObject) ItemObject
	RemoveItem(id string) ItemObject
}
