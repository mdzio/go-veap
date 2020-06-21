package model

import (
	"time"

	"github.com/mdzio/go-veap"
)

// Default property names.
const (
	IdentifierProperty  = "identifier"
	TitleProperty       = "title"
	DescriptionProperty = "description"
)

// Object is the base interface for all VEAP object.
type Object interface {
	// GetIdentifier returns an identifier, which uniquely identifies an object
	// in a collection. It is also used to generate the access URL. For this
	// reason, it should only contain characters permitted for an URL path
	// segment.
	GetIdentifier() string

	// GetTitle returns a human readable name for this object.
	GetTitle() string

	// GetDescription returns a short text describing this object.
	GetDescription() string
}

// AttributeReader specifies a function for getting the additional attributes of
// a VEAP object.
type AttributeReader interface {
	// ReadAttributes returns the additional attributes of the object.
	// "identifier", "title", "description" and "type" should not be included.
	// They are read from the Object interface. The returned map will not be modified.
	ReadAttributes() veap.AttrValues
}

// AttributeWriter specifies a function for setting attributes of a VEAP object.
type AttributeWriter interface {
	// WriteAttributes updates the attributes of the object. Attributes not
	// specified are not updated. A nil attribute value removes the
	// corresponding attribute. The keys "title", "description" and "type" may
	// update the returned values by the Object interface.
	WriteAttributes(veap.AttrValues) veap.Error
}

// Link represents a single, non hierarchical link to another Object.
type Link interface {
	GetTarget() Object
	GetRole() string
}

// LinkReader spcifies a function for reading the links of an Object.
type LinkReader interface {
	ReadLinks() []Link
}

// Collection is the interface for collection objects, which can contain other
// objects. Only one item role is supported. Different item roles can be
// realized via intermediate collections.
type Collection interface {
	// Items returns all items of this collection.
	Items() []ItemObject

	// Item returns the object with the specified identifier.
	Item(id string) (object ItemObject, ok bool)

	// Role of the items from this objects point of view.
	GetItemRole() string
}

// Item represents one object in a collection. It references his collection.
type Item interface {
	// GetCollection returns the collection of this object.
	GetCollection() CollectionObject

	// GetCollectionRole returns the role of the collection from this objects
	// point of view.
	GetCollectionRole() string
}

// ItemObject combines the two interface Object and Item.
type ItemObject interface {
	Object
	Item
}

// CollectionObject combines the two interface Object and Collection.
type CollectionObject interface {
	Object
	Collection
}

// A CollectionModifier can modify the items in a collection object. With this
// interface VEAP clients can modify a collection.
type CollectionModifier interface {
	// CreateItem creates a new item.
	CreateItem(id string, attr veap.AttrValues) veap.Error

	// DeleteItem deletes an item.
	DeleteItem(id string) veap.Error
}

// PVReader specifies a function for reading a PV.
type PVReader interface {
	// ReadPV gets the PV of the VEAP object.
	ReadPV() (veap.PV, veap.Error)
}

// PVWriter specifies functions for writing a PV.
type PVWriter interface {
	// WritePV sets the PV of the VEAP object.
	WritePV(veap.PV) veap.Error
}

// HistoryReader specifies functions for reading the history of a VEAP object.
type HistoryReader interface {
	// ReadHistory gets the history of the VEAP object.
	ReadHistory(begin time.Time, end time.Time, limit int64) ([]veap.PV, veap.Error)
}

// HistoryWriter specifies functions for writing the history of a VEAP object.
type HistoryWriter interface {
	// WriteHistory inserts into the history of the VEAP object. Existing
	// entries within the time range of the entries are deleted.
	WriteHistory(timeSeries []veap.PV) veap.Error
}
