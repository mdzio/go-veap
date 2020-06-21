package model

import (
	"time"

	"github.com/mdzio/go-veap"
)

// Root is a root object for the VEAP object tree.
type Root struct {
	BasicObject
	BasicCollection
}

// RootCfg configures a Root object.
type RootCfg struct {
	Identifier     string
	Title          string
	Description    string
	AdditionalAttr veap.AttrValues
	ItemRole       string
}

// NewRoot constructs a new Root.
func NewRoot(c *RootCfg) *Root {
	return &Root{
		BasicObject: BasicObject{
			Identifier:     c.Identifier,
			Title:          c.Title,
			Description:    c.Description,
			AdditionalAttr: c.AdditionalAttr,
		},
		BasicCollection: BasicCollection{
			ItemRole: c.ItemRole,
		},
	}
}

// Domain is an object, which can hold other objects as items.
type Domain struct {
	BasicObject
	BasicCollection
	BasicItem
}

// DomainCfg configures a Domain object.
type DomainCfg struct {
	Identifier     string
	Title          string
	Description    string
	AdditionalAttr veap.AttrValues
	ItemRole       string
	Collection     ChangeableCollection
	CollectionRole string
}

// NewDomain constructs a new Domain.
func NewDomain(c *DomainCfg) *Domain {
	domain := &Domain{
		BasicObject: BasicObject{
			Identifier:     c.Identifier,
			Title:          c.Title,
			Description:    c.Description,
			AdditionalAttr: c.AdditionalAttr,
		},
		BasicCollection: BasicCollection{
			ItemRole: c.ItemRole,
		},
		BasicItem: BasicItem{
			Collection:     c.Collection,
			CollectionRole: c.CollectionRole,
		},
	}
	if c.Collection != nil {
		c.Collection.PutItem(domain)
	}
	return domain
}

// LinkedDomain is an object, which can hold other objects as items and
// additionally links.
type LinkedDomain struct {
	BasicObject
	BasicCollection
	BasicItem
	BasicLinks
}

// NewLinkedDomain constructs a new Domain.
func NewLinkedDomain(c *DomainCfg) *LinkedDomain {
	domain := &LinkedDomain{
		BasicObject: BasicObject{
			Identifier:     c.Identifier,
			Title:          c.Title,
			Description:    c.Description,
			AdditionalAttr: c.AdditionalAttr,
		},
		BasicCollection: BasicCollection{
			ItemRole: c.ItemRole,
		},
		BasicItem: BasicItem{
			Collection:     c.Collection,
			CollectionRole: c.CollectionRole,
		},
	}
	if c.Collection != nil {
		c.Collection.PutItem(domain)
	}
	return domain
}

// ModifiableDomain is an object, which can hold other objects as items.
// Items can be created and deleted remotely by VEAP clients.
type ModifiableDomain struct {
	BasicObject
	BasicCollection
	BasicItem
	CreateFunc func(col ChangeableCollection, id string, attr veap.AttrValues) veap.Error
}

// CreateItem implements CollectionModifier.
func (d *ModifiableDomain) CreateItem(id string, attr veap.AttrValues) veap.Error {
	return d.CreateFunc(d, id, attr)
}

// DeleteItem implements CollectionModifier.
func (d *ModifiableDomain) DeleteItem(id string) veap.Error {
	pobj := d.RemoveItem(id)
	if pobj == nil {
		return veap.NewErrorf(veap.StatusNotFound, "Item not found: %s", id)
	}
	return nil
}

// ModifiableDomainCfg configures a ModifiableDomain object.
type ModifiableDomainCfg struct {
	Identifier     string
	Title          string
	Description    string
	AdditionalAttr veap.AttrValues
	ItemRole       string
	CreateItem     func(col ChangeableCollection, id string, attr veap.AttrValues) veap.Error
	Collection     ChangeableCollection
	CollectionRole string
}

// NewModifiableDomain constructs a new ModifiableDomain.
func NewModifiableDomain(c *ModifiableDomainCfg) *ModifiableDomain {
	domain := &ModifiableDomain{
		BasicObject: BasicObject{
			Identifier:     c.Identifier,
			Title:          c.Title,
			Description:    c.Description,
			AdditionalAttr: c.AdditionalAttr,
		},
		BasicCollection: BasicCollection{
			ItemRole: c.ItemRole,
		},
		BasicItem: BasicItem{
			Collection:     c.Collection,
			CollectionRole: c.CollectionRole,
		},
		CreateFunc: c.CreateItem,
	}
	if c.Collection != nil {
		c.Collection.PutItem(domain)
	}
	return domain
}

// ROVariable is a read only PV.
type ROVariable struct {
	BasicObject
	BasicItem
	FuncPVReader
}

// ROVariableCfg configures a ROVariable.
type ROVariableCfg struct {
	Identifier     string
	Title          string
	Description    string
	AdditionalAttr veap.AttrValues
	Collection     ChangeableCollection
	CollectionRole string
	ReadPVFunc     func() (veap.PV, veap.Error)
}

// NewROVariable constructs a new ROVariable.
func NewROVariable(c *ROVariableCfg) *ROVariable {
	domain := &ROVariable{
		BasicObject: BasicObject{
			Identifier:     c.Identifier,
			Title:          c.Title,
			Description:    c.Description,
			AdditionalAttr: c.AdditionalAttr,
		},
		BasicItem: BasicItem{
			Collection:     c.Collection,
			CollectionRole: c.CollectionRole,
		},
		FuncPVReader: FuncPVReader{
			ReadPVFunc: c.ReadPVFunc,
		},
	}
	if c.Collection != nil {
		c.Collection.PutItem(domain)
	}
	return domain
}

// Variable is a readable and writeable PV.
type Variable struct {
	BasicObject
	BasicItem
	FuncPVReader
	FuncPVWriter
}

// VariableCfg configures a Variable.
type VariableCfg struct {
	Identifier     string
	Title          string
	Description    string
	AdditionalAttr veap.AttrValues
	Collection     ChangeableCollection
	CollectionRole string
	ReadPVFunc     func() (veap.PV, veap.Error)
	WritePVFunc    func(veap.PV) veap.Error
}

// NewVariable constructs a new Variable.
func NewVariable(c *VariableCfg) *Variable {
	domain := &Variable{
		BasicObject: BasicObject{
			Identifier:     c.Identifier,
			Title:          c.Title,
			Description:    c.Description,
			AdditionalAttr: c.AdditionalAttr,
		},
		BasicItem: BasicItem{
			Collection:     c.Collection,
			CollectionRole: c.CollectionRole,
		},
		FuncPVReader: FuncPVReader{
			ReadPVFunc: c.ReadPVFunc,
		},
		FuncPVWriter: FuncPVWriter{
			WritePVFunc: c.WritePVFunc,
		},
	}
	if c.Collection != nil {
		c.Collection.PutItem(domain)
	}
	return domain
}

// ROHistory is a read-only history.
type ROHistory struct {
	BasicObject
	BasicItem
	FuncHistoryReader
}

// ROHistoryCfg configures a ROHistory.
type ROHistoryCfg struct {
	Identifier      string
	Title           string
	Description     string
	AdditionalAttr  veap.AttrValues
	Collection      ChangeableCollection
	CollectionRole  string
	ReadHistoryFunc func(begin time.Time, end time.Time, limit int64) ([]veap.PV, veap.Error)
}

// NewROHistory constructs a new ROHistory.
func NewROHistory(c *ROHistoryCfg) *ROHistory {
	domain := &ROHistory{
		BasicObject: BasicObject{
			Identifier:     c.Identifier,
			Title:          c.Title,
			Description:    c.Description,
			AdditionalAttr: c.AdditionalAttr,
		},
		BasicItem: BasicItem{
			Collection:     c.Collection,
			CollectionRole: c.CollectionRole,
		},
		FuncHistoryReader: FuncHistoryReader{
			ReadHistoryFunc: c.ReadHistoryFunc,
		},
	}
	if c.Collection != nil {
		c.Collection.PutItem(domain)
	}
	return domain
}

// History is a readable and writeable history.
type History struct {
	BasicObject
	BasicItem
	FuncHistoryReader
	FuncHistoryWriter
}

// HistoryCfg configures a History.
type HistoryCfg struct {
	Identifier       string
	Title            string
	Description      string
	AdditionalAttr   veap.AttrValues
	Collection       ChangeableCollection
	CollectionRole   string
	ReadHistoryFunc  func(begin time.Time, end time.Time, limit int64) ([]veap.PV, veap.Error)
	WriteHistoryFunc func(timeSeries []veap.PV) veap.Error
}

// NewHistory constructs a new History.
func NewHistory(c *HistoryCfg) *History {
	domain := &History{
		BasicObject: BasicObject{
			Identifier:     c.Identifier,
			Title:          c.Title,
			Description:    c.Description,
			AdditionalAttr: c.AdditionalAttr,
		},
		BasicItem: BasicItem{
			Collection:     c.Collection,
			CollectionRole: c.CollectionRole,
		},
		FuncHistoryReader: FuncHistoryReader{
			ReadHistoryFunc: c.ReadHistoryFunc,
		},
		FuncHistoryWriter: FuncHistoryWriter{
			WriteHistoryFunc: c.WriteHistoryFunc,
		},
	}
	if c.Collection != nil {
		c.Collection.PutItem(domain)
	}
	return domain
}

// ROVariableWithHistory is a read-only variable with history.
type ROVariableWithHistory struct {
	BasicObject
	BasicItem
	FuncPVReader
	FuncHistoryReader
}

// ROVariableWithHistoryCfg configures a ROVariableWithHistory.
type ROVariableWithHistoryCfg struct {
	Identifier      string
	Title           string
	Description     string
	AdditionalAttr  veap.AttrValues
	Collection      ChangeableCollection
	CollectionRole  string
	ReadPVFunc      func() (veap.PV, veap.Error)
	ReadHistoryFunc func(begin time.Time, end time.Time, limit int64) ([]veap.PV, veap.Error)
}

// NewROVariableWithHistory constructs a new ROVariableWithHistory.
func NewROVariableWithHistory(c *ROVariableWithHistoryCfg) *ROVariableWithHistory {
	domain := &ROVariableWithHistory{
		BasicObject: BasicObject{
			Identifier:     c.Identifier,
			Title:          c.Title,
			Description:    c.Description,
			AdditionalAttr: c.AdditionalAttr,
		},
		BasicItem: BasicItem{
			Collection:     c.Collection,
			CollectionRole: c.CollectionRole,
		},
		FuncPVReader: FuncPVReader{
			ReadPVFunc: c.ReadPVFunc,
		},
		FuncHistoryReader: FuncHistoryReader{
			ReadHistoryFunc: c.ReadHistoryFunc,
		},
	}
	if c.Collection != nil {
		c.Collection.PutItem(domain)
	}
	return domain
}

// VariableWithHistory is a readable and writeable variable history.
type VariableWithHistory struct {
	BasicObject
	BasicItem
	FuncPVReader
	FuncPVWriter
	FuncHistoryReader
	FuncHistoryWriter
}

// VariableWithHistoryCfg configures a VariableWithHistory.
type VariableWithHistoryCfg struct {
	Identifier       string
	Title            string
	Description      string
	AdditionalAttr   veap.AttrValues
	Collection       ChangeableCollection
	CollectionRole   string
	ReadPVFunc       func() (veap.PV, veap.Error)
	WritePVFunc      func(veap.PV) veap.Error
	ReadHistoryFunc  func(begin time.Time, end time.Time, limit int64) ([]veap.PV, veap.Error)
	WriteHistoryFunc func(timeSeries []veap.PV) veap.Error
}

// NewVariableWithHistory constructs a new VariableWithHistory.
func NewVariableWithHistory(c *VariableWithHistoryCfg) *VariableWithHistory {
	domain := &VariableWithHistory{
		BasicObject: BasicObject{
			Identifier:     c.Identifier,
			Title:          c.Title,
			Description:    c.Description,
			AdditionalAttr: c.AdditionalAttr,
		},
		BasicItem: BasicItem{
			Collection:     c.Collection,
			CollectionRole: c.CollectionRole,
		},
		FuncPVReader: FuncPVReader{
			ReadPVFunc: c.ReadPVFunc,
		},
		FuncPVWriter: FuncPVWriter{
			WritePVFunc: c.WritePVFunc,
		},
		FuncHistoryReader: FuncHistoryReader{
			ReadHistoryFunc: c.ReadHistoryFunc,
		},
		FuncHistoryWriter: FuncHistoryWriter{
			WriteHistoryFunc: c.WriteHistoryFunc,
		},
	}
	if c.Collection != nil {
		c.Collection.PutItem(domain)
	}
	return domain
}
