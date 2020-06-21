package model

import (
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/mdzio/go-veap"
)

// Service implements veap.Service for an object model. The root object is the entry point
// for the path evaluation.
type Service struct {
	Root Object
}

// ReadPV implements Service.
func (s *Service) ReadPV(path string) (veap.PV, veap.Error) {
	// find object
	obj, err := s.EvalPath(path)
	if err != nil {
		return veap.PV{}, err
	}
	// read PV
	if pvReader, ok := obj.(PVReader); ok {
		return pvReader.ReadPV()
	}
	return veap.PV{}, veap.NewErrorf(veap.StatusMethodNotAllowed, "Reading PV not supported: %s", path)
}

// WritePV implements Service.
func (s *Service) WritePV(path string, pv veap.PV) veap.Error {
	// find object
	obj, err := s.EvalPath(path)
	if err != nil {
		return err
	}
	// write PV
	if pvWriter, ok := obj.(PVWriter); ok {
		return pvWriter.WritePV(pv)
	}
	return veap.NewErrorf(veap.StatusMethodNotAllowed, "Writing PV not supported: %s", path)
}

// ReadHistory implements Service.
func (s *Service) ReadHistory(path string, begin time.Time, end time.Time, limit int64) ([]veap.PV, veap.Error) {
	// find object
	obj, err := s.EvalPath(path)
	if err != nil {
		return nil, err
	}
	// read history
	if historyReader, ok := obj.(HistoryReader); ok {
		return historyReader.ReadHistory(begin, end, limit)
	}
	return nil, veap.NewErrorf(veap.StatusMethodNotAllowed, "Reading History not supported: %s", path)
}

// WriteHistory implements Service.
func (s *Service) WriteHistory(path string, timeSeries []veap.PV) veap.Error {
	// find object
	obj, err := s.EvalPath(path)
	if err != nil {
		return err
	}
	// write history
	if historyWriter, ok := obj.(HistoryWriter); ok {
		return historyWriter.WriteHistory(timeSeries)
	}
	return veap.NewErrorf(veap.StatusMethodNotAllowed, "Writing History not supported: %s", path)
}

// ReadProperties implements Service.
func (s *Service) ReadProperties(path string) (veap.AttrValues, []veap.Link, veap.Error) {
	// find object
	obj, err := s.EvalPath(path)
	if err != nil {
		return nil, nil, err
	}
	// read attributes
	attr := make(veap.AttrValues)
	if attrReader, ok := obj.(AttributeReader); ok {
		for k, v := range attrReader.ReadAttributes() {
			attr[k] = v
		}
	}
	if s := obj.GetIdentifier(); s != "" {
		attr[IdentifierProperty] = s
	}
	if s := obj.GetTitle(); s != "" {
		attr[TitleProperty] = s
	}
	if s := obj.GetDescription(); s != "" {
		attr[DescriptionProperty] = s
	}
	// get items
	var links []veap.Link
	if container, ok := obj.(Collection); ok {
		items := container.Items()
		for _, item := range items {
			links = append(links, veap.Link{
				Role:   container.GetItemRole(),
				Target: url.PathEscape(item.GetIdentifier()),
				Title:  item.GetTitle(),
			})
		}
	}
	// get collection
	if item, ok := obj.(Item); ok {
		links = append(links, veap.Link{
			Role:   item.GetCollectionRole(),
			Target: "..",
			Title:  item.GetCollection().GetTitle(),
		})
	}
	// get links
	if linkReader, ok := obj.(LinkReader); ok {
		items := linkReader.ReadLinks()
		for _, item := range items {
			links = append(links, veap.Link{
				Role:   item.GetRole(),
				Target: AbsPath(item.GetTarget()),
				Title:  item.GetTarget().GetTitle(),
			})
		}
	}
	// PV service
	_, pvReader := obj.(PVReader)
	_, pvWriter := obj.(PVWriter)
	if pvReader || pvWriter {
		links = append(links, veap.Link{
			Role:   "service",
			Target: "~pv",
			Title:  "PV Service",
		})
	}
	// history service
	_, historyReader := obj.(HistoryReader)
	_, historyWriter := obj.(HistoryWriter)
	if historyReader || historyWriter {
		links = append(links, veap.Link{
			Role:   "service",
			Target: "~hist",
			Title:  "History Service",
		})
	}
	return attr, links, nil
}

// WriteProperties implements Service.
func (s *Service) WriteProperties(objPath string, attributes veap.AttrValues) (created bool, err veap.Error) {
	// special case root
	if objPath == "/" {
		return false, setAttr(objPath, s.Root, attributes)
	}
	// find container
	containerPath := path.Dir(objPath)
	containerObj, err := s.EvalPath(containerPath)
	if err != nil {
		return false, err
	}
	// find child
	childIdent := path.Base(objPath)
	childObj, err := GetItem(containerObj, childIdent)
	if err == nil {
		// child found
		return false, setAttr(objPath, childObj, attributes)
	}
	// supports the container creation of items?
	modifier, ok := containerObj.(CollectionModifier)
	if !ok {
		return false, veap.NewErrorf(veap.StatusMethodNotAllowed, "Create not supported: %s", objPath)
	}
	err = modifier.CreateItem(childIdent, attributes)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Delete implements Service.
func (s *Service) Delete(itemPath string) veap.Error {
	// special case root
	if itemPath == "/" {
		return veap.NewErrorf(veap.StatusMethodNotAllowed, "Root can not be deleted")
	}
	// find container
	containerPath := path.Dir(itemPath)
	containerObj, err := s.EvalPath(containerPath)
	if err != nil {
		return err
	}
	// delete supported?
	modifier, ok := containerObj.(CollectionModifier)
	if !ok {
		return veap.NewErrorf(veap.StatusMethodNotAllowed, "Delete not supported: %s", itemPath)
	}
	return modifier.DeleteItem(path.Base(itemPath))
}

// EvalPath follows the specified path to a object and returns it.
func (s *Service) EvalPath(path string) (Object, veap.Error) {
	// check path
	if len(path) < 1 || path[0] != '/' {
		return nil, veap.NewErrorf(veap.StatusBadRequest, "Path starts not with a slash: %s", path)
	}
	path = path[1:]
	// start recursion
	obj, err := evalPathRecursive(s.Root, path)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// GetItem tries to find an item in a container object.
func GetItem(obj Object, id string) (Object, veap.Error) {
	container, ok := obj.(Collection)
	if !ok {
		return nil, veap.NewErrorf(veap.StatusNotFound, "Not a collection: %s", AbsPath(obj))
	}
	item, ok := container.Item(id)
	if !ok {
		return nil, veap.NewErrorf(veap.StatusNotFound, "Item not found at %s: %s", AbsPath(obj), id)
	}
	return item, nil
}

// AbsPath returns the absolute path of a object.
func AbsPath(obj Object) string {
	var pathBuilder strings.Builder
	absPathRecursive(&pathBuilder, obj)
	p := pathBuilder.String()
	// root path?
	if p == "" {
		return "/"
	}
	return p
}

func setAttr(path string, obj Object, attr veap.AttrValues) veap.Error {
	if attrWriter, ok := obj.(AttributeWriter); ok {
		return attrWriter.WriteAttributes(attr)
	}
	return veap.NewErrorf(veap.StatusMethodNotAllowed, "Writing of attributes not supported: %s", path)
}

func evalPathRecursive(obj Object, path string) (Object, veap.Error) {
	// at end?
	if path == "" {
		return obj, nil
	}
	// find next slash
	pos := strings.IndexRune(path, '/')
	var id, rem string
	if pos != -1 {
		id = path[:pos]
		rem = path[pos+1:]
	} else {
		id = path
		rem = ""
	}
	// unescape path segment
	id, escErr := url.PathUnescape(id)
	if escErr != nil {
		return nil, veap.NewError(veap.StatusBadRequest, escErr)
	}
	// find child
	item, err := GetItem(obj, id)
	if err != nil {
		return nil, err
	}
	// recursion
	return evalPathRecursive(item, rem)
}

func absPathRecursive(pathBuilder *strings.Builder, obj Object) {
	// go up to root, root is not an Item
	comp, ok := obj.(Item)
	if ok {
		// not at root
		col := comp.GetCollection()
		// get path of container
		absPathRecursive(pathBuilder, col)
		// append current object
		pathBuilder.WriteRune('/')
		pathBuilder.WriteString(url.PathEscape(obj.GetIdentifier()))
	}
}
