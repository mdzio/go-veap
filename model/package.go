/*
Package model implements veap.Service. The service calls are forwarded to single
model objects, which are organized in a tree like structure. The request path is
used to find the target object.

It is assumed that structure queries are always successful. Error returns are
not implemented for this purpose.
*/
package model
