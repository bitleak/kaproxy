package util

import (
	"bytes"
	"errors"
	"io"
	"net/http"
)

// Parse the multipart/form-data in request.
// Unlike the canonical http.Request.ParseMultipartForm which returns a `map[string][]string` form values,
// we would like to parse the form data into a [][2]string, so to keep the order of the key/value
// pairs in the body. as we would use this parser to get a list of messages.
//
// NOTE: this only implements partial of the multipart/form-data protocol. we don't parse the filename
// parts of file uploading through form.
func ParseMultipartForm(r *http.Request) ([][2]string, error) {
	mr, err := r.MultipartReader()
	if err != nil {
		return nil, err
	}

	rv := make([][2]string, 0)

	maxValueBytes := int64(10 << 20) // 10MB, limit of the accumulated size of values
	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		name := p.FormName()
		var b bytes.Buffer
		n, err := io.CopyN(&b, p, maxValueBytes)
		if err != nil && err != io.EOF {
			return nil, err
		}
		maxValueBytes -= n
		if maxValueBytes == 0 {
			return nil, errors.New("multipart: message too large")
		}
		rv = append(rv, [2]string{name, b.String()})
	}
	return rv, nil
}
