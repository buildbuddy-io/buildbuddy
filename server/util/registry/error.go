package registry

import (
	"encoding/json"
	"net/http"
)

type regError struct {
	Status  int
	Code    string
	Message string
}

func (r *regError) Write(resp http.ResponseWriter) error {
	resp.WriteHeader(r.Status)

	type err struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	type wrap struct {
		Errors []err `json:"errors"`
	}
	return json.NewEncoder(resp).Encode(wrap{
		Errors: []err{
			{
				Code:    r.Code,
				Message: r.Message,
			},
		},
	})
}

// regErrInternal returns an internal server error.
func regErrInternal(err error) *regError {
	return &regError{
		Status:  http.StatusInternalServerError,
		Code:    "INTERNAL_SERVER_ERROR",
		Message: err.Error(),
	}
}

var regErrBlobUnknown = &regError{
	Status:  http.StatusNotFound,
	Code:    "BLOB_UNKNOWN",
	Message: "Unknown blob",
}

var regErrUnsupported = &regError{
	Status:  http.StatusMethodNotAllowed,
	Code:    "UNSUPPORTED",
	Message: "Unsupported operation",
}

var regErrDigestMismatch = &regError{
	Status:  http.StatusBadRequest,
	Code:    "DIGEST_INVALID",
	Message: "digest does not match contents",
}

var regErrDigestInvalid = &regError{
	Status:  http.StatusBadRequest,
	Code:    "NAME_INVALID",
	Message: "invalid digest",
}
