package scim

import (
	"flag"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
)

var (
	enableSCIM = flag.Bool("auth.enable_scim", false, "Whether or not to enable SCIM.")
)

type SCIMServer struct {
	env environment.Env
}

func Register(env *real_environment.RealEnv) error {
	if *enableSCIM {
		env.SetSCIMService(NewSCIMServer(env))
	}
	return nil
}

func NewSCIMServer(env environment.Env) *SCIMServer {
	return &SCIMServer{
		env: env,
	}
}

func (s *SCIMServer) Users() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			s.getUser(w, r)
		case "POST":
			s.createUser(w, r)
		case "PUT":
			s.updateUser(w, r)
		case "PATCH":
			s.updateUser(w, r)
		case "DELETE":
			s.deleteUser(w, r)
		}
	})
}

func (s *SCIMServer) getUser(w http.ResponseWriter, r *http.Request) {
	// TODO
}

func (s *SCIMServer) createUser(w http.ResponseWriter, r *http.Request) {
	// TODO
}

func (s *SCIMServer) updateUser(w http.ResponseWriter, r *http.Request) {
	// TODO
}

func (s *SCIMServer) deleteUser(w http.ResponseWriter, r *http.Request) {
	// TODO
}

func (s *SCIMServer) Groups() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			s.getGroup(w, r)
		case "POST":
			s.createGroup(w, r)
		case "PUT":
			s.updateGroup(w, r)
		case "PATCH":
			s.updateGroup(w, r)
		case "DELETE":
			s.deleteGroup(w, r)
		}
	})
}

func (s *SCIMServer) getGroup(w http.ResponseWriter, r *http.Request) {
	// TODO
}

func (s *SCIMServer) createGroup(w http.ResponseWriter, r *http.Request) {
	// TODO
}

func (s *SCIMServer) updateGroup(w http.ResponseWriter, r *http.Request) {
	// TODO
}

func (s *SCIMServer) deleteGroup(w http.ResponseWriter, r *http.Request) {
	// TODO
}
