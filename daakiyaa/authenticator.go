package daakiyaa

type Authenticator interface {
	Authenticate(token string) (decoded map[string]interface{}, err error)
}
