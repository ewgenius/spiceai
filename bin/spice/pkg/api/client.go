package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/spiceai/spiceai/bin/spice/pkg/version"
)

type SpiceAuthContext struct {
	Email    string   `json:"email,omitempty"`
	Username string   `json:"username,omitempty"`
	Org      SpiceOrg `json:"org,omitempty"`
	App      SpiceApp `json:"app,omitempty"`
}

type SpiceOrg struct {
	Id   int64  `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

type SpiceApp struct {
	Id     int64  `json:"id,omitempty"`
	Name   string `json:"name,omitempty"`
	ApiKey string `json:"api_key,omitempty"`
}

type AccessTokenResponse struct {
	AccessDenied bool   `json:"access_denied,omitempty"`
	AccessToken  string `json:"access_token,omitempty"`
}

type SpiceApiClient struct {
	baseUrl string
}

func NewSpiceApiClient() *SpiceApiClient {
	return &SpiceApiClient{}
}

func (s *SpiceApiClient) Init() error {
	if version.Version() == "local-dev" {
		s.baseUrl = "https://dev.spice.xyz"
	} else {
		s.baseUrl = "https://spice.ai"
	}

	if os.Getenv("SPICE_BASE_URL") != "" {
		s.baseUrl = os.Getenv("SPICE_BASE_URL")
	}

	return nil
}

func (s *SpiceApiClient) GetAuthUrl(authCode string) string {
	return fmt.Sprintf("%s/auth/token?code=%s", s.baseUrl, authCode)
}

func (s *SpiceApiClient) GetAuthContext(accessToken string, orgName *string, appName *string) (SpiceAuthContext, error) {
	var spiceAuthContext SpiceAuthContext

	url := fmt.Sprintf("%s/api/spice-cli/auth?org_name=%s&app_name=%s", s.baseUrl, *orgName, *appName)

	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return spiceAuthContext, err
	}

	request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return spiceAuthContext, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return spiceAuthContext, err
	}

	err = json.Unmarshal(body, &spiceAuthContext)

	if err != nil {
		return spiceAuthContext, err
	}

	return spiceAuthContext, nil
}

func (s *SpiceApiClient) ExchangeCode(authCode string) (AccessTokenResponse, error) {
	var authStatusResponse AccessTokenResponse

	payload := map[string]interface{}{
		"code": authCode,
	}

	jsonBody, err := json.Marshal(payload)
	if err != nil {
		return authStatusResponse, err
	}

	request, err := http.NewRequest("POST", fmt.Sprintf("%s/auth/token/exchange", s.baseUrl), bytes.NewReader(jsonBody))
	if err != nil {
		return authStatusResponse, err
	}
	request.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return authStatusResponse, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return authStatusResponse, err
	}

	err = json.Unmarshal(body, &authStatusResponse)

	if err != nil {
		return authStatusResponse, err
	}

	return authStatusResponse, nil
}
