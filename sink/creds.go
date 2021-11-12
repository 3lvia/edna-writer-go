package sink

import "github.com/3lvia/hn-config-lib-go/vault"

func setCredentials(v vault.SecretsManager) error {
	if v == nil {
		return nil
	}

	credsPath, err := credentialsPath()
	if err != nil {
		return err
	}

	err = v.SetDefaultGoogleCredentials(credsPath, "service-account-key")
	if err != nil {
		return err
	}

	return nil
}

func credentialsPath() (string, error) {
	return "", nil
}
